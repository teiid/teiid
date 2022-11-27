/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.sizing;

/**
 * This is the sizing application(https://access.redhat.com/labs/jbossdvsat/) formula simulation class, primary purpose of this class
 * is used to test and verify the web sizing application, whether the sizing recommendation are base on formula.
 *
 * The {@link #heapCaculation} method use to calculate the JVM heap size base on formula
 * <pre>
 * Size = #concurrency * (5mb)  * #source queries + 300mb
 * </pre>
 *
 * The {@link #coreCaculation} method use to calculate the core size base on formula
 * <pre>
 *   cpu_time = sum(source_processing) + engine_time + client_processing
 *   wall_time = low(source_latency) + cpu_time + additional_latency
 *   cpu_utilization_per_query = cpu_time/wall_time
 *   total_cpu_time_available = cpu_core_count * 2 * 1000ms
 *   queries/sec = total_cpu_time_available / (threads_used_per_query * cpu_utilization_per_query * cpu_time)
 * </pre>
 *
 * @author kylin
 *
 */
public class Caculation {

    // 1. How many data sources do you want to integrate?
    private int source_count;

    // 2. How many concurrent queries (at peak load time) do you want to support?
    private int queries_concurrent;

    // 3. Does the JBoss Data Virtualization run on a cloud platform?
    private boolean isRunOnCloud;

    // 1. How many client queries per second do you want to support?
    private int queries_per_sec;

    // 2. What is the average row count from each physical source?
    private int row_count_each;

    // 3. What is the average row size (in bytes) that returns from each source?
    private int row_size_each;

    // 4. What is the average time (in milliseconds) required by each source to return a result?
    private int avg_time_each;

    // 5. What is the expected average row size (in bytes) received as the result of a federated query?
    private int row_count_federated;

    // 6. What is the average expected row count received as the result of a federated query?
    private int row_size_federated;

    // 7. What is the average time (in milliseconds) required to execute a client query in sample runs?
    private int avg_time_sample;

    // 8. Does the client query perform aggregations, sorts, or view transformations that perform sorts and aggregations?
    private boolean isAggregation;

    /**
     * JVM Size Caculation Formula: Size = #concurrency * (5mb)  * #source queries + 300mb
     *
     * @return heap size in GB
     */
    public int heapCaculation() {
        int sources = this.getSource_count();
        int concurrent = this.getQueries_concurrent();

        int total_in_mb = concurrent * 5 * sources + 300 ;
        int heap = total_in_mb/1024 + 1;

        if(heap < 16 && !isRunOnCloud) {
            heap = 16 ;
        }

        return heap;
    }

    /**
     *
     * @return
     */
    public int coreCaculation() {

        int sources = this.getSource_count();
        int row_count_each = this.getRow_count_each();
        int row_size_each = this.getRow_size_each();
        int source_latency = this.getAvg_time_each();
        int row_count_federdated = this.getRow_count_federated();
        int row_size_federdated = this.getRow_size_federated();
        int walltime = this.getAvg_time_sample();
        boolean isAggregation = this.isAggregation();
        int queries_per_sec = this.getQueries_per_sec();

        long source_processing = getSourceProcessingTime(row_count_each, row_size_each, sources);
        long initial_latency = getInitialLatency(row_count_each, row_size_each, sources, source_latency);
        long additional_latency = getAdditionalLatency(sources, source_latency);
        long client_processing = getClientProcessing(row_count_federdated, row_size_federdated);
        long engine_time = getEngineTime(isAggregation, sources, row_count_each, row_size_each, row_size_federdated, row_count_federdated, walltime, source_latency);

        long cores = getcorenumbers(source_latency, sources, source_processing, initial_latency, additional_latency, engine_time, client_processing, queries_per_sec);

        return (int) cores;
    }

    /**
     * CPU calculation logic & Formula:
     *   cpu_time = sum(source_processing) + engine_time + client_processing
     *   wall_time = low(source_latency) + cpu_time + additional_latency
     *   cpu_utilization_per_query = cpu_time/wall_time
     *   total_cpu_time_available = cpu_core_count * 2 * 1000ms
     *   queries/sec = total_cpu_time_available / (threads_used_per_query * cpu_utilization_per_query * cpu_time)
     *
     */
    private long getcorenumbers(int source_latency,
                                int sources,
                                long source_processing,
                                long initial_latency,
                                long additional_latency,
                                long engine_time,
                                long client_processing,
                                int queries_per_sec) {

        double cpu_time = source_processing + engine_time + client_processing;
        double wall_time = cpu_time + initial_latency + additional_latency;
        double cpu_utilization_per_query = cpu_time / wall_time ;
        int threads_used_per_query = sources + 1 ;

        double cores = (cpu_time * queries_per_sec * cpu_utilization_per_query * threads_used_per_query)/(1000 * 2);

        if (cores < 16 && !isRunOnCloud) {
            cores = 16;
        }

        if(cores > 128) {
            cores = 128 ;
        }

        return Math.round(cores);
    }

    /**
     * If there are lot of sorting, aggregations this can be high, if not can be very low as in pass through scenarios.
     *
     * "how much time they took in their sample runs" will get a time for running a sample query, then remove all source and deserialization/Serialization latencies then we roughly have the engine time
     *
     * based on that time, and sorting and aggregation, we can say low, medium or high processing (< 25%, 60%, > 90%) of times
     */
    private long getEngineTime( boolean isAggregation
                              , int sources
                              , int row_count_each
                              , int row_size_each
                              , int row_size_federdated
                              , int row_count_federdated
                              , int walltime
                              , int source_latency) {

        long serializing_time = getSourceProcessingTime(row_count_each, row_size_each, sources);
        long deserializing_time = getClientProcessing(row_count_federdated, row_size_federdated);
        long initial_latency = getInitialLatency(row_count_each, row_size_each, sources, source_latency);
        long additional_latency = getAdditionalLatency(sources, source_latency);

        double engine_time = 0;

        //sampleruntime should large than latencies + serializing_time + deserializing_time
        double engine_time_rough = walltime - serializing_time - deserializing_time - initial_latency - additional_latency ;

        long total_fer_size = row_size_federdated * row_count_federdated ;

        if(engine_time_rough <= 10) {
            engine_time = 10 ;
        } else if (isAggregation && total_fer_size > 100000000) {
            engine_time = engine_time_rough * 0.9;
        } else if (isAggregation) {
            engine_time = engine_time_rough * 0.6;
        } else {
            engine_time = engine_time_rough * 0.3;
        }

        return Math.round(engine_time);
    }

    /**
     * How much time took for serializing the results and put on the socket.
     *
     *  The Source Serialize Processing Time(time) and Total Serialize Processing Size(size) are in a Linear Regression trend,
     *      time = K * time + V
    *  In previous test,
     *    if size > 1 MB, the K is 4.6, the V is 210, the formula like: time = 4.6 * size + 210
     *    if size < 1 MB, it's more complex, not get a precise value of K and V, 125, 75, 5 and 2 in below are come from test
     *
     *  TODO: need more trial, collect more tuples (time, size), use algorithm to get the K and V
     *        TEIID-3398
     */
    private long getClientProcessing(int row_count_federdated, int row_size_federdated) {

        double total_byte = row_count_federdated * row_size_federdated;
        double client_procesing = 0;

        if(total_byte > 10000000){
            double size_in_mb = total_byte/1000000 ;
            client_procesing = 210 + 4.6 * size_in_mb;
        } else if (total_byte > 1000000 && total_byte <= 10000000) {
            double percentage = 125.0 / 9000000.0 ;
            client_procesing = percentage * (total_byte - 1000000) + 85;
        } else if (total_byte > 100000 && total_byte <= 1000000) {
            double percentage = 75.0 / 900000.0 ;
            client_procesing = percentage * (total_byte - 100000) + 10;
        } else if (total_byte > 10000 && total_byte <= 100000) {
            double percentage = 5.0 / 90000.0 ;
            client_procesing = percentage * (total_byte - 10000) + 5;
        } else if (total_byte > 1000 && total_byte <= 10000) {
            double percentage = 2.0 / 9000.0 ;
            client_procesing = percentage * (total_byte - 1000) + 3;
        } else if (total_byte > 0 && total_byte <= 1000) {
            double percentage = 2.0 / 1000.0 ;
            client_procesing = percentage * total_byte;
        } else if (total_byte == 0) {
            client_procesing = 0;
        }

        return  Math.round(client_procesing);
    }

    /**
     * Even after the first row of results came(lowest of source_latency), how much more *additional* time spent on waiting for results. Consider a guess of half (0.5) when we parallelize,
     *
     * in serialized situations (XA) this will be 1. So, typically this should be 0.5(high(source_latency) - low(source_latency)) or in XA it should be 1 * sum(source_latency).
     */
    private long getAdditionalLatency(int sources, int source_latency) {

        double additional_latency = 0 ;

        if(sources == 1){
                additional_latency = source_latency * 0.5;
        } else {
                additional_latency = source_latency * 0.4;
        }

        return Math.round(additional_latency);
    }

    /**
     * this also variation "source latency", as to first source to return results, where processing starts. We can say this is "low(source_latency)".
     *
     * 'source_latency' is average source latency for each data source, so the formula used to estimate source latency like below method
     */
    private long getInitialLatency(int row_count_each, int row_size_each, int sources, int source_latency) {

        long total_byte = row_count_each * row_size_each;
        double initial_latency = 0 ;

        if (sources == 1) {
            initial_latency = source_latency;
        } else if (total_byte > 100000000) {
            initial_latency = source_latency * 0.8;
        } else {
            initial_latency = source_latency * 0.6;
        }

        return Math.round(initial_latency);
    }

    /**
     * How much time took to deserialize rows coming back from source?
     *
     *  The Source Deserialize Processing Time(time) and Total Deserialize Processing Size(size) are in a Linear Regression trend,
     *      time = K * time + V
     *
     *  https://github.com/kylinsoong/sizing-application/blob/master/deserialization-regression.adoc#conclusion
     *
     */
    private long getSourceProcessingTime(int row_count_each, int row_size_each, int sources) {

        double total_byte = row_count_each * row_size_each * sources;
        double source_processing = 0.0000075 * total_byte;
        return Math.round(source_processing);
    }

    public Caculation(int source_count, int queries_concurrent) {
        this(source_count, queries_concurrent, false);
    }

    public Caculation(int source_count, int queries_concurrent, boolean isRunOnCloud) {
        this.source_count = source_count;
        this.queries_concurrent = queries_concurrent;
        this.isRunOnCloud = isRunOnCloud;
    }

    public Caculation(int source_count
                    , int queries_concurrent
                    , int queries_per_sec
                    , int row_count_each
                    , int row_size_each
                    , int avg_time_each
                    , int row_count_federated
                    , int row_size_federated
                    , int avg_time_sample
                    , boolean isAggregation) {
        this(source_count, queries_concurrent, false, queries_per_sec, row_count_each, row_size_each, avg_time_each, row_count_federated, row_size_federated, avg_time_sample, isAggregation);
    }

    public Caculation(int source_count
            , int queries_concurrent
            , boolean isRunOnCloud
            , int queries_per_sec
            , int row_count_each
            , int row_size_each
            , int avg_time_each
            , int row_count_federated
            , int row_size_federated
            , int avg_time_sample
            , boolean isAggregation) {
        this.source_count = source_count;
        this.queries_concurrent = queries_concurrent;
        this.isRunOnCloud = isRunOnCloud;
        this.queries_per_sec = queries_per_sec;
        this.row_count_each = row_count_each;
        this.row_size_each = row_size_each;
        this.avg_time_each = avg_time_each;
        this.row_count_federated = row_count_federated;
        this.row_size_federated = row_size_federated;
        this.avg_time_sample = avg_time_sample;
        this.isAggregation = isAggregation;
    }

    public int getSource_count() {
        return source_count;
    }

    public void setSource_count(int source_count) {
        this.source_count = source_count;
    }

    public int getQueries_concurrent() {
        return queries_concurrent;
    }

    public void setQueries_concurrent(int queries_concurrent) {
        this.queries_concurrent = queries_concurrent;
    }

    public int getQueries_per_sec() {
        return queries_per_sec;
    }

    public void setQueries_per_sec(int queries_per_sec) {
        this.queries_per_sec = queries_per_sec;
    }

    public int getRow_count_each() {
        return row_count_each;
    }

    public void setRow_count_each(int row_count_each) {
        this.row_count_each = row_count_each;
    }

    public int getRow_size_each() {
        return row_size_each;
    }

    public void setRow_size_each(int row_size_each) {
        this.row_size_each = row_size_each;
    }

    public int getAvg_time_each() {
        return avg_time_each;
    }

    public void setAvg_time_each(int avg_time_each) {
        this.avg_time_each = avg_time_each;
    }

    public int getRow_count_federated() {
        return row_count_federated;
    }

    public void setRow_count_federated(int row_count_federated) {
        this.row_count_federated = row_count_federated;
    }

    public int getRow_size_federated() {
        return row_size_federated;
    }

    public void setRow_size_federated(int row_size_federated) {
        this.row_size_federated = row_size_federated;
    }

    public int getAvg_time_sample() {
        return avg_time_sample;
    }

    public void setAvg_time_sample(int avg_time_sample) {
        this.avg_time_sample = avg_time_sample;
    }

    public boolean isAggregation() {
        return isAggregation;
    }

    public void setAggregation(boolean isAggregation) {
        this.isAggregation = isAggregation;
    }

    public boolean isRunOnCloud() {
        return isRunOnCloud;
    }

    public void setRunOnCloud(boolean isRunOnCloud) {
        this.isRunOnCloud = isRunOnCloud;
    }

}
