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
package org.teiid.dqp.internal.process;

import java.util.Properties;

import org.teiid.PreParser;
import org.teiid.client.RequestMessage;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.jdbc.tracing.GlobalTracerInjector;
import org.teiid.query.util.Options;

import io.opentracing.Tracer;
import io.opentracing.contrib.concurrent.TracedRunnable;

public class DQPConfiguration{

    //Constants
    static final int DEFAULT_FETCH_SIZE = RequestMessage.DEFAULT_FETCH_SIZE * 10;
    static final int DEFAULT_PROCESSOR_TIMESLICE = 2000;
    static final int DEFAULT_MAX_RESULTSET_CACHE_ENTRIES = 1024;
    static final int DEFAULT_QUERY_THRESHOLD = 600000;
    static final String PROCESS_PLAN_QUEUE_NAME = "QueryProcessorQueue"; //$NON-NLS-1$
    public static final int DEFAULT_MAX_PROCESS_WORKERS = 64;
    public static final int DEFAULT_MAX_SOURCE_ROWS = -1;
    public static final int DEFAULT_MAX_ACTIVE_PLANS = 20;
    public static final int DEFAULT_USER_REQUEST_SOURCE_CONCURRENCY = 0;
    public static final int DEFAULT_MAX_STALENESS_SECONDS = 0;


    private int maxThreads = DEFAULT_MAX_PROCESS_WORKERS;
    private int timeSliceInMilli = DEFAULT_PROCESSOR_TIMESLICE;
    private int maxRowsFetchSize = DEFAULT_FETCH_SIZE;
    private int lobChunkSizeInKB = 100;
    private long queryThresholdInMilli = DEFAULT_QUERY_THRESHOLD;
    private boolean exceptionOnMaxSourceRows = true;
    private int maxSourceRows = -1;
    private int maxActivePlans = DEFAULT_MAX_ACTIVE_PLANS;

    private int userRequestSourceConcurrency = DEFAULT_USER_REQUEST_SOURCE_CONCURRENCY;
    private boolean detectingChangeEvents = true;
    private long queryTimeout;

    private transient AuthorizationValidator authorizationValidator;
    private transient PreParser preParser;

    private Properties properties;

    public DQPConfiguration() {
        properties = PropertiesUtils.getDefaultProperties();
    }

    public int getMaxActivePlans() {
        return maxActivePlans;
    }

    public void setMaxActivePlans(int maxActivePlans) {
        this.maxActivePlans = maxActivePlans;
    }

    public int getUserRequestSourceConcurrency() {
        return userRequestSourceConcurrency;
    }

    public void setUserRequestSourceConcurrency(int userRequestSourceConcurrency) {
        this.userRequestSourceConcurrency = userRequestSourceConcurrency;
    }

    public int getMaxThreads() {
        return maxThreads;
    }

    public void setMaxThreads(int maxThreads) {
        this.maxThreads = maxThreads;
    }

    public int getTimeSliceInMilli() {
        return timeSliceInMilli;
    }

    public void setTimeSliceInMilli(int timeSliceInMilli) {
        this.timeSliceInMilli = timeSliceInMilli;
    }

    public int getMaxRowsFetchSize() {
        return maxRowsFetchSize;
    }

    public void setMaxRowsFetchSize(int maxRowsFetchSize) {
        this.maxRowsFetchSize = maxRowsFetchSize;
    }

    public int getLobChunkSizeInKB() {
        return this.lobChunkSizeInKB;
    }

    public void setLobChunkSizeInKB(int lobChunkSizeInKB) {
        this.lobChunkSizeInKB = lobChunkSizeInKB;
    }

    public int getQueryThresholdInSecs() {
        return (int)queryThresholdInMilli/1000;
    }

    public long getQueryThresholdInMilli() {
        return queryThresholdInMilli;
    }

    public void setQueryThresholdInMilli(long queryThreshold) {
        this.queryThresholdInMilli = queryThreshold;
    }

    public void setQueryThresholdInSecs(int queryThresholdInSecs) {
        this.queryThresholdInMilli = queryThresholdInSecs * 1000;
    }

    /**
     * Throw exception if there are more rows in the result set than specified in the MaxSourceRows setting.
     * @return
     */
    public boolean isExceptionOnMaxSourceRows() {
        return exceptionOnMaxSourceRows;
    }

    public void setExceptionOnMaxSourceRows(boolean exceptionOnMaxSourceRows) {
        this.exceptionOnMaxSourceRows = exceptionOnMaxSourceRows;
    }

    /**
     * Maximum source set rows to fetch
     * @return
     */
    public int getMaxSourceRows() {
        return maxSourceRows;
    }

    public void setMaxSourceRows(int maxSourceRows) {
        this.maxSourceRows = maxSourceRows;
    }

    public AuthorizationValidator getAuthorizationValidator() {
        return authorizationValidator;
    }

    public void setAuthorizationValidator(
            AuthorizationValidator authorizationValidator) {
        this.authorizationValidator = authorizationValidator;
    }

    public boolean isDetectingChangeEvents() {
        return detectingChangeEvents;
    }

    public void setDetectingChangeEvents(boolean detectingChangeEvents) {
        this.detectingChangeEvents = detectingChangeEvents;
    }

    public void setQueryTimeout(long queryTimeout) {
        this.queryTimeout = queryTimeout;
    }

    public long getQueryTimeout() {
        return queryTimeout;
    }

    public TeiidExecutor getTeiidExecutor() {
        return new ThreadReuseExecutor(DQPConfiguration.PROCESS_PLAN_QUEUE_NAME, getMaxThreads()) {
            Tracer tracer = GlobalTracerInjector.getTracer();
            @Override
            public void execute(Runnable command) {
                super.execute(tracer.activeSpan() == null ? command :
                    new TracedRunnable(command, tracer));
            }
        };
    }

    public void setPreParser(PreParser preParser) {
        this.preParser = preParser;
    }

    public PreParser getPreParser() {
        return preParser;
    }

    /**
     * Get the properties used to initialize the engine {@link Options}.  Defaults to the System properties.
     */
    public Properties getProperties() {
        return this.properties;
    }

    /**
     * Set the properties used to initialize the engine {@link Options}
     * @param properties
     */
    public void setProperties(Properties properties) {
        this.properties = properties;
    }

}
