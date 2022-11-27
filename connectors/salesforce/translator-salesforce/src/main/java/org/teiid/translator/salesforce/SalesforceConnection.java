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
package org.teiid.translator.salesforce;

import java.io.IOException;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.List;

import org.teiid.resource.api.Connection;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.salesforce.execution.DataPayload;
import org.teiid.translator.salesforce.execution.DeletedResult;
import org.teiid.translator.salesforce.execution.UpdatedResult;

import com.sforce.async.BatchInfo;
import com.sforce.async.BatchResult;
import com.sforce.async.JobInfo;
import com.sforce.async.OperationEnum;
import com.sforce.async.SObject;
import com.sforce.soap.partner.DescribeGlobalResult;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.QueryResult;

public interface SalesforceConnection extends Connection {

    public static class BatchResultInfo {
        private String batchId;
        private int waitCount;

        //batch state
        private String[] resultList;
        private int resultNum;

        //pk chunk state
        private LinkedHashMap<String, BatchInfo> pkBatches;

        public BatchResultInfo(String batchInfo) {
            this.batchId = batchInfo;
        }

        public String[] getResultList() {
            return resultList;
        }

        public void setResultList(String[] resultList) {
            this.resultList = resultList;
            this.resultNum = 0;
        }

        public int getAndIncrementResultNum() {
            return resultNum++;
        }

        public void setResultNum(int resultNum) {
            this.resultNum = resultNum;
        }

        public String getBatchId() {
            return batchId;
        }

        public void setPkBatches(LinkedHashMap<String, BatchInfo> pkBatches) {
            this.pkBatches = pkBatches;
        }

        public LinkedHashMap<String, BatchInfo> getPkBatches() {
            return pkBatches;
        }

        public int incrementAndGetWaitCount() {
            return ++waitCount;
        }

        public void resetWaitCount() {
            waitCount = 0;
        }
    }

    public interface BulkBatchResult {

        public List<String> nextRecord() throws IOException;

        public void close();

    }

    public QueryResult query(String queryString, int maxBatchSize, boolean queryAll) throws TranslatorException;

    public QueryResult queryMore(String queryLocator, int batchSize) throws TranslatorException;

    public boolean isValid();

    public int delete(String[] ids) throws TranslatorException ;

    public int create(DataPayload data) throws TranslatorException;

    public int upsert(DataPayload data) throws TranslatorException;

    public int update(List<DataPayload> updateDataList) throws TranslatorException;

    public UpdatedResult getUpdated(String objectName, Calendar startCalendar, Calendar endCalendar) throws TranslatorException;

    public DeletedResult getDeleted(String objectName, Calendar startCalendar, Calendar endCalendar) throws TranslatorException;

    public com.sforce.soap.partner.sobject.SObject[] retrieve(String fieldList, String sObjectType, List<String> ids) throws TranslatorException;

    public DescribeGlobalResult getObjects() throws TranslatorException;

    public DescribeSObjectResult[] getObjectMetaData(String... objectName) throws TranslatorException;

    public BatchResult[] getBulkResults(JobInfo job, List<String> ids) throws TranslatorException;

    public void cancelBulkJob(JobInfo job) throws TranslatorException;

    JobInfo closeJob(String jobId) throws TranslatorException;

    String addBatch(List<SObject> payload, JobInfo job)
            throws TranslatorException;

    JobInfo createBulkJob(String objectName, OperationEnum operation, boolean usePkChunking) throws TranslatorException;

    Long getCardinality(String sobject) throws TranslatorException;

    String getVersion();

    BatchResultInfo addBatch(String query, JobInfo job) throws TranslatorException;

    BulkBatchResult getBatchQueryResults(String id, BatchResultInfo info) throws TranslatorException;

}
