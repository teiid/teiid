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
package org.teiid.salesforce;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.salesforce.SalesforceConnection;
import org.teiid.translator.salesforce.execution.DataPayload;
import org.teiid.translator.salesforce.execution.DeletedObject;
import org.teiid.translator.salesforce.execution.DeletedResult;
import org.teiid.translator.salesforce.execution.UpdatedResult;

import com.sforce.async.AsyncApiException;
import com.sforce.async.BatchInfo;
import com.sforce.async.BatchInfoList;
import com.sforce.async.BatchRequest;
import com.sforce.async.BatchResult;
import com.sforce.async.BatchStateEnum;
import com.sforce.async.BulkConnection;
import com.sforce.async.CSVReader;
import com.sforce.async.ConcurrencyMode;
import com.sforce.async.ContentType;
import com.sforce.async.JobInfo;
import com.sforce.async.JobStateEnum;
import com.sforce.async.OperationEnum;
import com.sforce.async.QueryResultList;
import com.sforce.soap.partner.DeleteResult;
import com.sforce.soap.partner.DeletedRecord;
import com.sforce.soap.partner.DescribeGlobalResult;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Error;
import com.sforce.soap.partner.GetDeletedResult;
import com.sforce.soap.partner.GetUpdatedResult;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.UpsertResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import com.sforce.ws.transport.JdkHttpTransport;

public abstract class BaseSalesforceConnection<T extends SalesforceConfiguration, C extends ConnectorConfig, P extends PartnerConnection> implements SalesforceConnection {

    private static final String ID_FIELD_NAME = "id"; //$NON-NLS-1$
    private static final String PK_CHUNKING_HEADER = "Sforce-Enable-PKChunking"; //$NON-NLS-1$
    private static final int MAX_CHUNK_SIZE = 100000;
    private BulkConnection bulkConnection;
    private int pollingInterval = 500; //TODO: this could be configurable

    private String restEndpoint;
    private String apiVersion;

    //set by subclasses during login
    private P partnerConnection;
    private C config;

    private volatile boolean valid = false;

    public BaseSalesforceConnection(T salesforceConfig) throws AsyncApiException, ConnectionException {
        config = createConnectorConfig(salesforceConfig);
        partnerConnection = login(salesforceConfig, config);

        String endpoint = config.getServiceEndpoint();
        // The endpoint for the Bulk API service is the same as for the normal
        // SOAP uri until the /Soap/ part. From here it's '/async/versionNumber'
        int index = endpoint.indexOf("Soap/u/"); //$NON-NLS-1$
        int endIndex = endpoint.indexOf('/', index+7);
        apiVersion = endpoint.substring(index+7,endIndex);
        String bulkEndpoint = endpoint.substring(0, endpoint.indexOf("Soap/"))+ "async/" + apiVersion;//$NON-NLS-1$ //$NON-NLS-2$
        config.setRestEndpoint(bulkEndpoint);
        // This value identifies Teiid as a SF certified solution.
        // It was provided by SF and should not be changed.
        partnerConnection.setCallOptions("RedHat/MetaMatrix/", null); //$NON-NLS-1$
        bulkConnection = new BulkConnection(config);
        restEndpoint = endpoint.substring(0, endpoint.indexOf("Soap/"))+ "data/" + "v30.0";//$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        // Test the connection.
        checkValid();
    }

    protected abstract C createConnectorConfig(T salesforceConfig) throws ConnectionException;

    protected BaseSalesforceConnection(P partnerConnection) {
        this.partnerConnection = partnerConnection;
        checkValid();
    }

    /**
     * This method must log the user in and create the appropriate {@link PartnerConnection} instance
     * @param salesforceConfig
     * @throws AsyncApiException
     * @throws ConnectionException
     */
    protected abstract P login(T salesforceConfig, C connectorConfig) throws AsyncApiException, ConnectionException;

    public boolean isValid() {
        return valid;
    }

    @Override
    public Long getCardinality(String sobject) throws TranslatorException {
        try {
        InputStream is = null;
        try {
            is = doRestHttpGet(new URL(restEndpoint + "/query/?explain=select+id+from+" + URLEncoder.encode(sobject, "UTF-8"))); //$NON-NLS-1$ //$NON-NLS-2$
            String s = ObjectConverterUtil.convertToString(new InputStreamReader(is, Charset.forName("UTF-8"))); //$NON-NLS-1$
            //TODO: introduce a json parser
            int index = s.indexOf("cardinality"); //$NON-NLS-1$
            if (index < 0) {
                return null;
            }
            index = s.indexOf(":", index); //$NON-NLS-1$
            if (index < 0) {
                return null;
            }
            int end = s.indexOf(",", index); //$NON-NLS-1$
            if (end < 0) {
                end = s.indexOf("}", index); //$NON-NLS-1$
            }
            if (end < 0) {
                return null;
            }
            s = s.substring(index+1, end);
            return Long.valueOf(s);
        } catch (NumberFormatException e) {
            throw new TranslatorException(e);
        } catch (MalformedURLException e) {
            throw new TranslatorException(e);
        } catch (UnsupportedEncodingException e) {
            throw new TranslatorException(e);
        } catch (IOException e) {
            throw new TranslatorException(e);
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    checkValid();
                }
            }
        }
        }catch(Throwable e) {
            checkValid();
            throw e;
        }
    }

    private InputStream doRestHttpGet(URL url) throws IOException {
        HttpURLConnection connection = JdkHttpTransport.createConnection(config, url, null);
        connection.setRequestProperty("Authorization", "Bearer " + config.getSessionId()); //$NON-NLS-1$ //$NON-NLS-2$

        InputStream in = connection.getInputStream();

        String encoding = connection.getHeaderField("Content-Encoding"); //$NON-NLS-1$
        if ("gzip".equals(encoding)) { //$NON-NLS-1$
            in = new GZIPInputStream(in);
        }

        return in;
    }

    public boolean checkValid() {
        if(partnerConnection != null) {
            try {
                partnerConnection.getServerTimestamp();
                valid = true;
                return valid;
            } catch (Throwable t) {
                LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Caught Throwable in isAlive", t); //$NON-NLS-1$
            }
        }
        valid = false;
        return valid;
    }

    protected P getPartnerConnection() {
        return partnerConnection;
    }

    public QueryResult query(String queryString, int batchSize, boolean queryAll) throws TranslatorException {
        try {
        if(batchSize > 2000) {
            batchSize = 2000;
            LogManager.logDetail(LogConstants.CTX_CONNECTOR, "reduced.batch.size"); //$NON-NLS-1$
        }

        QueryResult qr = null;
        partnerConnection.setQueryOptions(batchSize);
        try {
            if(queryAll) {
                qr = partnerConnection.queryAll(queryString);
            } else {
                partnerConnection.setMruHeader(false);
                qr = partnerConnection.query(queryString);
            }
        } catch (ConnectionException e) {
            throw new TranslatorException(e);
        } finally {
            partnerConnection.clearMruHeader();
            partnerConnection.clearQueryOptions();
        }
        return qr;
        }catch(Throwable e) {
            checkValid();
            throw e;
        }
    }

    public QueryResult queryMore(String queryLocator, int batchSize) throws TranslatorException {
        try {
        if(batchSize > 2000) {
            batchSize = 2000;
            LogManager.logDetail(LogConstants.CTX_CONNECTOR, "reduced.batch.size"); //$NON-NLS-1$
        }

        partnerConnection.setQueryOptions(batchSize);
        try {
            return partnerConnection.queryMore(queryLocator);
        } catch (ConnectionException e) {
            throw new TranslatorException(e);
        } finally {
            partnerConnection.clearQueryOptions();
        }
        }catch(Throwable e) {
            checkValid();
            throw e;
        }
    }

    public int delete(String[] ids) throws TranslatorException {
        try {
        DeleteResult[] results = null;
        try {
            results = partnerConnection.delete(ids);
        } catch (ConnectionException e) {
            throw new TranslatorException(e);
        }

        boolean allGood = true;
        StringBuffer errorMessages = new StringBuffer();
        for(int i = 0; i < results.length; i++) {
            DeleteResult result = results[i];
            if(!result.isSuccess()) {
                if(allGood) {
                    errorMessages.append("Error(s) executing DELETE: "); //$NON-NLS-1$
                    allGood = false;
                }
                Error[] errors = result.getErrors();
                if(null != errors && errors.length > 0) {
                    for(int x = 0; x < errors.length; x++) {
                        Error error = errors[x];
                        errorMessages.append(error.getMessage()).append(';');
                    }
                }

            }
        }
        if(!allGood) {
            throw new TranslatorException(errorMessages.toString());
        }
        return results.length;
        }catch(Throwable e) {
            checkValid();
            throw e;
        }
    }

    public int upsert(DataPayload data) throws TranslatorException {
        try {
        SObject toCreate = toUpdateSObject(new ArrayList<>(), data);
        SObject[] objects = new SObject[] {toCreate};
        UpsertResult[] results;
        try {
            results = partnerConnection.upsert(ID_FIELD_NAME, objects);
        } catch (ConnectionException e) {
            throw new TranslatorException(e);
        }
        for (UpsertResult result : results) {
            if(!result.isSuccess()) {
                throw new TranslatorException(result.getErrors()[0].getMessage());
            }
        }
        return results.length;
        }catch(Throwable e) {
            checkValid();
            throw e;
        }
    }

    public int create(DataPayload data) throws TranslatorException {
        try {
        SObject toCreate = new SObject();
        toCreate.setType(data.getType());
        for (DataPayload.Field field : data.getMessageElements()) {
            toCreate.addField(field.name, field.value);
        }
        SObject[] objects = new SObject[] {toCreate};
        SaveResult[] result;
        try {
            result = partnerConnection.create(objects);
        } catch (ConnectionException e) {
            throw new TranslatorException(e);
        }
        return analyzeResult(result);
        }catch(Throwable e) {
            checkValid();
            throw e;
        }
    }

    public int update(List<DataPayload> updateDataList) throws TranslatorException {
        try {
        List<SObject> params = new ArrayList<SObject>(updateDataList.size());
        List<String> nullFields = new ArrayList<>();
        for(int i = 0; i < updateDataList.size(); i++) {
            DataPayload data = updateDataList.get(i);
            SObject toCreate = toUpdateSObject(nullFields, data);
            params.add(i, toCreate);
        }
        SaveResult[] result;
            try {
                result = partnerConnection.update(params.toArray(new SObject[params.size()]));
            } catch (ConnectionException e) {
                throw new TranslatorException(e);
            }
        return analyzeResult(result);
        }catch(Throwable e) {
            checkValid();
            throw e;
        }
    }

    public static SObject toUpdateSObject(List<String> nullFields, DataPayload data) {
        SObject toCreate = new SObject();
        toCreate.setType(data.getType());
        toCreate.setId(data.getID());
        for (DataPayload.Field field : data.getMessageElements()) {
            if (field.value == null) {
                nullFields.add(field.name);
            } else {
                toCreate.addField(field.name, field.value);
            }
        }
        if (!nullFields.isEmpty()) {
            toCreate.setFieldsToNull(nullFields.toArray(new String[nullFields.size()]));
            nullFields.clear();
        }
        return toCreate;
    }

    private int analyzeResult(SaveResult[] results) throws TranslatorException {
        try {
        for (SaveResult result : results) {
            if(!result.isSuccess()) {
                throw new TranslatorException(result.getErrors()[0].getMessage());
            }
        }
        return results.length;
        }catch(Throwable e) {
            checkValid();
            throw e;
        }
    }

    public UpdatedResult getUpdated(String objectType, Calendar startDate, Calendar endDate) throws TranslatorException {
            try {
            GetUpdatedResult updated;
            try {
                updated = partnerConnection.getUpdated(objectType, startDate, endDate);
            } catch (ConnectionException e) {
                throw new TranslatorException(e);
            }
            UpdatedResult result = new UpdatedResult();
            result.setLatestDateCovered(updated.getLatestDateCovered());
            result.setIDs(Arrays.asList(updated.getIds()));
            return result;
            }catch(Throwable e) {
                checkValid();
                throw e;
            }
    }

    public DeletedResult getDeleted(String objectName, Calendar startCalendar,
            Calendar endCalendar) throws TranslatorException {
            try {
            GetDeletedResult deleted;
            try {
                deleted = partnerConnection.getDeleted(objectName, startCalendar, endCalendar);
            } catch (ConnectionException e) {
                throw new TranslatorException(e);
            }
            DeletedResult result = new DeletedResult();
            result.setLatestDateCovered(deleted.getLatestDateCovered());
            result.setEarliestDateAvailable(deleted.getEarliestDateAvailable());
            DeletedRecord[] records = deleted.getDeletedRecords();
            List<DeletedObject> resultRecords = new ArrayList<DeletedObject>();
            if(records != null) {
                for (DeletedRecord record : records) {
                    DeletedObject object = new DeletedObject();
                    object.setID(record.getId());
                    object.setDeletedDate(record.getDeletedDate());
                    resultRecords.add(object);
                }
            }
            result.setResultRecords(resultRecords);
            return result;
            }catch(Throwable e) {
                checkValid();
                throw e;
            }
    }

    public SObject[] retrieve(String fieldList, String sObjectType, List<String> ids) throws TranslatorException {
        try {
            return partnerConnection.retrieve(fieldList, sObjectType, ids.toArray(new String[ids.size()]));
        }catch (ConnectionException e) {
            checkValid();
            throw new TranslatorException(e);
        }catch (Throwable e) {
            checkValid();
            throw e;
        }

    }

    public DescribeGlobalResult getObjects() throws TranslatorException {
        try {
            return partnerConnection.describeGlobal();
        }catch (ConnectionException e) {
            checkValid();
            throw new TranslatorException(e);
        }catch (Throwable e) {
            checkValid();
            throw e;
        }
    }

    public DescribeSObjectResult[] getObjectMetaData(String... objectName) throws TranslatorException {
        try {
            return partnerConnection.describeSObjects(objectName);
        }catch (ConnectionException e) {
            checkValid();
            throw new TranslatorException(e);
        }catch (Throwable e) {
            checkValid();
            throw e;
        }
    }

    @Override
    public void close() {

    }

    @Override
    public JobInfo createBulkJob(String objectName, OperationEnum operation, boolean usePkChunking) throws TranslatorException {
        try {
            JobInfo job = new JobInfo();
            job.setObject(objectName);
            job.setOperation(operation);
            job.setContentType((operation==OperationEnum.insert||operation==OperationEnum.upsert)?ContentType.XML:ContentType.CSV);
            if (operation==OperationEnum.upsert) {
                job.setExternalIdFieldName(ID_FIELD_NAME);
            }
            job.setConcurrencyMode(ConcurrencyMode.Parallel);
            if (operation == OperationEnum.query && usePkChunking) {
                this.bulkConnection.addHeader(PK_CHUNKING_HEADER, "chunkSize=" + MAX_CHUNK_SIZE); //$NON-NLS-1$
            }
            JobInfo info = this.bulkConnection.createJob(job);
            //reset the header
            if (operation == OperationEnum.query) {
                this.bulkConnection = new BulkConnection(config);
            }
            return info;
        } catch (AsyncApiException e) {
            checkValid();
            throw new TranslatorException(e);
        } catch (Throwable e) {
            checkValid();
            throw e;
        }
    }

    @Override
    public String addBatch(List<com.sforce.async.SObject> payload, JobInfo job) throws TranslatorException {
        try {
            BatchRequest request = this.bulkConnection.createBatch(job);
            request.addSObjects(payload.toArray(new com.sforce.async.SObject[payload.size()]));
            return request.completeRequest().getId();
        } catch (AsyncApiException e) {
            checkValid();
            throw new TranslatorException(e);
        } catch (Throwable e) {
            checkValid();
            throw e;
        }
    }

    @Override
    public BatchResultInfo addBatch(String query, JobInfo job) throws TranslatorException {
        try {
            BatchInfo batch = this.bulkConnection.createBatchFromStream(job, new ByteArrayInputStream(query.getBytes(Charset.forName("UTF-8")))); //$NON-NLS-1$
            return new BatchResultInfo(batch.getId());
        } catch (AsyncApiException e) {
            checkValid();
            throw new TranslatorException(e);
        } catch (Throwable e) {
            checkValid();
            throw e;
        }
    }

    @Override
    public BulkBatchResult getBatchQueryResults(String jobId, BatchResultInfo info) throws TranslatorException {
        try {
        if (info.getResultList() == null && info.getPkBatches() == null) {
            try {
                BatchInfo batch = this.bulkConnection.getBatchInfo(jobId, info.getBatchId());
                switch (batch.getState()) {
                    case NotProcessed:
                        // we need more checks to ensure that chunking is being used.  since
                        // we don't know, then we'll explicitly check
                        JobInfo jobStatus = this.bulkConnection.getJobStatus(jobId);
                        if (jobStatus.getState() == JobStateEnum.Aborted) {
                            throw new TranslatorException(JobStateEnum.Aborted.name());
                        }
                        BatchInfoList batchInfoList = this.bulkConnection.getBatchInfoList(jobId);
                        LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Pk chunk batches", batchInfoList); //$NON-NLS-1$
                        BatchInfo[] batchInfo = batchInfoList.getBatchInfo();
                        LinkedHashMap<String, BatchInfo> pkBactches = new LinkedHashMap<String, BatchInfo>();
                        boolean anyComplete = false;
                        for (int i = 0; i < batchInfo.length; i++) {
                            BatchInfo batchInfoItem = batchInfo[i];
                            if (batchInfoItem.getId().equals(info.getBatchId())) {
                                continue; //disregard the initial batch
                            }
                            switch (batchInfoItem.getState()) {
                            case Failed:
                            case NotProcessed:
                                throw new TranslatorException(batchInfoItem.getStateMessage());
                            case Completed:
                                anyComplete = true;
                            default:
                                pkBactches.put(batchInfoItem.getId(), batchInfoItem);
                            }
                        }
                        info.setPkBatches(pkBactches);
                        if (!anyComplete) {
                            throwDataNotAvailable(info);
                        }
                        break;
                    case Completed:
                    {
                        QueryResultList list = this.bulkConnection.getQueryResultList(jobId, info.getBatchId());
                        info.setResultList(list.getResult());
                        break;
                    }
                    case InProgress:
                    case Queued:
                    throwDataNotAvailable(info);
                     default:
                        throw new TranslatorException(batch.getStateMessage());
                }
            } catch (AsyncApiException e) {
                throw new TranslatorException(e);
            }
        }
        try {
            BulkBatchResult result = null;
            if (info.getResultList() != null) {
                result = nextBulkBatch(jobId, info);
            }
            if (result == null && info.getPkBatches() != null) {
                getNextPkChunkResultList(jobId, info);
                result = nextBulkBatch(jobId, info);
            }
            info.resetWaitCount();
            return result;
        } catch (AsyncApiException e) {
            throw new TranslatorException(e);
        }
        } catch (Throwable e) {
            checkValid();
            throw e;
        }
    }

    private void throwDataNotAvailable(BatchResultInfo info) {
        int waitCount = info.incrementAndGetWaitCount();
        LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Waiting on queued/inprogress, wait number", waitCount); //$NON-NLS-1$
        throw new DataNotAvailableException(pollingInterval * Math.min(8, waitCount));
    }

    private void getNextPkChunkResultList(String jobId,
            BatchResultInfo info) throws AsyncApiException, TranslatorException {
        try {
        Map<String, BatchInfo> batches = info.getPkBatches();
        if (batches.isEmpty()) {
            return; //terminal condition
        }
        for (Iterator<Map.Entry<String, BatchInfo>> iter = batches.entrySet().iterator(); iter.hasNext();) {
            Map.Entry<String, BatchInfo> entry = iter.next();
            if (entry.getValue().getState() != BatchStateEnum.Completed) {
                continue;
            }
            iter.remove();
            QueryResultList list = this.bulkConnection.getQueryResultList(jobId, entry.getKey());
            info.setResultList(list.getResult());
            return;
        }

        //update the batchInfo
        BatchInfoList batchInfoList = this.bulkConnection.getBatchInfoList(jobId);
        BatchInfo[] batchInfo = batchInfoList.getBatchInfo();

        String completedId = null;
        for (BatchInfo bi : batchInfo) {
            if (!batches.containsKey(bi.getId())) {
                continue;
            }
            switch (bi.getState()) {
            case Failed:
            case NotProcessed:
                throw new TranslatorException(bi.getStateMessage());
            case Completed:
                if (completedId == null) {
                    completedId = bi.getId();
                    batches.remove(completedId);
                } else {
                    batches.put(bi.getId(), bi);
                }
                break;
            }
        }

        if (completedId == null) {
            throwDataNotAvailable(info);
        }
        QueryResultList list = this.bulkConnection.getQueryResultList(jobId, completedId);
        info.setResultList(list.getResult());
        } catch (Throwable e) {
            checkValid();
            throw e;
        }
    }

    private BulkBatchResult nextBulkBatch(String jobId, BatchResultInfo info)
            throws AsyncApiException {
        try {
        int index = info.getAndIncrementResultNum();
        if (index < info.getResultList().length) {
            String resultId = info.getResultList()[index];
            final InputStream inputStream = bulkConnection.getQueryResultStream(jobId, info.getBatchId(), resultId);
            final CSVReader reader = new CSVReader(inputStream);
            //get rid of the limits as much as possible
            reader.setMaxRowsInFile(Integer.MAX_VALUE);
            reader.setMaxCharsInFile(Integer.MAX_VALUE);
            return new BulkBatchResult() {
                @Override
                public List<String> nextRecord() throws IOException {
                    return reader.nextRecord();
                }

                @Override
                public void close() {
                    try {
                        inputStream.close();
                    } catch (IOException e) {
                        //bacause stream closing error it is strange behavior and may be need check
                        checkValid();
                    }
                }
            };
        }
        return null;
        } catch (Throwable e) {
            checkValid();
            throw e;
        }
    }

    @Override
    public JobInfo closeJob(String jobId) throws TranslatorException {
        try {
            return this.bulkConnection.closeJob(jobId);
        } catch (AsyncApiException e) {
            checkValid();
            throw new TranslatorException(e);
        } catch (Throwable e) {
            checkValid();
            throw e;
        }
    }

    @Override
    public BatchResult[] getBulkResults(JobInfo job, List<String> ids) throws TranslatorException {
        try {
            JobInfo info = this.bulkConnection.getJobStatus(job.getId());
            if (info.getNumberBatchesTotal() != info.getNumberBatchesFailed() + info.getNumberBatchesCompleted()) {
                throw new DataNotAvailableException(pollingInterval);
            }
            BatchResult[] results = new BatchResult[ids.size()];
            for (int i = 0; i < ids.size(); i++) {
                results[i] = this.bulkConnection.getBatchResult(job.getId(), ids.get(i));
            }
            return results;
        } catch (AsyncApiException e) {
            checkValid();
            throw new TranslatorException(e);
        } catch (Throwable e) {
            checkValid();
            throw e;
        }
    }

    @Override
    public void cancelBulkJob(JobInfo job) throws TranslatorException {
        try {
            this.bulkConnection.abortJob(job.getId());
        } catch (AsyncApiException e) {
            checkValid();
            throw new TranslatorException(e);
        } catch (Throwable e) {
            checkValid();
            throw e;
        }
    }

    @Override
    public String getVersion() {
        return apiVersion;
    }

}
