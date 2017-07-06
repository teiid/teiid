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
package org.teiid.resource.adapter.salesforce;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
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
import java.util.Properties;
import java.util.zip.GZIPInputStream;

import javax.resource.ResourceException;
import javax.security.auth.Subject;

import org.teiid.OAuthCredential;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.resource.adapter.salesforce.transport.SalesforceCXFTransport;
import org.teiid.resource.adapter.salesforce.transport.SalesforceConnectorConfig;
import org.teiid.resource.spi.BasicConnection;
import org.teiid.resource.spi.ConnectionContext;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.salesforce.SalesforceConnection;
import org.teiid.translator.salesforce.execution.DataPayload;
import org.teiid.translator.salesforce.execution.DeletedObject;
import org.teiid.translator.salesforce.execution.DeletedResult;
import org.teiid.translator.salesforce.execution.UpdatedResult;

import com.sforce.async.*;
import com.sforce.soap.partner.*;
import com.sforce.soap.partner.Error;
import com.sforce.soap.partner.fault.InvalidFieldFault;
import com.sforce.soap.partner.fault.InvalidIdFault;
import com.sforce.soap.partner.fault.InvalidQueryLocatorFault;
import com.sforce.soap.partner.fault.InvalidSObjectFault;
import com.sforce.soap.partner.fault.MalformedQueryFault;
import com.sforce.soap.partner.fault.UnexpectedErrorFault;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.transport.JdkHttpTransport;

public class SalesforceConnectionImpl extends BasicConnection implements SalesforceConnection {
	
	private static final String ID_FIELD_NAME = "id"; //$NON-NLS-1$
    private static final String PK_CHUNKING_HEADER = "Sforce-Enable-PKChunking"; //$NON-NLS-1$
    private static final int MAX_CHUNK_SIZE = 100000;
	private BulkConnection bulkConnection; 
	private PartnerConnection partnerConnection;
	private SalesforceConnectorConfig config;
	private String restEndpoint;
	private String apiVersion;
	private int pollingInterval = 500; //TODO: this could be configurable
	
	public SalesforceConnectionImpl(SalesForceManagedConnectionFactory mcf) throws ResourceException {
		login(mcf);
	}
	
	SalesforceConnectionImpl(PartnerConnection partnerConnection) {
		this.partnerConnection = partnerConnection;
	}
	
	private void login(SalesForceManagedConnectionFactory mcf) throws ResourceException {
	    config = new SalesforceConnectorConfig();
        String username = mcf.getUsername();
        String password = mcf.getPassword();

        // if security-domain is specified and caller identity is used; then use
        // credentials from subject
        boolean useCXFTransport = mcf.getConfigFile() != null;
        Subject subject = ConnectionContext.getSubject();
        if (subject != null) {
            OAuthCredential oauthCredential = ConnectionContext.getSecurityCredential(subject, OAuthCredential.class);
            if (oauthCredential != null) {
                config.setCredential(OAuthCredential.class.getName(), oauthCredential);
                useCXFTransport = true;
            } else {
                username = ConnectionContext.getUserName(subject, mcf, username);
                password = ConnectionContext.getPassword(subject, mcf, username, password);
            }
        }
		
		config.setCxfConfigFile(mcf.getConfigFile());
		if (useCXFTransport) {
		    config.setTransport(SalesforceCXFTransport.class);
		}
		
        config.setCompression(true);
        config.setTraceMessage(false);

		//set the catch all properties
		String props = mcf.getConfigProperties();
		if (props != null) {
			Properties p = new Properties();
			try {
				p.load(new StringReader(props));
			} catch (IOException e) {
				throw new ResourceException(e);
			}
			PropertiesUtils.setBeanProperties(config, p, null);
		}
		
        config.setUsername(username);
        config.setPassword(password);
        config.setAuthEndpoint(mcf.getURL());
        
        //set proxy if needed
        if (mcf.getProxyURL() != null) {
			try {
				URL proxyURL = new URL(mcf.getProxyURL());
				config.setProxy(proxyURL.getHost(), proxyURL.getPort());
		        config.setProxyUsername(mcf.getProxyUsername());
		        config.setProxyPassword(mcf.getProxyPassword());
			} catch (MalformedURLException e) {
				throw new ResourceException(e);
			}
        }
        if (mcf.getConnectTimeout() != null) {
        	config.setConnectionTimeout((int) Math.min(Integer.MAX_VALUE, mcf.getConnectTimeout()));
        }
        if (mcf.getRequestTimeout() != null) {
        	config.setReadTimeout((int) Math.min(Integer.MAX_VALUE, mcf.getRequestTimeout()));
        }
        
        try {
	        partnerConnection = new TeiidPartnerConnection(config);
	        
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
			// Test the connection.
			partnerConnection.getUserInfo();
			restEndpoint = endpoint.substring(0, endpoint.indexOf("Soap/"))+ "data/" + "v30.0";//$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        } catch (AsyncApiException e) {
        	throw new ResourceException(e);
        } catch (ConnectionException e) {
        	throw new ResourceException(e);
		}
        
		LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Login was successful for username", username); //$NON-NLS-1$
	}
	
	@Override
	public Long getCardinality(String sobject) throws ResourceException {
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
			throw new ResourceException(e);
		} catch (MalformedURLException e) {
			throw new ResourceException(e);
		} catch (UnsupportedEncodingException e) {
			throw new ResourceException(e);
		} catch (IOException e) {
			throw new ResourceException(e);
		} finally {
			if (is != null) {
				try {
					is.close();
				} catch (IOException e) {
				}
			}
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
	
	public boolean isValid() {
		if(partnerConnection != null) {
			try {
				partnerConnection.getServerTimestamp();
				return true;
			} catch (Throwable t) {
				LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Caught Throwable in isAlive", t); //$NON-NLS-1$
			}
		}
		return false;
	}

	public QueryResult query(String queryString, int batchSize, boolean queryAll) throws ResourceException {
		
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
		} catch (InvalidFieldFault e) {
			throw new ResourceException(e);
		} catch (MalformedQueryFault e) {
			throw new ResourceException(e);
		} catch (InvalidSObjectFault e) {
			throw new ResourceException(e);
		} catch (InvalidIdFault e) {
			throw new ResourceException(e);
		} catch (UnexpectedErrorFault e) {
			throw new ResourceException(e);
		} catch (InvalidQueryLocatorFault e) {
			throw new ResourceException(e);
		} catch (ConnectionException e) {
			throw new ResourceException(e);
		} finally {
			partnerConnection.clearMruHeader();
			partnerConnection.clearQueryOptions();
		}
		return qr;
	}

	public QueryResult queryMore(String queryLocator, int batchSize) throws ResourceException {
		if(batchSize > 2000) {
			batchSize = 2000;
			LogManager.logDetail(LogConstants.CTX_CONNECTOR, "reduced.batch.size"); //$NON-NLS-1$
		}

		partnerConnection.setQueryOptions(batchSize);
		try {
			return partnerConnection.queryMore(queryLocator);
		} catch (InvalidFieldFault e) {
			throw new ResourceException(e);
		} catch (UnexpectedErrorFault e) {
			throw new ResourceException(e);
		} catch (InvalidQueryLocatorFault e) {
			throw new ResourceException(e);
		} catch (ConnectionException e) {
			throw new ResourceException(e);
		} finally {
			partnerConnection.clearQueryOptions();
		}
		
	}

	public int delete(String[] ids) throws ResourceException {
		DeleteResult[] results = null;
		try {
			results = partnerConnection.delete(ids);
		} catch (UnexpectedErrorFault e) {
			throw new ResourceException(e);
		} catch (ConnectionException e) {
			throw new ResourceException(e);
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
			throw new ResourceException(errorMessages.toString());
		}
		return results.length;
	}
	
	public int upsert(DataPayload data) throws ResourceException {
	    SObject toCreate = new SObject();
        toCreate.setType(data.getType());
        for (DataPayload.Field field : data.getMessageElements()) {
            toCreate.addField(field.name, field.value);
        }
        SObject[] objects = new SObject[] {toCreate};
        UpsertResult[] results;
        try {
            results = partnerConnection.upsert(ID_FIELD_NAME, objects);
        } catch (InvalidFieldFault e) {
            throw new ResourceException(e);
        } catch (InvalidSObjectFault e) {
            throw new ResourceException(e);
        } catch (InvalidIdFault e) {
            throw new ResourceException(e);
        } catch (UnexpectedErrorFault e) {
            throw new ResourceException(e);
        } catch (ConnectionException e) {
            throw new ResourceException(e);
        }
        for (UpsertResult result : results) {
            if(!result.isSuccess()) {
                throw new ResourceException(result.getErrors()[0].getMessage());
            }
        }
        return results.length;
	}

	public int create(DataPayload data) throws ResourceException {
		SObject toCreate = new SObject();
		toCreate.setType(data.getType());
		for (DataPayload.Field field : data.getMessageElements()) {
			toCreate.addField(field.name, field.value);
		}
		SObject[] objects = new SObject[] {toCreate};
		SaveResult[] result;
		try {
			result = partnerConnection.create(objects);
		} catch (InvalidFieldFault e) {
			throw new ResourceException(e);
		} catch (InvalidSObjectFault e) {
			throw new ResourceException(e);
		} catch (InvalidIdFault e) {
			throw new ResourceException(e);
		} catch (UnexpectedErrorFault e) {
			throw new ResourceException(e);
		} catch (ConnectionException e) {
			throw new ResourceException(e);
		}
		return analyzeResult(result);
	}

	public int update(List<DataPayload> updateDataList) throws ResourceException {
		List<SObject> params = new ArrayList<SObject>(updateDataList.size());
		for(int i = 0; i < updateDataList.size(); i++) {
			DataPayload data = updateDataList.get(i);
			SObject toCreate = new SObject();
			toCreate.setType(data.getType());
			toCreate.setId(data.getID());
			for (DataPayload.Field field : data.getMessageElements()) {
				toCreate.addField(field.name, field.value);
			}
			params.add(i, toCreate);
		}
		SaveResult[] result;
			try {
				result = partnerConnection.update(params.toArray(new SObject[params.size()]));
			} catch (InvalidFieldFault e) {
				throw new ResourceException(e);
			} catch (InvalidSObjectFault e) {
				throw new ResourceException(e);
			} catch (InvalidIdFault e) {
				throw new ResourceException(e);
			} catch (UnexpectedErrorFault e) {
				throw new ResourceException(e);
			} catch (ConnectionException e) {
				throw new ResourceException(e);
			}
		return analyzeResult(result);
	}
	
	private int analyzeResult(SaveResult[] results) throws ResourceException {
		for (SaveResult result : results) {
			if(!result.isSuccess()) {
				throw new ResourceException(result.getErrors()[0].getMessage());
			}
		}
		return results.length;
	}

	public UpdatedResult getUpdated(String objectType, Calendar startDate, Calendar endDate) throws ResourceException {
			GetUpdatedResult updated;
			try {
				updated = partnerConnection.getUpdated(objectType, startDate, endDate);
			} catch (InvalidSObjectFault e) {
				throw new ResourceException(e);
			} catch (UnexpectedErrorFault e) {
				throw new ResourceException(e);
			} catch (ConnectionException e) {
				throw new ResourceException(e);
			}
			UpdatedResult result = new UpdatedResult(); 
			result.setLatestDateCovered(updated.getLatestDateCovered());
			result.setIDs(Arrays.asList(updated.getIds()));
			return result;
	}

	public DeletedResult getDeleted(String objectName, Calendar startCalendar,
			Calendar endCalendar) throws ResourceException {
			GetDeletedResult deleted;
			try {
				deleted = partnerConnection.getDeleted(objectName, startCalendar, endCalendar);
			} catch (InvalidSObjectFault e) {
				throw new ResourceException(e);
			} catch (UnexpectedErrorFault e) {
				throw new ResourceException(e);
			} catch (ConnectionException e) {
				throw new ResourceException(e);
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
	}
	
	public SObject[] retrieve(String fieldList, String sObjectType, List<String> ids) throws ResourceException {
		try {
			return partnerConnection.retrieve(fieldList, sObjectType, ids.toArray(new String[ids.size()]));
		} catch (InvalidFieldFault e) {
			throw new ResourceException(e);
		} catch (MalformedQueryFault e) {
			throw new ResourceException(e);
		} catch (InvalidSObjectFault e) {
			throw new ResourceException(e);
		} catch (InvalidIdFault e) {
			throw new ResourceException(e);
		} catch (UnexpectedErrorFault e) {
			throw new ResourceException(e);
		} catch (ConnectionException e) {
			throw new ResourceException(e);
		}
		
	}

	public DescribeGlobalResult getObjects() throws ResourceException {
		try {
			return partnerConnection.describeGlobal();
		} catch (UnexpectedErrorFault e) {
			throw new ResourceException(e);
		} catch (ConnectionException e) {
			throw new ResourceException(e);
		}
	}

	public DescribeSObjectResult[] getObjectMetaData(String... objectName) throws ResourceException {
		try {
			return partnerConnection.describeSObjects(objectName);
		} catch (InvalidSObjectFault e) {
			throw new ResourceException(e);
		} catch (UnexpectedErrorFault e) {
			throw new ResourceException(e);
		} catch (ConnectionException e) {
			throw new ResourceException(e);
		}
	}

	@Override
	public void close() throws ResourceException {
		
	}
	
	@Override
	public boolean isAlive() {
		return isValid();
	}

	@Override
	public JobInfo createBulkJob(String objectName, OperationEnum operation, boolean usePkChunking) throws ResourceException {
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
			throw new ResourceException(e);
		}
	}

	@Override
	public String addBatch(List<com.sforce.async.SObject> payload, JobInfo job) throws ResourceException {
		try {
			BatchRequest request = this.bulkConnection.createBatch(job);
			request.addSObjects(payload.toArray(new com.sforce.async.SObject[payload.size()]));
			return request.completeRequest().getId();
		} catch (AsyncApiException e) {
			throw new ResourceException(e);
		}
	}
	
	@Override
	public BatchResultInfo addBatch(String query, JobInfo job) throws ResourceException {
		try {
			BatchInfo batch = this.bulkConnection.createBatchFromStream(job, new ByteArrayInputStream(query.getBytes(Charset.forName("UTF-8")))); //$NON-NLS-1$
			return new BatchResultInfo(batch.getId());
		} catch (AsyncApiException e) {
			throw new ResourceException(e);
		}
	}
	
	@Override
	public BulkBatchResult getBatchQueryResults(String jobId, BatchResultInfo info) throws ResourceException {
		if (info.getResultList() == null && info.getPkBatches() == null) {
			try {
				BatchInfo batch = this.bulkConnection.getBatchInfo(jobId, info.getBatchId());
                switch (batch.getState()) {
    				case NotProcessed:
                        // we need more checks to ensure that chunking is being used.  since
    				    // we don't know, then we'll explicitly check
    				    JobInfo jobStatus = this.bulkConnection.getJobStatus(jobId);
    				    if (jobStatus.getState() == JobStateEnum.Aborted) {
                            throw new ResourceException(JobStateEnum.Aborted.name());
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
                                throw new ResourceException(batchInfoItem.getStateMessage());
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
	                    throw new ResourceException(batch.getStateMessage());
				}
			} catch (AsyncApiException e) {
				throw new ResourceException(e);
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
		    throw new ResourceException(e);
		}
	}

    private void throwDataNotAvailable(BatchResultInfo info) {
        int waitCount = info.incrementAndGetWaitCount();
        LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Waiting on queued/inprogress, wait number", waitCount); //$NON-NLS-1$
        throw new DataNotAvailableException(pollingInterval * Math.min(8, waitCount));
    }

    private void getNextPkChunkResultList(String jobId,
            BatchResultInfo info) throws AsyncApiException, ResourceException {
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
                throw new ResourceException(bi.getStateMessage());
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
    }
	
    private BulkBatchResult nextBulkBatch(String jobId, BatchResultInfo info)
            throws AsyncApiException {
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
                    }
                }
            };
		}
		return null;
    }

	@Override 
	public JobInfo closeJob(String jobId) throws ResourceException {
		try {
			return this.bulkConnection.closeJob(jobId);
		} catch (AsyncApiException e) {
			throw new ResourceException(e);
		}
	}

	@Override
	public BatchResult[] getBulkResults(JobInfo job, List<String> ids) throws ResourceException {
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
			throw new ResourceException(e);
		}
	}
	
	@Override
	public void cancelBulkJob(JobInfo job) throws ResourceException {
		try {
			this.bulkConnection.abortJob(job.getId());
		} catch (AsyncApiException e) {
			throw new ResourceException(e);
		}
	}	
	
	@Override
	public String getVersion() {
		return apiVersion;
	}
	
}
