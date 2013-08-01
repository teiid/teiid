/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */
package org.teiid.resource.adapter.salesforce;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.resource.ResourceException;
import javax.xml.datatype.XMLGregorianCalendar;
import javax.xml.ws.BindingProvider;

import org.apache.cxf.Bus;
import org.apache.cxf.BusFactory;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.resource.spi.BasicConnection;
import org.teiid.translator.DataNotAvailableException;
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
import com.sforce.async.BulkConnection;
import com.sforce.async.ContentType;
import com.sforce.async.JobInfo;
import com.sforce.async.OperationEnum;
import com.sforce.soap.partner.*;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectorConfig;

public class SalesforceConnectionImpl extends BasicConnection implements SalesforceConnection {
	
	private static final String CONNECTION_TIMEOUT = "javax.xml.ws.client.connectionTimeout"; //$NON-NLS-1$
	private static final String RECEIVE_TIMEOUT = "javax.xml.ws.client.receiveTimeout"; //$NON-NLS-1$

	private Soap sfSoap;
	private BulkConnection bulkConnection; 
	
	private ObjectFactory partnerFactory = new ObjectFactory();
	
	PackageVersionHeader pvHeader = partnerFactory.createPackageVersionHeader();
	
	public SalesforceConnectionImpl(String username, String password, URL url, SalesForceManagedConnectionFactory mcf) throws ResourceException {
		login(username, password, url, mcf);
	}
	
	protected SalesforceConnectionImpl(Soap soap) {
		this.sfSoap = soap;
	}
	
	String getUserName() throws ResourceException {
		try {
			return sfSoap.getUserInfo().getUserName();
		} catch (com.sforce.soap.partner.UnexpectedErrorFault e) {
			throw new ResourceException(e);
		}
	}
	
	Soap getBinding() {
		return sfSoap;
	}
	
	private void login(String username, String password, URL url, SalesForceManagedConnectionFactory mcf) throws ResourceException {
		LoginResult loginResult = null;
		SforceService sfService = null;
		SessionHeader sh = null;
		CallOptions co = new CallOptions();
		// This value identifies Teiid as a SF certified solution.
		// It was provided by SF and should not be changed.
		co.setClient("RedHat/MetaMatrix/"); //$NON-NLS-1$
		
		if(url == null) {
			throw new ResourceException("SalesForce URL is not specified, please provide a valid URL"); //$NON-NLS-1$
		}

		Bus bus = BusFactory.getThreadDefaultBus();
		BusFactory.setThreadDefaultBus(mcf.getBus());
		try {
			sfService = new SforceService();
			sh = new SessionHeader();
			
			// Session Id must be passed in soapHeader - add the handler
			sfService.setHandlerResolver(new SalesforceHandlerResolver(sh));
			
			sfSoap = sfService.getSoap();
			Map<String, Object> requestContext = ((BindingProvider)sfSoap).getRequestContext();
			if (mcf.getConnectTimeout() != null) {
				requestContext.put(CONNECTION_TIMEOUT, mcf.getConnectTimeout());
			}
			requestContext.put(BindingProvider.ENDPOINT_ADDRESS_PROPERTY, url.toExternalForm());
			loginResult = sfSoap.login(username, password);
			
			// Set the SessionId after login, for subsequent calls
			sh.setSessionId(loginResult.getSessionId());
			this.bulkConnection = getBulkConnection(loginResult.getServerUrl(), loginResult.getSessionId());
		} catch (LoginFault e) {
			throw new ResourceException(e);
		} catch (InvalidIdFault e) {
			throw new ResourceException(e);
		} catch (com.sforce.soap.partner.UnexpectedErrorFault e) {
			throw new ResourceException(e);
		} catch(AsyncApiException e) {
			throw new ResourceException(e);
		} finally {
			BusFactory.setThreadDefaultBus(bus);
		}
		LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Login was successful for username " + username); //$NON-NLS-1$
		Map<String, Object> requestContext = ((BindingProvider)sfSoap).getRequestContext();
		// Reset the SOAP endpoint to the returned server URL
		requestContext.put(BindingProvider.ENDPOINT_ADDRESS_PROPERTY,loginResult.getServerUrl());
		// or maybe org.apache.cxf.message.Message.ENDPOINT_ADDRESS
		requestContext.put(BindingProvider.SESSION_MAINTAIN_PROPERTY,Boolean.TRUE);
		// Set the timeout.
		if (mcf.getRequestTimeout() != null) {
			requestContext.put(RECEIVE_TIMEOUT, mcf.getRequestTimeout());
		}
		
		// Test the connection.
		try {
			sfSoap.getUserInfo();
		} catch (com.sforce.soap.partner.UnexpectedErrorFault e) {
			throw new ResourceException(e);
		}
	}
	
	
	public boolean isValid() {
		boolean result = true;
		if(sfSoap == null) {
			result = false;
		} else {
			try {
				sfSoap.getServerTimestamp();
			} catch (Throwable t) {
				LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Caught Throwable in isAlive", t); //$NON-NLS-1$
				result = false;
			}
		}
		return result;
	}

	public QueryResult query(String queryString, int batchSize, Boolean queryAll) throws ResourceException {
		
		if(batchSize > 2000) {
			batchSize = 2000;
			LogManager.logDetail(LogConstants.CTX_CONNECTOR, "reduced.batch.size"); //$NON-NLS-1$
		}
		
		QueryResult qr = null;
		QueryOptions qo = partnerFactory.createQueryOptions();
		qo.setBatchSize(batchSize);
		try {
			if(queryAll != null && queryAll) {
				qr = sfSoap.queryAll(queryString);
			} else {
				MruHeader mruHeader = partnerFactory.createMruHeader();
				mruHeader.setUpdateMru(false);
				
				qr = sfSoap.query(queryString);
			}
		} catch (InvalidFieldFault e) {
			throw new ResourceException(e);
		} catch (MalformedQueryFault e) {
			throw new ResourceException(e);
		} catch (com.sforce.soap.partner.InvalidSObjectFault e) {
			throw new ResourceException(e);
		} catch (InvalidIdFault e) {
			throw new ResourceException(e);
		} catch (com.sforce.soap.partner.UnexpectedErrorFault e) {
			throw new ResourceException(e);
		} catch (com.sforce.soap.partner.InvalidQueryLocatorFault e) {
			throw new ResourceException(e);
		}
		return qr;
	}

	public QueryResult queryMore(String queryLocator, int batchSize) throws ResourceException {
		QueryOptions qo = partnerFactory.createQueryOptions();
		qo.setBatchSize(batchSize);
		try {
			return sfSoap.queryMore(queryLocator);
		} catch (InvalidFieldFault e) {
			throw new ResourceException(e);
		} catch (com.sforce.soap.partner.UnexpectedErrorFault e) {
			throw new ResourceException(e);
		} catch (com.sforce.soap.partner.InvalidQueryLocatorFault e) {
			throw new ResourceException(e);
		}
		
	}

	public int delete(String[] ids) throws ResourceException {
		List<DeleteResult> results = null;
		try {
			results = sfSoap.delete(Arrays.asList(ids));
		} catch (com.sforce.soap.partner.UnexpectedErrorFault e) {
			throw new ResourceException(e);
		}
		
		boolean allGood = true;
		StringBuffer errorMessages = new StringBuffer();
		for(int i = 0; i < results.size(); i++) {
			DeleteResult result = results.get(i);
			if(!result.isSuccess()) {
				if(allGood) {
					errorMessages.append("Error(s) executing DELETE: "); //$NON-NLS-1$
					allGood = false;
				}
				List<com.sforce.soap.partner.Error> errors = result.getErrors();
				if(null != errors && errors.size() > 0) {
					for(int x = 0; x < errors.size(); x++) {
						com.sforce.soap.partner.Error error = errors.get(x);
						errorMessages.append(error.getMessage()).append(';');
					}
				}
				
			}
		}
		if(!allGood) {
			throw new ResourceException(errorMessages.toString());
		}
		return results.size();
	}

	public int create(DataPayload data) throws ResourceException {
		SObject toCreate = new SObject();
		toCreate.setType(data.getType());
		toCreate.getAny().addAll(data.getMessageElements());
		List<SObject> objects = new ArrayList<SObject>();
		objects.add(toCreate);
		List<SaveResult> result;
		try {
			result = sfSoap.create(objects);
		} catch (InvalidFieldFault e) {
			throw new ResourceException(e);
		} catch (com.sforce.soap.partner.InvalidSObjectFault e) {
			throw new ResourceException(e);
		} catch (InvalidIdFault e) {
			throw new ResourceException(e);
		} catch (com.sforce.soap.partner.UnexpectedErrorFault e) {
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
			toCreate.getAny().addAll(data.getMessageElements());
			params.add(i, toCreate);
		}
		List<SaveResult> result;
			try {
				result = sfSoap.update(params);
			} catch (InvalidFieldFault e) {
				throw new ResourceException(e);
			} catch (com.sforce.soap.partner.InvalidSObjectFault e) {
				throw new ResourceException(e);
			} catch (InvalidIdFault e) {
				throw new ResourceException(e);
			} catch (com.sforce.soap.partner.UnexpectedErrorFault e) {
				throw new ResourceException(e);
			}
		return analyzeResult(result);
	}
	
	private int analyzeResult(List<SaveResult> results) throws ResourceException {
		for (SaveResult result : results) {
			if(!result.isSuccess()) {
				throw new ResourceException(result.getErrors().get(0).getMessage());
			}
		}
		return results.size();
	}

	public UpdatedResult getUpdated(String objectType, XMLGregorianCalendar startDate, XMLGregorianCalendar endDate) throws ResourceException {
			GetUpdatedResult updated;
			try {
				updated = sfSoap.getUpdated(objectType, startDate, endDate);
			} catch (com.sforce.soap.partner.InvalidSObjectFault e) {
				throw new ResourceException(e);
			} catch (com.sforce.soap.partner.UnexpectedErrorFault e) {
				throw new ResourceException(e);
			}
			UpdatedResult result = new UpdatedResult(); 
			result.setLatestDateCovered(updated.getLatestDateCovered().toGregorianCalendar());
			result.setIDs(updated.getIds());
			return result;
	}

	public DeletedResult getDeleted(String objectName, XMLGregorianCalendar startCalendar,
			XMLGregorianCalendar endCalendar) throws ResourceException {
			GetDeletedResult deleted;
			try {
				deleted = sfSoap.getDeleted(objectName, startCalendar, endCalendar);
			} catch (com.sforce.soap.partner.InvalidSObjectFault e) {
				throw new ResourceException(e);
			} catch (com.sforce.soap.partner.UnexpectedErrorFault e) {
				throw new ResourceException(e);
			}
			DeletedResult result = new DeletedResult();
			result.setLatestDateCovered(deleted.getLatestDateCovered().toGregorianCalendar());
			result.setEarliestDateAvailable(deleted.getEarliestDateAvailable().toGregorianCalendar());
			List<DeletedRecord> records = deleted.getDeletedRecords();
			List<DeletedObject> resultRecords = new ArrayList<DeletedObject>();
			if(records != null) {
				for (DeletedRecord record : records) {
					DeletedObject object = new DeletedObject();
					object.setID(record.getId());
					object.setDeletedDate(record.getDeletedDate().toGregorianCalendar());
					resultRecords.add(object);
				}
			}
			result.setResultRecords(resultRecords);
			return result;
	}
	
	public  QueryResult retrieve(String fieldList, String sObjectType, List<String> ids) throws ResourceException {
		try {
			List<SObject> objects = sfSoap.retrieve(fieldList, sObjectType, ids);
			QueryResult result = new QueryResult();
			for (SObject sObject : objects) {
			    if (sObject != null) {
					result.getRecords().add(sObject);
			    }
			}
			result.setSize(result.getRecords().size());
			result.setDone(true);
			return result;			
		} catch (InvalidFieldFault e) {
			throw new ResourceException(e);
		} catch (MalformedQueryFault e) {
			throw new ResourceException(e);
		} catch (com.sforce.soap.partner.InvalidSObjectFault e) {
			throw new ResourceException(e);
		} catch (InvalidIdFault e) {
			throw new ResourceException(e);
		} catch (com.sforce.soap.partner.UnexpectedErrorFault e) {
			throw new ResourceException(e);
		}
		
	}

	public DescribeGlobalResult getObjects() throws ResourceException {
		try {
			return sfSoap.describeGlobal();
		} catch (com.sforce.soap.partner.UnexpectedErrorFault e) {
			throw new ResourceException(e);
		}
	}

	public DescribeSObjectResult getObjectMetaData(String objectName) throws ResourceException {
		try {
			return sfSoap.describeSObject(objectName);
		} catch (com.sforce.soap.partner.InvalidSObjectFault e) {
			throw new ResourceException(e);
		} catch (com.sforce.soap.partner.UnexpectedErrorFault e) {
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

	private JobInfo createBulkJob(String objectName) throws ResourceException {
        try {
			JobInfo job = new JobInfo();
			job.setObject(objectName);
			job.setOperation(OperationEnum.insert);
			job.setContentType(ContentType.XML);
			return this.bulkConnection.createJob(job);
		} catch (AsyncApiException e) {
			throw new ResourceException(e);
		}
	}

	@Override
	public JobInfo executeBulkJob(String objectName, List<com.sforce.async.SObject> payload) throws ResourceException {
		try {
			JobInfo job = createBulkJob(objectName);
			BatchRequest request = this.bulkConnection.createBatch(job);
			request.addSObjects(payload.toArray(new com.sforce.async.SObject[payload.size()]));
			request.completeRequest();
			return this.bulkConnection.closeJob(job.getId());
		} catch (AsyncApiException e) {
			throw new ResourceException(e);
		}
	}

	@Override
	public BatchResult getBulkResults(JobInfo job) throws ResourceException {
		try {
			BatchInfoList batchInfo = this.bulkConnection.getBatchInfoList(job.getId());
			BatchInfo[] batches = batchInfo.getBatchInfo();
			if (batches.length > 0) {
				BatchResult batchResult = this.bulkConnection.getBatchResult(job.getId(), batches[0].getId());
				if (batchResult.isPartialResult()) {
					throw new DataNotAvailableException(500);
				}
				return batchResult;
			}
			throw new DataNotAvailableException(500);
		} catch (AsyncApiException e) {
			throw new ResourceException(e);
		}
	}
	
    private BulkConnection getBulkConnection(String endpoint, String sessionid) throws AsyncApiException {
          ConnectorConfig config = new ConnectorConfig();
          config.setSessionId(sessionid);
          // The endpoint for the Bulk API service is the same as for the normal
          // SOAP uri until the /Soap/ part. From here it's '/async/versionNumber'
          int index = endpoint.indexOf("Soap/u/"); //$NON-NLS-1$
          int endIndex = endpoint.indexOf('/', index+7);
          String apiVersion = endpoint.substring(index+7,endIndex);
          String restEndpoint = endpoint.substring(0, endpoint.indexOf("Soap/"))+ "async/" + apiVersion;//$NON-NLS-1$ //$NON-NLS-2$
          config.setRestEndpoint(restEndpoint);
          config.setCompression(true);
          config.setTraceMessage(false);
          BulkConnection connection = new BulkConnection(config);
          return connection;
      }

	@Override
	public void cancelBulkJob(JobInfo job) throws ResourceException {
		try {
			this.bulkConnection.abortJob(job.getId());
		} catch (AsyncApiException e) {
			throw new ResourceException(e);
		}
	}	
}
