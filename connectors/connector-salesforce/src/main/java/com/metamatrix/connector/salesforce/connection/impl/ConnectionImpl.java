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
package com.metamatrix.connector.salesforce.connection.impl;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.xml.datatype.XMLGregorianCalendar;
import javax.xml.ws.BindingProvider;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;

import com.metamatrix.connector.salesforce.execution.DataPayload;
import com.metamatrix.connector.salesforce.execution.DeletedObject;
import com.metamatrix.connector.salesforce.execution.DeletedResult;
import com.metamatrix.connector.salesforce.execution.UpdatedResult;
import com.sforce.soap.partner.CallOptions;
import com.sforce.soap.partner.DeleteResult;
import com.sforce.soap.partner.DeletedRecord;
import com.sforce.soap.partner.DescribeGlobalResult;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.GetDeletedResult;
import com.sforce.soap.partner.GetUpdatedResult;
import com.sforce.soap.partner.InvalidFieldFault;
import com.sforce.soap.partner.InvalidIdFault;
import com.sforce.soap.partner.LoginFault;
import com.sforce.soap.partner.LoginResult;
import com.sforce.soap.partner.LoginScopeHeader;
import com.sforce.soap.partner.MalformedQueryFault;
import com.sforce.soap.partner.MruHeader;
import com.sforce.soap.partner.ObjectFactory;
import com.sforce.soap.partner.PackageVersionHeader;
import com.sforce.soap.partner.QueryOptions;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.SessionHeader;
import com.sforce.soap.partner.SforceService;
import com.sforce.soap.partner.Soap;
import com.sforce.soap.partner.sobject.SObject;

public class ConnectionImpl {
	private SforceService sfService;
	private Soap sfSoap;
	private SessionHeader sh;
	private CallOptions co;
	private ConnectorLogger logger;
	
	private ObjectFactory partnerFactory = new ObjectFactory();
	
	PackageVersionHeader pvHeader = partnerFactory.createPackageVersionHeader();
	
	public ConnectionImpl(String username, String password, URL url, long pingInterval, ConnectorLogger logger, int timeout) throws ConnectorException {
		this.logger = logger;
		login(username, password, url, timeout);
	}
	
	String getUserName() throws ConnectorException {
			try {
				return sfSoap.getUserInfo(sh).getUserName();
			} catch (com.sforce.soap.partner.UnexpectedErrorFault e) {
				throw new ConnectorException(e.getMessage());
			}
	}
	
	Soap getBinding() {
		return sfSoap;
	}
	
	private void login(String username, String password, URL url, int timeout)
			throws ConnectorException {
		if (!isAlive()) {
			LoginResult loginResult = null;
			sfSoap = null;
			sfService = null;
			co = new CallOptions();
			co.setClient("RedHat/MetaMatrix/");

			try {
				/*
				if(null != url) {
					sfService = new SforceService(url);
					sfSoap = sfService.getSoap();
				} else {
					*/
					sfService = new SforceService();
					sfSoap = sfService.getSoap();
				//}
				loginResult = sfSoap.login(username, password);
			} catch (LoginFault e) {
				throw new ConnectorException(e.getCause().getMessage());
			} catch (InvalidIdFault e) {
				throw new ConnectorException(e.getCause().getMessage());
			} catch (com.sforce.soap.partner.UnexpectedErrorFault e) {
				throw new ConnectorException(e.getCause().getMessage());
			}
			logger.logTrace("Login was successful for username " + username);

			sh = new SessionHeader();
			sh.setSessionId(loginResult.getSessionId());
			// Reset the SOAP endpoint to the returned server URL
			((BindingProvider)sfSoap).getRequestContext().put(BindingProvider.ENDPOINT_ADDRESS_PROPERTY,
					loginResult.getServerUrl());
			// or maybe org.apache.cxf.message.Message.ENDPOINT_ADDRESS
			((BindingProvider)sfSoap).getRequestContext().put(BindingProvider.SESSION_MAINTAIN_PROPERTY,
					Boolean.TRUE);
			// Set the timeout.
			//((BindingProvider)sfSoap).getRequestContext().put(JAXWSProperties.CONNECT_TIMEOUT, timeout);

			
			// Test the connection.
			try {
				sfSoap.getUserInfo(sh);
			} catch (com.sforce.soap.partner.UnexpectedErrorFault e) {
				throw new ConnectorException(e.getMessage());
			}
		}
	}
	
	
	public boolean isAlive() {
		boolean result = true;
		if(sfSoap == null) {
			result = false;
		} else {
			try {
				sfSoap.getServerTimestamp(sh);
			} catch (Throwable t) {
				logger.logDetail("Caught Throwable in isAlive", t);
				result = false;
			}
		}
		return result;
	}

	public QueryResult query(String queryString, int batchSize, Boolean queryAll) throws ConnectorException {
		QueryResult qr = null;
		QueryOptions qo = partnerFactory.createQueryOptions();
		qo.setBatchSize(batchSize);
		try {
			if(queryAll) {
				qr = sfSoap.queryAll(queryString, sh);
			} else {
				MruHeader mruHeader = partnerFactory.createMruHeader();
				mruHeader.setUpdateMru(false);
				
				qr = sfSoap.query(queryString, sh);
			}
		} catch (InvalidFieldFault e) {
			throw new ConnectorException(e, e.getMessage());
		} catch (MalformedQueryFault e) {
			throw new ConnectorException(e, e.getMessage());
		} catch (com.sforce.soap.partner.InvalidSObjectFault e) {
			throw new ConnectorException(e, e.getMessage());
		} catch (InvalidIdFault e) {
			throw new ConnectorException(e, e.getMessage());
		} catch (com.sforce.soap.partner.UnexpectedErrorFault e) {
			throw new ConnectorException(e, e.getMessage());
		} catch (com.sforce.soap.partner.InvalidQueryLocatorFault e) {
			throw new ConnectorException(e, e.getMessage());
		}
		return qr;
	}

	public QueryResult queryMore(String queryLocator, int batchSize) throws ConnectorException {
		QueryOptions qo = partnerFactory.createQueryOptions();
		qo.setBatchSize(batchSize);
		try {
			return sfSoap.queryMore(queryLocator, sh);
		} catch (InvalidFieldFault e) {
			throw new ConnectorException(e, e.getMessage());
		} catch (com.sforce.soap.partner.UnexpectedErrorFault e) {
			throw new ConnectorException(e, e.getMessage());
		} catch (com.sforce.soap.partner.InvalidQueryLocatorFault e) {
			throw new ConnectorException(e, e.getMessage());
		}
		
	}

	public int delete(String[] ids) throws ConnectorException {
		List<DeleteResult> results = null;
		try {
			results = sfSoap.delete(Arrays.asList(ids), sh);
		} catch (com.sforce.soap.partner.UnexpectedErrorFault e) {
			throw new ConnectorException(e, e.getMessage());
		}
		
		boolean allGood = true;
		StringBuffer errorMessages = new StringBuffer();
		for(int i = 0; i < results.size(); i++) {
			DeleteResult result = results.get(i);
			if(!result.isSuccess()) {
				if(allGood) {
					errorMessages.append("Error(s) executing DELETE: ");
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
			throw new ConnectorException(errorMessages.toString());
		}
		return results.size();
	}

	public int create(DataPayload data) throws ConnectorException {
		SObject toCreate = new SObject();
		toCreate.setType(data.getType());
		toCreate.getAny().addAll(data.getMessageElements());
		List<SObject> objects = new ArrayList<SObject>();
		objects.add(toCreate);
		List<SaveResult> result;
		try {
			result = sfSoap.create(objects, sh);
		} catch (InvalidFieldFault e) {
			throw new ConnectorException(e, e.getMessage());
		} catch (com.sforce.soap.partner.InvalidSObjectFault e) {
			throw new ConnectorException(e, e.getMessage());
		} catch (InvalidIdFault e) {
			throw new ConnectorException(e, e.getMessage());
		} catch (com.sforce.soap.partner.UnexpectedErrorFault e) {
			throw new ConnectorException(e, e.getMessage());
		}
		return analyzeResult(result);
	}

	public int update(List<DataPayload> updateDataList) throws ConnectorException {
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
				result = sfSoap.update(params, sh);
			} catch (InvalidFieldFault e) {
				throw new ConnectorException(e, e.getMessage());
			} catch (com.sforce.soap.partner.InvalidSObjectFault e) {
				throw new ConnectorException(e, e.getMessage());
			} catch (InvalidIdFault e) {
				throw new ConnectorException(e, e.getMessage());
			} catch (com.sforce.soap.partner.UnexpectedErrorFault e) {
				throw new ConnectorException(e, e.getMessage());
			}
		return analyzeResult(result);
	}
	
	private int analyzeResult(List<SaveResult> results) throws ConnectorException {
		for (SaveResult result : results) {
			if(!result.isSuccess()) {
				throw new ConnectorException(result.getErrors().get(0).getMessage());
			}
		}
		return results.size();
	}

	public UpdatedResult getUpdated(String objectType, XMLGregorianCalendar startDate, XMLGregorianCalendar endDate) throws ConnectorException {
			GetUpdatedResult updated;
			try {
				updated = sfSoap.getUpdated(objectType, startDate, endDate, sh);
			} catch (com.sforce.soap.partner.InvalidSObjectFault e) {
				throw new ConnectorException(e, e.getMessage());
			} catch (com.sforce.soap.partner.UnexpectedErrorFault e) {
				throw new ConnectorException(e, e.getMessage());
			}
			UpdatedResult result = new UpdatedResult(); 
			result.setLatestDateCovered(updated.getLatestDateCovered().toGregorianCalendar());
			result.setIDs(updated.getIds());
			return result;
	}

	public DeletedResult getDeleted(String objectName, XMLGregorianCalendar startCalendar,
			XMLGregorianCalendar endCalendar) throws ConnectorException {
			GetDeletedResult deleted;
			try {
				deleted = sfSoap.getDeleted(objectName, startCalendar, endCalendar, sh);
			} catch (com.sforce.soap.partner.InvalidSObjectFault e) {
				throw new ConnectorException(e, e.getMessage());
			} catch (com.sforce.soap.partner.UnexpectedErrorFault e) {
				throw new ConnectorException(e, e.getMessage());
			}
			DeletedResult result = new DeletedResult();
			result.setLatestDateCovered(deleted.getLatestDateCovered().toGregorianCalendar());
			result.setEarliestDateAvailable(deleted.getEarliestDateAvailable().toGregorianCalendar());
			List<DeletedRecord> records = deleted.getDeletedRecords();
			List<DeletedObject> resultRecords = new ArrayList<DeletedObject>();
			DeletedObject object;
			if(null !=records) {
				for (DeletedObject record : resultRecords) {
					object = new DeletedObject();
					object.setID(record.getID());
					object.setDeletedDate(record.getDeletedDate());
					resultRecords.add(object);
				}
			}
			result.setResultRecords(resultRecords);
			return result;
	}
	
	public List<SObject> retrieve(String fieldList, String sObjectType, List<String> ids) throws ConnectorException {
		try {
			return sfSoap.retrieve(fieldList, sObjectType, ids, sh);
		} catch (InvalidFieldFault e) {
			throw new ConnectorException(e, e.getMessage());
		} catch (MalformedQueryFault e) {
			throw new ConnectorException(e, e.getMessage());
		} catch (com.sforce.soap.partner.InvalidSObjectFault e) {
			throw new ConnectorException(e, e.getMessage());
		} catch (InvalidIdFault e) {
			throw new ConnectorException(e, e.getMessage());
		} catch (com.sforce.soap.partner.UnexpectedErrorFault e) {
			throw new ConnectorException(e, e.getMessage());
		}
		
	}

	public DescribeGlobalResult getObjects() throws ConnectorException {
		try {
			return sfSoap.describeGlobal(sh);
		} catch (com.sforce.soap.partner.UnexpectedErrorFault e) {
			ConnectorException ce = new ConnectorException(e.getCause().getMessage());
			ce.initCause(e.getCause());
			throw ce;
		}
	}

	public DescribeSObjectResult getObjectMetaData(String objectName) throws ConnectorException {
		try {
			return sfSoap.describeSObject(objectName, sh);
		} catch (com.sforce.soap.partner.InvalidSObjectFault e) {
			throw new ConnectorException(e, e.getMessage());
		} catch (com.sforce.soap.partner.UnexpectedErrorFault e) {
			throw new ConnectorException(e, e.getMessage());
		}
	}
}
