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
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import javax.xml.rpc.ServiceException;

import org.apache.axis.EngineConfiguration;
import org.apache.axis.Handler;
import org.apache.axis.SimpleChain;
import org.apache.axis.SimpleTargetedChain;
import org.apache.axis.client.AxisClient;
import org.apache.axis.configuration.SimpleProvider;
import org.apache.axis.handlers.SimpleSessionHandler;
import org.apache.axis.transport.http.CommonsHTTPSender;
import org.apache.axis.transport.http.HTTPTransport;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;

import com.metamatrix.connector.salesforce.execution.DataPayload;
import com.metamatrix.connector.salesforce.execution.DeletedObject;
import com.metamatrix.connector.salesforce.execution.DeletedResult;
import com.metamatrix.connector.salesforce.execution.UpdatedResult;
import com.sforce.soap.partner.CallOptions;
import com.sforce.soap.partner.DeleteResult;
import com.sforce.soap.partner.DeletedRecord;
import com.sforce.soap.partner.GetDeletedResult;
import com.sforce.soap.partner.GetUpdatedResult;
import com.sforce.soap.partner.LoginResult;
import com.sforce.soap.partner.QueryOptions;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.SessionHeader;
import com.sforce.soap.partner.SforceServiceLocator;
import com.sforce.soap.partner.SoapBindingStub;
import com.sforce.soap.partner.fault.ApiFault;
import com.sforce.soap.partner.fault.InvalidSObjectFault;
import com.sforce.soap.partner.fault.UnexpectedErrorFault;
import com.sforce.soap.partner.fault.InvalidQueryLocatorFault;
import com.sforce.soap.partner.sobject.SObject;

public class ConnectionImpl {
	private SoapBindingStub binding;
	private ConnectorLogger logger;
	private long prevTime;
	private long pingInterval;
	
	public ConnectionImpl(String username, String password, URL url, long pingInterval, ConnectorLogger logger) throws ConnectorException {
		this.pingInterval = pingInterval;
		this.logger = logger;
		login(username, password, url);
		prevTime = System.currentTimeMillis();
	}
	
	String getUserName() throws ConnectorException {
		try {
			return binding.getUserInfo().getUserName();
		} catch (UnexpectedErrorFault e) {
			throw new ConnectorException(e.getExceptionMessage());
		} catch (RemoteException e) {
			throw new ConnectorException(e.getCause().getMessage());
		}
	}
	
	SoapBindingStub getBinding() {
		return binding;
	}
	
	private void login(String username, String password, URL url)
			throws ConnectorException {
		if (!isAlive()) {
			LoginResult loginResult = null;
			binding = null;
			SforceServiceLocator locator = new SforceServiceLocator();
		  	EngineConfiguration myConfig = getStaticConnectionConfig();
		  	locator.setEngineConfiguration(myConfig);
		  	locator.setEngine(new AxisClient(myConfig));
			try {
				if(null != url) {
					binding = (SoapBindingStub) new SforceServiceLocator().getSoap(url);
				} else {
					binding = (SoapBindingStub) new SforceServiceLocator().getSoap();
				}
				CallOptions co = new CallOptions();
				co.setClient("RedHat/MetaMatrix/");
				binding.setHeader("SforceService", "CallOptions", co);
				loginResult = binding.login(username, password);
			} catch (ApiFault ex) {
				throw new ConnectorException(ex.getExceptionMessage());
			} catch (RemoteException e) {
				throw new ConnectorException(e.getCause().getMessage());
			} catch (ServiceException e) {
				throw new ConnectorException(e.getCause().getMessage());
			}
			logger.logTrace("Login was successful for username " + username);

			// Reset the SOAP endpoint to the returned server URL
			binding._setProperty(SoapBindingStub.ENDPOINT_ADDRESS_PROPERTY,
					loginResult.getServerUrl());

			// Create a new session header object
			// add the session ID returned from the login
			SessionHeader sh = new SessionHeader();
			sh.setSessionId(loginResult.getSessionId());
			// Set the session header for subsequent call authentication
			binding.setHeader(new SforceServiceLocator().getServiceName()
					.getNamespaceURI(), "SessionHeader", sh);

			// Test the connection.
			try {
				binding.getUserInfo();
			} catch (UnexpectedErrorFault e) {
				throw new ConnectorException(e.getExceptionMessage());
			} catch (RemoteException e) {
				throw new ConnectorException(e.getCause().getMessage());
			}
		}
	}
	
	// Replace the non-static Axis connection with the static HTTP Commons connection.
	private EngineConfiguration getStaticConnectionConfig() {
		SimpleProvider clientConfig=new SimpleProvider(); 
		Handler sessionHandler =(Handler)new SimpleSessionHandler(); 
		SimpleChain reqHandler = new SimpleChain();
		SimpleChain respHandler =new SimpleChain(); 
		reqHandler.addHandler(sessionHandler); 
		respHandler.addHandler(sessionHandler); 
		Handler pivot=(Handler)new CommonsHTTPSender(); 
		Handler transport=new SimpleTargetedChain(reqHandler, pivot, respHandler); 
		clientConfig.deployTransport(HTTPTransport.DEFAULT_TRANSPORT_NAME,transport); 
		return clientConfig;    
	}
	
	public boolean isAlive() {
		boolean result = true;
		if(null != binding) {
			try {
				long currentTime = System.currentTimeMillis();
				if ((currentTime - prevTime)/1000 > pingInterval) {
					prevTime = currentTime;
					binding.getServerTimestamp();
				}
			} catch (UnexpectedErrorFault e) {
				result = false;
			} catch (RemoteException e) {
				result = false;
			}
		} else {
			result = false;
		}
		return result;
	}

	public QueryResult query(String queryString, int batchSize, Boolean queryAll) throws ConnectorException {
		QueryResult qr = null;
		QueryOptions qo = new QueryOptions();
		qo.setBatchSize(batchSize);
		binding.setHeader(new SforceServiceLocator().getServiceName()
				.getNamespaceURI(), "QueryOptions", qo);
		try {
			if(queryAll) {
				qr = binding.queryAll(queryString);
			} else {
				qr = binding.query(queryString);
			}
		} catch (ApiFault ex) {
			throw new ConnectorException(ex.getExceptionMessage());
		} catch (RemoteException ex) {
			throw new ConnectorException(ex, ex.getMessage());
		}
		return qr;
	}

	public QueryResult queryMore(String queryLocator) throws ConnectorException {
		try {
			return binding.queryMore(queryLocator);
		} catch ( InvalidQueryLocatorFault e ) {
			throw new ConnectorException(e.getMessage()); 
		} catch (UnexpectedErrorFault e) {
			throw new ConnectorException(e.getMessage());
		} catch (ApiFault e) {
			throw new ConnectorException(e.getMessage());
		} catch (RemoteException e) {
			throw new ConnectorException(e.getCause());
		}
	}

	public int delete(String[] ids) throws ConnectorException {
		DeleteResult[] results = null;
		try {
			results = binding.delete(ids);
		} catch (UnexpectedErrorFault e) {
			throw new ConnectorException(e.getExceptionMessage());
		} catch (RemoteException e) {
			throw new ConnectorException(e.getCause());
		}
		
		boolean allGood = true;
		StringBuffer errorMessages = new StringBuffer();
		for(int i = 0; i < results.length; i++) {
			DeleteResult result = results[i];
			if(!result.isSuccess()) {
				if(allGood) {
					errorMessages.append("Error(s) executing DELETE: ");
					allGood = false;
				}
				com.sforce.soap.partner.Error[] errors = result.getErrors();
				if(null != errors && errors.length > 0) {
					for(int x = 0; x < errors.length; x++) {
						com.sforce.soap.partner.Error error = errors[x];
						errorMessages.append(error.getMessage()).append(';');
					}
				}
				
			}
		}
		if(!allGood) {
			throw new ConnectorException(errorMessages.toString());
		}
		return results.length;
	}

	public int create(DataPayload data) throws ConnectorException {
		SObject toCreate = new SObject();
		toCreate.setType(data.getType());
		toCreate.set_any(data.getMessageElements());
		SObject[] create = new SObject[] {toCreate};
		SaveResult[] result;
		try {
			result = binding.create(create);
		} catch (ApiFault e) {
			String message = e.getExceptionMessage();
			throw new ConnectorException(e, message);
		} catch (RemoteException e) {
			throw new ConnectorException(e.getCause());
		}
		return analyzeResult(result);
	}

	public int update(List<DataPayload> updateDataList) throws ConnectorException {
		SObject[] params = new SObject[updateDataList.size()];
		for(int i = 0; i < updateDataList.size(); i++) {
			DataPayload data = updateDataList.get(i);
			SObject toCreate = new SObject();
			toCreate.setType(data.getType());
			toCreate.setId(data.getID());
			toCreate.set_any(data.getMessageElements());
			params[i] = toCreate;
		}
		SaveResult[] result;
		try {
			result = binding.update(params);
		} catch (ApiFault e) {
			String message = e.getExceptionMessage();
			throw new ConnectorException(e, message);
		} catch (RemoteException e) {
			throw new ConnectorException(e.getCause());
		}
		return analyzeResult(result);
	}
	
	private int analyzeResult(SaveResult[] results) throws ConnectorException {
		for(int i = 0; i < results.length; i++) {
			SaveResult result = results[i];
			if(!result.isSuccess()) {
				throw new ConnectorException(result.getErrors()[0].getMessage());
			}
		}
		return results.length;
	}

	public UpdatedResult getUpdated(String objectType, Calendar startDate, Calendar endDate) throws ConnectorException {
		try {
			GetUpdatedResult updated = binding.getUpdated(objectType, startDate, endDate);
			UpdatedResult result = new UpdatedResult(); 
			result.setLatestDateCovered(updated.getLatestDateCovered());
			result.setIDs(updated.getIds());
			return result;
		} catch (InvalidSObjectFault e) {
			throw new ConnectorException(e.getExceptionMessage());
		} catch (UnexpectedErrorFault e) {
			throw new ConnectorException(e.getMessage());
		} catch (RemoteException e) {
			throw new ConnectorException(e, e.getMessage());
		}
	}

	public DeletedResult getDeleted(String objectName, Calendar startCalendar,
			Calendar endCalendar) throws ConnectorException {
		try {
			GetDeletedResult deleted = binding.getDeleted(objectName, startCalendar, endCalendar);
			DeletedResult result = new DeletedResult();
			result.setLatestDateCovered(deleted.getLatestDateCovered());
			result.setEarliestDateAvailable(deleted.getEarliestDateAvailable());
			DeletedRecord[] records = deleted.getDeletedRecords();
			List<DeletedObject> resultRecords = new ArrayList<DeletedObject>();
			DeletedObject object;
			if(null !=records) {
				for (int i = 0; i < records.length; i++) {
					DeletedRecord record = records[i];
					object = new DeletedObject();
					object.setID(record.getId());
					object.setDeletedDate(record.getDeletedDate());
					resultRecords.add(object);
				}
			}
			result.setResultRecords(resultRecords);
			return result;
		} catch (InvalidSObjectFault e) {
			throw new ConnectorException(e.getExceptionMessage());
		} catch (UnexpectedErrorFault e) {
			throw new ConnectorException(e.getMessage());
		} catch (RemoteException e) {
			throw new ConnectorException(e, e.getMessage());
		}
	}
}
