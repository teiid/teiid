/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package com.metamatrix.connector.salesforce.connection;

import java.net.URL;
import java.util.List;

import com.metamatrix.connector.salesforce.Messages;
import com.metamatrix.connector.salesforce.SalesforceCapabilities;
import com.metamatrix.connector.salesforce.connection.impl.ConnectionImpl;
import com.metamatrix.connector.salesforce.execution.DataPayload;
import com.metamatrix.connector.salesforce.execution.QueryExecutionImpl;
import com.metamatrix.connector.salesforce.execution.UpdateExecutionParent;
import com.metamatrix.data.api.ConnectorCapabilities;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ConnectorMetadata;
import com.metamatrix.data.api.Execution;
import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;
import com.metamatrix.data.pool.PoolAwareConnection;
import com.sforce.soap.partner.QueryResult;

public class SalesforceConnection implements com.metamatrix.data.api.Connection, PoolAwareConnection {

	private SalesforceCapabilities salesforceCapabilites;
	private ConnectorEnvironment connectorEnv;
	private ConnectionImpl connection;
	
	public SalesforceConnection(String username, String password, URL url, ConnectorEnvironment env) throws ConnectorException {
		try {
			connectorEnv = env;
			String capabilitiesClass = env.getProperties().getProperty("ConnectorCapabilities");
			if(capabilitiesClass != null) {
	        	try {
	        		Class clazz = Thread.currentThread().getContextClassLoader().loadClass(capabilitiesClass);
	        		salesforceCapabilites = (SalesforceCapabilities) clazz.newInstance();
				} catch (Exception e) {
					throw new ConnectorException(e, "Unable to load Capabilities Class");
				}
	        } else {
	        	throw new ConnectorException("Capabilities Class name not found");
	        }
			try {
				String inLimitString = env.getProperties().getProperty("InLimit");
				int inLimit = Integer.decode(inLimitString).intValue();
				salesforceCapabilites.setMaxInCriteriaSize(inLimit);
			} catch (NumberFormatException e) {
				throw new ConnectorException(Messages.getString("SalesforceConnection.bad.IN.value"));
			}
			
			long pingInterval = 5000;
			try {
				String pingIntervalString = env.getProperties().getProperty("SourceConnectionTestInterval");
				if(null != pingIntervalString) {
					pingInterval = Long.decode(pingIntervalString);
				}
			}	catch (NumberFormatException e) {
				throw new ConnectorException(Messages.getString("SalesforceConnection.bad.ping.value"));
			} 
			connection = new ConnectionImpl(username, password, url, pingInterval, env.getLogger());
		} catch(Throwable t) {
			env.getLogger().logError("SalesforceConnection() ErrorMessage: " + t.getMessage());
			if(t instanceof ConnectorException) {
				// don't wrap it again
				throw (ConnectorException) t;
			} else {
				throw new ConnectorException(t);
			}
		}
	}

	public Execution createExecution(int executionMode, ExecutionContext context, 
			RuntimeMetadata metadata) throws ConnectorException {
		try {
			Execution retVal = null;
			String errKey = null;
			switch (executionMode) {
			case ConnectorCapabilities.EXECUTION_MODE.SYNCH_QUERY:
				retVal = new QueryExecutionImpl(this, metadata, context, connectorEnv);
				break;
			case ConnectorCapabilities.EXECUTION_MODE.UPDATE:
				retVal = new UpdateExecutionParent(this, metadata, context, connectorEnv);
				break;
			case ConnectorCapabilities.EXECUTION_MODE.PROCEDURE:
				errKey = Messages.getString("SalesforceConnection.procedures.not.supported");
				break;
			default:
				errKey = Messages
						.getString("SalesforceConnection.invalid.execution.mode");
			}
			if (errKey != null) {
				throw new ConnectorException(errKey);
			}
			return retVal;
		} catch (RuntimeException e) {
			throw new ConnectorException(e);
		}
	}

	public ConnectorCapabilities getCapabilities() {
		return this.salesforceCapabilites;
	}

	public ConnectorMetadata getMetadata() {
		return null;
	}

	public void release() {
	}

	public QueryResult query(String queryString, int maxBatchSize) throws ConnectorException {
		return connection.query(queryString, maxBatchSize);
	}

	public QueryResult queryMore(String queryLocator) throws ConnectorException {
		return connection.queryMore(queryLocator);
	}
	
	public boolean isAlive() {
		return connection.isAlive();
	}
	
	@Override
	public void connectionReleased() {
		
	}

	public int delete(String[] ids) throws ConnectorException {
		return connection.delete(ids);
	}

	public int create(DataPayload data) throws ConnectorException {
		return connection.create(data);
	}

	public int update(List<DataPayload> updateDataList) throws ConnectorException {
		return connection.update(updateDataList);
	}
}
