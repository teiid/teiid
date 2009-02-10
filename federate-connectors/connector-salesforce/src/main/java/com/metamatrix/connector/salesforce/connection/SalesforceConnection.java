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

import com.metamatrix.connector.api.ConnectorCapabilities;
import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.api.ResultSetExecution;
import com.metamatrix.connector.api.UpdateExecution;
import com.metamatrix.connector.basic.BasicConnection;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.language.ICommand;
import com.metamatrix.connector.language.IQueryCommand;
import com.metamatrix.connector.metadata.runtime.RuntimeMetadata;
import com.metamatrix.connector.pool.PoolAwareConnection;
import com.metamatrix.connector.salesforce.Messages;
import com.metamatrix.connector.salesforce.connection.impl.ConnectionImpl;
import com.metamatrix.connector.salesforce.execution.DataPayload;
import com.metamatrix.connector.salesforce.execution.QueryExecutionImpl;
import com.metamatrix.connector.salesforce.execution.UpdateExecutionParent;
import com.sforce.soap.partner.QueryResult;

public class SalesforceConnection extends BasicConnection implements PoolAwareConnection {

	private ConnectorEnvironment connectorEnv;
	private ConnectionImpl connection;
	
	public SalesforceConnection(String username, String password, URL url, ConnectorEnvironment env) throws ConnectorException {
		try {
			connectorEnv = env;
			
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
	
	@Override
	public ResultSetExecution createResultSetExecution(IQueryCommand command,
			ExecutionContext executionContext, RuntimeMetadata metadata)
			throws ConnectorException {
		return new QueryExecutionImpl(command, this, metadata, executionContext, connectorEnv);
	}
	
	@Override
	public UpdateExecution createUpdateExecution(ICommand command,
			ExecutionContext executionContext, RuntimeMetadata metadata)
			throws ConnectorException {
		return new UpdateExecutionParent(command, this, metadata, executionContext, connectorEnv);

	}
	
	@Override
	public ConnectorCapabilities getCapabilities() {
		return null;
	}

	@Override
	public void close() {
	}

	public QueryResult query(String queryString, int maxBatchSize) throws ConnectorException {
		if(maxBatchSize > 2000) {
			maxBatchSize = 2000;
			connectorEnv.getLogger().logInfo(
					Messages.getString("SalesforceQueryExecutionImpl.reduced.batch.size"));
		}
		return connection.query(queryString, maxBatchSize);
	}

	public QueryResult queryMore(String queryLocator) throws ConnectorException {
		return connection.queryMore(queryLocator);
	}
	
	@Override
	public boolean isAlive() {
		return connection.isAlive();
	}
	
	@Override
	public void closeCalled() {
		
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
