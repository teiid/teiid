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
package com.metamatrix.connector.salesforce.connection;

import java.net.URL;
import java.util.Calendar;
import java.util.List;

import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.ProcedureExecution;
import org.teiid.connector.api.ResultSetExecution;
import org.teiid.connector.api.UpdateExecution;
import org.teiid.connector.basic.BasicConnection;
import org.teiid.connector.language.ICommand;
import org.teiid.connector.language.IProcedure;
import org.teiid.connector.language.IQueryCommand;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

import com.metamatrix.connector.salesforce.Messages;
import com.metamatrix.connector.salesforce.connection.impl.ConnectionImpl;
import com.metamatrix.connector.salesforce.execution.DataPayload;
import com.metamatrix.connector.salesforce.execution.DeletedResult;
import com.metamatrix.connector.salesforce.execution.ProcedureExecutionParentImpl;
import com.metamatrix.connector.salesforce.execution.QueryExecutionImpl;
import com.metamatrix.connector.salesforce.execution.UpdateExecutionParent;
import com.metamatrix.connector.salesforce.execution.UpdatedResult;
import com.sforce.soap.partner.QueryResult;

public class SalesforceConnection extends BasicConnection {

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
	public ProcedureExecution createProcedureExecution(IProcedure command,
			ExecutionContext executionContext, RuntimeMetadata metadata)
			throws ConnectorException {
		return new ProcedureExecutionParentImpl(command, this, metadata, executionContext, connectorEnv);
	}

	@Override
	public void close() {
	}

	public QueryResult query(String queryString, int maxBatchSize, Boolean queryAll) throws ConnectorException {
		if(maxBatchSize > 2000) {
			maxBatchSize = 2000;
			connectorEnv.getLogger().logInfo(
					Messages.getString("SalesforceQueryExecutionImpl.reduced.batch.size"));
		}
		return connection.query(queryString, maxBatchSize, queryAll);
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

	public UpdatedResult getUpdated(String objectName, Calendar startCalendar,
			Calendar endCalendar) throws ConnectorException {
		return connection.getUpdated(objectName, startCalendar, endCalendar);
	}

	public DeletedResult getDeleted(String objectName, Calendar startCalendar,
			Calendar endCalendar) throws ConnectorException {
		return connection.getDeleted(objectName, startCalendar, endCalendar);
	}
}
