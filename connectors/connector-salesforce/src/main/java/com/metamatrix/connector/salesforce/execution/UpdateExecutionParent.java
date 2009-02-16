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
package com.metamatrix.connector.salesforce.execution;

import java.util.ArrayList;

import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.api.ConnectorException;
import com.metamatrix.connector.api.DataNotAvailableException;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.api.UpdateExecution;
import com.metamatrix.connector.basic.BasicExecution;
import com.metamatrix.connector.language.ICommand;
import com.metamatrix.connector.language.ICompareCriteria;
import com.metamatrix.connector.language.ICriteria;
import com.metamatrix.connector.language.IDelete;
import com.metamatrix.connector.language.IInsert;
import com.metamatrix.connector.language.IUpdate;
import com.metamatrix.connector.metadata.runtime.RuntimeMetadata;
import com.metamatrix.connector.salesforce.Util;
import com.metamatrix.connector.salesforce.connection.SalesforceConnection;
import com.metamatrix.connector.salesforce.execution.visitors.IQueryProvidingVisitor;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;

/**
 * 
 * Parent class to the Update, Delete, and Insert execution classes.
 * Provisions the correct impl and contains some common code to 
 * get IDs of Salesforce objects.
 *
 */
public class UpdateExecutionParent extends BasicExecution implements UpdateExecution {

	private SalesforceConnection connection;
	private RuntimeMetadata metadata;
	private ExecutionContext context;
	private ConnectorEnvironment connectorEnv;
	private ICommand command;
	private int result;

	public UpdateExecutionParent(ICommand command, SalesforceConnection salesforceConnection,
			RuntimeMetadata metadata, ExecutionContext context,
			ConnectorEnvironment connectorEnv) {
		this.connection = salesforceConnection;
		this.metadata = metadata;
		this.context = context;
		this.connectorEnv = connectorEnv;
		this.command = command;
	}

	@Override
	public void cancel() throws ConnectorException {
	}

	@Override
	public void close() throws ConnectorException {
	}

	@Override
	public void execute() throws ConnectorException {
		if(command instanceof com.metamatrix.connector.language.IDelete) {
			DeleteExecutionImpl ex = new DeleteExecutionImpl();
			result = ex.execute(((IDelete)command), this);
		} else if (command instanceof com.metamatrix.connector.language.IInsert) {
			InsertExecutionImpl ex = new InsertExecutionImpl();
			result = ex.execute(((IInsert)command), this);
		} else if (command instanceof com.metamatrix.connector.language.IUpdate) {
			UpdateExecutionImpl ex = new UpdateExecutionImpl();
			result = ex.execute(((IUpdate)command), this);
		}
	}
	
	@Override
	public int[] getUpdateCounts() throws DataNotAvailableException,
			ConnectorException {
		return new int[] {result};
	}

	public RuntimeMetadata getMetadata() {
		return metadata;
	}

	public ConnectorEnvironment getConnectorEnv() {
		return connectorEnv;
	}
	
	public SalesforceConnection getConnection() {
		return connection;
	}

	String[] getIDs(ICriteria criteria, IQueryProvidingVisitor visitor) throws ConnectorException {
		String[] Ids = null;
		if (visitor.hasOnlyIDCriteria()) {
			try {
				String Id = ((ICompareCriteria)criteria).getRightExpression().toString();
				Id = Util.stripQutes(Id);
				Ids = new String[] { Id };
			} catch (ClassCastException cce) {
				throw new RuntimeException(
						"Error:  The delete criteria is not a CompareCriteria");
			}
	
		} else if (visitor.hasCriteria()) {
			String query = visitor.getQuery();
			QueryResult results = getConnection().query(query, context.getBatchSize());
			if (null != results && results.getSize() > 0) {
				ArrayList<String> idList = new ArrayList<String>(results
						.getRecords().length);
				for (int i = 0; i < results.getRecords().length; i++) {
					SObject sObject = results.getRecords(i);
					idList.add(sObject.getId());
				}
				Ids = idList.toArray(new String[0]);
			}
		}
		return Ids;
	}
}
