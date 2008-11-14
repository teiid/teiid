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
package com.metamatrix.connector.salesforce.execution;

import java.util.ArrayList;

import com.metamatrix.connector.salesforce.Util;
import com.metamatrix.connector.salesforce.connection.SalesforceConnection;
import com.metamatrix.connector.salesforce.execution.visitors.IQueryProvidingVisitor;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.data.api.UpdateExecution;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.ICommand;
import com.metamatrix.data.language.ICompareCriteria;
import com.metamatrix.data.language.ICriteria;
import com.metamatrix.data.language.IDelete;
import com.metamatrix.data.language.IInsert;
import com.metamatrix.data.language.IUpdate;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;

/**
 * 
 * Parent class to the Update, Delete, and Insert execution classes.
 * Provisions the correct impl and contains some common code to 
 * get IDs of Salesforce objects.
 *
 */
public class UpdateExecutionParent implements UpdateExecution {

	private SalesforceConnection connection;
	private RuntimeMetadata metadata;
	private ExecutionContext context;
	private ConnectorEnvironment connectorEnv;
	private String connectionIdentifier;
	private String connectorIdentifier;
	private String requestIdentifier;
	private String partIdentifier;

	public UpdateExecutionParent(SalesforceConnection salesforceConnection,
			RuntimeMetadata metadata, ExecutionContext context,
			ConnectorEnvironment connectorEnv) {
		this.connection = salesforceConnection;
		this.metadata = metadata;
		this.context = context;
		this.connectorEnv = connectorEnv;

		String connectionIdentifier = context.getConnectionIdentifier();
		String connectorIdentifier = context.getConnectorIdentifier();
		String requestIdentifier = context.getRequestIdentifier();
		partIdentifier = context.getPartIdentifier();
	}

	public void cancel() throws ConnectorException {
	}

	public void close() throws ConnectorException {
	}

	public int execute(ICommand command) throws ConnectorException {
		int result = 0;
		if(command instanceof com.metamatrix.data.language.IDelete) {
			DeleteExecutionImpl ex = new DeleteExecutionImpl();
			result = ex.execute(((IDelete)command), this);
		} else if (command instanceof com.metamatrix.data.language.IInsert) {
			InsertExecutionImpl ex = new InsertExecutionImpl();
			result = ex.execute(((IInsert)command), this);
		} else if (command instanceof com.metamatrix.data.language.IUpdate) {
			UpdateExecutionImpl ex = new UpdateExecutionImpl();
			result = ex.execute(((IUpdate)command), this);
		}
		return result;
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
			QueryResult results = getConnection().query(query, 500);
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
