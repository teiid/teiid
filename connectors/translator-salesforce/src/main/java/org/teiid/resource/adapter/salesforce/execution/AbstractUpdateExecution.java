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
package org.teiid.resource.adapter.salesforce.execution;

import java.util.ArrayList;

import org.teiid.connector.language.Command;
import org.teiid.connector.language.Comparison;
import org.teiid.connector.language.Condition;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;
import org.teiid.resource.ConnectorException;
import org.teiid.resource.adapter.BasicExecution;
import org.teiid.resource.adapter.salesforce.SalesforceConnection;
import org.teiid.resource.adapter.salesforce.Util;
import org.teiid.resource.adapter.salesforce.execution.visitors.IQueryProvidingVisitor;
import org.teiid.resource.cci.DataNotAvailableException;
import org.teiid.resource.cci.ExecutionContext;
import org.teiid.resource.cci.UpdateExecution;

import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;

/**
 * 
 * Parent class to the Update, Delete, and Insert execution classes.
 * Provisions the correct impl and contains some common code to 
 * get IDs of Salesforce objects.
 *
 */
public abstract class AbstractUpdateExecution extends BasicExecution implements UpdateExecution {

	protected SalesforceConnection connection;
	protected RuntimeMetadata metadata;
	protected ExecutionContext context;
	protected Command command;
	protected int result;

	public AbstractUpdateExecution(Command command,
			SalesforceConnection salesforceConnection,
			RuntimeMetadata metadata, ExecutionContext context) {
		this.connection = salesforceConnection;
		this.metadata = metadata;
		this.context = context;
		this.command = command;
	}

	@Override
	public void cancel() throws ConnectorException {
	}

	@Override
	public void close() throws ConnectorException {
	}
	
	@Override
	public int[] getUpdateCounts() throws DataNotAvailableException,
			ConnectorException {
		return new int[] {result};
	}

	public RuntimeMetadata getMetadata() {
		return metadata;
	}

	public SalesforceConnection getConnection() {
		return connection;
	}

	String[] getIDs(Condition criteria, IQueryProvidingVisitor visitor) throws ConnectorException {
		String[] Ids = null;
		if (visitor.hasOnlyIDCriteria()) {
			try {
				String Id = ((Comparison)criteria).getRightExpression().toString();
				Id = Util.stripQutes(Id);
				Ids = new String[] { Id };
			} catch (ClassCastException cce) {
				throw new RuntimeException(
						"Error:  The delete criteria is not a CompareCriteria");
			}
	
		} else if (visitor.hasCriteria()) {
			String query = visitor.getQuery();
			QueryResult results = getConnection().query(query, context.getBatchSize(), Boolean.FALSE);
			if (null != results && results.getSize() > 0) {
				ArrayList<String> idList = new ArrayList<String>(results
						.getRecords().size());
				for (int i = 0; i < results.getRecords().size(); i++) {
					SObject sObject = results.getRecords().get(i);
					idList.add(sObject.getId());
				}
				Ids = idList.toArray(new String[0]);
			}
		}
		return Ids;
	}
}
