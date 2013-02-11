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
package org.teiid.translator.salesforce.execution;

import java.util.ArrayList;

import javax.resource.ResourceException;

import org.teiid.language.Command;
import org.teiid.language.Comparison;
import org.teiid.language.Condition;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;
import org.teiid.translator.salesforce.SalesForceExecutionFactory;
import org.teiid.translator.salesforce.SalesForcePlugin;
import org.teiid.translator.salesforce.SalesforceConnection;
import org.teiid.translator.salesforce.Util;
import org.teiid.translator.salesforce.execution.visitors.IQueryProvidingVisitor;

import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;

/**
 * 
 * Parent class to the Update, Delete, and Insert execution classes.
 * Provisions the correct impl and contains some common code to 
 * get IDs of Salesforce objects.
 *
 */
public abstract class AbstractUpdateExecution implements UpdateExecution {
	protected SalesForceExecutionFactory executionFactory;
	protected SalesforceConnection connection;
	protected RuntimeMetadata metadata;
	protected ExecutionContext context;
	protected Command command;
	protected int result;

	public AbstractUpdateExecution(SalesForceExecutionFactory ef, Command command,
			SalesforceConnection salesforceConnection,
			RuntimeMetadata metadata, ExecutionContext context) {
		this.executionFactory = ef;
		this.connection = salesforceConnection;
		this.metadata = metadata;
		this.context = context;
		this.command = command;
	}

	@Override
	public void cancel() throws TranslatorException {
	}

	@Override
	public void close() {
	}
	
	@Override
	public int[] getUpdateCounts() throws DataNotAvailableException,
			TranslatorException {
		return new int[] {result};
	}

	public RuntimeMetadata getMetadata() {
		return metadata;
	}

	public SalesforceConnection getConnection() {
		return connection;
	}

	String[] getIDs(Condition criteria, IQueryProvidingVisitor visitor) throws TranslatorException {
		String[] Ids = null;
		if (visitor.hasOnlyIDCriteria()) {
			try {
				String Id = ((Comparison)criteria).getRightExpression().toString();
				Id = Util.stripQutes(Id);
				Ids = new String[] { Id };
			} catch (ClassCastException cce) {
				throw new RuntimeException(SalesForcePlugin.Util.gs(SalesForcePlugin.Event.TEIID13008));
			}
	
		} else if (visitor.hasCriteria()) {
			try {
				String query = visitor.getQuery();
				QueryResult results = getConnection().query(query, context.getBatchSize(), Boolean.FALSE);
				if (results != null && results.getSize() > 0) {
					ArrayList<String> idList = new ArrayList<String>(results.getRecords().size());
					for (int i = 0; i < results.getRecords().size(); i++) {
						SObject sObject = results.getRecords().get(i);
						idList.add(sObject.getId());
					}
					Ids = idList.toArray(new String[0]);
				}
			} catch (ResourceException e) {
				throw new TranslatorException(e);
			}
		}
		return Ids;
	}
}
