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
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import javax.resource.ResourceException;
import javax.xml.bind.JAXBElement;
import javax.xml.namespace.QName;

import org.teiid.language.Argument;
import org.teiid.language.Command;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.salesforce.SalesForcePlugin;
import org.teiid.translator.salesforce.SalesforceConnection;
import org.w3c.dom.Element;

import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;

public class DirectQueryExecution implements ProcedureExecution  {
	
	private static final String DELETE_IDS = "ids"; //$NON-NLS-1$
	private static final String ATTRIBUTES = "attributes"; //$NON-NLS-1$
	private static final String TYPE = "type"; //$NON-NLS-1$
	private static final String ID = "id"; //$NON-NLS-1$
	
	private List<Argument> arguments;
	private Command command;
	private SalesforceConnection connection; 
	private RuntimeMetadata metadata;
	private ExecutionContext context;
	private QueryResult results;
	private List<List<Object>> currentBatch;
	private int updateCount = -1;
	private boolean updateQuery = false; 
	
	public DirectQueryExecution(List<Argument> arguments, Command command, SalesforceConnection connection, RuntimeMetadata metadata, ExecutionContext context) {
		this.arguments = arguments;
		this.command = command;
		this.connection = connection;
		this.metadata = metadata;
		this.context = context;
	}

	@Override
	public void execute() throws TranslatorException {
		String query = (String)this.arguments.get(0).getArgumentValue().getValue();
		if (query.startsWith("search;")) { //$NON-NLS-1$
			doSelect(query.substring(7));
		}
		else if (query.startsWith("create;")) { //$NON-NLS-1$
			doInsert(query.substring(7));
		}
		else if (query.startsWith("update;")) { //$NON-NLS-1$
			doUpdate(query.substring(7));
		}
		else if (query.startsWith("delete;")) { //$NON-NLS-1$
			doDelete(query.substring(7));
		}
		else {
			throw new TranslatorException(SalesForcePlugin.Util.gs(SalesForcePlugin.Event.TEIID13002));
		}
		
	}

	private void doDelete(String query) throws TranslatorException {
		List<String> ids = getIds(query);
		try {
			this.updateCount = this.connection.delete(ids.toArray(new String[ids.size()]));
			this.updateQuery = true;
		} catch (ResourceException e) {
			throw new TranslatorException(e);
		}
	}

	private void doUpdate(String query)  throws TranslatorException {
		DataPayload payload = buildDataPlayload(query, this.arguments);
		try {
			this.updateCount = this.connection.update(Arrays.asList(payload));
			this.updateQuery = true;
		} catch (ResourceException e) {
			throw new TranslatorException(e);
		}
	}

	private void doInsert(String query) throws TranslatorException {
		DataPayload payload = buildDataPlayload(query, this.arguments);
		try {
			this.updateCount = this.connection.create(payload);
			this.updateQuery = true;
		} catch (ResourceException e) {
			throw new TranslatorException(e);
		}
	}

	private void doSelect(String query) throws TranslatorException {
		try {
			this.results = this.connection.query(query, this.context.getBatchSize(), Boolean.FALSE);
		} catch (ResourceException e) {
			throw new TranslatorException(e);
		}
	}

	@Override
	public List<?> next() throws TranslatorException, DataNotAvailableException {
		List<?> vals = getRow(this.results);
		if (vals == null) {
			return null;
		}
		List<Object[]> row = new ArrayList<Object[]>(1);
		row.add(vals.toArray(new Object[vals.size()]));
		return row;		
	}
	
	private List<Object> getRow(QueryResult result) throws TranslatorException {

		// for insert/update/delete clauses
		if (this.updateQuery) {
			if (this.updateCount != -1) {
				List updateResult = Arrays.asList(this.updateCount);
				this.updateCount = -1;
				return updateResult;
			}
			return null;
		}
		
		// select clauses
		List<Object> row = null;
		
		if(this.currentBatch == null) {
			this.currentBatch = loadBatch(this.results);
		}
		
		if(!this.currentBatch.isEmpty()) {
			row = this.currentBatch.remove(0);
		} 
		else {
			if(!result.isDone()) {
				// fetch more results
				try {
					this.results = this.connection.queryMore(results.getQueryLocator(), context.getBatchSize());
				} catch (ResourceException e) {
					throw new TranslatorException(e);
				}
				this.currentBatch = loadBatch(this.results);
				
				// read next row
				row = this.currentBatch.remove(0);
			}
		}
		return row;
	}	
	
	private List<List<Object>> loadBatch(QueryResult queryResult) {
		List<List<Object>> batch = new ArrayList<List<Object>>();
		for(SObject sObject : queryResult.getRecords()) {
			List<Object> fields = sObject.getAny();
			List<Object> row = new ArrayList<Object>();
			if (sObject.getId() != null) {
				row.add(sObject.getId());
			}
			for (Object field:fields) {
				Element elem = (Element)field;
				String value = elem.getTextContent();
				row.add(value);
			}
			batch.add(row);
		}
		return batch;
	}
	
	@Override
	public void close() {
	}

	@Override
	public void cancel() throws TranslatorException {
	}
	
	private ArrayList<String> getIds(String query) throws TranslatorException {
		StringTokenizer st = new StringTokenizer(query, ";"); //$NON-NLS-1$
		if (!st.hasMoreTokens()) {
			throw new TranslatorException(SalesForcePlugin.Util.gs(SalesForcePlugin.Event.TEIID13003));
		}
		
		ArrayList<String> ids = new ArrayList<String>();
		
		while(st.hasMoreElements()) {
			String var = st.nextToken();
			int index = var.indexOf('=');
			if (index == -1) {
				continue;
			}
			String key = var.substring(0, index).trim().toLowerCase();
			String value = var.substring(index+1).trim();
			
			if (key.equalsIgnoreCase(DELETE_IDS)) {
				StringTokenizer attrTokens = new StringTokenizer(value, ","); //$NON-NLS-1$
				while (attrTokens.hasMoreElements()) {
					ids.add(attrTokens.nextToken());
				}
			}
		}
		return ids;
	}	
	
	private DataPayload buildDataPlayload(String query, List<Argument> arguments) throws TranslatorException {
		StringTokenizer st = new StringTokenizer(query, ";"); //$NON-NLS-1$
		if (!st.hasMoreTokens()) {
			throw new TranslatorException(SalesForcePlugin.Util.gs(SalesForcePlugin.Event.TEIID13004));
		}
		
		ArrayList<JAXBElement> attributes = new ArrayList<JAXBElement>();
		String type = null;
		String id = null;
		
		while(st.hasMoreElements()) {
			String var = st.nextToken();
			
			int index = var.indexOf('=');
			if (index == -1) {
				continue;
			}
			String key = var.substring(0, index).trim().toLowerCase();
			String value = var.substring(index+1).trim();
			
			
			if (key.equalsIgnoreCase(ATTRIBUTES)) {
				StringTokenizer attrTokens = new StringTokenizer(value, ","); //$NON-NLS-1$
				int attrCount = 1;
				while(attrTokens.hasMoreElements()) {
					String name = attrTokens.nextToken().trim();
					if (arguments.size() <= attrCount) {
						throw new TranslatorException(SalesForcePlugin.Util.gs(SalesForcePlugin.Event.TEIID13005, name));
					}
					Argument argument = arguments.get(attrCount++);
					Object  anObj = argument.getArgumentValue().getValue();
					QName qname = new QName(name);
				    @SuppressWarnings( "unchecked" )
				    JAXBElement jbe = new JAXBElement( qname, String.class, anObj );
					attributes.add(jbe);
				}
			}
			else if (key.equalsIgnoreCase(TYPE)) {
				type = value;
			}
			else if (key.equalsIgnoreCase(ID)) {
				id = value;
			}
		}
		DataPayload payload = new DataPayload();
		payload.setID(id);
		payload.setType(type);
		payload.setMessageElements(attributes);
		return payload;
	}

	@Override
	public List<?> getOutputParameterValues() throws TranslatorException {
		return null;
	}	
}
