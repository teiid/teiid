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

import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.resource.ResourceException;
import javax.xml.namespace.QName;

import org.teiid.language.AggregateFunction;
import org.teiid.language.Join;
import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.language.TableReference;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.salesforce.SalesForcePlugin;
import org.teiid.translator.salesforce.SalesforceConnection;
import org.teiid.translator.salesforce.Util;
import org.teiid.translator.salesforce.execution.visitors.JoinQueryVisitor;
import org.teiid.translator.salesforce.execution.visitors.SelectVisitor;
import org.w3c.dom.Element;

import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;

public class QueryExecutionImpl implements ResultSetExecution {

	private SalesforceConnection connection;

	private RuntimeMetadata metadata;

	private ExecutionContext context;

	
	private SelectVisitor visitor;
	
	private QueryResult results;
	
	private List<List<Object>> resultBatch;

	// Identifying values
	private String connectionIdentifier;

	private String connectorIdentifier;

	private String requestIdentifier;

	private String partIdentifier;

	private String logPreamble;
	
	private QueryExpression query;
	
	Map<String, Map<String,Integer>> sObjectToResponseField = new HashMap<String, Map<String,Integer>>();
	
	private int topResultIndex = 0;
	
	public QueryExecutionImpl(QueryExpression command, SalesforceConnection connection, RuntimeMetadata metadata, ExecutionContext context) {
		this.connection = connection;
		this.metadata = metadata;
		this.context = context;
		this.query = command;

		connectionIdentifier = context.getConnectionIdentifier();
		connectorIdentifier = context.getConnectorIdentifier();
		requestIdentifier = context.getRequestIdentifier();
		partIdentifier = context.getPartIdentifier();
	}

	public void cancel() throws TranslatorException {
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, SalesForcePlugin.Util.getString("SalesforceQueryExecutionImpl.cancel"));//$NON-NLS-1$
	}

	public void close() {
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, SalesForcePlugin.Util.getString("SalesforceQueryExecutionImpl.close")); //$NON-NLS-1$
	}

	@Override
	public void execute() throws TranslatorException {
		try {
			LogManager.logDetail(LogConstants.CTX_CONNECTOR, getLogPreamble(), "Incoming Query:", query); //$NON-NLS-1$
			List<TableReference> from = ((Select)query).getFrom();
			String finalQuery;
			if(from.get(0) instanceof Join) {
				visitor = new JoinQueryVisitor(metadata);
				visitor.visitNode(query);
				finalQuery = visitor.getQuery().trim();
				LogManager.logDetail(LogConstants.CTX_CONNECTOR, getLogPreamble(), "Executing Query:", finalQuery); //$NON-NLS-1$
				
				results = connection.query(finalQuery, this.context.getBatchSize(), visitor.getQueryAll());
			} else {
				visitor = new SelectVisitor(metadata);
				visitor.visitNode(query);
				if(visitor.canRetrieve()) {
					results = connection.retrieve(visitor.getRetrieveFieldList(),
							visitor.getTableName(), visitor.getIdInCriteria());
				} else {
					finalQuery = visitor.getQuery().trim();
					LogManager.logDetail(LogConstants.CTX_CONNECTOR,  getLogPreamble(), "Executing Query:", finalQuery); //$NON-NLS-1$
					results = connection.query(finalQuery, this.context.getBatchSize(), visitor.getQueryAll());
				}
			}
		} catch (ResourceException e) {
			throw new TranslatorException(e);
		}
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public List next() throws TranslatorException, DataNotAvailableException {
		List<?> result;
		if (query.getProjectedQuery().getDerivedColumns().get(0)
				.getExpression() instanceof AggregateFunction) {
			if (results == null) {
				return null;
			}
			result = Arrays.asList(results.getSize());
			results = null;
			
		} else {
			result = getRow(results);
		}
		return result;
	}

	private List<Object> getRow(QueryResult result) throws TranslatorException {
		List<Object> row;
		if(null == resultBatch) {
			loadBatch();
		}
		if(resultBatch.size() == topResultIndex) {
			row = null;
		} else {
			row = resultBatch.get(topResultIndex);
			topResultIndex++;
			if(resultBatch.size() == topResultIndex) {
				if(!result.isDone()) {
					loadBatch();
				}
			}
			
		}
		return row;
	}

		private void loadBatch() throws TranslatorException {
			try {
				if(null != resultBatch) { // if we have an old batch, then we have to get new results
					results = connection.queryMore(results.getQueryLocator(), context.getBatchSize());
				}
				resultBatch = new ArrayList<List<Object>>();
				topResultIndex = 0;
				for(SObject sObject : results.getRecords()) {
					List<Object[]> result = getObjectData(sObject);
					for(Iterator<Object[]> i = result.iterator(); i.hasNext(); ) {
						resultBatch.add(Arrays.asList(i.next()));
					}
				}
			} catch (ResourceException e) {
				throw new TranslatorException(e);
			}
		}

		private List<Object[]> getObjectData(SObject sObject) throws TranslatorException {
			List<Object> topFields = sObject.getAny();
			logAndMapFields(sObject.getType(), topFields);
			List<Object[]> result = new ArrayList<Object[]>();
			for(int i = 0; i < topFields.size(); i++) {
				Element element = (Element) topFields.get(i);
				QName qName = new QName(element.getNamespaceURI(), element.getLocalName());
				if(null != qName) {
					String type = qName.getLocalPart();
					if(type.equals("sObject")) { //$NON-NLS-1$
						//SObject parent = (SObject)element.;
						//result.addAll(getObjectData(parent));
					} else if(type.equals("QueryResult")) { //$NON-NLS-1$
						//QueryResult subResult = (QueryResult)element.getValue();
						//for(int resultIndex = 0; resultIndex < subResult.getSize(); resultIndex++) {
						//	SObject subObject = subResult.getRecords().get(resultIndex);
						//	result.addAll(getObjectData(subObject));
						//}
					}
				}
			}
			return extractDataFromFields(sObject, topFields, result);
			
		}

		private List<Object[]> extractDataFromFields(SObject sObject,
			List<Object> fields, List<Object[]> result) throws TranslatorException {
			Map<String,Integer> fieldToIndexMap = sObjectToResponseField.get(sObject.getType());
			for (int j = 0; j < visitor.getSelectSymbolCount(); j++) {
				Column element = visitor.getSelectSymbolMetadata(j);
				AbstractMetadataRecord parent = element.getParent();
				Table table;
				if(parent instanceof Table) {
					table = (Table)parent;
				} else {
					parent = parent.getParent();
					if(parent instanceof Table) {
						table = (Table)parent;
					} else {
						throw new TranslatorException("Could not resolve Table for column " + element.getName()); //$NON-NLS-1$
					}
				}
				if(table.getNameInSource().equals(sObject.getType())) {
					Integer index = fieldToIndexMap.get(element.getNameInSource());
					// id gets dropped from the result if it is not the
					// first field in the querystring. Add it back in.
					if (null == index) {
						if (element.getNameInSource().equalsIgnoreCase("id")) { //$NON-NLS-1$
							setValueInColumn(j, sObject.getId(), result);
						} else {
							throw new TranslatorException(SalesForcePlugin.Util.getString("SalesforceQueryExecutionImpl.missing.field")+ element.getNameInSource()); //$NON-NLS-1$
						}
					} else {
						Object cell;
						cell = getCellDatum(element, (Element)fields.get(index));
						setValueInColumn(j, cell, result);
					}
				}
			}
			return result;
	}
		
	private void setValueInColumn(int columnIndex, Object value, List<Object[]> result) {
		if(result.isEmpty()) {
			Object[] row = new Object[visitor.getSelectSymbolCount()];
			result.add(row);
		}
		Iterator<Object[]> iter = result.iterator();
		while (iter.hasNext()) {
			Object[] row = iter.next();
			row[columnIndex] = value;
		}	
	}

	/**
	 * Load the map of response field names to index.
	 * @param fields
	 * @throws TranslatorException 
	 */
	private void logAndMapFields(String sObjectName,
			List<Object> fields) throws TranslatorException {
		if (!sObjectToResponseField.containsKey(sObjectName)) {
			logFields(sObjectName, fields);
			Map<String, Integer> responseFieldToIndexMap;
			responseFieldToIndexMap = new HashMap<String, Integer>();
			for (int x = 0; x < fields.size(); x++) {
				Element element = (Element) fields.get(x);
				responseFieldToIndexMap.put(element.getLocalName(), x);
			}
			sObjectToResponseField.put(sObjectName, responseFieldToIndexMap);
		}
	}

	private void logFields(String sObjectName, List<Object> fields) throws TranslatorException {
		if (!LogManager.isMessageToBeRecorded(LogConstants.CTX_CONNECTOR, MessageLevel.DETAIL)) {
			return;
		}
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, "SalesForce Object Name = " + sObjectName); //$NON-NLS-1$
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, "FieldCount = " + fields.size()); //$NON-NLS-1$
		for(int i = 0; i < fields.size(); i++) {
			Element element;
			element = (Element) fields.get(i);
			LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Field # " + i + " is " + element.getLocalName()); //$NON-NLS-1$ //$NON-NLS-2$
		}
		
	}

	@SuppressWarnings("unchecked")
	private Object getCellDatum(Column element, Element elem) throws TranslatorException {
		if(!element.getNameInSource().equals(elem.getLocalName())) {
			throw new TranslatorException(SalesForcePlugin.Util.getString("SalesforceQueryExecutionImpl.column.mismatch1") + element.getNameInSource() + SalesForcePlugin.Util.getString("SalesforceQueryExecutionImpl.column.mismatch2") + elem.getLocalName()); //$NON-NLS-1$ //$NON-NLS-2$
		}
		String value = elem.getTextContent();
		Object result = null;
		Class type = element.getJavaType();
		
		if(type.equals(String.class)) {
			result = value;
		}
		else if (type.equals(Boolean.class)) {
			result = Boolean.valueOf(value);
		} else if (type.equals(Double.class)) {
			if (null != value) {
				if(!value.isEmpty()) {
					result = Double.valueOf(value);
				}
			}
		} else if (type.equals(Integer.class)) {
			if (null != value) {
				if(!value.isEmpty()) {
					result = Integer.valueOf(value);
				}
			}
		} else if (type.equals(java.sql.Date.class)) {
			if (null != value) {
				if(!value.isEmpty()) {
					result = java.sql.Date.valueOf(value);
				}
			}
		} else if (type.equals(java.sql.Timestamp.class)) {
			if (null != value) {
				if(!value.isEmpty()) { 
					try {
						Date date = Util.getSalesforceDateTimeFormat().parse(value);
						result = new Timestamp(date.getTime());
					} catch (ParseException e) {
						throw new TranslatorException(e, SalesForcePlugin.Util.getString("SalesforceQueryExecutionImpl.datatime.parse") + value); //$NON-NLS-1$
					}
				}
			}
		} else {
			result = value;
		}
		return result;
	}


	private String getLogPreamble() {
		if (null == logPreamble) {
			StringBuffer preamble = new StringBuffer();
			preamble.append(connectorIdentifier);
			preamble.append('.');
			preamble.append(connectionIdentifier);
			preamble.append('.');
			preamble.append(requestIdentifier);
			preamble.append('.');
			preamble.append(partIdentifier);
			preamble.append(": "); //$NON-NLS-1$
			logPreamble = preamble.toString();
		}
		return logPreamble;
	}
}
