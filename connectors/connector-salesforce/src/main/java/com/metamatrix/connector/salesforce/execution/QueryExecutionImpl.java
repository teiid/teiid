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

import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBElement;
import javax.xml.namespace.QName;

import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;
import org.teiid.connector.api.DataNotAvailableException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.ResultSetExecution;
import org.teiid.connector.basic.BasicExecution;
import org.teiid.connector.language.AggregateFunction;
import org.teiid.connector.language.Join;
import org.teiid.connector.language.QueryExpression;
import org.teiid.connector.language.Select;
import org.teiid.connector.language.TableReference;
import org.teiid.connector.metadata.runtime.Column;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

import com.metamatrix.connector.salesforce.Messages;
import com.metamatrix.connector.salesforce.Util;
import com.metamatrix.connector.salesforce.connection.SalesforceConnection;
import com.metamatrix.connector.salesforce.execution.visitors.JoinQueryVisitor;
import com.metamatrix.connector.salesforce.execution.visitors.SelectVisitor;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;

public class QueryExecutionImpl extends BasicExecution implements ResultSetExecution {

	private SalesforceConnection connection;

	private RuntimeMetadata metadata;

	private ExecutionContext context;

	private ConnectorEnvironment connectorEnv;
	
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

	public QueryExecutionImpl(QueryExpression command, SalesforceConnection connection,
			RuntimeMetadata metadata, ExecutionContext context,
			ConnectorEnvironment connectorEnv) {
		this.connection = connection;
		this.metadata = metadata;
		this.context = context;
		this.connectorEnv = connectorEnv;
		this.query = command;

		connectionIdentifier = context.getConnectionIdentifier();
		connectorIdentifier = context.getConnectorIdentifier();
		requestIdentifier = context.getRequestIdentifier();
		partIdentifier = context.getPartIdentifier();
	}

	public void cancel() throws ConnectorException {
		connectorEnv.getLogger().logInfo(Messages.getString("SalesforceQueryExecutionImpl.cancel"));
	}

	public void close() throws ConnectorException {
		connectorEnv.getLogger().logInfo(Messages.getString("SalesforceQueryExecutionImpl.close"));
	}

	@Override
	public void execute() throws ConnectorException {
		connectorEnv.getLogger().logInfo(
				getLogPreamble() + "Incoming Query: " + query.toString());
		List<TableReference> from = ((Select)query).getFrom();
		String finalQuery;
		if(from.get(0) instanceof Join) {
			visitor = new JoinQueryVisitor(metadata);
			visitor.visitNode(query);
			finalQuery = visitor.getQuery().trim();
			connectorEnv.getLogger().logInfo(
					getLogPreamble() + "Executing Query: " + finalQuery);
			
			results = connection.query(finalQuery, this.context.getBatchSize(), visitor.getQueryAll());
		} else {
			visitor = new SelectVisitor(metadata);
			visitor.visitNode(query);
			if(visitor.canRetrieve()) {
				results = connection.retrieve(visitor.getRetrieveFieldList(),
						visitor.getTableName(), visitor.getIdInCriteria());
			} else {
				finalQuery = visitor.getQuery().trim();
				connectorEnv.getLogger().logInfo(
						getLogPreamble() + "Executing Query: " + finalQuery);
				results = connection.query(finalQuery, this.context.getBatchSize(), visitor.getQueryAll());
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public List next() throws ConnectorException, DataNotAvailableException {
		List<?> result;
		if (query.getProjectedQuery().getDerivedColumns().get(0)
				.getExpression() instanceof AggregateFunction) {
			result = Arrays.asList(results.getSize());
			results = null;
			
		} else {
			result = getRow(results);
		}
		return result;
	}

	private List<Object> getRow(QueryResult result) throws ConnectorException {
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

		private void loadBatch() throws ConnectorException {
			if(null != resultBatch) { // if we have an old batch, then we have to get new results
				results = connection.queryMore(results.getQueryLocator(), context.getBatchSize());
			}
			resultBatch = new ArrayList<List<Object>>();
				
			for(int resultIndex = 0; resultIndex < results.getSize(); resultIndex++) {
				SObject sObject = results.getRecords().get(resultIndex);
				List<Object[]> result = getObjectData(sObject);
				for(Iterator<Object[]> i = result.iterator(); i.hasNext(); ) {
					resultBatch.add(Arrays.asList(i.next()));
				}
			}
		}

		private List<Object[]> getObjectData(SObject sObject) throws ConnectorException {
			List<Object> topFields = sObject.getAny();
			logAndMapFields(sObject.getType(), topFields);
			List<Object[]> result = new ArrayList<Object[]>();
			for(int i = 0; i < topFields.size(); i++) {
				JAXBElement element = (JAXBElement) topFields.get(i);
				QName qName = element.getName();
				if(null != qName) {
					String type = qName.getLocalPart();
					if(type.equals("sObject")) {
						SObject parent = (SObject)element.getValue();
						result.addAll(getObjectData(parent));
					} else if(type.equals("QueryResult")) {
						QueryResult subResult = (QueryResult)element.getValue();
						for(int resultIndex = 0; resultIndex < subResult.getSize(); resultIndex++) {
							SObject subObject = subResult.getRecords().get(resultIndex);
							result.addAll(getObjectData(subObject));
						}
					}
				}
			}
			return extractDataFromFields(sObject, topFields,result);
			
		}

		private List<Object[]> extractDataFromFields(SObject sObject,
			List<Object> fields, List<Object[]> result) throws ConnectorException {
			Map<String,Integer> fieldToIndexMap = sObjectToResponseField.get(sObject.getType());
			for (int j = 0; j < visitor.getSelectSymbolCount(); j++) {
				Column element = visitor.getSelectSymbolMetadata(j);
				if(element.getParent().getNameInSource().equals(sObject.getType())) {
					Integer index = fieldToIndexMap.get(element.getNameInSource());
					// id gets dropped from the result if it is not the
					// first field in the querystring. Add it back in.
					if (null == index) {
						if (element.getNameInSource().equalsIgnoreCase("id")) {
							setValueInColumn(j, sObject.getId(), result);
						} else {
							throw new ConnectorException("SalesforceQueryExecutionImpl.missing.field"
										+ element.getNameInSource());
						}
					} else {
						Object cell;
						cell = getCellDatum(element, (JAXBElement)fields.get(index));
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
	 */
	private void logAndMapFields(String sObjectName,
			List<Object> fields) {
		if (!sObjectToResponseField.containsKey(sObjectName)) {
			logFields(sObjectName, fields);
			Map<String, Integer> responseFieldToIndexMap;
			responseFieldToIndexMap = new HashMap<String, Integer>();
			for (int x = 0; x < fields.size(); x++) {
				JAXBElement element = (JAXBElement) fields.get(x);
				responseFieldToIndexMap.put(element.getName().getLocalPart(), x);
			}
			sObjectToResponseField.put(sObjectName, responseFieldToIndexMap);
		}
	}

	@SuppressWarnings("unchecked")
	private void logFields(String sObjectName, List<Object> fields) {
		ConnectorLogger logger = connectorEnv.getLogger();
		logger.logDetail("SalesForce Object Name = " + sObjectName);
		logger.logDetail("FieldCount = " + fields.size());
		for(int i = 0; i < fields.size(); i++) {
			JAXBElement element = (JAXBElement) fields.get(i);
			logger.logDetail("Field # " + i + " is " + element.getName().getLocalPart());
		}
		
	}

	@SuppressWarnings("unchecked")
	private Object getCellDatum(Column element, JAXBElement jbe)
			throws ConnectorException {
		if(!element.getNameInSource().equals(jbe.getName().getLocalPart())) {
			throw new ConnectorException("SalesforceQueryExecutionImpl.column.mismatch1" + element.getNameInSource() +
					"SalesforceQueryExecutionImpl.column.mismatch2" + jbe.getName().getLocalPart());
		}
		String value = (String) jbe.getValue();
		Object result = null;
		Class type = element.getJavaType();
		
		if(type.equals(String.class)) {
			result = value;
		}
		else if (type.equals(Boolean.class)) {
			result = Boolean.valueOf(value);
		} else if (type.equals(Double.class)) {
			if (null != value) {
				result = Double.valueOf(value);
			}
		} else if (type.equals(Integer.class)) {
			if (null != value) {
				result = Integer.valueOf(value);
			}
		} else if (type.equals(java.sql.Date.class)) {
			if (null != value) {
				result = java.sql.Date.valueOf(value);
			}
		} else if (type.equals(java.sql.Timestamp.class)) {
			if (null != value) {
				try {
					Date date = Util.getSalesforceDateTimeFormat().parse(value);
					result = new Timestamp(date.getTime());
				} catch (ParseException e) {
					throw new ConnectorException(e, "SalesforceQueryExecutionImpl.datatime.parse" + value);
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
			preamble.append(": ");
			logPreamble = preamble.toString();
		}
		return logPreamble;
	}
}
