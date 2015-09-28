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

import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.resource.ResourceException;

import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.language.AggregateFunction;
import org.teiid.language.ColumnReference;
import org.teiid.language.Expression;
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
import org.teiid.translator.salesforce.execution.visitors.JoinQueryVisitor;
import org.teiid.translator.salesforce.execution.visitors.SelectVisitor;

import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.bind.XmlObject;

public class QueryExecutionImpl implements ResultSetExecution {

	private static final String TYPE = "type"; //$NON-NLS-1$

	private static final String AGGREGATE_RESULT = "AggregateResult"; //$NON-NLS-1$

	private static final Pattern dateTimePattern = Pattern.compile("^(?:(\\d{4}-\\d{2}-\\d{2})T)?(\\d{2}:\\d{2}:\\d{2}(?:.\\d+)?)(.*)"); //$NON-NLS-1$
	
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
	
	private Calendar cal;
	
	public QueryExecutionImpl(QueryExpression command, SalesforceConnection connection, RuntimeMetadata metadata, ExecutionContext context) {
		this.connection = connection;
		this.metadata = metadata;
		this.context = context;
		this.query = command;

		connectionIdentifier = context.getConnectionId();
		connectorIdentifier = context.getConnectorIdentifier();
		requestIdentifier = context.getRequestId();
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
			if(from.get(0) instanceof Join) {
				visitor = new JoinQueryVisitor(metadata);
			} else {
				visitor = new SelectVisitor(metadata);
			}
			visitor.visitNode(query);
			if(visitor.canRetrieve()) {
				results = connection.retrieve(visitor.getRetrieveFieldList(),
						visitor.getTableName(), visitor.getIdInCriteria());
			} else {
				String finalQuery = visitor.getQuery().trim();
				LogManager.logDetail(LogConstants.CTX_CONNECTOR,  getLogPreamble(), "Executing Query:", finalQuery); //$NON-NLS-1$
				results = connection.query(finalQuery, this.context.getBatchSize(), visitor.getQueryAll());
			}
		} catch (ResourceException e) {
			throw new TranslatorException(e);
		}
	}
	
	@Override
	public List<?> next() throws TranslatorException, DataNotAvailableException {
		List<?> result = getRow(results);
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
			Iterator<XmlObject> topFields = sObject.getChildren();
			ArrayList<XmlObject> children = new ArrayList<XmlObject>();
			while (topFields.hasNext()) {
				children.add(topFields.next());
			}
			logAndMapFields(sObject.getType(), children);
			List<Object[]> result = new ArrayList<Object[]>();
			if(visitor instanceof JoinQueryVisitor) {
				for(int i = 0; i < children.size(); i++) {
					XmlObject element = children.get(i);
					extactJoinResults(element, result);
				}
			}
			return extractDataFromFields(sObject, children, result);

		}

		private void extactJoinResults(XmlObject node, List<Object[]> result) throws TranslatorException {
			Object val = node.getField(TYPE); 
			if(val instanceof String) {
				extractValuesFromElement(node, result, (String)val);
			} else if (node.hasChildren()) {
				Iterator<XmlObject> children = node.getChildren();
				while (children.hasNext()) {
					XmlObject item = children.next();
					extactJoinResults(item, result);
				}
			}
		}
		
		//TODO: this looks inefficient as getChild is linear
		private List<Object[]> extractValuesFromElement(XmlObject sObject,
				List<Object[]> result, String sObjectName) throws TranslatorException {
			Object[] row = new Object[visitor.getSelectSymbolCount()];
			for (int j = 0; j < visitor.getSelectSymbolCount(); j++) {
				//must be a column reference as we won't allow an agg over a join
				Column element = ((ColumnReference)visitor.getSelectSymbolMetadata(j)).getMetadataObject();
				AbstractMetadataRecord table = element.getParent();
				if(table.getSourceName().equals(sObjectName)) {
					XmlObject child = sObject.getChild(element.getSourceName());
					Object cell = getCellDatum(element.getSourceName(), element.getJavaType(), child);
					setElementValueInColumn(j, cell, row);
				}
			}
			result.add(row);
			return result;
		}

	private List<Object[]> extractDataFromFields(SObject sObject,
		List<XmlObject> fields, List<Object[]> result) throws TranslatorException {
		Map<String,Integer> fieldToIndexMap = sObjectToResponseField.get(sObject.getType());
		int aggCount = 0;
		for (int j = 0; j < visitor.getSelectSymbolCount(); j++) {
			Expression ex = visitor.getSelectSymbolMetadata(j);
			if (ex instanceof ColumnReference) {
				Column element = ((ColumnReference)ex).getMetadataObject();
				Table table = (Table)element.getParent();
				if(table.getSourceName().equals(sObject.getType()) || AGGREGATE_RESULT.equalsIgnoreCase(sObject.getType())) {
					Integer index = fieldToIndexMap.get(element.getSourceName());
					if (null == index) {
						throw new TranslatorException(SalesForcePlugin.Util.getString("SalesforceQueryExecutionImpl.missing.field")+ element.getSourceName()); //$NON-NLS-1$
					}
					Object cell = getCellDatum(element.getSourceName(), element.getJavaType(), fields.get(index));
					setValueInColumn(j, cell, result);
				}
			} else if (ex instanceof AggregateFunction) {
				String name = SelectVisitor.AGG_PREFIX + (aggCount++);
				Integer index = fieldToIndexMap.get(name); 
				if (null == index) {
					throw new TranslatorException(SalesForcePlugin.Util.getString("SalesforceQueryExecutionImpl.missing.field")+ ex); //$NON-NLS-1$
				}
				Object cell = getCellDatum(name, ex.getType(), fields.get(index));
				setValueInColumn(j, cell, result);
			}
		}
		return result;
	}
		
	private void setElementValueInColumn(int columnIndex, Object value, Object[] row) {
		if(value instanceof XmlObject) {
			XmlObject element = (XmlObject)value;
			if (element.hasChildren()) {
				row[columnIndex] = element.getChildren().next().getValue();
			} else {
				row[columnIndex] = element.getValue();
			}
		} else {
			row[columnIndex] = value;
		}
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
			List<XmlObject> fields) throws TranslatorException {
		if (!sObjectToResponseField.containsKey(sObjectName)) {
			logFields(sObjectName, fields);
			Map<String, Integer> responseFieldToIndexMap = new HashMap<String, Integer>();
			for (int x = 0; x < fields.size(); x++) {
				XmlObject element = fields.get(x);
				responseFieldToIndexMap.put(element.getName().getLocalPart(), x);
			}
			sObjectToResponseField.put(sObjectName, responseFieldToIndexMap);
		}
	}

	private void logFields(String sObjectName, List<XmlObject> fields) {
		if (!LogManager.isMessageToBeRecorded(LogConstants.CTX_CONNECTOR, MessageLevel.DETAIL)) {
			return;
		}
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, "SalesForce Object Name = " + sObjectName); //$NON-NLS-1$
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, "FieldCount = " + fields.size()); //$NON-NLS-1$
		for(int i = 0; i < fields.size(); i++) {
			XmlObject element = fields.get(i);
			LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Field # " + i + " is " + element.getName().getLocalPart()); //$NON-NLS-1$ //$NON-NLS-2$
		}
		
	}

	/**
	 * TODO: the logic here should be aware of xsi:type information and use a standard conversion
	 * library.  Conversion to teiid types should then be a secondary effort - and will be automatically handled above here.
	 */
	private Object getCellDatum(String name, Class<?> type, XmlObject elem) throws TranslatorException {
		if(!name.equals(elem.getName().getLocalPart())) {
			throw new TranslatorException(SalesForcePlugin.Util.getString("SalesforceQueryExecutionImpl.column.mismatch1") + name + SalesForcePlugin.Util.getString("SalesforceQueryExecutionImpl.column.mismatch2") + elem.getName().getLocalPart()); //$NON-NLS-1$ //$NON-NLS-2$
		}
		Object value = elem.getValue();
		if (value == null) {
			return null;
		}
		if (value instanceof String && (((String) value).isEmpty())) {
			if (type == String.class) {
				return value;
			}
			return null;
		} else if (type.equals(java.sql.Timestamp.class) || type.equals(java.sql.Time.class) && !(value instanceof Date)) {
			if (cal == null) {
				cal = Calendar.getInstance();
			}
			return parseDateTime(value.toString(), type, cal);
		}
		return value;
	}

	static Object parseDateTime(String value, Class<?> type, Calendar cal)
			throws TranslatorException {
		try {
			Matcher m = dateTimePattern.matcher(value);
			if (m.matches()) {
				String date = m.group(1);
				String time = m.group(2);
				String timeZone = m.group(3);
				Date d = null;
				if (date == null) {
					//sql times don't care about fractional seconds
					int milli = time.lastIndexOf('.');
					if (milli > 0) {
						time = time.substring(0, milli);
					}
					d = Time.valueOf(time);
				} else {
					d = Timestamp.valueOf(date + " " + time); //$NON-NLS-1$
				}
				TimeZone tz = null;
				if (timeZone != null) {
					if (timeZone.equals("Z")) { //$NON-NLS-1$
						tz = TimeZone.getTimeZone("GMT"); //$NON-NLS-1$
					} else if (timeZone.contains(":")) { //$NON-NLS-1$
						tz = TimeZone.getTimeZone("GMT" + timeZone); //$NON-NLS-1$
					} else {
						//this is probably an exceptional case
						tz = TimeZone.getTimeZone(timeZone); 
					}
					cal.setTimeZone(tz);
				} else {
					cal = null;
				}
				return TimestampWithTimezone.create(d, TimeZone.getDefault(), cal, type);
			}
			throw new TranslatorException(SalesForcePlugin.Util.getString("SalesforceQueryExecutionImpl.datatime.parse") + value); //$NON-NLS-1$
		} catch (IllegalArgumentException e) {
			throw new TranslatorException(e, SalesForcePlugin.Util.getString("SalesforceQueryExecutionImpl.datatime.parse") + value); //$NON-NLS-1$
		}
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
