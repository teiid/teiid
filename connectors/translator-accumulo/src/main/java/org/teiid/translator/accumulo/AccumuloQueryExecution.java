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
package org.teiid.translator.accumulo;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.teiid.language.Select;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;

public class AccumuloQueryExecution implements ResultSetExecution {
	enum ValueIn{CQ,VALUE};
	
	private AccumuloConnection connection;	
	private Iterator<Entry<Key,Value>> results;
	private Entry<Key, Value> prevEntry;
	private Class<?>[] expectedColumnTypes;
	private AccumuloExecutionFactory aef;
	private AccumuloQueryVisitor visitor;
	
	public AccumuloQueryExecution(AccumuloExecutionFactory aef, Select command,
			@SuppressWarnings("unused") ExecutionContext executionContext,
			@SuppressWarnings("unused") RuntimeMetadata metadata,
			AccumuloConnection connection) {
		this.aef = aef;
		this.connection = connection;
		this.expectedColumnTypes = command.getColumnTypes();
		this.visitor = new AccumuloQueryVisitor();
		this.visitor.visitNode(command);
	}

	@Override
	public void execute() throws TranslatorException {
		try {
			Connector connector = this.connection.getInstance();
			Authorizations auths = new Authorizations();
			if (this.connection.getAuthorizations() != null) {
				auths = new Authorizations(this.connection.getAuthorizations());
			}
			List<Range> ranges = this.visitor.getRanges();
			Table scanTable = this.visitor.getScanTable();			
			this.results = runQuery(this.aef, connector, auths, ranges, scanTable);
		} catch (TableNotFoundException e) {
			// Teiid will not let the query come this far with out validating metadata for given table
			// so table in user's mind exists, it may be not be in the Accumulo, which should be treated as
			// now rows.
			this.results = null;
		}
	}

	static Iterator<Entry<Key,Value>> runQuery(AccumuloExecutionFactory aef, Connector connector, Authorizations auths, List<Range> ranges, Table scanTable) throws TableNotFoundException {
		if (ranges.size() <= 1) {
			Scanner scanner = connector.createScanner(SQLStringVisitor.getRecordName(scanTable), auths);
			if (!ranges.isEmpty()) {
				scanner.setRange(ranges.get(0));
			}
			return scanner.iterator();
		}
		// use batch scanner
		BatchScanner scanner = connector.createBatchScanner(SQLStringVisitor.getRecordName(scanTable), auths, aef.getQueryThreadsCount());
		scanner.setRanges(ranges);
		return scanner.iterator();
	}

	@Override
	public List<?> next() throws TranslatorException, DataNotAvailableException {
		if (this.prevEntry != null || (this.results != null && this.results.hasNext())) {
			Text previousRow = null;
			LinkedHashMap<String, Object> values = new LinkedHashMap();
			boolean rowIdAdded = false;

			while(true) {
				Entry<Key, Value> entry = this.prevEntry;
				if (entry == null && this.results.hasNext()) {
					entry = this.results.next();
				}
				if (entry == null) {
					break;
				}
				this.prevEntry = null;
				Key key = entry.getKey();
				Text cf = key.getColumnFamily();
				Text cq = key.getColumnQualifier();
				Text row = key.getRow();
				Value value = entry.getValue();
				if (previousRow == null || previousRow.equals(row)) {
					previousRow = row;
					if (!rowIdAdded) {
						values.put(AccumuloMetadataProcessor.ROWID, new Text(row.getBytes()));
						rowIdAdded = true;
					}
					Column column = getMatchingColumn(cf, cq);
					if (column != null) {
						String valueIn = column.getProperty(AccumuloMetadataProcessor.VALUE_IN, false);
						values.put(column.getName(), buildValue(valueIn, cq, value));
					}
				}
				else {
					//done with row; but preserve what has been read..
					this.prevEntry = entry;
					return lastRow(values);
				}
			}
			return lastRow(values);
		}
		return null;
	}
	
	private Column getMatchingColumn(Text rowCF, Text rowCQ) {
		String CF = new String(rowCF.getBytes());
		String CQ = new String(rowCQ.getBytes());
		Column column = this.visitor.lookupColumn(CF+"/"+CQ); //$NON-NLS-1$
		if (column == null) {
			// this means CQ is not defined; In this pattern CQ is used for value
			column = this.visitor.lookupColumn(CF);
		}
		return column;
	}
	
	private List<?> lastRow(Map<String, Object> values) {
		if (!values.isEmpty()) {
			ArrayList list = new ArrayList();
			for(int i = 0; i < this.visitor.projectedColumns().size(); i++) {
				Column column = this.visitor.projectedColumns().get(i);
				Object value = values.get(SQLStringVisitor.getRecordName(column));
				if (value instanceof Value) {
					list.add(this.aef.retrieveValue((Value)value, this.expectedColumnTypes[i]));
				}
				else {
					list.add(this.aef.retrieveValue((Text)value, this.expectedColumnTypes[i]));
				}
			}
			return list;
		}
		return null;
	}

	private Object buildValue(String pattern, Text cq, Value value) {
		if (pattern == null) {
			pattern = AccumuloMetadataProcessor.DEFAULT_VALUE_PATTERN;
		}
		pattern = pattern.substring(1, pattern.length()-1); // remove the curleys
		if (pattern.equals(ValueIn.VALUE.name())) {
			return value;
		}
		else if (pattern.equals(ValueIn.CQ.name())) {
			// CQ is always stored in character based bytes
			return cq;
		}
		return null;
	}

	@Override
	public void close() {
	}

	@Override
	public void cancel() throws TranslatorException {
	}	
}
