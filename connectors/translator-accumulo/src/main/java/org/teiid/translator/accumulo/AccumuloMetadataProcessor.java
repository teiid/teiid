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

import java.util.Arrays;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.teiid.metadata.Column;
import org.teiid.metadata.Column.SearchType;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.translator.TypeFacility;

public class AccumuloMetadataProcessor {
	public static final String CF = MetadataFactory.ACCUMULO_URI+"CF"; //$NON-NLS-1$
	public static final String CQ = MetadataFactory.ACCUMULO_URI+"CQ"; //$NON-NLS-1$	
	public static final String VALUE_IN = MetadataFactory.ACCUMULO_URI+"VALUE-IN"; //$NON-NLS-1$

	// allowed patterns {CF}, {CQ}, {VALUE}, {ROWID}
	public static final String COLUMN_NAME_PATTERN = "ColumnNamePattern"; //$NON-NLS-1$
	public static final String COLUMN_VALUE_PATTERN = "ValueIn"; //$NON-NLS-1$
	public static final String DEFAULT_COLUMN_NAME_PATTERN = "{CF}:{CQ}"; //$NON-NLS-1$
	public static final String DEFAULT_VALUE_PATTERN = "{VALUE}"; //$NON-NLS-1$
	public static final String ROWID = "rowid"; //$NON-NLS-1$
	public enum ValueIn{CQ,VALUE};
	
	private MetadataFactory mf;
	private AccumuloConnection conn;
	
	public AccumuloMetadataProcessor(MetadataFactory metadataFactory, AccumuloConnection conn) {
		this.mf = metadataFactory;
		this.conn = conn;
	}

	public void processMetadata() {
		Connector connector = this.conn.getInstance();
		
		Set<String> tableNames = connector.tableOperations().list();
		for (String tableName:tableNames) {
			try {
				
				if (tableName.equals("!METADATA")) { //$NON-NLS-1$
					continue;
				}
				
				Text previousRow = null;
				Table table = null;
				Scanner scanner = connector.createScanner(tableName, new Authorizations());
				for (Entry<Key, Value> entry : scanner) {
					Key key = entry.getKey();
					Text cf = key.getColumnFamily();
					Text cq = key.getColumnQualifier();
					Text row = key.getRow();
					if (previousRow != null && !previousRow.equals(row)) {
						break;
					}
					previousRow = row;
					if (table == null) {
						table = mf.addTable(tableName);
						Column column = mf.addColumn(AccumuloMetadataProcessor.ROWID, TypeFacility.RUNTIME_NAMES.VARBINARY, table);
						column.setSearchType(SearchType.All_Except_Like);
						mf.addPrimaryKey("PK0", Arrays.asList(AccumuloMetadataProcessor.ROWID), table); //$NON-NLS-1$
					}
					Column column = mf.addColumn(buildColumnName(cf, cq, row), TypeFacility.RUNTIME_NAMES.VARBINARY, table); 
					column.setSearchType(SearchType.All_Except_Like);
					column.setProperty(CF, cf.toString());
					column.setProperty(CQ, cq.toString());
					column.setProperty(VALUE_IN, mf.getModelProperties().getProperty(COLUMN_VALUE_PATTERN, DEFAULT_VALUE_PATTERN));
				}
				scanner.close();
				table.setSupportsUpdate(true);
				
			} catch (TableNotFoundException e) {
				continue;
			}
		}
	}

	private String buildColumnName(Text cf, Text cq, Text rowid) {
		String pattern = mf.getModelProperties().getProperty(COLUMN_NAME_PATTERN, DEFAULT_COLUMN_NAME_PATTERN);
		pattern = pattern.replace("{CF}", cf.toString()); //$NON-NLS-1$
		pattern = pattern.replace("{CQ}", cq.toString()); //$NON-NLS-1$
		pattern = pattern.replace("{ROWID}", rowid.toString()); //$NON-NLS-1$
		return pattern;
	}
}
