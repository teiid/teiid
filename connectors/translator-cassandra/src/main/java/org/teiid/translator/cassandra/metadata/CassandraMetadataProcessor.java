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

package org.teiid.translator.cassandra.metadata;

import java.util.ArrayList;
import java.util.List;

import org.teiid.metadata.Column;
import org.teiid.metadata.Column.SearchType;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.translator.TypeFacility;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;

public class CassandraMetadataProcessor {
	private MetadataFactory metadataFactory;
	private KeyspaceMetadata keyspaceMetadata;
	
	public CassandraMetadataProcessor(MetadataFactory factory, KeyspaceMetadata keyspaceInfo){
		this.metadataFactory = factory;
		this.keyspaceMetadata = keyspaceInfo;
	}
	
	/**
	 * Creates metadata from all column families in current keyspace.
	 */
	public void processMetadata() {
		for (TableMetadata columnFamily : keyspaceMetadata.getTables()){
			addTable(columnFamily);
		}
	}

	/**
	 * Adds table.
	 * @param columnFamily
	 */
	private void addTable(TableMetadata columnFamily) {
		Table table = metadataFactory.addTable(columnFamily.getName());
		addColumnsToTable(table, columnFamily);
		addPrimaryKey(table, columnFamily);
		table.setSupportsUpdate(true);
	}

	/**
	 * Adds a primary key from columnFamily to given table.
	 * @param table			Teiid table
	 * @param columnFamily
	 */
	private void addPrimaryKey(Table table, TableMetadata columnFamily) {
		List<ColumnMetadata> primaryKeys = new ArrayList<ColumnMetadata>();
		primaryKeys = columnFamily.getPrimaryKey();
		List<String> PKNames = new ArrayList<String>();
		
		for (ColumnMetadata columnName : primaryKeys){
			PKNames.add(columnName.getName());
			table.getColumnByName(columnName.getName()).setSearchType(SearchType.Searchable);
		}
		metadataFactory.addPrimaryKey("PK_" + columnFamily.getName(), PKNames, table); //$NON-NLS-1$
	}

	/**
	 * Adds all columns of column family.
	 * @param table			Teiid table
	 * @param columnFamily	Column family
	 */
	private void addColumnsToTable(Table table, TableMetadata columnFamily) {
		for (ColumnMetadata column : columnFamily.getColumns()){
			
			Class<?> cqlTypeToJavaClass = column.getType().asJavaClass();
			Class<?> teiidRuntimeTypeFromJavaClass = TypeFacility.getRuntimeType(cqlTypeToJavaClass);
			String type = TypeFacility.getDataTypeName(teiidRuntimeTypeFromJavaClass);
			
			Column c = metadataFactory.addColumn(column.getName(), type, table);
			c.setUpdatable(true);
			c.setSearchType(SearchType.Unsearchable);
		}
	}
}
