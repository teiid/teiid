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

package org.teiid.translator.cassandra;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.KeyspaceNotDefinedException;
import org.teiid.metadata.Column;
import org.teiid.metadata.Column.SearchType;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.cassandra.CassandraExecutionFactory.Event;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.TableMetadata;

public class CassandraMetadataProcessor implements MetadataProcessor<CassandraConnection>{
	
	/**
	 * Creates metadata from all column families in current keyspace.
	 */
	public void process(MetadataFactory factory, CassandraConnection connection) throws TranslatorException {
        try {
    		for (TableMetadata columnFamily : connection.keyspaceInfo().getTables()){
    			addTable(factory, columnFamily);
    		}
        } catch (KeyspaceNotDefinedException e) {
            throw new TranslatorException(Event.TEIID22000, e, CassandraExecutionFactory.UTIL.gs(Event.TEIID22000));
        }
	}

	/**
	 * Adds table.
	 * @param columnFamily
	 */
	private void addTable(MetadataFactory factory, TableMetadata columnFamily) {
		Table table = factory.addTable(columnFamily.getName());
		addColumnsToTable(factory, table, columnFamily);
		addPrimaryKey(factory, table, columnFamily);
		table.setSupportsUpdate(true);
	}

	/**
	 * Adds a primary key from columnFamily to given table.
	 * @param table			Teiid table
	 * @param columnFamily
	 */
	private void addPrimaryKey(MetadataFactory factory, Table table, TableMetadata columnFamily) {
		List<ColumnMetadata> primaryKeys = new ArrayList<ColumnMetadata>();
		primaryKeys = columnFamily.getPrimaryKey();
		List<String> PKNames = new ArrayList<String>();
		
		for (ColumnMetadata columnName : primaryKeys){
			PKNames.add(columnName.getName());
			table.getColumnByName(columnName.getName()).setSearchType(SearchType.Searchable);
		}
		factory.addPrimaryKey("PK_" + columnFamily.getName(), PKNames, table); //$NON-NLS-1$
	}

	/**
	 * Adds all columns of column family.
	 * @param table			Teiid table
	 * @param columnFamily	Column family
	 */
	private void addColumnsToTable(MetadataFactory factory, Table table, TableMetadata columnFamily) {
		for (ColumnMetadata column : columnFamily.getColumns()){

			Class<?> cqlTypeToJavaClass = column.getType().asJavaClass();
			Class<?> teiidRuntimeTypeFromJavaClass = TypeFacility.getRuntimeType(cqlTypeToJavaClass);
			String type = TypeFacility.getDataTypeName(teiidRuntimeTypeFromJavaClass);
			
			if (column.getType().getName().equals(com.datastax.driver.core.DataType.Name.TIMESTAMP)) {
				type = TypeFacility.getDataTypeName(Timestamp.class);
			}
			
			Column c = factory.addColumn(column.getName(), type, table);
			c.setUpdatable(true);
			if (column.getIndex() != null) {
				c.setSearchType(SearchType.Searchable);
			}
			else {
				c.setSearchType(SearchType.Unsearchable);
			}
		}
	}
}
