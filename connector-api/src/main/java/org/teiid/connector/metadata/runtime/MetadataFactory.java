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

package org.teiid.connector.metadata.runtime;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.teiid.connector.DataPlugin;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.TypeFacility;

import com.metamatrix.core.id.UUIDFactory;

/**
 * Allows connectors to build metadata for use by the engine.
 * 
 * TODO: add support for datatype import
 * TODO: add support for unique constraints
 */
public class MetadataFactory {
	
	private Schema schema;
	private UUIDFactory factory = new UUIDFactory();
	private Map<String, Datatype> dataTypes;
	private Properties importProperties;
	private MetadataStore store = new MetadataStore();
	
	public MetadataFactory(String modelName, Map<String, Datatype> dataTypes, Properties importProperties) {
		this.dataTypes = dataTypes;
		this.importProperties = importProperties;
		schema = new Schema();
		schema.setName(modelName);
		setUUID(schema);	
		store.addSchema(schema);
	}
	
	public MetadataStore getMetadataStore() {
		return store;
	}
	
	public Properties getImportProperties() {
		return importProperties;
	}
	
	private void setUUID(AbstractMetadataRecord record) {
		record.setUUID(factory.create().toString());
	}

	/**
	 * Add a table with the given name to the model.  
	 * @param name
	 * @return
	 * @throws ConnectorException 
	 */
	public Table addTable(String name) throws ConnectorException {
		Table table = new Table();
		table.setTableType(Table.Type.Table);
		table.setName(name);
		table.setColumns(new LinkedList<Column>());
		table.setAccessPatterns(new LinkedList<KeyRecord>());
		table.setIndexes(new LinkedList<KeyRecord>());
		table.setForiegnKeys(new LinkedList<ForeignKey>());
		table.setUniqueKeys(new LinkedList<KeyRecord>());
		setUUID(table);
		this.schema.addTable(table);
		return table;
	}
	
	/**
	 * Adds a column to the table with the given name and type.
	 * @param name
	 * @param type should be one of {@link TypeFacility.RUNTIME_NAMES}
	 * @param table
	 * @return
	 * @throws ConnectorException
	 */
	public Column addColumn(String name, String type, ColumnSet<?> table) throws ConnectorException {
		if (name.indexOf(AbstractMetadataRecord.NAME_DELIM_CHAR) != -1) {
			throw new ConnectorException(DataPlugin.Util.getString("MetadataFactory.invalid_name", name)); //$NON-NLS-1$
		}
		Column column = new Column();
		column.setName(name);
		table.getColumns().add(column);
		column.setPosition(table.getColumns().size()); //1 based indexing
		Datatype datatype = setColumnType(type, column);
		column.setCaseSensitive(datatype.isCaseSensitive());
		column.setAutoIncrementable(datatype.isAutoIncrement());
		column.setSigned(datatype.isSigned());		
		setUUID(column);
		column.setParent(table);
		return column;
	}

	private Datatype setColumnType(String type,
			BaseColumn column) throws ConnectorException {
		Datatype datatype = dataTypes.get(type);
		if (datatype == null) {
			throw new ConnectorException(DataPlugin.Util.getString("MetadataFactory.unknown_datatype", type)); //$NON-NLS-1$
		}
		column.setDatatype(datatype);
		column.setDatatypeUUID(datatype.getUUID());
		column.setLength(datatype.getLength());
		column.setPrecision(datatype.getPrecisionLength());
		column.setRadix(datatype.getRadix());
		column.setRuntimeType(datatype.getRuntimeTypeName());
		return datatype;
	}
	
	/**
	 * Adds a primary key to the given table.  The column names should be in key order.
	 * @param name
	 * @param columnNames
	 * @param table
	 * @return
	 * @throws ConnectorException
	 */
	public KeyRecord addPrimaryKey(String name, List<String> columnNames, Table table) throws ConnectorException {
		KeyRecord primaryKey = new KeyRecord(KeyRecord.Type.Primary);
		primaryKey.setParent(table);
		primaryKey.setColumns(new ArrayList<Column>(columnNames.size()));
		primaryKey.setName(name);
		setUUID(primaryKey);
		assignColumns(columnNames, table, primaryKey);
		table.setPrimaryKey(primaryKey);
		return primaryKey;
	}
	
	/**
	 * Adds an access pattern to the given table.
	 * @param name
	 * @param columnNames
	 * @param table
	 * @return
	 * @throws ConnectorException
	 */
	public KeyRecord addAccessPattern(String name, List<String> columnNames, Table table) throws ConnectorException {
		KeyRecord ap = new KeyRecord(KeyRecord.Type.AccessPattern);
		ap.setParent(table);
		ap.setColumns(new ArrayList<Column>(columnNames.size()));
		ap.setName(name);
		setUUID(ap);
		assignColumns(columnNames, table, ap);
		table.getAccessPatterns().add(ap);
		return ap;
	}	
	
	/**
	 * Adds an index to the given table.
	 * @param name
	 * @param nonUnique true indicates that an index is being added.
	 * @param columnNames
	 * @param table
	 * @return
	 * @throws ConnectorException
	 */
	public KeyRecord addIndex(String name, boolean nonUnique, List<String> columnNames, Table table) throws ConnectorException {
		KeyRecord index = new KeyRecord(nonUnique?KeyRecord.Type.NonUnique:KeyRecord.Type.Index);
		index.setParent(table);
		index.setColumns(new ArrayList<Column>(columnNames.size()));
		index.setName(name);
		setUUID(index);
		assignColumns(columnNames, table, index);
		table.getIndexes().add(index);
		return index;
	}
		
	/**
	 * Adds a foreign key to the given table.  The column names should be in key order.
	 * @param name
	 * @param columnNames
	 * @param pkTable
	 * @param table
	 * @return
	 * @throws ConnectorException
	 */
	public ForeignKey addForiegnKey(String name, List<String> columnNames, Table pkTable, Table table) throws ConnectorException {
		ForeignKey foreignKey = new ForeignKey();
		foreignKey.setParent(table);
		foreignKey.setColumns(new ArrayList<Column>(columnNames.size()));
		foreignKey.setName(name);
		setUUID(foreignKey);
		if (pkTable.getPrimaryKey() == null) {
			throw new ConnectorException("No primary key defined for table " + pkTable); //$NON-NLS-1$
		}
		foreignKey.setPrimaryKey(pkTable.getPrimaryKey());
		foreignKey.setUniqueKeyID(pkTable.getPrimaryKey().getUUID());
		assignColumns(columnNames, table, foreignKey);
		table.getForeignKeys().add(foreignKey);
		return foreignKey;
	}
	
	/**
	 * Add a procedure with the given name to the model.  
	 * @param name
	 * @return
	 * @throws ConnectorException 
	 */
	public ProcedureRecordImpl addProcedure(String name) throws ConnectorException {
		ProcedureRecordImpl procedure = new ProcedureRecordImpl();
		procedure.setName(name);
		setUUID(procedure);
		procedure.setParameters(new LinkedList<ProcedureParameter>());
		this.schema.addProcedure(procedure);
		return procedure;
	}
	
	/**
	 * Add a procedure parameter.
	 * @param name
	 * @param type should be one of {@link TypeFacility.RUNTIME_NAMES}
	 * @param parameterType should be one of {@link ProcedureParameter.Type}
	 * @param procedure
	 * @return
	 * @throws ConnectorException 
	 */
	public ProcedureParameter addProcedureParameter(String name, String type, ProcedureParameter.Type parameterType, ProcedureRecordImpl procedure) throws ConnectorException {
		ProcedureParameter param = new ProcedureParameter();
		param.setName(name);
		setUUID(param);
		param.setType(parameterType);
		param.setProcedure(procedure);
		setColumnType(type, param);
		procedure.getParameters().add(param);
		param.setPosition(procedure.getParameters().size()); //1 based indexing
		return param;
	}
	
	/**
	 * Add a procedure resultset column to the given procedure.
	 * @param name
	 * @param type should be one of {@link TypeFacility.RUNTIME_NAMES}
	 * @param procedure
	 * @return
	 * @throws ConnectorException 
	 */
	public Column addProcedureResultSetColumn(String name, String type, ProcedureRecordImpl procedure) throws ConnectorException {
		if (procedure.getResultSet() == null) {
			ColumnSet<ProcedureRecordImpl> resultSet = new ColumnSet<ProcedureRecordImpl>();
			resultSet.setParent(procedure);
			resultSet.setName("RSParam"); //$NON-NLS-1$
			setUUID(resultSet);
			procedure.setResultSet(resultSet);
		}
		return addColumn(name, type, procedure.getResultSet());
	}

	private void assignColumns(List<String> columnNames, Table table,
			ColumnSet<?> columns) throws ConnectorException {
		for (String columnName : columnNames) {
			boolean match = false;
			for (Column column : table.getColumns()) {
				if (column.getName().equals(columnName)) {
					match = true;
					columns.getColumns().add(column);
					break;
				}
			}
			if (!match) {
				throw new ConnectorException(DataPlugin.Util.getString("MetadataFactory.no_column_found", columnName)); //$NON-NLS-1$
			}
		}
	}
		
}
