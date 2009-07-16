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

package org.teiid.connector.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.TypeFacility;
import org.teiid.connector.metadata.runtime.AbstractMetadataRecord;
import org.teiid.connector.metadata.runtime.BaseColumn;
import org.teiid.connector.metadata.runtime.ColumnRecordImpl;
import org.teiid.connector.metadata.runtime.MetadataFactory;
import org.teiid.connector.metadata.runtime.ProcedureRecordImpl;
import org.teiid.connector.metadata.runtime.TableRecordImpl;
import org.teiid.connector.metadata.runtime.MetadataConstants.PARAMETER_TYPES;

/**
 * Reads from {@link DatabaseMetaData} and creates metadata through the {@link MetadataFactory}.
 */
public class JDBCMetdataProcessor {
	
	/**
	 * A holder for table records that keeps track of catalog and schema information.
	 */
	private static class TableInfo {
		private String catalog;
		private String schema;
		private String name;
		private TableRecordImpl table;
		
		public TableInfo(String catalog, String schema, String name, TableRecordImpl table) {
			this.catalog = catalog;
			this.schema = schema;
			this.name = name;
			this.table = table;
		}
	}
	
	private String catalog;
	private String schemaPattern;
	private String tableNamePattern;
	private String procedureNamePattern;
	private String[] tableTypes;
	
	private boolean useFullSchemaName = true;
	private boolean importKeys = true;
	private boolean importIndexes = true;
	private boolean importApproximateIndexes = true;
	private boolean importProcedures = true;
	
	public void getConnectorMetadata(Connection conn, MetadataFactory metadataFactory)
			throws SQLException, ConnectorException {
		DatabaseMetaData metadata = conn.getMetaData();
		
		Map<String, TableInfo> tableMap = getTables(metadataFactory, metadata);
		
		if (importKeys) {
			getPrimaryKeys(metadataFactory, metadata, tableMap);
			
			getForeignKeys(metadataFactory, metadata, tableMap);
		}
		
		if (importIndexes) {
			getIndexes(metadataFactory, metadata, tableMap);
		}
		
		if (importProcedures) {
			getProcedures(metadataFactory, metadata);
		}
	}

	private void getProcedures(MetadataFactory metadataFactory,
			DatabaseMetaData metadata) throws SQLException, ConnectorException {
		ResultSet procedures = metadata.getProcedures(catalog, schemaPattern, procedureNamePattern);
		while (procedures.next()) {
			String procedureCatalog = procedures.getString(1);
			String procedureSchema = procedures.getString(2);
			String procedureName = procedures.getString(3);
			String fullProcedureName = getTableName(procedureCatalog, procedureSchema, procedureName);
			ProcedureRecordImpl procedure = metadataFactory.addProcedure(useFullSchemaName?fullProcedureName:procedureName);
			procedure.setNameInSource(fullProcedureName);
			ResultSet columns = metadata.getProcedureColumns(catalog, procedureSchema, procedureName, null);
			while (columns.next()) {
				String columnName = columns.getString(4);
				short columnType = columns.getShort(5);
				int sqlType = columns.getInt(6);
				if (columnType == DatabaseMetaData.procedureColumnUnknown) {
					continue; //there's a good chance this won't work
				}
				BaseColumn record = null;
				if (columnType == DatabaseMetaData.procedureColumnResult) {
					ColumnRecordImpl column = metadataFactory.addProcedureResultSetColumn(columnName, TypeFacility.getDataTypeNameFromSQLType(sqlType), procedure);
					record = column;
					column.setNativeType(columns.getString(7));
				} else {
					record = metadataFactory.addProcedureParameter(columnName, TypeFacility.getDataTypeNameFromSQLType(sqlType), getParameterType(columnType), procedure);
				}
				record.setPrecision(columns.getInt(8));
				record.setLength(columns.getInt(9));
				record.setScale(columns.getInt(10));
				record.setRadix(columns.getInt(11));
				record.setNullType(columns.getShort(12));
				String remarks = columns.getString(13);
				if (remarks != null) {
					metadataFactory.addAnnotation(remarks, record);
				}
			}
		}
		procedures.close();
	}
	
	private static short getParameterType(short type) {
		switch (type) {
		case DatabaseMetaData.procedureColumnIn:
			return PARAMETER_TYPES.IN_PARM;
		case DatabaseMetaData.procedureColumnInOut:
			return PARAMETER_TYPES.INOUT_PARM;
		case DatabaseMetaData.procedureColumnOut:
			return PARAMETER_TYPES.OUT_PARM;
		case DatabaseMetaData.procedureColumnReturn:
			return PARAMETER_TYPES.RETURN_VALUE;
		}
		throw new AssertionError();
	}

	private Map<String, TableInfo> getTables(MetadataFactory metadataFactory,
			DatabaseMetaData metadata) throws SQLException, ConnectorException {
		ResultSet tables = metadata.getTables(catalog, schemaPattern, tableNamePattern, tableTypes);
		Map<String, TableInfo> tableMap = new HashMap<String, TableInfo>();
		while (tables.next()) {
			String tableCatalog = tables.getString(1);
			String tableSchema = tables.getString(2);
			String tableName = tables.getString(3);
			String fullName = getTableName(tableCatalog, tableSchema, tableName);
			TableRecordImpl table = metadataFactory.addTable(useFullSchemaName?fullName:tableName);
			table.setNameInSource(fullName);
			table.setSupportsUpdate(true);
			String remarks = tables.getString(5);
			if (remarks != null) {
				metadataFactory.addAnnotation(remarks, table);
			}
			tableMap.put(fullName, new TableInfo(tableCatalog, tableSchema, tableName, table));
		}
		tables.close();
		
		getColumns(metadataFactory, metadata, tableMap);
		return tableMap;
	}

	private void getColumns(MetadataFactory metadataFactory,
			DatabaseMetaData metadata, Map<String, TableInfo> tableMap)
			throws SQLException, ConnectorException {
		ResultSet columns = metadata.getColumns(catalog, schemaPattern, tableNamePattern, null);
		int rsColumns = columns.getMetaData().getColumnCount();
		while (columns.next()) {
			String tableCatalog = columns.getString(1);
			String tableSchema = columns.getString(2);
			String tableName = columns.getString(3);
			String fullTableName = getTableName(tableCatalog, tableSchema, tableName);
			TableInfo tableInfo = tableMap.get(fullTableName);
			if (tableInfo == null) {
				continue;
			}
			String columnName = columns.getString(4);
			int type = columns.getInt(5);
			//note that the resultset is already ordered by position, so we can rely on just adding columns in order
			ColumnRecordImpl column = metadataFactory.addColumn(columnName, TypeFacility.getDataTypeNameFromSQLType(type), tableInfo.table);
			column.setNativeType(columns.getString(6));
			column.setRadix(columns.getInt(10));
			column.setNullType(columns.getInt(11));
			column.setUpdatable(true);
			String remarks = columns.getString(12);
			if (remarks != null) {
				metadataFactory.addAnnotation(remarks, column);
			}
			column.setCharOctetLength(columns.getInt(16));
			if (rsColumns >= 23) {
				column.setAutoIncrementable("YES".equalsIgnoreCase(columns.getString(23))); //$NON-NLS-1$
			}
		}
		columns.close();
	}

	private static void getPrimaryKeys(MetadataFactory metadataFactory,
			DatabaseMetaData metadata, Map<String, TableInfo> tableMap)
			throws SQLException, ConnectorException {
		for (TableInfo tableInfo : tableMap.values()) {
			ResultSet pks = metadata.getPrimaryKeys(tableInfo.catalog, tableInfo.schema, tableInfo.name);
			TreeMap<Short, String> keyColumns = null;
			String pkName = null;
			while (pks.next()) {
				String columnName = pks.getString(4);
				short seqNum = pks.getShort(5);
				if (keyColumns == null) {
					keyColumns = new TreeMap<Short, String>();
				}
				keyColumns.put(seqNum, columnName);
				if (pkName == null) {
					pkName = pks.getString(6);
					if (pkName == null) {
						pkName = "PK_" + tableInfo.table.getName().toUpperCase(); //$NON-NLS-1$
					}
				}
			}
			if (keyColumns != null) {
				metadataFactory.addPrimaryKey(pkName, new ArrayList<String>(keyColumns.values()), tableInfo.table);
			}
			pks.close();
		}
	}
	
	private void getForeignKeys(MetadataFactory metadataFactory,
			DatabaseMetaData metadata, Map<String, TableInfo> tableMap) throws SQLException, ConnectorException {
		for (TableInfo tableInfo : tableMap.values()) {
			ResultSet fks = metadata.getImportedKeys(tableInfo.catalog, tableInfo.schema, tableInfo.name);
			TreeMap<Short, String> keyColumns = null;
			String fkName = null;
			TableInfo pkTable = null;
			short savedSeqNum = Short.MAX_VALUE;
			while (fks.next()) {
				String columnName = fks.getString(8);
				short seqNum = fks.getShort(9);
				if (seqNum <= savedSeqNum) {
					if (keyColumns != null) {
						metadataFactory.addForiegnKey(fkName, new ArrayList<String>(keyColumns.values()), pkTable.table, tableInfo.table);
					}
					keyColumns = new TreeMap<Short, String>();
					fkName = null;
				}
				savedSeqNum = seqNum;
				keyColumns.put(seqNum, columnName);
				if (fkName == null) {
					String tableCatalog = fks.getString(1);
					String tableSchema = fks.getString(2);
					String tableName = fks.getString(3);
					String fullTableName = getTableName(tableCatalog, tableSchema, tableName);
					pkTable = tableMap.get(fullTableName);
					if (pkTable == null) {
						throw new ConnectorException(JDBCPlugin.Util.getString("JDBCMetadataProcessor.cannot_find_primary", fullTableName)); //$NON-NLS-1$
					}
					fkName = fks.getString(12);
					if (fkName == null) {
						fkName = "FK_" + tableInfo.table.getName().toUpperCase(); //$NON-NLS-1$
					}
				} 
			}
			if (keyColumns != null) {
				metadataFactory.addForiegnKey(fkName, new ArrayList<String>(keyColumns.values()), pkTable.table, tableInfo.table);
			}
			fks.close();
		}
	}

	private void getIndexes(MetadataFactory metadataFactory,
			DatabaseMetaData metadata, Map<String, TableInfo> tableMap) throws SQLException, ConnectorException {
		for (TableInfo tableInfo : tableMap.values()) {
			ResultSet indexInfo = metadata.getIndexInfo(tableInfo.catalog, tableInfo.schema, tableInfo.name, false, importApproximateIndexes);
			TreeMap<Short, String> indexColumns = null;
			String indexName = null;
			short savedOrdinalPosition = Short.MAX_VALUE;
			boolean nonUnique = false;
			while (indexInfo.next()) {
				short type = indexInfo.getShort(7);
				if (type == DatabaseMetaData.tableIndexStatistic) {
					tableInfo.table.setCardinality(indexInfo.getInt(11));
					continue;
				}
				short ordinalPosition = indexInfo.getShort(8);
				if (ordinalPosition <= savedOrdinalPosition) {
					if (indexColumns != null) {
						metadataFactory.addIndex(indexName, nonUnique, new ArrayList<String>(indexColumns.values()), tableInfo.table);
					}
					indexColumns = new TreeMap<Short, String>();
					indexName = null;
				}
				savedOrdinalPosition = ordinalPosition;
				String columnName = indexInfo.getString(9);
				nonUnique = indexInfo.getBoolean(4);
				indexColumns.put(ordinalPosition, columnName);
				if (indexName == null) {
					indexName = indexInfo.getString(6);
					if (indexName == null) {
						indexName = "NDX_" + tableInfo.table.getName().toUpperCase(); //$NON-NLS-1$
					}
				}
			}
			if (indexColumns != null) {
				metadataFactory.addIndex(indexName, nonUnique, new ArrayList<String>(indexColumns.values()), tableInfo.table);
			}
			indexInfo.close();
		}
	}

	private static String getTableName(String tableCatalog, String tableSchema,
			String tableName) {
		String fullName = tableName;
		if (tableSchema != null && tableSchema.length() > 0) {
			fullName = tableSchema + AbstractMetadataRecord.NAME_DELIM_CHAR + fullName;
		}
		if (tableCatalog != null && tableCatalog.length() > 0) {
			fullName = tableCatalog + AbstractMetadataRecord.NAME_DELIM_CHAR + fullName;
		}
		return fullName;
	}
		
	public void setCatalog(String catalog) {
		this.catalog = catalog;
	}

	public void setSchemaPattern(String schemaPattern) {
		this.schemaPattern = schemaPattern;
	}

	public void setTableNamePattern(String tableNamePattern) {
		this.tableNamePattern = tableNamePattern;
	}

	public void setTableTypes(String[] tableTypes) {
		this.tableTypes = tableTypes;
	}

	public void setUseFullSchemaName(boolean useFullSchemaName) {
		this.useFullSchemaName = useFullSchemaName;
	}

	public void setProcedureNamePattern(String procedureNamePattern) {
		this.procedureNamePattern = procedureNamePattern;
	}
	
	public void setImportIndexes(boolean importIndexes) {
		this.importIndexes = importIndexes;
	}
	
	public void setImportKeys(boolean importKeys) {
		this.importKeys = importKeys;
	}
	
	public void setImportProcedures(boolean importProcedures) {
		this.importProcedures = importProcedures;
	}

}
