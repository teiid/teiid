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
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;
import org.teiid.connector.api.TypeFacility;
import org.teiid.connector.metadata.runtime.AbstractMetadataRecord;
import org.teiid.connector.metadata.runtime.BaseColumn;
import org.teiid.connector.metadata.runtime.Column;
import org.teiid.connector.metadata.runtime.MetadataFactory;
import org.teiid.connector.metadata.runtime.ProcedureRecordImpl;
import org.teiid.connector.metadata.runtime.Table;
import org.teiid.connector.metadata.runtime.BaseColumn.NullType;
import org.teiid.connector.metadata.runtime.ProcedureParameter.Type;

import com.metamatrix.core.util.StringUtil;

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
		private Table table;
		
		public TableInfo(String catalog, String schema, String name, Table table) {
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
	private boolean widenUnsingedTypes = true;
	private boolean quoteNameInSource = true;
	//TODO add an option to not fully qualify name in source
	
	private ConnectorLogger logger;
	private Set<String> unsignedTypes = new HashSet<String>();
	private String quoteString;
	
	public JDBCMetdataProcessor(ConnectorLogger logger) {
		this.logger = logger;
	}

	public void getConnectorMetadata(Connection conn, MetadataFactory metadataFactory)
			throws SQLException, ConnectorException {
		DatabaseMetaData metadata = conn.getMetaData();
		
		quoteString = metadata.getIdentifierQuoteString();
		if (quoteString != null && quoteString.trim().length() == 0) {
			quoteString = null;
		}
		
		if (widenUnsingedTypes) {
			ResultSet rs = metadata.getTypeInfo();
			while (rs.next()) {
				String name = rs.getString(1);
				boolean unsigned = rs.getBoolean(10);
				if (unsigned) {
					unsignedTypes.add(name);
				}
			}
		}
		
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
		logger.logDetail("JDBCMetadataProcessor - Importing procedures"); //$NON-NLS-1$
		ResultSet procedures = metadata.getProcedures(catalog, schemaPattern, procedureNamePattern);
		while (procedures.next()) {
			String procedureCatalog = procedures.getString(1);
			String procedureSchema = procedures.getString(2);
			String procedureName = procedures.getString(3);
			String fullProcedureName = getFullyQualifiedName(procedureCatalog, procedureSchema, procedureName);
			ProcedureRecordImpl procedure = metadataFactory.addProcedure(useFullSchemaName?fullProcedureName:procedureName);
			procedure.setNameInSource(getFullyQualifiedName(procedureCatalog, procedureSchema, procedureName, true));
			ResultSet columns = metadata.getProcedureColumns(catalog, procedureSchema, procedureName, null);
			while (columns.next()) {
				String columnName = columns.getString(4);
				short columnType = columns.getShort(5);
				int sqlType = columns.getInt(6);
				String typeName = columns.getString(7);
				sqlType = checkForUnsigned(sqlType, typeName);
				if (columnType == DatabaseMetaData.procedureColumnUnknown) {
					continue; //there's a good chance this won't work
				}
				BaseColumn record = null;
				if (columnType == DatabaseMetaData.procedureColumnResult) {
					Column column = metadataFactory.addProcedureResultSetColumn(columnName, TypeFacility.getDataTypeNameFromSQLType(sqlType), procedure);
					record = column;
					column.setNativeType(typeName);
				} else {
					record = metadataFactory.addProcedureParameter(columnName, TypeFacility.getDataTypeNameFromSQLType(sqlType), Type.values()[columnType], procedure);
				}
				record.setPrecision(columns.getInt(8));
				record.setLength(columns.getInt(9));
				record.setScale(columns.getInt(10));
				record.setRadix(columns.getInt(11));
				record.setNullType(NullType.values()[columns.getShort(12)]);
				record.setAnnotation(columns.getString(13));
			}
		}
		procedures.close();
	}

	private int checkForUnsigned(int sqlType, String typeName) {
		if (widenUnsingedTypes && unsignedTypes.contains(typeName)) {
			switch (sqlType) {
			case Types.TINYINT:
				sqlType = Types.SMALLINT;
				break;
			case Types.SMALLINT:
				sqlType = Types.INTEGER;
				break;
			case Types.INTEGER:
				sqlType = Types.BIGINT;
				break;
			}
		}
		return sqlType;
	}
	
	private Map<String, TableInfo> getTables(MetadataFactory metadataFactory,
			DatabaseMetaData metadata) throws SQLException, ConnectorException {
		logger.logDetail("JDBCMetadataProcessor - Importing tables"); //$NON-NLS-1$
		ResultSet tables = metadata.getTables(catalog, schemaPattern, tableNamePattern, tableTypes);
		Map<String, TableInfo> tableMap = new HashMap<String, TableInfo>();
		while (tables.next()) {
			String tableCatalog = tables.getString(1);
			String tableSchema = tables.getString(2);
			String tableName = tables.getString(3);
			String fullName = getFullyQualifiedName(tableCatalog, tableSchema, tableName);
			Table table = metadataFactory.addTable(useFullSchemaName?fullName:tableName);
			table.setNameInSource(getFullyQualifiedName(tableCatalog, tableSchema, tableName, true));
			table.setSupportsUpdate(true);
			String remarks = tables.getString(5);
			table.setAnnotation(remarks);
			tableMap.put(fullName, new TableInfo(tableCatalog, tableSchema, tableName, table));
			
			ResultSet columns = metadata.getColumns(tableCatalog, tableSchema, tableName, null);
			int rsColumns = columns.getMetaData().getColumnCount();
			while (columns.next()) {
				String columnName = columns.getString(4);
				int type = columns.getInt(5);
				String typeName = columns.getString(6);
				type = checkForUnsigned(type, typeName);
				//note that the resultset is already ordered by position, so we can rely on just adding columns in order
				Column column = metadataFactory.addColumn(columnName, TypeFacility.getDataTypeNameFromSQLType(type), table);
				column.setNameInSource(quoteName(columnName));
				column.setNativeType(columns.getString(6));
				column.setRadix(columns.getInt(10));
				column.setNullType(NullType.values()[columns.getShort(11)]);
				column.setUpdatable(true);
				column.setAnnotation(columns.getString(12));
				column.setCharOctetLength(columns.getInt(16));
				if (rsColumns >= 23) {
					column.setAutoIncrementable("YES".equalsIgnoreCase(columns.getString(23))); //$NON-NLS-1$
				}
			}
			columns.close();
		}
		tables.close();
		
		return tableMap;
	}

	private String quoteName(String name) {
		if (quoteNameInSource) {
			return quoteString + StringUtil.replaceAll(name, quoteString, quoteString + quoteString) + quoteString;
		}
		return name;
	}

	private void getPrimaryKeys(MetadataFactory metadataFactory,
			DatabaseMetaData metadata, Map<String, TableInfo> tableMap)
			throws SQLException, ConnectorException {
		logger.logDetail("JDBCMetadataProcessor - Importing primary keys"); //$NON-NLS-1$
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
		logger.logDetail("JDBCMetadataProcessor - Importing foreign keys"); //$NON-NLS-1$
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
					String fullTableName = getFullyQualifiedName(tableCatalog, tableSchema, tableName);
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
		logger.logDetail("JDBCMetadataProcessor - Importing index info"); //$NON-NLS-1$
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

	private String getFullyQualifiedName(String catalogName, String schemaName, String objectName) {
		return getFullyQualifiedName(catalogName, schemaName, objectName, false);
	}
	
	private String getFullyQualifiedName(String catalogName, String schemaName, String objectName, boolean quoted) {
		String fullName = (quoted?quoteName(objectName):objectName);
		if (schemaName != null && schemaName.length() > 0) {
			fullName = (quoted?quoteName(schemaName):schemaName) + AbstractMetadataRecord.NAME_DELIM_CHAR + fullName;
		}
		if (catalogName != null && catalogName.length() > 0) {
			fullName = (quoted?quoteName(catalogName):catalogName) + AbstractMetadataRecord.NAME_DELIM_CHAR + fullName;
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
	
	public void setImportApproximateIndexes(boolean importApproximateIndexes) {
		this.importApproximateIndexes = importApproximateIndexes;
	}

	public void setWidenUnsingedTypes(boolean widenUnsingedTypes) {
		this.widenUnsingedTypes = widenUnsingedTypes;
	}
	
	public void setQuoteNameInSource(boolean quoteIdentifiers) {
		this.quoteNameInSource = quoteIdentifiers;
	}

}
