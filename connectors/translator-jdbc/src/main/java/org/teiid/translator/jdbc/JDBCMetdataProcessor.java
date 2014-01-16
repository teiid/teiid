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

package org.teiid.translator.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.teiid.core.util.StringUtil;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.*;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.translator.TypeFacility;


/**
 * Reads from {@link DatabaseMetaData} and creates metadata through the {@link MetadataFactory}.
 */
public class JDBCMetdataProcessor {
	
	/**
	 * A holder for table records that keeps track of catalog and schema information.
	 */
	static class TableInfo {
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
	
	private boolean importProcedures;
	private boolean importKeys = true;
	private boolean importForeignKeys = true;
	private boolean importIndexes;
	private String procedureNamePattern;
	protected boolean useFullSchemaName = true;	
	private String[] tableTypes;
	private String tableNamePattern;
	private String catalog;
	private String schemaPattern;	
	private boolean importApproximateIndexes = true;
	private boolean widenUnsingedTypes = true;
	private boolean quoteNameInSource = true;
	private boolean useProcedureSpecificName;
	private boolean useCatalogName = true;
	private boolean autoCreateUniqueConstraints = true;
	private boolean useQualifiedName = true;
	
	private Set<String> unsignedTypes = new HashSet<String>();
	private String quoteString;
	
	private Pattern excludeTables;
	private Pattern excludeProcedures;
	
	private int excludedTables;
	
	private boolean useAnyIndexCardinality;
	private boolean importStatistics;
	
	private String columnNamePattern;
	
	public void getConnectorMetadata(Connection conn, MetadataFactory metadataFactory)
			throws SQLException {
		DatabaseMetaData metadata = conn.getMetaData();
		
		quoteString = metadata.getIdentifierQuoteString();
		if (quoteString != null && quoteString.trim().length() == 0) {
			quoteString = null;
		}
		
		if (widenUnsingedTypes) {
			ResultSet rs = null;
			try {
				rs = metadata.getTypeInfo();
			} catch (SQLException e) {
				LogManager.logWarning(LogConstants.CTX_CONNECTOR, e, JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID11021));
			}
			if (rs != null) {
				while (rs.next()) {
					String name = rs.getString(1);
					boolean unsigned = rs.getBoolean(10);
					if (unsigned) {
						unsignedTypes.add(name);
					}
				}
			}
		}
		
		Map<String, TableInfo> tableMap = getTables(metadataFactory, metadata);
		HashSet<TableInfo> tables = new LinkedHashSet<TableInfo>(tableMap.values());
		if (importKeys) {
			getPrimaryKeys(metadataFactory, metadata, tables);
			getIndexes(metadataFactory, metadata, tables, !importIndexes);
			if (importForeignKeys) {
				getForeignKeys(metadataFactory, metadata, tables, tableMap);
			}
		} else if (importIndexes) {
			getIndexes(metadataFactory, metadata, tables, false);
		}
		
		if (importStatistics) {
			for (TableInfo tableInfo : tables) {
				if (tableInfo.table.getCardinality() == Table.UNKNOWN_CARDINALITY) {
					getTableStatistics(conn, tableInfo.catalog, tableInfo.schema, tableInfo.name, tableInfo.table);
				}
			}
		}
		
		if (importProcedures) {
			getProcedures(metadataFactory, metadata);
		}
		
	}

	/**
	 * 
	 * @param conn
	 * @param catalog
	 * @param schema
	 * @param name
	 * @param table
	 * @throws SQLException 
	 */
	protected void getTableStatistics(Connection conn, String catalog, String schema, String name, Table table) throws SQLException {
		
	}

	private void getProcedures(MetadataFactory metadataFactory,
			DatabaseMetaData metadata) throws SQLException {
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, "JDBCMetadataProcessor - Importing procedures"); //$NON-NLS-1$
		ResultSet procedures = metadata.getProcedures(catalog, schemaPattern, procedureNamePattern);
		int rsColumns = procedures.getMetaData().getColumnCount();
		while (procedures.next()) {
			String procedureCatalog = procedures.getString(1);
			String procedureSchema = procedures.getString(2);
			String procedureName = procedures.getString(3);
			String nameInSource = procedureName;
			if (useProcedureSpecificName && rsColumns >= 9) {
				procedureName = procedures.getString(9);
				if (procedureName == null) {
					procedureName = nameInSource;
				}
			}
			String fullProcedureName = getFullyQualifiedName(procedureCatalog, procedureSchema, procedureName);
			if (excludeProcedures != null && excludeProcedures.matcher(fullProcedureName).matches()) {
				continue;
			}
			Procedure procedure = metadataFactory.addProcedure(useFullSchemaName?fullProcedureName:procedureName);
			procedure.setNameInSource(getFullyQualifiedName(procedureCatalog, procedureSchema, nameInSource, true));
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
				int precision = columns.getInt(8);
				String runtimeType = getRuntimeType(sqlType, typeName, precision);
				switch (columnType) {
				case DatabaseMetaData.procedureColumnResult:
					record = metadataFactory.addProcedureResultSetColumn(columnName, runtimeType, procedure);
					break;
				case DatabaseMetaData.procedureColumnIn:
					record = metadataFactory.addProcedureParameter(columnName, runtimeType, Type.In, procedure);
					break;
				case DatabaseMetaData.procedureColumnInOut:
					record = metadataFactory.addProcedureParameter(columnName, runtimeType, Type.InOut, procedure);
					break;
				case DatabaseMetaData.procedureColumnOut:
					record = metadataFactory.addProcedureParameter(columnName, runtimeType, Type.Out, procedure);
					break;
				case DatabaseMetaData.procedureColumnReturn:
					record = metadataFactory.addProcedureParameter(columnName, runtimeType, Type.ReturnValue, procedure);
					break;
				default:
					continue; //shouldn't happen
				}
				record.setNativeType(typeName);
				record.setPrecision(columns.getInt(8));
				record.setLength(columns.getInt(9));
				record.setScale(columns.getInt(10));
				record.setRadix(columns.getInt(11));
				record.setNullType(NullType.values()[columns.getInt(12)]);
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
			DatabaseMetaData metadata) throws SQLException {
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, "JDBCMetadataProcessor - Importing tables"); //$NON-NLS-1$
		ResultSet tables = metadata.getTables(catalog, schemaPattern, tableNamePattern, tableTypes);
		Map<String, TableInfo> tableMap = new HashMap<String, TableInfo>();
		while (tables.next()) {
			String tableCatalog = tables.getString(1);
			String tableSchema = tables.getString(2);
			String tableName = tables.getString(3);
			String remarks = tables.getString(5);
			String fullName = getFullyQualifiedName(tableCatalog, tableSchema, tableName);
			if (shouldExclude(fullName)) {
				excludedTables++;
				continue;
			}
			Table table = addTable(metadataFactory, tableCatalog, tableSchema,
					tableName, remarks, fullName, tables);
			if (table == null) {
				continue;
			}
			TableInfo ti = new TableInfo(tableCatalog, tableSchema, tableName, table);
			tableMap.put(fullName, ti);
			tableMap.put(tableName, ti);
		}
		tables.close();
		
		getColumns(metadataFactory, metadata, tableMap);
		return tableMap;
	}
	
	
	protected boolean shouldExclude(String fullName) {
		return excludeTables != null && excludeTables.matcher(fullName).matches();
	}

	/**
	 * @throws SQLException  
	 */
	protected Table addTable(MetadataFactory metadataFactory,
			String tableCatalog, String tableSchema, String tableName,
			String remarks, String fullName, ResultSet tables) throws SQLException {
		return addTable(metadataFactory, tableCatalog, tableSchema, tableName,
				remarks, fullName);
	}

	/**
	 * 
	 * @param metadataFactory
	 * @param tableCatalog
	 * @param tableSchema
	 * @param tableName
	 * @param remarks
	 * @param fullName
	 * @return
	 */
	protected Table addTable(MetadataFactory metadataFactory,
			String tableCatalog, String tableSchema, String tableName,
			String remarks, String fullName) {
		Table table = metadataFactory.addTable(useFullSchemaName?fullName:tableName);
		table.setNameInSource(getFullyQualifiedName(tableCatalog, tableSchema, tableName, true));
		table.setSupportsUpdate(true);
		table.setAnnotation(remarks);
		return table;
	}

	private void getColumns(MetadataFactory metadataFactory,
			DatabaseMetaData metadata, Map<String, TableInfo> tableMap)
			throws SQLException {
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, "JDBCMetadataProcessor - Importing columns"); //$NON-NLS-1$
		boolean singleSchema = schemaPattern != null && !schemaPattern.contains("_") && !schemaPattern.contains("%"); //$NON-NLS-1$ //$NON-NLS-2$
		if ((excludeTables == null && schemaPattern == null && tableNamePattern == null) //getting everything
			|| (singleSchema && tableNamePattern == null && 
					(excludeTables == null //getting all from a single schema 
					|| tableMap.size()/2 > Math.sqrt(tableMap.size()/2 + excludedTables)))) {  //not excluding enough from a single schema
			ResultSet columns = metadata.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
			processColumns(metadataFactory, tableMap, columns);
		} else {
			for (TableInfo ti : new LinkedHashSet<TableInfo>(tableMap.values())) {
				ResultSet columns = metadata.getColumns(ti.catalog, ti.schema, ti.name, columnNamePattern);
				processColumns(metadataFactory, tableMap, columns);
			}
		}
	}

	private void processColumns(MetadataFactory metadataFactory,
			Map<String, TableInfo> tableMap, ResultSet columns)
			throws SQLException {
		int rsColumns = columns.getMetaData().getColumnCount();
		while (columns.next()) {
			String tableCatalog = columns.getString(1);
			String tableSchema = columns.getString(2);
			String tableName = columns.getString(3);
			String fullTableName = getFullyQualifiedName(tableCatalog, tableSchema, tableName);
			TableInfo tableInfo = tableMap.get(fullTableName);
			if (tableInfo == null) {
				tableInfo = tableMap.get(tableName);
				if (tableInfo == null) {
					continue;
				}
			}
			addColumn(columns, tableInfo.table, metadataFactory, rsColumns);
		}
		columns.close();
	}
	
	/**
	 * Add a column to the given table based upon the current row of the columns resultset
	 * @param columns
	 * @param table
	 * @param metadataFactory
	 * @param rsColumns
	 * @return the column added
	 * @throws SQLException
	 */
	protected Column addColumn(ResultSet columns, Table table, MetadataFactory metadataFactory, int rsColumns) throws SQLException {
		String columnName = columns.getString(4);
		int type = columns.getInt(5);
		String typeName = columns.getString(6);
		int columnSize = columns.getInt(7);
		String runtimeType = getRuntimeType(type, typeName, columnSize);
		//note that the resultset is already ordered by position, so we can rely on just adding columns in order
		Column column = metadataFactory.addColumn(columnName, runtimeType, table);
		column.setNameInSource(quoteName(columnName));
		column.setPrecision(columnSize);
		column.setLength(columnSize);
		column.setNativeType(typeName);
		column.setRadix(columns.getInt(10));
		column.setNullType(NullType.values()[columns.getInt(11)]);
		column.setUpdatable(true);
		String remarks = columns.getString(12);
		column.setAnnotation(remarks);
		String defaultValue = columns.getString(13);
		column.setDefaultValue(defaultValue);
		if (defaultValue != null && type == Types.BIT && TypeFacility.RUNTIME_NAMES.BOOLEAN.equals(runtimeType)) {
			//try to determine a usable boolean value
            if(defaultValue.length() == 1) {
                int charIntVal = defaultValue.charAt(0);
                // Set boolean FALse for incoming 0, TRUE for 1
                if(charIntVal==0) {
                    column.setDefaultValue(Boolean.FALSE.toString());
                } else if(charIntVal==1) {
                    column.setDefaultValue(Boolean.TRUE.toString());
                }
			} else { //SQLServer quotes bit values
                String trimedDefault = defaultValue.trim();
                if (defaultValue.startsWith("(") && defaultValue.endsWith(")")) { //$NON-NLS-1$ //$NON-NLS-2$
                    trimedDefault = defaultValue.substring(1, defaultValue.length() - 1);
                }
                column.setDefaultValue(trimedDefault);
            }
		}
		column.setCharOctetLength(columns.getInt(16));
		if (rsColumns >= 23) {
			column.setAutoIncremented("YES".equalsIgnoreCase(columns.getString(23))); //$NON-NLS-1$
		}
		return column;
	}

	protected String getRuntimeType(int type, String typeName, int precision) {
		if (type == Types.BIT && precision > 1) {
			type = Types.BINARY;
		}
		type = checkForUnsigned(type, typeName);
		return TypeFacility.getDataTypeNameFromSQLType(type);
	}
	
	protected String quoteName(String name) {
		if (quoteNameInSource && quoteString != null) {
			return quoteString + StringUtil.replaceAll(name, quoteString, quoteString + quoteString) + quoteString;
		}
		return name;
	}

	private void getPrimaryKeys(MetadataFactory metadataFactory,
			DatabaseMetaData metadata, Collection<TableInfo> tables)
			throws SQLException {
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, "JDBCMetadataProcessor - Importing primary keys"); //$NON-NLS-1$
		for (TableInfo tableInfo : tables) {
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
			DatabaseMetaData metadata, Collection<TableInfo> tables, Map<String, TableInfo> tableMap) throws SQLException {
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, "JDBCMetadataProcessor - Importing foreign keys"); //$NON-NLS-1$
		for (TableInfo tableInfo : tables) {
			ResultSet fks = metadata.getImportedKeys(tableInfo.catalog, tableInfo.schema, tableInfo.name);
			TreeMap<Short, String> keyColumns = null;
			TreeMap<Short, String> referencedKeyColumns = null;
			String fkName = null;
			TableInfo pkTable = null;
			short savedSeqNum = Short.MAX_VALUE;
			while (fks.next()) {
				String columnName = fks.getString(8);
				short seqNum = fks.getShort(9);
				String pkColumnName = fks.getString(4);
				
				if (seqNum <= savedSeqNum) {
					if (keyColumns != null) {
						KeyRecord record = autoCreateUniqueKeys(autoCreateUniqueConstraints, metadataFactory, fkName, referencedKeyColumns, pkTable.table);						
						ForeignKey fk = metadataFactory.addForiegnKey(fkName, new ArrayList<String>(keyColumns.values()), new ArrayList<String>(referencedKeyColumns.values()), pkTable.table.getName(), tableInfo.table);
						if (record != null) {
							fk.setPrimaryKey(record);
						}
					}
					keyColumns = new TreeMap<Short, String>();
					referencedKeyColumns = new TreeMap<Short, String>();
					fkName = null;
				}
				savedSeqNum = seqNum;
				keyColumns.put(seqNum, columnName);
				referencedKeyColumns.put(seqNum, pkColumnName);
				if (fkName == null) {
					String tableCatalog = fks.getString(1);
					String tableSchema = fks.getString(2);
					String tableName = fks.getString(3);
					String fullTableName = getFullyQualifiedName(tableCatalog, tableSchema, tableName);
					pkTable = tableMap.get(fullTableName);
					if (pkTable == null) {
						//throw new TranslatorException(JDBCPlugin.Util.getString("JDBCMetadataProcessor.cannot_find_primary", fullTableName)); //$NON-NLS-1$
						continue; //just drop the foreign key, the user probably didn't import the other table
					}
					fkName = fks.getString(12);
					if (fkName == null) {
						fkName = "FK_" + tableInfo.table.getName().toUpperCase(); //$NON-NLS-1$
					}
				} 
			}
			if (keyColumns != null) {
				KeyRecord record = autoCreateUniqueKeys(autoCreateUniqueConstraints, metadataFactory, fkName, referencedKeyColumns, pkTable.table);
				ForeignKey fk = metadataFactory.addForiegnKey(fkName, new ArrayList<String>(keyColumns.values()), new ArrayList<String>(referencedKeyColumns.values()), pkTable.table.getName(), tableInfo.table);
				if (record != null) {
					fk.setPrimaryKey(record);
				}
			}
			fks.close();
		}
	}
	
	private KeyRecord autoCreateUniqueKeys(boolean create, MetadataFactory factory, String name, TreeMap<Short, String> referencedKeyColumns, Table pkTable) {
		if (referencedKeyColumns != null && pkTable.getPrimaryKey() == null && pkTable.getUniqueKeys().isEmpty()) {
			factory.addIndex(name + "_unique", false, new ArrayList<String>(referencedKeyColumns.values()), pkTable); //$NON-NLS-1$
		}
		
		KeyRecord uniqueKey = null;
		if (referencedKeyColumns == null) {
			uniqueKey = pkTable.getPrimaryKey();
		} else {
			for (KeyRecord record : pkTable.getUniqueKeys()) {
				if (keyMatches(new ArrayList<String>(referencedKeyColumns.values()), record)) {
					uniqueKey = record;
					break;
				}
			}
			if (uniqueKey == null && pkTable.getPrimaryKey() != null && keyMatches(new ArrayList<String>(referencedKeyColumns.values()), pkTable.getPrimaryKey())) {
				uniqueKey = pkTable.getPrimaryKey();
			}
		}
		if (uniqueKey == null && create) {
			uniqueKey = factory.addIndex(name + "_unique", false, new ArrayList<String>(referencedKeyColumns.values()), pkTable); //$NON-NLS-1$
		}
		return uniqueKey;
	}
	
	private boolean keyMatches(List<String> names, KeyRecord record) {
		if (names.size() != record.getColumns().size()) {
			return false;
		}
		for (int i = 0; i < names.size(); i++) {
			if (!names.get(i).equals(record.getColumns().get(i).getName())) {
				return false;
			}
		}
		return true;
	}	

	void getIndexes(MetadataFactory metadataFactory,
			DatabaseMetaData metadata, Collection<TableInfo> tables, boolean uniqueOnly) throws SQLException {
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, "JDBCMetadataProcessor - Importing index info"); //$NON-NLS-1$
		for (TableInfo tableInfo : tables) {
			ResultSet indexInfo = metadata.getIndexInfo(tableInfo.catalog, tableInfo.schema, tableInfo.name, uniqueOnly, importApproximateIndexes);
			TreeMap<Short, String> indexColumns = null;
			String indexName = null;
			short savedOrdinalPosition = Short.MAX_VALUE;
			boolean nonUnique = false;
			boolean valid = true;
			boolean cardinalitySet = false;
			while (indexInfo.next()) {
				short type = indexInfo.getShort(7);
				if (type == DatabaseMetaData.tableIndexStatistic) {
					tableInfo.table.setCardinality(indexInfo.getInt(11));
					cardinalitySet = true;
					continue;
				}
				short ordinalPosition = indexInfo.getShort(8);
				if (useAnyIndexCardinality && !cardinalitySet) {
					int cardinality = indexInfo.getInt(11);
					tableInfo.table.setCardinality(Math.max(cardinality, tableInfo.table.getCardinality()));
				}
				if (ordinalPosition <= savedOrdinalPosition) {
					if (valid && indexColumns != null && (!uniqueOnly || !nonUnique)) {
						metadataFactory.addIndex(indexName, nonUnique, new ArrayList<String>(indexColumns.values()), tableInfo.table);
					}
					indexColumns = new TreeMap<Short, String>();
					indexName = null;
					valid = true;
				}
				savedOrdinalPosition = ordinalPosition;
				String columnName = indexInfo.getString(9);
				if (valid && columnName == null || tableInfo.table.getColumnByName(columnName) == null) {
					LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Skipping the import of non-simple index", indexInfo.getString(6)); //$NON-NLS-1$
					valid = false;
				}
				nonUnique = indexInfo.getBoolean(4);
				indexColumns.put(ordinalPosition, columnName);
				if (indexName == null) {
					indexName = indexInfo.getString(6);
					if (indexName == null) {
						indexName = "NDX_" + tableInfo.table.getName().toUpperCase(); //$NON-NLS-1$
					}
				}
			}
			if (valid && indexColumns != null && (!uniqueOnly || !nonUnique)) {
				metadataFactory.addIndex(indexName, nonUnique, new ArrayList<String>(indexColumns.values()), tableInfo.table);
			}
			indexInfo.close();
		}
	}

	private String getFullyQualifiedName(String catalogName, String schemaName, String objectName) {
		return getFullyQualifiedName(catalogName, schemaName, objectName, false);
	}
	
	protected String getFullyQualifiedName(String catalogName, String schemaName, String objectName, boolean quoted) {
		String fullName = (quoted?quoteName(objectName):objectName);
		if (useQualifiedName) {
			if (schemaName != null && schemaName.length() > 0) {
				fullName = (quoted?quoteName(schemaName):schemaName) + AbstractMetadataRecord.NAME_DELIM_CHAR + fullName;
			}
			if (useCatalogName && catalogName != null && catalogName.length() > 0) {
				fullName = (quoted?quoteName(catalogName):catalogName) + AbstractMetadataRecord.NAME_DELIM_CHAR + fullName;
			}
		}
		return fullName;
	}

	public void setTableNamePattern(String tableNamePattern) {
		this.tableNamePattern = tableNamePattern;
	}

	public void setTableTypes(String[] tableTypes) {
		this.tableTypes = tableTypes;
	}
	
	public String[] getTableTypes() {
		return tableTypes;
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

	/**
	 * @deprecated
	 * @see #setWidenUnsignedTypes
	 */
	public void setWidenUnsingedTypes(boolean widenUnsingedTypes) {
		this.widenUnsingedTypes = widenUnsingedTypes;
	}
	
	public void setWidenUnsignedTypes(boolean widenUnsignedTypes) {
		this.widenUnsingedTypes = widenUnsignedTypes;
	}	
	
	public void setQuoteNameInSource(boolean quoteIdentifiers) {
		this.quoteNameInSource = quoteIdentifiers;
	}

	// Importer specific properties
	public void setCatalog(String catalog) {
		this.catalog = catalog;
	}

	public void setSchemaPattern(String schema) {
		this.schemaPattern = schema;
	}
	
	public void setUseProcedureSpecificName(boolean useProcedureSpecificName) {
		this.useProcedureSpecificName = useProcedureSpecificName;
	}
	
	public void setUseCatalogName(boolean useCatalog) {
		this.useCatalogName = useCatalog;
	}
	
	public void setAutoCreateUniqueConstraints(
			boolean autoCreateUniqueConstraints) {
		this.autoCreateUniqueConstraints = autoCreateUniqueConstraints;
	}
	
	public void setExcludeProcedures(String excludeProcedures) {
		this.excludeProcedures = Pattern.compile(excludeProcedures, Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
	}
	
	public void setExcludeTables(String excludeTables) {
		this.excludeTables = Pattern.compile(excludeTables, Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
	}
	
	public void setUseQualifiedName(boolean useQualifiedName) {
		this.useQualifiedName = useQualifiedName;
	}
	
	public void setUseAnyIndexCardinality(boolean useAnyIndexCardinality) {
		this.useAnyIndexCardinality = useAnyIndexCardinality;
	}
	
	public void setImportStatistics(boolean importStatistics) {
		this.importStatistics = importStatistics;
	}
	
	public void setImportForeignKeys(boolean importForeignKeys) {
		this.importForeignKeys = importForeignKeys;
	}
	
	protected void setColumnNamePattern(String columnNamePattern) {
		this.columnNamePattern = columnNamePattern;
	}
	
}
