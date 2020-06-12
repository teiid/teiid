/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.translator.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;
import java.util.regex.Pattern;

import org.teiid.core.types.AbstractGeospatialType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.GeographyType;
import org.teiid.core.types.GeometryType;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.core.util.StringUtil;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.*;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.TranslatorProperty.PropertyType;
import org.teiid.translator.TypeFacility;
import org.teiid.util.FullyQualifiedName;


/**
 * Reads from {@link DatabaseMetaData} and creates metadata through the {@link MetadataFactory}.
 */
public class JDBCMetadataProcessor implements MetadataProcessor<Connection>{

    private static boolean USE_FULL_SCHEMA_NAME_DEFAULT = PropertiesUtils.getHierarchicalProperty("org.teiid.translator.jdbc.useFullSchemaNameDefault", Boolean.FALSE, Boolean.class); //$NON-NLS-1$

    @ExtensionMetadataProperty(applicable= {FunctionMethod.class}, datatype=String.class, display="Sequence Used By This Function")
    static final String SEQUENCE = AbstractMetadataRecord.RELATIONAL_PREFIX+"sequence"; //$NON-NLS-1$

    @ExtensionMetadataProperty(applicable= {Table.class, Procedure.class}, datatype=String.class, display="type of object")
    static final String TYPE = SOURCE_PREFIX+"type"; //$NON-NLS-1$

    /**
     * A holder for table records that keeps track of catalog and schema information.
     */
    static class TableInfo {
        private String catalog;
        private String schema;
        private String name;
        private Table table;
        private String type;

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
    private boolean importSequences;
    private String procedureNamePattern;
    private String sequenceNamePattern;
    protected boolean useFullSchemaName = USE_FULL_SCHEMA_NAME_DEFAULT;
    private boolean useFullSchemaNameSet;
    private String[] tableTypes;
    private String tableNamePattern;
    private String catalog;
    private String schemaPattern;
    private String schemaName;
    private boolean importApproximateIndexes = true;
    private boolean widenUnsingedTypes = true;
    private boolean quoteNameInSource = true;
    private boolean useProcedureSpecificName;
    private boolean useCatalogName = true;
    private String catalogSeparator = String.valueOf(AbstractMetadataRecord.NAME_DELIM_CHAR);
    private boolean autoCreateUniqueConstraints = true;
    private boolean useQualifiedName = true;

    private String startQuoteString;
    private String endQuoteString;

    private Pattern excludeTables;
    private Pattern excludeProcedures;
    private Pattern excludeSequences;

    private boolean useAnyIndexCardinality;
    private boolean importStatistics;

    private String columnNamePattern;

    //type options
    private boolean importRowIdAsBinary;
    private boolean importLargeAsLob;
    private boolean useIntegralTypes;
    private boolean useTypeInfo = true;
    Set<String> unsignedTypes = new HashSet<String>();
    Map<String, Integer> typeMapping = new TreeMap<>();

    public void process(MetadataFactory metadataFactory, Connection conn) throws TranslatorException {
        try {
            getConnectorMetadata(conn, metadataFactory);
        } catch (SQLException e) {
            throw new TranslatorException(JDBCPlugin.Event.TEIID11010, e);
        }
    }

    private String getDefaultQuoteStr(DatabaseMetaData metadata, String quoteString) {
        if (quoteString == null) {
            try {
                quoteString = metadata.getIdentifierQuoteString();
            } catch (SQLException e) {
                LogManager.logDetail(LogConstants.CTX_CONNECTOR, e, "Assuming identifier quoting not supported"); //$NON-NLS-1$
            }
        }
        if (quoteString != null && quoteString.trim().length() == 0) {
            quoteString = null;
        }
        return quoteString;
    }

    public void getConnectorMetadata(Connection conn, MetadataFactory metadataFactory)
            throws SQLException, TranslatorException {

        if (this.useFullSchemaName && useFullSchemaNameSet) {
            LogManager.logInfo(LogConstants.CTX_CONNECTOR, JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID11028));
        }

        if (this.schemaPattern == null && this.schemaName == null) {
            LogManager.logWarning(LogConstants.CTX_CONNECTOR, JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID11027));
        }

        DatabaseMetaData metadata = conn.getMetaData();

        this.startQuoteString = getDefaultQuoteStr(metadata, startQuoteString);
        this.endQuoteString = getDefaultQuoteStr(metadata, endQuoteString);

        if (useTypeInfo) {
            try (ResultSet rs = metadata.getTypeInfo()) {
                while (rs.next()) {
                    String name = rs.getString(1);
                    int type = rs.getInt(2);
                    this.typeMapping.put(name, type);
                    if (widenUnsingedTypes) {
                        boolean unsigned = rs.getBoolean(10);
                        if (unsigned && isUnsignedTypeName(name)) {
                            unsignedTypes.add(name);
                        }
                    }
                }
            } catch (Exception e) {
                LogManager.logWarning(LogConstants.CTX_CONNECTOR, e, JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID11021));
            }
        }

        if (useCatalogName) {
            String separator = metadata.getCatalogSeparator();
            if (separator != null && !separator.isEmpty()) {
                this.catalogSeparator = separator;
            }
        }

        if (this.schemaName != null) {
            String escapeStr = metadata.getSearchStringEscape();
            String escapedSchemaName = this.schemaName;
            if (escapeStr != null) {
                escapedSchemaName = escapedSchemaName.replace(escapeStr, escapeStr+escapeStr);
                escapedSchemaName = escapedSchemaName.replace("_", escapeStr+"_");
                escapedSchemaName = escapedSchemaName.replace("%", escapeStr+"%");
            }
            this.schemaPattern = escapedSchemaName;
        }

        Map<String, TableInfo> tableMap = getTables(metadataFactory, metadata, conn);
        HashSet<TableInfo> tables = new LinkedHashSet<TableInfo>(tableMap.values());

        //TODO: this should be expanded to procedures and the other import constructs
        if (tables.stream().map((t) -> {
            return Arrays.asList(t.catalog, t.schema);
        }).distinct().count() > 1) {
            if (!useFullSchemaName) {
                if (USE_FULL_SCHEMA_NAME_DEFAULT) {
                    LogManager.logWarning(LogConstants.CTX_CONNECTOR,
                        JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID11029));
                } else {
                    throw new TranslatorException(JDBCPlugin.Event.TEIID11029, JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID11029));
                }
            }
        }

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

        if (importSequences) {
            getSequences(metadataFactory, conn);
        }

    }

    /**
     * Determine if the type name represents an unsigned type
     * @param name
     * @return
     */
    protected boolean isUnsignedTypeName(String name) {
        return true;
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
            nameInSource = modifyProcedureNameInSource(nameInSource);
            String fullProcedureName = getFullyQualifiedName(procedureCatalog, procedureSchema, procedureName);
            if ((excludeProcedures != null && excludeProcedures.matcher(fullProcedureName).matches()) || isHiddenSchema(procedureCatalog, procedureSchema)) {
                continue;
            }
            Procedure procedure = metadataFactory.addProcedure(useFullSchemaName?fullProcedureName:procedureName);
            procedure.setNameInSource(getFullyQualifiedName(procedureCatalog, procedureSchema, nameInSource, true));
            ResultSet columns = metadata.getProcedureColumns(procedureCatalog, procedureSchema, procedureName, null);
            int rsProcColumns = procedures.getMetaData().getColumnCount();
            while (columns.next()) {
                String columnName = columns.getString(4);
                short columnType = 0;
                try {
                    columnType = columns.getShort(5);
                } catch (SQLException e) {
                    // PI jdbc driver has bug and treats column 5 as int
                    int type = columns.getInt(5);
                    columnType = new Integer(type).shortValue();
                }
                int sqlType = columns.getInt(6);
                String typeName = columns.getString(7);
                sqlType = checkForUnsigned(sqlType, typeName);
                if (columnType == DatabaseMetaData.procedureColumnUnknown) {
                    continue; //there's a good chance this won't work
                }
                BaseColumn record = null;
                int precision = columns.getInt(8);
                int scale = columns.getInt(10);
                String runtimeType = getRuntimeType(sqlType, typeName, precision, scale);
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
                    if (columnName == null) {
                        columnName = "return"; //$NON-NLS-1$
                    }
                    record = metadataFactory.addProcedureParameter(columnName, runtimeType, Type.ReturnValue, procedure);
                    break;
                default:
                    continue; //shouldn't happen
                }
                record.setNameInSource(quoteName(columnName));
                record.setNativeType(typeName);
                record.setPrecision(precision);
                record.setLength(columns.getInt(9));
                record.setScale(scale);
                record.setRadix(columns.getInt(11));
                record.setNullType(NullType.values()[columns.getInt(12)]);
                record.setAnnotation(columns.getString(13));
                if (rsProcColumns >= 14) {
                    String def = columns.getString(14);
                    if (def != null) {
                        if (def.equalsIgnoreCase("null")) { //$NON-NLS-1$
                            record.setNullType(NullType.Nullable);
                        } else {
                            record.setDefaultValue(def);
                            //we can't assume that the default is something we can handle
                            record.setProperty(BaseColumn.DEFAULT_HANDLING, BaseColumn.OMIT_DEFAULT);
                        }
                    }
                }
            }
        }
        procedures.close();
    }

    /**
     * Override to modify the nameInSource for a procedure
     * @param nameInSource
     * @return
     */
    protected String modifyProcedureNameInSource(String nameInSource) {
        return nameInSource;
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
            case Types.BIGINT:
                sqlType = Types.NUMERIC;
                break;
            }
        }
        return sqlType;
    }

    private Map<String, TableInfo> getTables(MetadataFactory metadataFactory,
            DatabaseMetaData metadata, Connection conn) throws SQLException {
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, "JDBCMetadataProcessor - Importing tables"); //$NON-NLS-1$
        ResultSet tables = metadata.getTables(catalog, schemaPattern, tableNamePattern, tableTypes);
        Map<String, TableInfo> tableMap = new HashMap<String, TableInfo>();
        while (tables.next()) {
            String tableCatalog = tables.getString(1);
            String tableSchema = tables.getString(2);
            String tableName = tables.getString(3);
            String remarks = tables.getString(5);
            String fullName = getFullyQualifiedName(tableCatalog, tableSchema, tableName);
            if (shouldExclude(fullName) || isHiddenSchema(tableCatalog, tableSchema)) {
                continue;
            }
            Table table = addTable(metadataFactory, tableCatalog, tableSchema,
                    tableName, remarks, fullName, tables);
            if (table == null) {
                continue;
            }
            TableInfo ti = new TableInfo(tableCatalog, tableSchema, tableName, table);
            ti.type = tables.getString(4);
            table.setProperty(TYPE, ti.type);
            tableMap.put(fullName, ti);
            tableMap.put(tableName, ti);
        }
        tables.close();

        getColumns(metadataFactory, metadata, tableMap, conn);
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

    protected String getCatalogTerm() {
        return "catalog"; //$NON-NLS-1$
    }

    protected String getSchemaTerm() {
        return "schema"; //$NON-NLS-1$
    }

    protected String getTableTerm() {
        return "table"; //$NON-NLS-1$
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
        //create a fqn for the table
        FullyQualifiedName fqn = new FullyQualifiedName();
        if (tableCatalog != null && !tableCatalog.isEmpty()) {
            fqn.append(getCatalogTerm(), tableCatalog);
        }
        if (tableSchema != null && !tableSchema.isEmpty()) {
            fqn.append(getSchemaTerm(), tableSchema);
        }
        fqn.append(getTableTerm(), tableName);
        table.setProperty(FQN, fqn.toString());
        table.setSupportsUpdate(true);
        table.setAnnotation(remarks);
        return table;
    }

    private void getColumns(MetadataFactory metadataFactory,
            DatabaseMetaData metadata, Map<String, TableInfo> tableMap, Connection conn)
            throws SQLException {
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, "JDBCMetadataProcessor - Importing columns"); //$NON-NLS-1$
        for (TableInfo ti : new LinkedHashSet<TableInfo>(tableMap.values())) {
            ResultSet columns = metadata.getColumns(ti.catalog, ti.schema, ti.name, columnNamePattern);
            processColumns(metadataFactory, tableMap, columns, conn);
        }
    }

    private void processColumns(MetadataFactory metadataFactory,
            Map<String, TableInfo> tableMap, ResultSet columns, Connection conn)
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
            //TODO: geometry/geography arrays
            Column c = addColumn(columns, tableInfo.table, metadataFactory, rsColumns);
            if (TypeFacility.RUNTIME_TYPES.GEOMETRY.equals(c.getJavaType())) {
                String columnName = columns.getString(4);
                getGeometryMetadata(c, conn, tableCatalog, tableSchema, tableName, columnName);
            } else if (TypeFacility.RUNTIME_TYPES.GEOGRAPHY.equals(c.getJavaType())) {
                String columnName = columns.getString(4);
                getGeographyMetadata(c, conn, tableCatalog, tableSchema, tableName, columnName);
            }
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
        int scale = columns.getInt(9);
        String runtimeType = getRuntimeType(type, typeName, columnSize, scale);
        //note that the resultset is already ordered by position, so we can rely on just adding columns in order
        Column column = metadataFactory.addColumn(columnName, runtimeType, table);
        column.setNameInSource(quoteName(columnName));
        column.setPrecision(columnSize);
        column.setScale(scale); //assume that null means 0
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

    protected String getRuntimeType(int type, String typeName, int precision, int scale) {
        if (useIntegralTypes && scale == 0 && (type == Types.NUMERIC || type == Types.DECIMAL)) {
            if (precision <= 2) {
                return TypeFacility.RUNTIME_NAMES.BYTE;
            }
            if (precision <= 4) {
                return TypeFacility.RUNTIME_NAMES.SHORT;
            }
            if (precision <= 9) {
                return TypeFacility.RUNTIME_NAMES.INTEGER;
            }
            if (precision <= 18) {
                return TypeFacility.RUNTIME_NAMES.LONG;
            }
            return TypeFacility.RUNTIME_NAMES.BIG_INTEGER;
        }
        if (type == Types.ARRAY) {
            return getComponentRuntimeType(typeName, precision, scale) + DataTypeManager.ARRAY_SUFFIX;
        }
        return getRuntimeType(type, typeName, precision);
    }

    /**
     * Extract the component runtime type from the typeName
     * The default is simply object.
     * @param typeName
     * @param precision
     * @param scale
     * @return
     */
    private String getComponentRuntimeType(String typeName, int precision, int scale) {
        String componentTypeName = getNativeComponentType(typeName);
        if (componentTypeName != null) {
            Integer val = this.typeMapping.get(componentTypeName);
            if (val != null) {
                return getRuntimeType(val, componentTypeName, precision, scale);
            }
        }
        return TypeFacility.RUNTIME_NAMES.OBJECT;
    }

    /**
     * Extract the native component type from the typeName,
     * or return null if that is not possible.
     * @param typeName
     * @return
     */
    protected String getNativeComponentType(String typeName) {
        return null;
    }

    protected String getRuntimeType(int type, String typeName, int precision) {
        if (importRowIdAsBinary && type == Types.ROWID) {
            return TypeFacility.RUNTIME_NAMES.VARBINARY;
        }
        if (type == Types.BIT && precision > 1) {
            type = Types.BINARY;
        }
        boolean wasBigInt = type == Types.BIGINT;
        type = checkForUnsigned(type, typeName);
        if (useIntegralTypes && wasBigInt && (type == Types.NUMERIC || type == Types.DECIMAL)) {
            return TypeFacility.RUNTIME_NAMES.BIG_INTEGER;
        }
        String result = TypeFacility.getDataTypeNameFromSQLType(type);
        if (importLargeAsLob) {
            if (result.equals(TypeFacility.RUNTIME_NAMES.STRING) && precision > DataTypeManager.MAX_STRING_LENGTH) {
                result = TypeFacility.RUNTIME_NAMES.CLOB;
            } else if (result.equals(TypeFacility.RUNTIME_NAMES.VARBINARY) && precision > DataTypeManager.MAX_VARBINARY_BYTES) {
                result = TypeFacility.RUNTIME_NAMES.BLOB;
            }
        }
        return result;
    }

    protected String quoteName(String name) {
        if (quoteNameInSource && startQuoteString != null && endQuoteString != null) {
            String str = StringUtil.replaceAll(name, startQuoteString, startQuoteString + startQuoteString);
            str = StringUtil.replaceAll(str, endQuoteString, endQuoteString + endQuoteString);
            return startQuoteString + str + endQuoteString;
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
                short seqNum = safeGetShort(pks, 5);
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

    /**
     * some sources, such as PI, don't consistently support getShort
     * @param rs
     * @param pos
     * @return
     * @throws SQLException
     */
    private short safeGetShort(ResultSet rs, int pos) throws SQLException {
        short val;
        try {
            val = rs.getShort(pos);
        } catch (SQLException e) {
            int valInt = rs.getInt(pos);
            if (valInt > Short.MAX_VALUE) {
                throw new SQLException("invalid short value " + valInt); //$NON-NLS-1$
            }
            val = (short)valInt;
        }
        return val;
    }

    private class FKInfo {
        TableInfo pkTable;
        TreeMap<Short, String> keyColumns = new TreeMap<Short, String>();
        TreeMap<Short, String> referencedKeyColumns = new TreeMap<Short, String>();
        boolean valid = true;
    }

    private void getForeignKeys(MetadataFactory metadataFactory,
            DatabaseMetaData metadata, Collection<TableInfo> tables, Map<String, TableInfo> tableMap) throws SQLException {
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, "JDBCMetadataProcessor - Importing foreign keys"); //$NON-NLS-1$
        for (TableInfo tableInfo : tables) {
            if (!getForeignKeysForTable(tableInfo.catalog, tableInfo.schema, tableInfo.name, tableInfo.type)) {
                continue;
            }
            ResultSet fks = null;
            try {
                fks = metadata.getImportedKeys(tableInfo.catalog, tableInfo.schema, tableInfo.name);
            } catch (SQLException e) {
                if (tableInfo.type != null && StringUtil.indexOfIgnoreCase(tableInfo.type, "TABLE") < 0) { //$NON-NLS-1$
                    LogManager.logDetail(LogConstants.CTX_CONNECTOR, e, "skipping foreign keys for non-table", tableInfo.table.getFullName()); //$NON-NLS-1$
                } else {
                    LogManager.logWarning(LogConstants.CTX_CONNECTOR, e, JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID11026, tableInfo.table.getFullName(), tableInfo.type));
                }
                continue;
            }
            HashMap<String, FKInfo> allKeys = new HashMap<String, FKInfo>();
            while (fks.next()) {
                String columnName = fks.getString(8);
                short seqNum = safeGetShort(fks, 9);
                String pkColumnName = fks.getString(4);

                String fkName = fks.getString(12);
                if (fkName == null) {
                    fkName = "FK_" + tableInfo.table.getName().toUpperCase(); //$NON-NLS-1$
                }

                FKInfo fkInfo = allKeys.get(fkName);

                if (fkInfo == null) {
                    fkInfo = new FKInfo();
                    allKeys.put(fkName, fkInfo);

                    String tableCatalog = fks.getString(1);
                    String tableSchema = fks.getString(2);
                    String tableName = fks.getString(3);
                    String fullTableName = getFullyQualifiedName(tableCatalog, tableSchema, tableName);
                    fkInfo.pkTable = tableMap.get(fullTableName);
                    if (fkInfo.pkTable == null) {
                        //throw new TranslatorException(JDBCPlugin.Util.getString("JDBCMetadataProcessor.cannot_find_primary", fullTableName)); //$NON-NLS-1$
                        fkInfo.valid = false;
                        continue;
                    }
                }

                if (!fkInfo.valid) {
                    continue;
                }

                if (fkInfo.keyColumns.put(seqNum, columnName) != null) {
                    //We can't gracefully handle two unnamed fks
                    fkInfo.valid = false;
                }
                fkInfo.referencedKeyColumns.put(seqNum, pkColumnName);
            }

            for (Map.Entry<String, FKInfo> entry : allKeys.entrySet()) {
                FKInfo info = entry.getValue();
                if (!info.valid) {
                    continue;
                }

                KeyRecord record = autoCreateUniqueKeys(autoCreateUniqueConstraints, metadataFactory, entry.getKey(), info.referencedKeyColumns, info.pkTable.table);
                ForeignKey fk = metadataFactory.addForeignKey(entry.getKey(), new ArrayList<String>(info.keyColumns.values()), new ArrayList<String>(info.referencedKeyColumns.values()), info.pkTable.table.getName(), tableInfo.table);
                if (record != null) {
                    fk.setReferenceKey(record);
                }
            }
            fks.close();
        }
    }

    /**
     * Override to control or disable the default foreign key logic
     * @param catalogName
     * @param schemaName
     * @param tableName
     * @param tableType
     * @return true if the default logic should still be used, or false if the default foreign key logic should not run
     */
    private boolean getForeignKeysForTable(String catalogName, String schemaName,
            String tableName, String tableType) {
        return true;
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
            if (!getIndexInfoForTable(tableInfo.catalog, tableInfo.schema, tableInfo.name, uniqueOnly, importApproximateIndexes, tableInfo.type)) {
                continue;
            }
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
                    tableInfo.table.setCardinality(getCardinality(indexInfo));
                    cardinalitySet = true;
                    continue;
                }
                short ordinalPosition = indexInfo.getShort(8);
                if (useAnyIndexCardinality && !cardinalitySet) {
                    long cardinality = getCardinality(indexInfo);
                    tableInfo.table.setCardinality(Math.max(cardinality, (long)tableInfo.table.getCardinalityAsFloat()));
                }
                if (ordinalPosition <= savedOrdinalPosition) {
                    if (valid && indexColumns != null && (!uniqueOnly || !nonUnique)
                            && (indexName == null || nonUnique || tableInfo.table.getPrimaryKey() == null || !indexName.equals(tableInfo.table.getPrimaryKey().getName()))) {
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
            if (valid && indexColumns != null && (!uniqueOnly || !nonUnique)
                    && (indexName == null || nonUnique || tableInfo.table.getPrimaryKey() == null || !indexName.equals(tableInfo.table.getPrimaryKey().getName()))) {
                metadataFactory.addIndex(indexName, nonUnique, new ArrayList<String>(indexColumns.values()), tableInfo.table);
            }
            indexInfo.close();
        }
    }

    /**
     * Get the cardinality, trying first using getLong for tables with more than max int rows
     * @param indexInfo
     * @return
     * @throws SQLException
     */
    private long getCardinality(ResultSet indexInfo) throws SQLException {
        long result = Table.UNKNOWN_CARDINALITY;
        try {
            result = indexInfo.getLong(11);

        } catch (SQLException e) {
            //can't get as long, try int
            result = indexInfo.getInt(11);
        }
        if (indexInfo.wasNull()) {
            return Table.UNKNOWN_CARDINALITY;
        }
        return result;
    }

    public void getSequences(MetadataFactory metadataFactory, Connection conn) throws SQLException {
        ResultSet sequences = executeSequenceQuery(conn); //TODO: may need version information
        if (sequences == null) {
            LogManager.logInfo(LogConstants.CTX_CONNECTOR, "JDBCMetadataProcessor - sequence import requested, but is not supported by the translator."); //$NON-NLS-1$
            return;
        }
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, "JDBCMetadataProcessor - Importing sequences"); //$NON-NLS-1$
        try {
            while (sequences.next()) {
                String sequenceCatalog = sequences.getString(1);
                String sequenceSchema = sequences.getString(2);
                String sequenceName = sequences.getString(3);
                if (isHiddenSchema(sequenceCatalog, sequenceSchema)) {
                    continue;
                }
                //TODO: type
                String sequenceTeiidName = getFullyQualifiedName(sequenceCatalog, sequenceSchema, sequenceName);
                if (excludeSequences != null && excludeSequences.matcher(sequenceTeiidName).matches()) {
                    continue;
                }
                String fullyQualifiedName = getFullyQualifiedName(sequenceCatalog, sequenceSchema, sequenceName, true);
                String sequenceNext = getSequenceNextSQL(fullyQualifiedName);
                FunctionMethod method = metadataFactory.addFunction((useFullSchemaName?sequenceTeiidName:sequenceName) + "_nextval", TypeFacility.RUNTIME_NAMES.LONG); //$NON-NLS-1$
                method.setProperty(SQLConversionVisitor.TEIID_NATIVE_QUERY, sequenceNext);
                method.setProperty(SEQUENCE, fullyQualifiedName);
                method.setDeterminism(Determinism.NONDETERMINISTIC);
            }
        } finally {
            sequences.close();
        }
    }

    /**
     * Return a result set with three columns - sequence_catalog, sequence_schema, and sequence_name
     * or null if sequences are not supported
     * @param conn
     * @return
     * @throws SQLException
     */
    protected ResultSet executeSequenceQuery(Connection conn) throws SQLException {
        return null;
    }

    /**
     * Return the native sql for getting the next value or null if not supported
     * @param fullyQualifiedName
     * @return
     */
    protected String getSequenceNextSQL(String fullyQualifiedName) {
        return null;
    }

    /**
     * Override to control or disable the default index logic
     * @param catalogName
     * @param schemaName
     * @param tableName
     * @param uniqueOnly
     * @param approximateIndexes
     * @param tableType
     * @return true if the default logic should still be used, or false if the default index logic should not run
     */
    protected boolean getIndexInfoForTable(String catalogName, String schemaName, String tableName, boolean uniqueOnly,
            boolean approximateIndexes, String tableType) {
        return true;
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
                fullName = (quoted?quoteName(catalogName):catalogName) + catalogSeparator + fullName;
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

    @TranslatorProperty (display="Table Types", category=PropertyType.IMPORT, description="Comma separated list - without spaces - of imported table types. null returns all types")
    public String[] getTableTypes() {
        return tableTypes;
    }

    public void setUseFullSchemaName(boolean useFullSchemaName) {
        this.useFullSchemaName = useFullSchemaName;
        this.useFullSchemaNameSet = true;
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
    @Deprecated
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

    public void setSchemaName(String schema) {
        this.schemaName = schema;
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

    public void setColumnNamePattern(String columnNamePattern) {
        this.columnNamePattern = columnNamePattern;
    }

    @TranslatorProperty(display="Import Procedures", category=PropertyType.IMPORT, description="true to import procedures and procedure columns - Note that it is not always possible to import procedure result set columns due to database limitations. It is also not currently possible to import overloaded procedures.")
    public boolean isImportProcedures() {
        return importProcedures;
    }

    @TranslatorProperty(display="Import Keys", category=PropertyType.IMPORT, description="true to import primary and foreign keys - NOTE foreign keys to tables that are not imported will be ignored")
    public boolean isImportKeys() {
        return importKeys;
    }

    @TranslatorProperty(display="Import Foreign Key", category=PropertyType.IMPORT, description="true to import foreign keys")
    public boolean isImportForeignKeys() {
        return importForeignKeys;
    }

    @TranslatorProperty(display="Import Indexes", category=PropertyType.IMPORT, description="true to import index/unique key/cardinality information")
    public boolean isImportIndexes() {
        return importIndexes;
    }

    @TranslatorProperty(display="Procedure Name Pattern", category=PropertyType.IMPORT, description=" a procedure name pattern; must match the procedure name as it is stored in the database")
    public String getProcedureNamePattern() {
        return procedureNamePattern;
    }

    @TranslatorProperty(display="Use Full Schema Name", category=PropertyType.IMPORT, description="When false, directs the importer to drop the source catalog/schema from the Teiid object name, so that the Teiid fully qualified name will be in the form of <model name>.<table name> - Note: when false this may lead to objects with duplicate names when importing from multiple schemas, which results in an exception.  This option does not affect the name in source property.")
    public boolean isUseFullSchemaName() {
        return useFullSchemaName;
    }

    @TranslatorProperty(display="Table Name Pattern", category=PropertyType.IMPORT, description=" a table name pattern; must match the table name as it is stored in the database")
    public String getTableNamePattern() {
        return tableNamePattern;
    }

    @TranslatorProperty(display="catalog", category=PropertyType.IMPORT, description="a catalog name; must match the catalog name as it is stored in the database; \"\" retrieves those without a catalog; null means that the catalog name should not be used to narrow the search")
    public String getCatalog() {
        return catalog;
    }

    @TranslatorProperty(display="Schema Pattern", category=PropertyType.IMPORT, description="a schema name pattern; must match the schema name as it is stored in the database; \"\" retrieves those without a schema; null means that the schema name should not be used to narrow the search")
    public String getSchemaPattern() {
        return schemaPattern;
    }

    @TranslatorProperty(display="Schema Name", category=PropertyType.IMPORT, description="an exact schema name; must match the schema name as it is stored in the database; Takes precedence over schemaPatten if supplied.")
    public String getSchemaName() {
        return schemaName;
    }

    @TranslatorProperty(display="Import Approximate Indexes", category=PropertyType.IMPORT, description="true to import approximate index information. when true, result is allowed to reflect approximate or out of data values; when false, results are requested to be accurate")
    public boolean isImportApproximateIndexes() {
        return importApproximateIndexes;
    }

    @TranslatorProperty(display="Widen unSigned Types", category=PropertyType.IMPORT, description="true to convert unsigned types to the next widest type. For example SQL Server reports tinyint as an unsigned type. With this option enabled, tinyint would be imported as a short instead of a byte.")
    public boolean isWidenUnsingedTypes() {
        return widenUnsingedTypes;
    }

    @TranslatorProperty(display="Quote NameInSource", category=PropertyType.IMPORT, description="false will override the default and direct Teiid to create source queries using unquoted identifiers.")
    public boolean isQuoteNameInSource() {
        return quoteNameInSource;
    }

    @TranslatorProperty(display="Use Procedure Specific Name", category=PropertyType.IMPORT, description="true will allow the import of overloaded procedures (which will normally result in a duplicate procedure error) by using the unique procedure specific name as the Teiid name. This option will only work with JDBC 4.0 compatible drivers that report specific names.")
    public boolean isUseProcedureSpecificName() {
        return useProcedureSpecificName;
    }

    @TranslatorProperty(display="Use Catalog Name", category=PropertyType.IMPORT, description="true will use any non-null/non-empty catalog name as part of the name in source, e.g. \"catalog\".\"schema\".\"table\".\"column\", and in the Teiid runtime name if useFullSchemaName is also true. false will not use the catalog name in either the name in source or the Teiid runtime name. Should be set to false for sources that do not fully support a catalog concept, but return a non-null catalog name in their metadata - such as HSQL.")
    public boolean isUseCatalogName() {
        return useCatalogName;
    }

    @TranslatorProperty(display="Auto Create Unique Constraints", category=PropertyType.IMPORT, description="true to create a unique constraint if one is not found for a foreign keys")
    public boolean isAutoCreateUniqueConstraints() {
        return autoCreateUniqueConstraints;
    }

    @TranslatorProperty(display="use Qualified Name", category=PropertyType.IMPORT, description="true will use name qualification for both the Teiid name and name in source as dictated by the useCatalogName and useFullSchemaName properties.  Set to false to disable all qualification for both the Teiid name and the name in source, which effectively ignores the useCatalogName and useFullSchemaName properties.  Note: when false this may lead to objects with duplicate names when importing from multiple schemas, which results in an exception.")
    public boolean isUseQualifiedName() {
        return useQualifiedName;
    }

    @TranslatorProperty(display="Use Any Index Cardinality", category=PropertyType.IMPORT, description="true will use the maximum cardinality returned from DatabaseMetaData.getIndexInfo. importKeys or importIndexes needs to be enabled for this setting to have an effect. This allows for better stats gathering from sources that don't support returning a statistical index.")
    public boolean isUseAnyIndexCardinality() {
        return useAnyIndexCardinality;
    }

    @TranslatorProperty(display="Import Statistics", category=PropertyType.IMPORT, description="true will use database dependent logic to determine the cardinality if none is determined. Not yet supported by all database types - currently only supported by Oracle and MySQL.")
    public boolean isImportStatistics() {
        return importStatistics;
    }

    @TranslatorProperty(display="Column Name Pattern", category=PropertyType.IMPORT, description="a column name pattern; must match the column name as it is stored in the database. Used to import columns of tables.  Leave unset to import all columns.", advanced=true)
    public String getColumnNamePattern() {
        return columnNamePattern;
    }

    @TranslatorProperty(display="Exclude Tables", category=PropertyType.IMPORT, description="A case-insensitive regular expression that when matched against a fully qualified Teiid table name will exclude it from import.  Applied after table names are retrieved.  Use a negative look-ahead (?!<inclusion pattern>).* to act as an inclusion filter")
    public String getExcludeTables() {
        if (this.excludeTables == null) {
            return null;
        }
        return this.excludeTables.pattern();
    }

    @TranslatorProperty(display="Exclude Procedures", category=PropertyType.IMPORT, description="A case-insensitive regular expression that when matched against a fully qualified Teiid procedure name will exclude it from import.  Applied after procedure names are retrieved.  Use a negative look-ahead (?!<inclusion pattern>).* to act as an inclusion filter")
    public String getExcludeProcedures() {
        if (this.excludeProcedures == null) {
            return null;
        }
        return this.excludeProcedures.pattern();
    }

    public void setQuoteString(String quoteString) {
        this.startQuoteString = quoteString;
        this.endQuoteString = quoteString;
    }

    public void setStartQuoteString(String quoteString) {
        this.startQuoteString = quoteString;
    }

    public void setEndQuoteString(String quoteString) {
        this.endQuoteString = quoteString;
    }

    @TranslatorProperty(display="Import RowId as binary", category=PropertyType.IMPORT, description="Import RowId values as varbinary rather than object.")
    public boolean isImportRowIdAsBinary() {
        return this.importRowIdAsBinary;
    }

    public void setImportRowIdAsBinary(boolean importRowIdAsBinary) {
        this.importRowIdAsBinary = importRowIdAsBinary;
    }

    @TranslatorProperty(display="Import Large as LOB", category=PropertyType.IMPORT, description="Import character and binary types larger than the Teiid max as clob or blob respectively.")
    public boolean isImportLargeAsLob() {
        return importLargeAsLob;
    }

    public void setImportLargeAsLob(boolean importLargeAsLob) {
        this.importLargeAsLob = importLargeAsLob;
    }

    @TranslatorProperty(display="Import Sequences", category=PropertyType.IMPORT, description="true to import sequences")
    public boolean isImportSequences() {
        return importSequences;
    }

    public void setImportSequences(boolean importSequences) {
        this.importSequences = importSequences;
    }

    @TranslatorProperty(display="Exclude Tables", category=PropertyType.IMPORT, description="A case-insensitive regular expression that when matched against a fully qualified Teiid sequence name will exclude it from import.  Applied after table names are retrieved.  Use a negative look-ahead (?!<inclusion pattern>).* to act as an inclusion filter")
    public String getExcludeSequences() {
        if (this.excludeSequences == null) {
            return null;
        }
        return excludeSequences.pattern();
    }

    public void setExcludeSequences(String excludeSequences) {
        this.excludeSequences = Pattern.compile(excludeSequences, Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
    }

    @TranslatorProperty(display="Sequence Name Pattern", category=PropertyType.IMPORT, description="a sequence name pattern; must match the sequence name as it is stored in the database")
    public String getSequenceNamePattern() {
        return sequenceNamePattern;
    }

    public void setSequenceNamePattern(String sequenceNamePattern) {
        this.sequenceNamePattern = sequenceNamePattern;
    }

    @TranslatorProperty (display="Use Integral Types", category=PropertyType.IMPORT, description="Use integral types rather than decimal when the scale is 0.")
    public boolean isUseIntegralTypes() {
        return useIntegralTypes;
    }

    public void setUseIntegralTypes(boolean useIntegralTypes) {
        this.useIntegralTypes = useIntegralTypes;
    }

    /**
     * If the schema is hidden regardless of the specified schema pattern
     * @param catalog
     * @param schema
     * @return true if no objects should be imported from the given schema
     */
    protected boolean isHiddenSchema(String catalog, String schema) {
        return false;
    }

    /**
     * Get the geospatial metadata for the column, including srid, type, and dimensionality
     * @param c
     * @param conn
     * @param tableCatalog
     * @param tableSchema
     * @param tableName
     * @param columnName
     */
    protected void getGeometryMetadata(Column c, Connection conn,
            String tableCatalog, String tableSchema, String tableName,
            String columnName) {
        getGeospatialMetadata(c, conn, tableCatalog, tableSchema, tableName,
                columnName, GeometryType.class);
    }

    /**
     * Get the geospatial metadata for the column, including srid, type, and dimensionality
     * @param c
     * @param conn
     * @param tableCatalog
     * @param tableSchema
     * @param tableName
     * @param columnName
     */
    protected void getGeographyMetadata(Column c, Connection conn,
            String tableCatalog, String tableSchema, String tableName,
            String columnName) {
        getGeospatialMetadata(c, conn, tableCatalog, tableSchema, tableName,
                columnName, GeographyType.class);
    }

    protected void getGeospatialMetadata(Column c, Connection conn,
            String tableCatalog, String tableSchema, String tableName,
            String columnName, Class<? extends AbstractGeospatialType> clazz) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            if (tableCatalog == null) {
                tableCatalog = conn.getCatalog();
            }
            String viewName = clazz==GeometryType.class?getGeometryMetadataTableName():getGeographyMetadataTableName();
            if (viewName == null) {
                return;
            }
            ps = conn.prepareStatement("select coord_dimension, srid, type from " + viewName //$NON-NLS-1$
                    + " where f_table_catalog=? and f_table_schema=? and f_table_name=? and f_geometry_column=?"); //$NON-NLS-1$
            ps.setString(1, tableCatalog);
            ps.setString(2, tableSchema);
            ps.setString(3, tableName);
            ps.setString(4, columnName);
            rs = ps.executeQuery();
            if (rs.next()) {
                c.setProperty(MetadataFactory.SPATIAL_PREFIX + "coord_dimension", rs.getString(1)); //$NON-NLS-1$
                c.setProperty(MetadataFactory.SPATIAL_PREFIX + "srid", rs.getString(2)); //$NON-NLS-1$
                c.setProperty(MetadataFactory.SPATIAL_PREFIX + "type", rs.getString(3)); //$NON-NLS-1$
            }
        } catch (SQLException e) {
            LogManager.logDetail(LogConstants.CTX_CONNECTOR, e, "Could not get geometry metadata for column", tableSchema, tableName, columnName); //$NON-NLS-1$
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                }
            }
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                }
            }
        }
    }

    /**
     * Return the name of the metadata table or view for geography metadata
     * that conforms to the simple features specification or null if none exists
     * @return
     */
    protected String getGeographyMetadataTableName() {
        return null;
    }

    /**
     * Return the name of the metadata table or view for geometry metadata
     * that conforms to the simple features specification or null if none exists
     * @return
     */
    protected String getGeometryMetadataTableName() {
        return null;
    }

    @TranslatorProperty(display="Use Type Info", category=PropertyType.IMPORT, description="If JDBC DatabaseMetaData getTypeInfo should be used.  If false, array component types and unsigned types may not be able to be determined.")
    public boolean isUseTypeInfo() {
        return useTypeInfo;
    }

    public void setUseTypeInfo(boolean useTypeInfo) {
        this.useTypeInfo = useTypeInfo;
    }

}
