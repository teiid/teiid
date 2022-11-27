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
package org.teiid.translator.couchbase;

import static org.teiid.language.SQLConstants.Tokens.*;
import static org.teiid.metadata.BaseColumn.NullType.*;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.*;
import static org.teiid.translator.couchbase.CouchbaseProperties.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.teiid.core.types.DataTypeManager;
import org.teiid.couchbase.CouchbaseConnection;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Column;
import org.teiid.metadata.Datatype;
import org.teiid.metadata.ExtensionMetadataProperty;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.metadata.Table;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.TranslatorProperty.PropertyType;
import org.teiid.translator.TypeFacility;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.document.json.JsonValue;
import com.couchbase.client.java.query.N1qlQueryRow;

public class CouchbaseMetadataProcessor implements MetadataProcessor<CouchbaseConnection> {

    @ExtensionMetadataProperty(applicable = Table.class, datatype = Boolean.class, display = "Is Array Table", description = "Declare whether the table is array table")
    public static final String IS_ARRAY_TABLE = MetadataFactory.COUCHBASE_PREFIX + "ISARRAYTABLE"; //$NON-NLS-1$

    @ExtensionMetadataProperty(applicable = Table.class, datatype = String.class, display = "Named Type Pair", description = "Declare the name of typed key/value pair")
    public static final String NAMED_TYPE_PAIR = MetadataFactory.COUCHBASE_PREFIX + "NAMEDTYPEPAIR"; //$NON-NLS-1$

    private Integer sampleSize;

    private String typeNameList;

    private String sampleKeyspaces = ALL_COLS;

    private Map<String, List<String>> typeNameMap;

    private boolean useDouble;

    @Override
    public void process(MetadataFactory mf, CouchbaseConnection conn) throws TranslatorException {
        if (this.typeNameList == null) {
            LogManager.logWarning(LogConstants.CTX_CONNECTOR, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29009));
        }

        List<String> keyspaces = loadKeyspaces(conn);
        for(String keyspace : keyspaces) {
            addTable(mf, conn, conn.getNamespace(), keyspace);
        }

        addProcedures(mf, conn);
    }

    private List<String> loadKeyspaces(CouchbaseConnection conn) throws TranslatorException {

        String namespace = conn.getNamespace();

        boolean isValidSchema = false;
        String n1qlNamespaces = buildN1QLNamespaces();
        Iterator<N1qlQueryRow> namespaces = conn.execute(n1qlNamespaces).iterator();
        while(namespaces.hasNext()) {
            JsonObject row = namespaces.next().value();
            if(row.getString(NAME).equals(namespace)){
                isValidSchema = true;
                break;
            }
        }
        if (!isValidSchema) {
            LogManager.logDetail(LogConstants.CTX_CONNECTOR, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29010, DEFAULT_NAMESPACE));
            namespace = DEFAULT_NAMESPACE;
        }

        Set<String> keyspaceSet = null;
        if(sampleKeyspaces.trim().length() > 0 && !sampleKeyspaces.equals(ALL_COLS)) {
            String[] array= sampleKeyspaces.split(COMMA);
            keyspaceSet = new HashSet<>(Arrays.asList(array));
        }

        List<String> results = new ArrayList<>();
        String n1qlKeyspaces = buildN1QLKeyspaces(namespace);
        List<N1qlQueryRow> keyspaces = conn.execute(n1qlKeyspaces).allRows();
        for(N1qlQueryRow row : keyspaces){
            String keyspace = row.value().getString(NAME);
            if(keyspaceSet == null) {
                results.add(keyspace);
            } else if(keyspaceSet.contains(keyspace) || keyspaceSet.contains(nameInSource(keyspace))) {
                results.add(keyspace);
            }
        }

        Collections.sort(results);
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29011, n1qlKeyspaces, results));

        return results;
    }

    /**
     * Basically, a keyspace be map to a table, keyspace name is the table name, if TranslatorProperty TypeNameList defined,
     * a keyspace may map to several tables, for example, if the TypeNameList=`default`:`type`,
     * then the {@link CouchbaseMetadataProcessor#addTable(String, String, boolean, String, Dimension, MetadataFactory)}
     * will get all distinct `type` attribute referenced values from keyspace, and use all these values as table name.
     *
     * If multiple keyspaces has same typed value, for example, like TypeNameList=`default`:`type`,`default2`:`type`, both default and default2
     * has document defined {"type": "Customer"}, then the default's table name is 'Customer', default2's table name is 'default2_Customer'.
     *
     * Scan row will add columns to table or create sub-table, nested array be map to a separated table.
     *
     * @param mf - MetadataFactory
     * @param conn - CouchbaseConnection
     * @param namespace - couchbase namespace
     * @param keyspace - couchbase  keyspace
     * @throws TranslatorException
     */
    private void addTable(MetadataFactory mf, CouchbaseConnection conn, String namespace, String keyspace) throws TranslatorException {

        String nameInSource = nameInSource(keyspace);

        Map<String, String> tableNameTypeMap = new HashMap<>();
        List<String> typeNameList = getTypeName(nameInSource);
        List<String> dataSrcTableList = new ArrayList<>();
        if(typeNameList != null && typeNameList.size() > 0) {
            String typeQuery = buildN1QLTypeQuery(typeNameList, namespace, keyspace);
            LogManager.logTrace(LogConstants.CTX_CONNECTOR, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29003, typeQuery));
            List<N1qlQueryRow> rows = conn.execute(typeQuery).allRows();

            for(N1qlQueryRow row : rows) {
                JsonObject rowJson = row.value();
                for(String typeName : typeNameList) {
                    String type = trimWave(typeName);
                    String value = rowJson.getString(type);
                    if(value != null) {
                        dataSrcTableList.add(value);
                        tableNameTypeMap.put(value, typeName);
                        break;
                    }
                }
            }
        } else {
            dataSrcTableList.add(keyspace);
        }

        for(String name : dataSrcTableList) {

            String tableName = name;
            if (mf.getSchema().getTable(name) != null && !name.equals(keyspace)) { // handle multiple keyspaces has same typed table name
                tableName = keyspace + UNDERSCORE + name;
            }

            Table table = mf.addTable(tableName);
            table.setNameInSource(nameInSource);
            table.setSupportsUpdate(true);
            table.setProperty(IS_ARRAY_TABLE, FALSE_VALUE);

            Column column = mf.addColumn(DOCUMENTID, STRING, table);
            column.setUpdatable(true);
            mf.addPrimaryKey("PK0", Arrays.asList(DOCUMENTID), table); //$NON-NLS-1$

            if(!name.equals(keyspace)) {
                String namedTypePair = buildNamedTypePair(tableNameTypeMap.get(name), name);
                table.setProperty(NAMED_TYPE_PAIR, namedTypePair);
            }

            // scan row
            boolean hasTypeIdentifier = true;
            if(dataSrcTableList.size() == 1 && dataSrcTableList.get(0).equals(keyspace)) {
                hasTypeIdentifier = false;
            }

            if(this.sampleSize == null || this.sampleSize == 0) {
                this.sampleSize = 100; // default sample size is 100
                LogManager.logInfo(LogConstants.CTX_CONNECTOR, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29008, this.sampleSize));
            }

            String query = buildN1QLQuery(tableNameTypeMap.get(name), name, namespace, keyspace, this.sampleSize, hasTypeIdentifier);
            LogManager.logTrace(LogConstants.CTX_CONNECTOR, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29003, query));
            Iterator<N1qlQueryRow> result = conn.execute(query).iterator();
            while(result.hasNext()) {
                JsonObject row = result.next().value(); // result.next() always can not be null
                JsonObject currentRowJson = row.getObject(keyspace);
                scanRow(keyspace, nameInSource(keyspace), currentRowJson, mf, table, table.getName(), false, new Dimension());
            }
        }
    }


    /**
     * A dispatcher of scan jsonValue(document, of a segment of document), the jsonValue either can be a JsonObject, or JsonArray,
     * different type dispatch to different scan method.
     *
     * @param key - The attribute name in document, which mapped with value
     * @param value - JsonObject/JsonArray which may contain nested JsonObject/JsonArray
     * @param mf
     * @param table
     * @param referenceTableName - The top table name, used to add foreign key
     * @param isNestedType - Whether the jsonValue are a nested value, or the jsonValue is a segment of document
     * @param dimension - The dimension of nested array, for example, "{"nestedArray": [[["nestedArray"]]]}", the dimension
     *                    deepest array is 3
     */
    protected void scanRow(String key, String keyInSource, JsonValue value, MetadataFactory mf, Table table, String referenceTableName, boolean isNestedType, Dimension dimension) {

        LogManager.logTrace(LogConstants.CTX_CONNECTOR, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29013, table, key, value));

        if(isObjectJsonType(value)) {
            scanObjectRow(key, keyInSource, (JsonObject)value, mf, table, referenceTableName, isNestedType, dimension);
        } else if (isArrayJsonType(value)) {
            scanArrayRow(key, keyInSource, (JsonArray)value, mf, table, referenceTableName, isNestedType, dimension);
        }
    }

    private void scanObjectRow(String key, String keyInSource, JsonObject value, MetadataFactory mf, Table table, String referenceTableName, boolean isNestedType, Dimension dimension) {

        Set<String> names = new TreeSet<String>(value.getNames());

        for(String name : names) {
            String columnName = name;
            Object columnValue = value.get(columnName);
            String columnType = getDataType(columnValue);

            if(columnType.equals(OBJECT)) {
                JsonValue jsonValue = (JsonValue) columnValue;
                String newKey = key + UNDERSCORE + columnName;
                String newKeyInSource = keyInSource + SOURCE_SEPARATOR + nameInSource(columnName);

                if(isObjectJsonType(columnValue)) {
                    scanRow(newKey, newKeyInSource, jsonValue, mf, table, referenceTableName, true, dimension);
                } else if(isArrayJsonType(columnValue)) {
                    String tableName = repleaceTypedName(table.getName(), newKey);
                    String tableNameInSource = newKeyInSource + SQUARE_BRACKETS ;
                    Dimension d = new Dimension();
                    Table subTable = addTable(tableName, tableNameInSource, true, referenceTableName, d, mf);
                    scanRow(newKey, newKeyInSource, jsonValue, mf, subTable, referenceTableName, true, d);
                }
            } else {
                if(isNestedType) {
                    columnName = key + UNDERSCORE + columnName;
                }
                String columnNameInSource = keyInSource + SOURCE_SEPARATOR +nameInSource(name);
                addColumn(columnName, columnType, columnValue, true, columnNameInSource, table, mf);
            }
        }
    }

    private void scanArrayRow(String keyspace, String keyInSource, JsonArray array, MetadataFactory mf, Table table, String referenceTableName, boolean isNestedType, Dimension dimension) {

        if(array.size() > 0) {
            for(int i = 0 ; i < array.size() ; i ++) {
                Object element = array.get(i);
                if(isObjectJsonType(element)) {
                    JsonObject json = (JsonObject) element;

                    for(String name : new TreeSet<String>(json.getNames())) {
                        Object columnValue = json.get(name);
                        String columnType = this.getDataType(columnValue);
                        if(columnType.equals(OBJECT)) {
                            JsonValue jsonValue = (JsonValue) columnValue;
                            if(isObjectJsonType(jsonValue)) {
                                scanRow(keyspace, keyInSource, jsonValue, mf, table, referenceTableName, true, dimension);
                            } else if (isArrayJsonType(jsonValue)) {
                                String tableName = table.getName() + UNDERSCORE + name + UNDERSCORE + dimension.get();
                                String tableNameInSrc = table.getNameInSource() + SOURCE_SEPARATOR + nameInSource(name) + SQUARE_BRACKETS;
                                Dimension d = new Dimension();
                                Table subTable = addTable(tableName, tableNameInSrc, true, referenceTableName, d, mf);
                                scanRow(keyspace, keyInSource, jsonValue, mf, subTable, referenceTableName, true, d);
                            }
                        } else {
                            String columnName = table.getName() + UNDERSCORE + name;
                            String columnNameInSource = table.getNameInSource() + SOURCE_SEPARATOR + nameInSource(name);
                            addColumn(columnName, columnType, columnValue, true, columnNameInSource, table, mf);
                        }
                    }

                } else if(isArrayJsonType(element)) {
                    String tableName = table.getName() + UNDERSCORE + dimension.get();
                    String tableNameInSrc = table.getNameInSource() + SQUARE_BRACKETS;
                    Dimension d = dimension.copy();
                    Table subTable = addTable(tableName, tableNameInSrc, true, referenceTableName, d, mf);
                    scanRow(keyspace, keyInSource, (JsonValue)element, mf, subTable, referenceTableName, true, d);
                } else {
                    String elementType = getDataType(element);
                    String columnName = table.getName();
                    String columnNameInSource = table.getNameInSource();
                    addColumn(columnName, elementType, element, true, columnNameInSource, table, mf);
                }
            }
        } else {
            String columnName = table.getName();
            addColumn(columnName, null, null, true, null, table, mf);
        }
    }


    /**
     * Principle used to map document-format nested JsonArray to JDBC-compatible Table:
     *   1) If nested array contains differently-typed elements and no elements are Json document, all elements be
     *      map to same column with Object type.
     *   2) If nested array contains Json document, all keys be map to Column name, and reference value data type be
     *      map to Column data type
     *   3) If nested array contains other nested array, or nested array's Json document item contains nested arrays/documents
     *      these nested arrays/documents be treated as Object
     *   4) A index column used to indicate the index in array.
     */
    private Table addTable(String tableName, String nameInSource, boolean updatable, String referenceTableName, Dimension dimension, MetadataFactory mf) {

        Table table = null;
        if (mf.getSchema().getTable(tableName) != null) {
            table = mf.getSchema().getTable(tableName);
        } else {
            table = mf.addTable(tableName);
            table.setNameInSource(nameInSource);
            table.setSupportsUpdate(updatable);
            table.setProperty(IS_ARRAY_TABLE, TRUE_VALUE);
            Column column = mf.addColumn(DOCUMENTID, STRING, table);
            column.setUpdatable(true);
            mf.addForeignKey("FK0", Arrays.asList(DOCUMENTID), referenceTableName, table); //$NON-NLS-1$

            for(int i = 1 ; i <= dimension.dimension ; i ++) {
                String idxName = buildArrayTableIdxName(nameInSource, i);
                idxName = repleaceTypedName(referenceTableName, idxName);
                Column idx = mf.addColumn(idxName, INTEGER, table);
                idx.setUpdatable(true);
            }
        }
        dimension.increment();
        return table;
    }

    private void addColumn(String name, String type, Object columnValue, boolean updatable, String nameInSource, Table table, MetadataFactory mf) {

        String columnName = name;
        String columnType = type;

        if(columnType == null && columnValue == null && table.getColumnByName(columnName) == null) {
            columnType = TypeFacility.RUNTIME_NAMES.STRING;
        } else if (columnType == null && columnValue == null && table.getColumnByName(columnName) != null) {
            columnType = table.getColumnByName(columnName).getDatatype().getName();
        }

        if(DataTypeManager.DefaultDataTypes.NULL.equals(columnType)) {
            columnType = DataTypeManager.DefaultDataTypes.STRING; // how to handle null type?
        }

        String tableNameInSource = trimWave(table.getNameInSource());
        if(table.getProperty(IS_ARRAY_TABLE, false).equals(FALSE_VALUE) && columnName.startsWith(tableNameInSource)){
            columnName = columnName.substring(tableNameInSource.length() + 1);
        }

        if (table.getColumnByName(columnName) == null) {
            Column column = mf.addColumn(columnName, columnType, table);
            column.setUpdatable(updatable);
            if(nameInSource != null){
                column.setNameInSource(nameInSource);
            }
        } else {
            Column column = table.getColumnByName(columnName);
            String existColumnType = column.getDatatype().getName();
            if(!existColumnType.equals(columnType) && !existColumnType.equals(OBJECT) && columnValue != null) {
                Datatype datatype = mf.getDataTypes().get(OBJECT);
                column.setDatatype(datatype, true, 0);
            }
        }
    }

    private boolean isObjectJsonType(Object jsonValue) {
        return jsonValue instanceof JsonObject;
    }

    private boolean isArrayJsonType(Object jsonValue) {
        return jsonValue instanceof JsonArray;
    }

    /**
     * For handle typed table name, replace the root keyspace to typed table name.
     * @param name - typed table name.
     * @param path - path of document
     * @return
     */
    private String repleaceTypedName(String name, String path) {
        String tableName = path.substring(path.indexOf(UNDERSCORE));
        return name + tableName;
    }

    private String buildArrayTableIdxName(String nameInSource, int dimension) {
        StringBuilder sb = new StringBuilder();
        String dim1Name = nameInSource.substring(0, nameInSource.indexOf(SQUARE_BRACKETS));
        String[] names = dim1Name.split(Pattern.quote(SOURCE_SEPARATOR));
        boolean isFirst = true;
        for(String name : names) {
            if(isFirst) {
                isFirst = false;
                sb.append(trimWave(name));
            } else {
                sb.append(UNDERSCORE);
                sb.append(trimWave(name));
            }
        }

        for(int i = 1 ; i <= dimension ; i ++) {
            if(i == 1) {
                continue;
            }
            sb.append(UNDERSCORE);
            sb.append(DIM_SUFFIX).append(i);
        }
        sb.append(IDX_SUFFIX);
        return sb.toString();
    }

    private String buildNamedTypePair(String columnIdentifierName, String typedValue) {
        StringBuilder sb = new StringBuilder();
        sb.append(columnIdentifierName).append(COLON).append(QUOTE).append(typedValue).append(QUOTE);
        return sb.toString();
    }

    private List<String> getTypeName(String keyspace) {

        if(this.typeNameList == null) {
            return null;
        }

        if(this.typeNameMap == null) {
            this.typeNameMap = new HashMap<>();
            try {
                Pattern typeNamePattern = Pattern.compile(CouchbaseProperties.TPYENAME_MATCHER_PATTERN);
                Matcher typeGroupMatch = typeNamePattern.matcher(typeNameList);
                while (typeGroupMatch.find()) {
                    String key = typeGroupMatch.group(1);
                    String value = typeGroupMatch.group(2);
                    if(typeNameMap.get(key) == null) {
                        typeNameMap.put(key, new ArrayList<>(3));
                    }
                    typeNameMap.get(key).add(value);
                }
            } catch (Exception e) {
                LogManager.logError(LogConstants.CTX_CONNECTOR, e, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29012, typeNameList));
            }
        }

        return this.typeNameMap.get(keyspace);
    }

    protected void addProcedures(MetadataFactory metadataFactory, CouchbaseConnection connection) {

        Procedure getDocuments = metadataFactory.addProcedure(GETDOCUMENTS);
        getDocuments.setAnnotation(CouchbasePlugin.Util.getString("getDocuments.Annotation")); //$NON-NLS-1$
        ProcedureParameter param = metadataFactory.addProcedureParameter(ID, TypeFacility.RUNTIME_NAMES.STRING, Type.In, getDocuments);
        param.setNullType(No_Nulls);
        param.setAnnotation(CouchbasePlugin.Util.getString("getDocuments.id.Annotation")); //$NON-NLS-1$
        param = metadataFactory.addProcedureParameter(KEYSPACE, TypeFacility.RUNTIME_NAMES.STRING, Type.In, getDocuments);
        param.setNullType(No_Nulls);
        param.setAnnotation(CouchbasePlugin.Util.getString("getDocuments.keyspace.Annotation")); //$NON-NLS-1$
        metadataFactory.addProcedureResultSetColumn(RESULT, TypeFacility.RUNTIME_NAMES.BLOB, getDocuments);

        Procedure getDocument = metadataFactory.addProcedure(GETDOCUMENT);
        getDocument.setAnnotation(CouchbasePlugin.Util.getString("getDocument.Annotation")); //$NON-NLS-1$
        param = metadataFactory.addProcedureParameter(ID, TypeFacility.RUNTIME_NAMES.STRING, Type.In, getDocument);
        param.setNullType(No_Nulls);
        param.setAnnotation(CouchbasePlugin.Util.getString("getDocument.id.Annotation")); //$NON-NLS-1$
        param = metadataFactory.addProcedureParameter(KEYSPACE, TypeFacility.RUNTIME_NAMES.STRING, Type.In, getDocument);
        param.setNullType(No_Nulls);
        param.setAnnotation(CouchbasePlugin.Util.getString("getDocument.keyspace.Annotation")); //$NON-NLS-1$
        metadataFactory.addProcedureResultSetColumn(RESULT, TypeFacility.RUNTIME_NAMES.BLOB, getDocument);
    }

    /**
     * All supported type in a Couchbase JSON item:
     *   null, String, Integer, Long, Double, Boolean,
     *   BigInteger, BigDecimal, JsonObject, JsonArray
     * @param value
     * @return
     */
    private String getDataType(Object value) {

        if(value == null) {
            return TypeFacility.RUNTIME_NAMES.NULL;
        } else if (value instanceof String) {
            return TypeFacility.RUNTIME_NAMES.STRING;
        } else if (value instanceof Integer) {
            return TypeFacility.RUNTIME_NAMES.INTEGER;
        } else if (value instanceof Long) {
            if (useDouble) {
                return TypeFacility.RUNTIME_NAMES.DOUBLE;
            }
            return TypeFacility.RUNTIME_NAMES.LONG;
        } else if (value instanceof Double) {
            return TypeFacility.RUNTIME_NAMES.DOUBLE;
        } else if (value instanceof Boolean) {
            return TypeFacility.RUNTIME_NAMES.BOOLEAN;
        } else if (value instanceof BigInteger) {
            if (useDouble) {
                return TypeFacility.RUNTIME_NAMES.DOUBLE;
            }
            return TypeFacility.RUNTIME_NAMES.BIG_INTEGER;
        } else if (value instanceof BigDecimal) {
            if (useDouble) {
                return TypeFacility.RUNTIME_NAMES.DOUBLE;
            }
            return TypeFacility.RUNTIME_NAMES.BIG_DECIMAL;
        }

        return TypeFacility.RUNTIME_NAMES.OBJECT;
    }

    private String buildN1QLNamespaces() {
        return "SELECT name FROM system:namespaces"; //$NON-NLS-1$
    }

    private String buildN1QLKeyspaces(String namespace) {
        return "SELECT name, namespace_id FROM system:keyspaces WHERE namespace_id = '" + namespace + "'"; //$NON-NLS-1$ //$NON-NLS-2$
    }

    private String buildN1QLTypeQuery(List<String> typeNameList, String namespace, String keyspace) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT DISTINCT "); //$NON-NLS-1$
        boolean comma = false;
        for(String typeName : typeNameList) {
            if(comma) {
                sb.append(COMMA).append(SPACE);
            } else {
                comma = true;
            }
            sb.append(typeName);
        }
        sb.append(buildN1QLFrom(namespace, keyspace));
        return sb.toString();
    }

    private String buildN1QLQuery(String columnIdentifierName, String typedValue, String namespace, String keyspace, int sampleSize, boolean hasTypeIdentifier) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT meta(").append(WAVE); //$NON-NLS-1$
        sb.append(keyspace);
        sb.append(WAVE).append(").id as PK, "); //$NON-NLS-1$
        sb.append(WAVE).append(keyspace).append(WAVE);
        sb.append(buildN1QLFrom(namespace, keyspace));
        if(hasTypeIdentifier) {
            sb.append(" WHERE ").append(columnIdentifierName).append("='").append(typedValue).append("'"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        }
        sb.append(" LIMIT ").append(sampleSize); //$NON-NLS-1$
        return sb.toString();
    }


    private String buildN1QLFrom(String namespace, String keyspace) {
        StringBuilder sb = new StringBuilder();
        sb.append(" FROM "); //$NON-NLS-1$
        sb.append(WAVE).append(namespace).append(WAVE);
        sb.append(COLON);
        sb.append(WAVE).append(keyspace).append(WAVE);
        return sb.toString();
    }

    static String trimWave(String value) {
        String results = value;
        if(results.startsWith(WAVE) && results.endsWith(WAVE)) {
            results = results.substring(1, results.length() - 1);
        }
        return results;
    }

    static String nameInSource(String path) {
        if(path.startsWith(WAVE) && path.endsWith(WAVE)) {
            return path;
        }
        return WAVE + path + WAVE;
    }

    @TranslatorProperty(display = "SampleSize", category = PropertyType.IMPORT, description = "Maximum number of documents per keyspace that should be map") //$NON-NLS-1$ //$NON-NLS-2$
    public int getSampleSize() {
        return sampleSize;
    }

    public void setSampleSize(int sampleSize) {
        this.sampleSize = sampleSize;
    }

    @TranslatorProperty(display = "TypeNameList", category = PropertyType.IMPORT, description = "A comma-separated list of the attributes that the buckets use to specify document types. Each list item must be a bucket name surrounded by back quotes (`), a colon (:), and an attribute name surrounded by back quotes (`).") //$NON-NLS-1$ //$NON-NLS-2$
    public String getTypeNameList() {
        return typeNameList;
    }

    public void setTypeNameList(String typeNameList) {
        this.typeNameList = typeNameList;
    }

    @TranslatorProperty(display = "SampleKeyspaces", category = PropertyType.IMPORT, description = "A comma-separated list of the keyspace names to define which keyspaces that should be map, default map all keyspaces") //$NON-NLS-1$ //$NON-NLS-2$
    public String getSampleKeyspaces() {
        return sampleKeyspaces;
    }

    public void setSampleKeyspaces(String sampleKeyspaces) {
        this.sampleKeyspaces = sampleKeyspaces;
    }

    /**
     * The dimension of nested array, a dimension is a hint of nested array table name, and index name.
     *
     * For example, the following JsonObject is a 3 dimension nested array,
     * <pre> {@code
     *  {
     *    "default": {"nested": [[["dimension 3"]]]}
     *  }
     * }</pre>
     * each dimension reference with a table, total 3 tables will be generated:
     *   default_nested
     *   default_nested_dim2
     *   default_nested_dim2_dim3
     *
     * The nested array contains it's index of it's parent array, the return of query
     * 'SELECT * FROM default_nested_dim2_dim3' looks
     * <pre> {@code
     *  +-------------+--------------------+-------------------------+------------------------------+--------------------------+
     *  |     ID      | default_nested_idx | default_nested_dim2_idx | default_nested_dim2_dim3_idx | default_nested_dim2_dim3 |
     *  +-------------+--------------------+-------------------------+------------------------------+--------------------------+
     *  | nestedArray | 0                  | 0                       | 0                            | dimension 3              |
     *  +-------------+--------------------+-------------------------+------------------------------+--------------------------+
     *}</pre>
     */
    public static class Dimension{

        int dimension = 1;

        public void increment() {
            dimension++;
        }

        public Dimension copy() {
            Dimension result = new Dimension();
            result.dimension = dimension;
            return result;
        }

        public String get(){
            return DIM_SUFFIX + this.dimension;
        }

    }

    public void setUseDouble(boolean useDouble) {
        this.useDouble = useDouble;
    }

}
