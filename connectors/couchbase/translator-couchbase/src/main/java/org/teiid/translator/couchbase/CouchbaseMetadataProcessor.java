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
package org.teiid.translator.couchbase;

import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.STRING;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.OBJECT;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.INTEGER;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.DOUBLE;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.BOOLEAN;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.SHORT;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.BYTE;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.LONG;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.FLOAT;

import java.util.Arrays;
import java.util.Iterator;

import org.teiid.couchbase.CouchbaseConnection;
import org.teiid.metadata.Column;
import org.teiid.metadata.Datatype;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.metadata.Table;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQueryRow;

public class CouchbaseMetadataProcessor implements MetadataProcessor<CouchbaseConnection> {
    
    public static final String IS_TOP_TABLE = MetadataFactory.COUCHBASE_URI + "ISTOPTABLE"; //$NON-NLS-1$
    public static final String IS_ARRAY_TABLE = MetadataFactory.COUCHBASE_URI + "ISARRAYTABLE"; //$NON-NLS-1$
    public static final String ARRAY_TABLE_GROUP = MetadataFactory.COUCHBASE_URI + "ARRAYTABLEGROUP"; //$NON-NLS-1$
    
    public static final String GETTEXTDOCUMENTS = "getTextDocuments"; //$NON-NLS-1$
    public static final String GETDOCUMENTS = "getDocuments"; //$NON-NLS-1$
    public static final String GETTEXTDOCUMENT = "getTextDocument"; //$NON-NLS-1$
    public static final String GETDOCUMENT = "getDocument"; //$NON-NLS-1$
    public static final String SAVEDOCUMENT = "saveDocument"; //$NON-NLS-1$
    public static final String DELETEDOCUMENT = "deleteDocument"; //$NON-NLS-1$
    public static final String GETMETADATADOCUMENT  = "getMetadataDocument"; //$NON-NLS-1$
    public static final String GETTEXTMETADATADOCUMENT  = "getTextMetadataDocument"; //$NON-NLS-1$
    
    public static final String TRUE = "true"; //$NON-NLS-1$
    public static final String FALSE = "false"; //$NON-NLS-1$
    
    public static final String PK = "PK"; //$NON-NLS-1$
    
    public static final String ID = "id"; //$NON-NLS-1$ 
    public static final String RESULT = "result"; //$NON-NLS-1$ 
    
    private static final String JSON = "json"; //$NON-NLS-1$ 
    private static final String WAVE = "`"; //$NON-NLS-1$
    private static final String DOT = "."; //$NON-NLS-1$
    private static final String PLACEHOLDER = "$"; //$NON-NLS-1$
    private static final String LINE = "_"; //$NON-NLS-1$
    
    @Override
    public void process(MetadataFactory metadataFactory, CouchbaseConnection connection) throws TranslatorException {
       
        String keyspace = connection.getKeyspaceName();
        
        // Map data documents to tables
        Iterator<N1qlQueryRow> result = connection.executeQuery(buildN1ql(keyspace)).iterator();
        while(result.hasNext()) {
            N1qlQueryRow row = result.next();
            JsonObject doc = row.value().getObject(JSON);
            addTable(metadataFactory, keyspace, doc, null);       
        }
        
        addProcedures(metadataFactory, connection);
        
    }

    protected void addTable(MetadataFactory metadataFactory, String key, JsonObject doc, Table parent) {

        Table table = null;
        String tableName = key;
        if(parent != null) {
            tableName = parent.getName() + LINE + key;
        }
        if (metadataFactory.getSchema().getTable(tableName) != null) {
            table = metadataFactory.getSchema().getTable(tableName);
        } else {
            table = metadataFactory.addTable(tableName);
            metadataFactory.addColumn(PK, STRING, table);
            table.setSupportsUpdate(true);
            if(parent == null) {
                // The top Table have a unique primary key.
                table.setNameInSource(buildNameInSource(key, null));
                table.setProperty(IS_TOP_TABLE, TRUE);
                metadataFactory.addPrimaryKey("PK0", Arrays.asList(PK), table); //$NON-NLS-1$
            } else {
                table.setNameInSource(buildNameInSource(key, parent.getNameInSource()));
                table.setProperty(IS_TOP_TABLE, FALSE);
            }
            table.setProperty(IS_ARRAY_TABLE, FALSE);
        }

        // add more columns
        for(String keyCol : doc.getNames()) {
            addColumn(metadataFactory, table, keyCol, doc.get(keyCol));
        }
    }
    
    private void addColumn(MetadataFactory metadataFactory, Table table, String key, Object value) {
        
        //TODO-- couchbase is case sensitive
        if (table.getColumnByName(key) != null) {            
            return ;
        }
        
        if(value instanceof JsonObject) {
            addTable(metadataFactory, key, (JsonObject)value, table);
        } else if(value instanceof JsonArray) {
            addTable(metadataFactory, key, (JsonArray)value, table);
        } else {
            Column column = metadataFactory.addColumn(key, getDataType(value), table);
            column.setUpdatable(true);
        }
    }

    /**
     * Map document-format nested JsonArray to JDBC-compatible Table:
     *  If nested array contains differently-typed elements and no elements are Json document, all elements be 
     *  map to same column with Object type.
     *  If nested array contains Json document, all keys be map to Column name, and reference value data type be
     *  map to Column data type
     *  If nested array contains other nested array, or nested array's Json document item contains nested arrays/documents
     *  these nested arrays/documents be treated as Object
     * @param metadataFactory
     * @param key
     * @param value
     * @param parent
     */
    private void addTable(MetadataFactory metadataFactory, String key, JsonArray value, Table parent) {
        
        Table table = null;
        String tableName = parent.getName() + LINE  + key;
        if (metadataFactory.getSchema().getTable(tableName) != null) {
            table = metadataFactory.getSchema().getTable(tableName);
        } else {
            table = metadataFactory.addTable(tableName);
            metadataFactory.addColumn(PK, STRING, table);
            table.setSupportsUpdate(true);
            table.setNameInSource(buildNameInSource(key, parent.getNameInSource()));
            table.setProperty(IS_ARRAY_TABLE, TRUE);
            table.setProperty(IS_TOP_TABLE, FALSE);
            table.setProperty(ARRAY_TABLE_GROUP, key);
        }
        
        Iterator<Object> items = value.iterator();
        while(items.hasNext()) {
            Object item = items.next();
            if(item instanceof JsonObject) {
                JsonObject nestedJson = (JsonObject) item;
                for(String keyCol : nestedJson.getNames()) {
                    String arrayType = getDataType(nestedJson.get(keyCol));
                    if(table.getColumnByName(keyCol) != null) {
                        Column column = table.getColumnByName(keyCol);
                        if(!column.getDatatype().getName().equals(arrayType) && !column.getDatatype().getName().equals(OBJECT)) {
                            Datatype datatype = metadataFactory.getDataTypes().get(OBJECT);
                            column.setDatatype(datatype, true, 0);
                        }
                    } else {
                        Column column = metadataFactory.addColumn(keyCol, arrayType, table);
                        column.setUpdatable(true);
                    }
                }
            } else {
                String arrayType = getDataType(item);
                if (table.getColumnByName(key) != null) {
                    Column column = table.getColumnByName(key);
                    if(!column.getDatatype().getName().equals(arrayType) && !column.getDatatype().getName().equals(OBJECT)) {
                        Datatype datatype = metadataFactory.getDataTypes().get(OBJECT);
                        column.setDatatype(datatype, true, 0);
                    }
                } else {
                    Column column = metadataFactory.addColumn(key, arrayType, table);
                    column.setUpdatable(true);
                }
            }
        }
    }
    
    protected void addProcedures(MetadataFactory metadataFactory, CouchbaseConnection connection) {
        
        Procedure getTextDocuments = metadataFactory.addProcedure(GETTEXTDOCUMENTS);
        getTextDocuments.setAnnotation(CouchbasePlugin.Util.getString("getTextDocuments.Annotation")); //$NON-NLS-1$
        ProcedureParameter param = metadataFactory.addProcedureParameter("id", TypeFacility.RUNTIME_NAMES.STRING, Type.In, getTextDocuments); //$NON-NLS-1$
        param.setAnnotation(CouchbasePlugin.Util.getString("getTextDocuments.id.Annotation")); //$NON-NLS-1$
        metadataFactory.addProcedureResultSetColumn("id", TypeFacility.RUNTIME_NAMES.STRING, getTextDocuments); //$NON-NLS-1$
        metadataFactory.addProcedureResultSetColumn("result", TypeFacility.RUNTIME_NAMES.CLOB, getTextDocuments); //$NON-NLS-1$
        
        Procedure getDocuments = metadataFactory.addProcedure(GETDOCUMENTS);
        getDocuments.setAnnotation(CouchbasePlugin.Util.getString("getDocuments.Annotation")); //$NON-NLS-1$
        param = metadataFactory.addProcedureParameter("id", TypeFacility.RUNTIME_NAMES.STRING, Type.In, getDocuments); //$NON-NLS-1$
        param.setAnnotation(CouchbasePlugin.Util.getString("getDocuments.id.Annotation")); //$NON-NLS-1$
        metadataFactory.addProcedureResultSetColumn("result", TypeFacility.RUNTIME_NAMES.BLOB, getDocuments); //$NON-NLS-1$

        Procedure getTextDocument = metadataFactory.addProcedure(GETTEXTDOCUMENT);
        getTextDocument.setAnnotation(CouchbasePlugin.Util.getString("getTextDocument.Annotation")); //$NON-NLS-1$
        param = metadataFactory.addProcedureParameter("id", TypeFacility.RUNTIME_NAMES.STRING, Type.In, getTextDocument); //$NON-NLS-1$
        param.setAnnotation(CouchbasePlugin.Util.getString("getTextDocument.id.Annotation")); //$NON-NLS-1$
        metadataFactory.addProcedureResultSetColumn("id", TypeFacility.RUNTIME_NAMES.STRING, getTextDocument); //$NON-NLS-1$
        metadataFactory.addProcedureResultSetColumn("result", TypeFacility.RUNTIME_NAMES.CLOB, getTextDocument); //$NON-NLS-1$
        
        Procedure getDocument = metadataFactory.addProcedure(GETDOCUMENT);
        getDocument.setAnnotation(CouchbasePlugin.Util.getString("getDocument.Annotation")); //$NON-NLS-1$
        param = metadataFactory.addProcedureParameter("id", TypeFacility.RUNTIME_NAMES.STRING, Type.In, getDocument); //$NON-NLS-1$
        param.setAnnotation(CouchbasePlugin.Util.getString("getDocument.id.Annotation")); //$NON-NLS-1$
        metadataFactory.addProcedureResultSetColumn("result", TypeFacility.RUNTIME_NAMES.BLOB, getDocument); //$NON-NLS-1$
        
        Procedure saveDocument = metadataFactory.addProcedure(SAVEDOCUMENT);
        saveDocument.setAnnotation(CouchbasePlugin.Util.getString("saveDocument.Annotation")); //$NON-NLS-1$
        param = metadataFactory.addProcedureParameter("id", TypeFacility.RUNTIME_NAMES.STRING, Type.In, saveDocument); //$NON-NLS-1$
        param.setAnnotation(CouchbasePlugin.Util.getString("saveDocument.id.Annotation")); //$NON-NLS-1$
        param = metadataFactory.addProcedureParameter("document", TypeFacility.RUNTIME_NAMES.OBJECT, Type.In, saveDocument); //$NON-NLS-1$
        param.setAnnotation(CouchbasePlugin.Util.getString("saveDocument.document.Annotation")); //$NON-NLS-1$
        metadataFactory.addProcedureResultSetColumn("result", TypeFacility.RUNTIME_NAMES.CLOB, saveDocument); //$NON-NLS-1$
        
        Procedure deleteDocument = metadataFactory.addProcedure(DELETEDOCUMENT);
        deleteDocument.setAnnotation(CouchbasePlugin.Util.getString("deleteDocument.Annotation")); //$NON-NLS-1$
        param = metadataFactory.addProcedureParameter("id", TypeFacility.RUNTIME_NAMES.STRING, Type.In, deleteDocument); //$NON-NLS-1$
        param.setAnnotation(CouchbasePlugin.Util.getString("deleteDocument.id.Annotation")); //$NON-NLS-1$
        metadataFactory.addProcedureResultSetColumn("result", TypeFacility.RUNTIME_NAMES.CLOB, deleteDocument); //$NON-NLS-1$
        
        Procedure getTextMetadataDocument = metadataFactory.addProcedure(GETTEXTMETADATADOCUMENT);
        getTextMetadataDocument.setAnnotation(CouchbasePlugin.Util.getString("getTextMetadataDocument.Annotation")); //$NON-NLS-1$
        metadataFactory.addProcedureResultSetColumn("result", TypeFacility.RUNTIME_NAMES.CLOB, getTextMetadataDocument); //$NON-NLS-1$
        
        Procedure getMetadataDocument = metadataFactory.addProcedure(GETMETADATADOCUMENT);
        getMetadataDocument.setAnnotation(CouchbasePlugin.Util.getString("getMetadataDocument.Annotation")); //$NON-NLS-1$
        metadataFactory.addProcedureResultSetColumn("result", TypeFacility.RUNTIME_NAMES.BLOB, getMetadataDocument); //$NON-NLS-1$
        
    }

    private String getDataType(Object value) {
                
        if (value instanceof String) {
            return STRING;
        } else if (value instanceof Integer) {
            return INTEGER;
        } else if (value instanceof Double) {
            return DOUBLE;
        } else if (value instanceof Boolean) {
            return BOOLEAN;
        } else if (value instanceof Short) {
            return SHORT;
        } else if (value instanceof Byte) {
            return BYTE;
        } else if (value instanceof Long) {
            return LONG;
        } else if (value instanceof Float) {
            return FLOAT;
        }
        return OBJECT;
    }
    
    private String buildN1ql(String keyspace) {
        return "SELECT json FROM " + buildNameInSource(keyspace, null) + " as json"; //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    protected static String buildNameInSource(String path, String parentPath) {
        StringBuilder sb = new StringBuilder();
        if(parentPath != null) {
            sb.append(parentPath);
            sb.append(DOT);
        }
        sb.append(WAVE);
        sb.append(path);
        sb.append(WAVE);
        String nameInSource = sb.toString();
        return nameInSource;
    }
    
    public static String buildPlaceholder(int i) {
        return PLACEHOLDER + i; 
    }
   
}
