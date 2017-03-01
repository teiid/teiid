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

import java.util.Iterator;

import org.teiid.couchbase.CouchbaseConnection;
import org.teiid.metadata.Column;
import org.teiid.metadata.Datatype;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorException;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQueryRow;

public class CouchbaseMetadataProcessor implements MetadataProcessor<CouchbaseConnection> {
    
    public static final String ID = "id"; //$NON-NLS-1$
    public static final String JSON = "json"; //$NON-NLS-1$
    
    public static final String WAVE = "`"; //$NON-NLS-1$
    public static final String DOT = "."; //$NON-NLS-1$
    
    public static final String LINE = "_"; //$NON-NLS-1$
    public static final String IS_TOP_TABLE = "isTopTable"; //$NON-NLS-1$
    public static final String IS_ARRAY_TABLE = "isArrayTable"; //$NON-NLS-1$
    public static final String TRUE = "true"; //$NON-NLS-1$
    public static final String FALSE = "false"; //$NON-NLS-1$

    @Override
    public void process(MetadataFactory metadataFactory, CouchbaseConnection connection) throws TranslatorException {
       
        String keyspace = connection.getKeyspaceName();
        Iterator<N1qlQueryRow> result = connection.executeQuery(buildMetaN1ql(keyspace)).rows();
        while(result.hasNext()) {
            N1qlQueryRow row = result.next();
            JsonObject doc = row.value().getObject(JSON);
            addTable(metadataFactory, keyspace, doc, null);       
        }
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
//            metadataFactory.addColumn(ID, STRING, table);
            table.setSupportsUpdate(true);
            if(parent == null) {
                // Base on N1QL, the top Table map to keyspace in Couchbase, each rows represent a document in keyspace. 
                // The top Table have a unique primary key.
                table.setNameInSource(buildNameInSource(key, null));
                table.setProperty(IS_TOP_TABLE, TRUE);
//                metadataFactory.addPrimaryKey("PK0", Arrays.asList(ID), table); //$NON-NLS-1$
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

    /**
     * JsonArray be map to a one Column Table.
     * If all array items are same type, the mapped Table column type use this type, 
     * else, the Table column type use Object.
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
            table.setSupportsUpdate(true);
//            metadataFactory.addColumn(ID, STRING, table);
            table.setNameInSource(buildNameInSource(key, parent.getNameInSource()));
            table.setProperty(IS_ARRAY_TABLE, TRUE);
            table.setProperty(IS_TOP_TABLE, FALSE);
        }
        
        Iterator<Object> items = value.iterator();
        while(items.hasNext()) {
            Object item = items.next();
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

    private String buildMetaN1ql(String keyspace) {
        return "SELECT META(json).id as id, json FROM " + buildNameInSource(keyspace, null) + " as json"; //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    private String buildNameInSource(String path, String parentPath) {
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
   
}
