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
package org.teiid.translator.mongodb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

import org.bson.types.Binary;
import org.teiid.metadata.*;
import org.teiid.metadata.Column.SearchType;
import org.teiid.mongodb.MongoDBConnection;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;

import com.mongodb.*;

public class MongoDBMetadataProcessor implements MetadataProcessor<MongoDBConnection> {

    @ExtensionMetadataProperty(applicable=Table.class, datatype=String.class, display="Merge Into Table", description="Declare the name of table that this table needs to be merged into. No separate copy maintained")
    public static final String MERGE = MetadataFactory.MONGO_URI+"MERGE"; //$NON-NLS-1$

    @ExtensionMetadataProperty(applicable=Table.class, datatype=String.class, display="Embedded Into Table", description="Declare the name of table that this table needs to be embedded into. A separate copy is also maintained")
    public static final String EMBEDDABLE = MetadataFactory.MONGO_URI+"EMBEDDABLE"; //$NON-NLS-1$

    private static final String ID = "_id"; //$NON-NLS-1$
    private static final String TOP_LEVEL_DOC = "TOP_LEVEL_DOC"; //$NON-NLS-1$
    private static final String ASSOSIATION = "ASSOSIATION"; //$NON-NLS-1$

    @Override
    public void process(MetadataFactory metadataFactory, MongoDBConnection connection) throws TranslatorException {
        DB db = connection.getDatabase();
        for (String tableName:db.getCollectionNames()) {
            
            DBCollection rows = db.getCollection(tableName);
            BasicDBObject row = (BasicDBObject)rows.findOne();
            Table table = addTable(metadataFactory, tableName, row);

            // top level documents can not be seen as merged
            table.setProperty(TOP_LEVEL_DOC, String.valueOf(Boolean.TRUE));
        }

        for (Table table:metadataFactory.getSchema().getTables().values()) {
            String merge = table.getProperty(MERGE, false);
            if (merge != null) {
                addForeignKey(metadataFactory, table, metadataFactory.getSchema().getTable(merge));
            }
        }
        
        for (Table table:metadataFactory.getSchema().getTables().values()) {
            String top = table.getProperty(TOP_LEVEL_DOC, false);
            String merge = table.getProperty(MERGE, false);
            if ( top != null) {
                table.setProperty(TOP_LEVEL_DOC, null);
                if (merge != null) {
                    table.setProperty(MERGE, null);
                    table.setProperty(EMBEDDABLE, "true"); //$NON-NLS-1$
                }
            }
        }
    }

    private Table addTable(MetadataFactory metadataFactory, String tableName, BasicDBObject row) {
        if (metadataFactory.getSchema().getTable(tableName) != null) {
            Table t = metadataFactory.getSchema().getTable(tableName);
            return t;
        }
        
        Table table = metadataFactory.addTable(tableName);
        table.setSupportsUpdate(true);
        
        for (String columnKey:row.keySet()) {
            Object value = row.get(columnKey);
            
            Column column = addColumn(metadataFactory, table, columnKey, value);
            
            if (column != null) {
                column.setUpdatable(true);
            }
        }
        return table;
    }

    private Column addColumn(MetadataFactory metadataFactory, Table table, String columnKey, Object value) {
        Column column = null;
        
        if (columnKey.equals(ID)) {
            if (value instanceof BasicDBObject) {
                BasicDBObject compositeKey = (BasicDBObject)value;
                for (String key:compositeKey.keySet()) {                    
                    column = addColumn(metadataFactory, table, key, compositeKey.get(key));  
                    column.setUpdatable(true);
                }                
            }
        }
        
        if (!columnKey.equals(ID) && value instanceof BasicDBObject) {
            // embedded doc - one to one
            Table childTable = addTable(metadataFactory, columnKey, (BasicDBObject)value);
            childTable.setProperty(MERGE, table.getName());
            childTable.setProperty(ASSOSIATION, MutableDBRef.Association.ONE.name()); 
        }
        else if (value instanceof BasicDBList) {
            // embedded doc, list one to many
            if (((BasicDBList)value).get(0) instanceof BasicDBObject) {
                Table childTable = addTable(metadataFactory, columnKey, (BasicDBObject)((BasicDBList)value).get(0));
                childTable.setProperty(MERGE, table.getName());
                childTable.setProperty(ASSOSIATION, MutableDBRef.Association.MANY.name());
            }
            else {
                column = metadataFactory.addColumn(columnKey, TypeFacility.RUNTIME_NAMES.OBJECT+"[]", table); //$NON-NLS-1$
                column.setSearchType(SearchType.Unsearchable);
            }                
        }
        else if (value instanceof DBRef) {
            Object obj = ((DBRef)value).getId();
            column = addColumn(metadataFactory, table, columnKey, obj);
            String ref = ((DBRef)value).getRef();
            metadataFactory.addForiegnKey("FK_"+columnKey, Arrays.asList(columnKey), ref, table); //$NON-NLS-1$
        }
        else {
            column = metadataFactory.addColumn(columnKey, getDataType(value), table);
        }
        
        // create a PK out of _id
        if (columnKey.equals(ID)) { 
            if (value instanceof BasicDBObject) {
                BasicDBObject compositeKey = (BasicDBObject)value;
                ArrayList<String> columns = new ArrayList<String>();
                for (String key:compositeKey.keySet()) {                    
                    columns.add(key);                    
                }                
                metadataFactory.addPrimaryKey("PK0", columns, table); //$NON-NLS-1$
            }
            else {
                metadataFactory.addPrimaryKey("PK0", Arrays.asList(ID), table); //$NON-NLS-1$
            }
        }
        return column;
    }
    
    private void addForeignKey(MetadataFactory metadataFactory, Table childTable, Table table) {
        MutableDBRef.Association association = MutableDBRef.Association.valueOf(childTable.getProperty(ASSOSIATION, false));
        childTable.setProperty(ASSOSIATION, null);
        if (association == MutableDBRef.Association.ONE) {
            KeyRecord record = table.getPrimaryKey();
            if (record != null) {
                ArrayList<String> pkColumns = new ArrayList<String>(); 
                for (Column column:record.getColumns()) {
                    Column c = metadataFactory.getSchema().getTable(childTable.getName()).getColumnByName(column.getName());
                    if (c == null) {
                        c = metadataFactory.addColumn(column.getName(), column.getRuntimeType(), childTable);
                    }
                    pkColumns.add(c.getName());
                }
                metadataFactory.addPrimaryKey("PK0", pkColumns, childTable); //$NON-NLS-1$
            }
        }
        else {
            KeyRecord record = table.getPrimaryKey();
            if (record != null) {
                ArrayList<String> pkColumns = new ArrayList<String>(); 
                for (Column column:record.getColumns()) {
                    Column c = metadataFactory.getSchema().getTable(childTable.getName()).getColumnByName(table.getName()+"_"+column.getName()); //$NON-NLS-1$
                    if (c == null) {
                        c = metadataFactory.addColumn(table.getName()+"_"+column.getName(), column.getRuntimeType(), childTable); //$NON-NLS-1$
                    }
                    pkColumns.add(c.getName());
                }
                metadataFactory.addForiegnKey("FK0", pkColumns, table.getName(), childTable); //$NON-NLS-1$                
            }            
        }
    }

    private String getDataType(Object value) {
        if (value instanceof Integer) {
            return TypeFacility.RUNTIME_NAMES.INTEGER;
        }
        else if (value instanceof Double) {
            return TypeFacility.RUNTIME_NAMES.DOUBLE;
        }
        else if (value instanceof Boolean) {
            return TypeFacility.RUNTIME_NAMES.BOOLEAN;
        }
        else if (value instanceof Long) {
            return TypeFacility.RUNTIME_NAMES.LONG;
        }                
        else if (value instanceof String) {
            return TypeFacility.RUNTIME_NAMES.STRING;
        }
        else if (value instanceof Date) {
            return TypeFacility.RUNTIME_NAMES.TIMESTAMP;
        } 
        else if (value instanceof Binary || value instanceof byte[]) {
            return TypeFacility.RUNTIME_NAMES.VARBINARY;
        }
        else {
            return TypeFacility.RUNTIME_NAMES.OBJECT;
        }        
    }
}
