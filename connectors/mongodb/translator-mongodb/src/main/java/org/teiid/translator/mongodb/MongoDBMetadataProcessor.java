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
package org.teiid.translator.mongodb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.bson.types.Binary;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Column;
import org.teiid.metadata.Column.SearchType;
import org.teiid.metadata.ExtensionMetadataProperty;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.mongodb.MongoDBConnection;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.TranslatorProperty.PropertyType;
import org.teiid.translator.TypeFacility;
import org.teiid.util.FullyQualifiedName;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBRef;
import com.mongodb.MongoException;

public class MongoDBMetadataProcessor implements MetadataProcessor<MongoDBConnection> {

    @ExtensionMetadataProperty(applicable=Table.class, datatype=String.class, display="Merge Into Table", description="Declare the name of table that this table needs to be merged into. No separate copy maintained")
    public static final String MERGE = MetadataFactory.MONGO_PREFIX+"MERGE"; //$NON-NLS-1$

    @ExtensionMetadataProperty(applicable=Table.class, datatype=String.class, display="Embedded Into Table", description="Declare the name of table that this table needs to be embedded into. A separate copy is also maintained")
    public static final String EMBEDDABLE = MetadataFactory.MONGO_PREFIX+"EMBEDDABLE"; //$NON-NLS-1$

    static final String ID = "_id"; //$NON-NLS-1$
    private static final String TOP_LEVEL_DOC = "TOP_LEVEL_DOC"; //$NON-NLS-1$
    private static final String ASSOSIATION = "ASSOSIATION"; //$NON-NLS-1$

    private static Set<String> STRING_COMPATIBLE_TYPES = new HashSet<String>(Arrays.asList(TypeFacility.RUNTIME_NAMES.INTEGER,
            TypeFacility.RUNTIME_NAMES.DOUBLE,
            TypeFacility.RUNTIME_NAMES.BOOLEAN,
            TypeFacility.RUNTIME_NAMES.LONG,
            TypeFacility.RUNTIME_NAMES.STRING,
            TypeFacility.RUNTIME_NAMES.TIMESTAMP));

    private Pattern excludeTables;
    private Pattern includeTables;

    private int sampleSize = 1;

    private boolean fullEmbeddedNames;

    @Override
    public void process(MetadataFactory metadataFactory, MongoDBConnection connection) throws TranslatorException {
        DB db = connection.getDatabase();
        for (String tableName:db.getCollectionNames()) {

            if (getExcludeTables() != null && shouldExclude(tableName)) {
                continue;
            }

            if (getIncludeTables() != null && !shouldInclude(tableName)) {
                continue;
            }

            try {
                DBCollection collection = db.getCollection(tableName);
                DBCursor cursor = collection.find();
                while(cursor.hasNext()) {
                    BasicDBObject row = (BasicDBObject)cursor.next();
                    if (row == null) {
                        continue;
                    }
                    Table table = addTable(metadataFactory, tableName, row, null);
                    if (table != null) {
                        // top level documents can not be seen as merged
                        table.setProperty(TOP_LEVEL_DOC, String.valueOf(Boolean.TRUE));
                    }
                    if (cursor.numSeen() >= sampleSize) {
                        break;
                    }
                }
                cursor.close();
            } catch (MongoException e) {
                LogManager.logWarning(LogConstants.CTX_CONNECTOR, MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18037, e));
            }
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

        //prevent the creation of tables without columns
        for (Table table : new HashSet<Table>(metadataFactory.getSchema().getTables().values())) {
            if (table.isPhysical() && (table.getColumns() == null || table.getColumns().isEmpty())) {
                metadataFactory.getSchema().removeTable(table.getName());
            }
        }
    }

    private Table addTable(MetadataFactory metadataFactory, String tableName, BasicDBObject row, Table parent) {
        Table table = metadataFactory.getSchema().getTable(tableName);
        Set<String> keys = row.keySet();
        if (keys != null && !keys.isEmpty()) {
            String fqnString = null;
            if (parent != null) {
                FullyQualifiedName rn = new FullyQualifiedName("embedded", tableName); //$NON-NLS-1$
                String parentfqn = parent.getProperty(FQN, false);
                fqnString = parentfqn + FullyQualifiedName.SEPARATOR + rn.toString();
                if (fullEmbeddedNames) {
                    tableName = parent.getName() + "_" + tableName; //$NON-NLS-1$
                }
            } else {
                FullyQualifiedName fqn = new FullyQualifiedName("collection", tableName); //$NON-NLS-1$
                fqnString = fqn.toString();
            }

            if (table != null && !fqnString.equals(table.getProperty(FQN))) {
                table = null;
                //the user will need to turn on duplicate renaming
            }

            if (table == null) {
                table = metadataFactory.addTable(tableName);
                table.setSupportsUpdate(true);
                table.setProperty(FQN, fqnString);
            }

            for (String columnKey:keys) {
                Object value = row.get(columnKey);

                Column column = addColumn(metadataFactory, table, columnKey, value);

                if (column != null) {
                    column.setUpdatable(true);
                }
            }
            return table;
        }
        return null;
    }

    private Column addColumn(MetadataFactory metadataFactory, Table table, String columnKey, Object value) {
        Column column = null;

        if (value instanceof BasicDBObject) {
            if (columnKey.equals(ID)) {
                ArrayList<String> columns = new ArrayList<String>();
                BasicDBObject compositeKey = (BasicDBObject)value;
                for (String key:compositeKey.keySet()) {
                    column = addColumn(metadataFactory, table, key, compositeKey.get(key));
                    column.setUpdatable(true);
                    columns.add(key);
                }
                metadataFactory.addPrimaryKey("PK0", columns, table); //$NON-NLS-1$
            } else {
                // embedded doc - one to one
                Table childTable = addTable(metadataFactory, columnKey, (BasicDBObject)value, table);
                if (childTable != null) {
                    childTable.setProperty(MERGE, table.getName());
                    childTable.setProperty(ASSOSIATION, MergeDetails.Association.ONE.name());
                }
            }
        }
        else if (value instanceof BasicDBList) {
            // embedded doc, list one to many
            BasicDBList basicDBList = (BasicDBList)value;
            if (!basicDBList.isEmpty()) {
                if (basicDBList.get(0) instanceof BasicDBObject) {
                    Table childTable = addTable(metadataFactory, columnKey, (BasicDBObject)basicDBList.get(0), table);
                    if (childTable != null) {
                        childTable.setProperty(MERGE, table.getName());
                        childTable.setProperty(ASSOSIATION, MergeDetails.Association.MANY.name());
                    }
                }
                else {
                    column = table.getColumnByName(columnKey);
                    Object nestedValue = basicDBList.get(0);
                    String dataType = getDataType(nestedValue)+"[]"; //$NON-NLS-1$
                    if (column == null) {
                        column = metadataFactory.addColumn(columnKey, dataType, table);
                        setNativeType(column, nestedValue);
                    } else if (!column.getRuntimeType().equals(dataType)) {
                        //type conflict
                        MetadataFactory.setDataType(TypeFacility.RUNTIME_NAMES.OBJECT, column, metadataFactory.getDataTypes(), false);
                        column.setNativeType(null);
                    }
                    column.setSearchType(SearchType.Unsearchable);
                }
            }
        }
        else if (value instanceof DBRef) {
            Object obj = ((DBRef)value).getId();
            column = addColumn(metadataFactory, table, columnKey, obj);
            //String ref = ((DBRef)value).getCollectionName();
            //metadataFactory.addForeignKey("FK_"+columnKey, Arrays.asList(columnKey), ref, table); //$NON-NLS-1$
        }
        else {
            column = table.getColumnByName(columnKey);
            String dataType = getDataType(value);
            if (column == null) {
                column = metadataFactory.addColumn(columnKey, dataType, table);
                setNativeType(column, value);
            } else if (!column.getRuntimeType().equals(getDataType(value))) {
                //type conflict
                if (STRING_COMPATIBLE_TYPES.contains(column.getRuntimeType()) && STRING_COMPATIBLE_TYPES.contains(dataType)) {
                    MetadataFactory.setDataType(TypeFacility.RUNTIME_NAMES.STRING, column, metadataFactory.getDataTypes(), false);
                } else {
                    MetadataFactory.setDataType(TypeFacility.RUNTIME_NAMES.OBJECT, column, metadataFactory.getDataTypes(), false);
                }
                column.setNativeType(null);
                column.setSearchType(SearchType.Unsearchable);
            }
        }

        // create a PK out of _id
        if (columnKey.equals(ID) && !(value instanceof BasicDBObject)) {
            metadataFactory.addPrimaryKey("PK0", Arrays.asList(ID), table); //$NON-NLS-1$
        }
        return column;
    }

    private void addForeignKey(MetadataFactory metadataFactory, Table childTable, Table table) {
        MergeDetails.Association association = MergeDetails.Association.valueOf(childTable.getProperty(ASSOSIATION, false));
        childTable.setProperty(ASSOSIATION, null);
        KeyRecord record = table.getPrimaryKey();
        if (record == null) {
            return;
        }
        if (association == MergeDetails.Association.ONE) {
            ArrayList<String> pkColumns = new ArrayList<String>();
            for (Column column:record.getColumns()) {
                Column c = childTable.getColumnByName(column.getName());
                if (c == null) {
                    c = metadataFactory.addColumn(column.getName(), column.getRuntimeType(), childTable);
                }
                pkColumns.add(c.getName());
            }
            metadataFactory.addPrimaryKey("PK0", pkColumns, childTable); //$NON-NLS-1$
            metadataFactory.addForeignKey("FK0", pkColumns, table.getName(), childTable); //$NON-NLS-1$
        }
        else {
            ArrayList<String> pkColumns = new ArrayList<String>();
            for (Column column:record.getColumns()) {
                Column c = childTable.getColumnByName(table.getName()+"_"+column.getName()); //$NON-NLS-1$
                if (c == null) {
                    c = metadataFactory.addColumn(table.getName()+"_"+column.getName(), column.getRuntimeType(), childTable); //$NON-NLS-1$
                }
                pkColumns.add(c.getName());
            }
            metadataFactory.addForeignKey("FK0", pkColumns, table.getName(), childTable); //$NON-NLS-1$
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
        else if (value instanceof org.bson.types.ObjectId ) {
            return TypeFacility.RUNTIME_NAMES.STRING;
        }
        else {
            return TypeFacility.RUNTIME_NAMES.OBJECT;
        }
    }

    private void setNativeType(Column column, Object value) {
        if (value instanceof Binary ) {
            column.setNativeType(Binary.class.getName());
        }
        else if (column.getName().equals("_id") && value instanceof org.bson.types.ObjectId ) { //$NON-NLS-1$
            column.setNativeType(org.bson.types.ObjectId.class.getName());
            column.setAutoIncremented(true);
        }
    }

    @TranslatorProperty(display="Exclude Tables", category=PropertyType.IMPORT, description="A case-insensitive regular expression that when matched against a fully qualified Teiid table name will exclude it from import.  Applied after table names are retrieved.  Use a negative look-ahead (?!<inclusion pattern>).* to act as an inclusion filter.")
    public String getExcludeTables() {
        if (this.excludeTables == null) {
            return null;
        }
        return this.excludeTables.pattern();
    }

    protected boolean shouldExclude(String fullName) {
        return excludeTables != null && excludeTables.matcher(fullName).matches();
    }

    public void setExcludeTables(String excludeTables) {
        this.excludeTables = Pattern.compile(excludeTables, Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
    }

    @TranslatorProperty(display="Include Tables", category=PropertyType.IMPORT, description="A case-insensitive regular expression that when matched against a fully qualified Teiid table name will include it from import.  Applied after table names are retrieved.  Use a negative look-ahead (?!<inclusion pattern>).* to act as an exclusion filter")
    public String getIncludeTables() {
        if (this.includeTables == null) {
            return null;
        }
        return this.includeTables.pattern();
    }

    protected boolean shouldInclude(String fullName) {
        return includeTables != null && includeTables.matcher(fullName).matches();
    }

    public void setIncludeTables(String tableNamePattern) {
        this.includeTables = Pattern.compile(tableNamePattern, Pattern.DOTALL | Pattern.CASE_INSENSITIVE);;
    }

    @TranslatorProperty(display="Sample Size", category=PropertyType.IMPORT, description="The number of top level documents of a given collection name to sample.")
    public int getSampleSize() {
        return sampleSize;
    }

    public void setSampleSize(int sampleSize) {
        this.sampleSize = sampleSize;
    }

    @TranslatorProperty(display="Full Embedded Names", category=PropertyType.IMPORT, description="Whether to prefix embedded table names with their parents, e.g. parent_embedded")
    public boolean isFullEmbeddedNames() {
        return this.fullEmbeddedNames;
    }

    public void setFullEmbeddedNames(boolean fullEmbeddedNames) {
        this.fullEmbeddedNames = fullEmbeddedNames;
    }
}
