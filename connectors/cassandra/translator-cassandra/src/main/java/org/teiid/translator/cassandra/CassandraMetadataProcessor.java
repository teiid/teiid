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

package org.teiid.translator.cassandra;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.core.util.StringUtil;
import org.teiid.metadata.Column;
import org.teiid.metadata.Column.SearchType;
import org.teiid.metadata.ExtensionMetadataProperty;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.cassandra.CassandraExecutionFactory.Event;
import org.teiid.util.FullyQualifiedName;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType.Name;
import com.datastax.driver.core.IndexMetadata;
import com.datastax.driver.core.TableMetadata;

public class CassandraMetadataProcessor implements MetadataProcessor<CassandraConnection>{

    @ExtensionMetadataProperty(applicable=Table.class, datatype=Boolean.class, display="Allow Filtering", description="This is to avoid the warning from Cassandra when it might not be able to execute the query in an efficient way", required=false)
    public static final String ALLOWFILTERING = "ALLOWFILTERING";

    /**
     * Creates metadata from all column families in current keyspace.
     */
    public void process(MetadataFactory factory, CassandraConnection connection) throws TranslatorException {
        try {
            for (TableMetadata columnFamily : connection.keyspaceInfo().getTables()){
                addTable(factory, columnFamily);
            }
        } catch (TranslatorException e) {
            throw new TranslatorException(Event.TEIID22000, e, CassandraExecutionFactory.UTIL.gs(Event.TEIID22000));
        }
    }

    /**
     * Adds table.
     * @param columnFamily
     */
    private void addTable(MetadataFactory factory, TableMetadata columnFamily) {
        Table table = factory.addTable(columnFamily.getName());
        table.setNameInSource(quoteName(table.getName()));
        addColumnsToTable(factory, table, columnFamily);
        addPrimaryKey(factory, table, columnFamily);
        for (IndexMetadata index : columnFamily.getIndexes()) {
            Column c = table.getColumnByName(index.getTarget());
            if (c != null) {
                c.setSearchType(SearchType.Searchable);
                factory.addIndex(index.getName(), false, Arrays.asList(index.getTarget()), table);
            }
        }
        table.setSupportsUpdate(true);

        FullyQualifiedName fqn = new FullyQualifiedName();
        fqn.append("keyspace", columnFamily.getKeyspace().getName()); //$NON-NLS-1$
        fqn.append("table", columnFamily.getName()); //$NON-NLS-1$
        table.setProperty(FQN, fqn.toString());
    }

    protected String quoteName(String name) {
        return "\"" + StringUtil.replaceAll(name, "\"", "\"\"") + "\""; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    /**
     * Adds a primary key from columnFamily to given table.
     * @param table            Teiid table
     * @param columnFamily
     */
    private void addPrimaryKey(MetadataFactory factory, Table table, TableMetadata columnFamily) {
        List<ColumnMetadata> primaryKeys = columnFamily.getPrimaryKey();
        List<String> names = new ArrayList<String>();

        for (ColumnMetadata columnName : primaryKeys){
            names.add(columnName.getName());
            table.getColumnByName(columnName.getName()).setSearchType(SearchType.Searchable);
        }
        factory.addPrimaryKey("PK_" + columnFamily.getName(), names, table); //$NON-NLS-1$
    }

    /**
     * Adds all columns of column family.
     * @param table            Teiid table
     * @param columnFamily    Column family
     */
    private void addColumnsToTable(MetadataFactory factory, Table table, TableMetadata columnFamily) {
        for (ColumnMetadata column : columnFamily.getColumns()){
            String type = asTeiidRuntimeType(column.getType().getName());
            Column c = factory.addColumn(column.getName(), type, table);
            c.setUpdatable(true);
            c.setSearchType(SearchType.Unsearchable);
            c.setNameInSource(quoteName(c.getName()));
        }
    }

    private String asTeiidRuntimeType(Name name) {

        switch(name) {
            case ASCII:
                return TypeFacility.RUNTIME_NAMES.STRING;
            case BIGINT:
                return TypeFacility.RUNTIME_NAMES.LONG;
            case BLOB:
                return TypeFacility.RUNTIME_NAMES.BLOB;
            case BOOLEAN:
                return TypeFacility.RUNTIME_NAMES.BOOLEAN;
            case COUNTER:
                return TypeFacility.RUNTIME_NAMES.LONG;
            case DECIMAL:
                return TypeFacility.RUNTIME_NAMES.BIG_DECIMAL;
            case DOUBLE:
                return TypeFacility.RUNTIME_NAMES.DOUBLE;
            case FLOAT:
                return TypeFacility.RUNTIME_NAMES.FLOAT;
            case INET:
                return TypeFacility.RUNTIME_NAMES.OBJECT;
            case INT:
                return TypeFacility.RUNTIME_NAMES.INTEGER;
            case TEXT:
                return TypeFacility.RUNTIME_NAMES.STRING;
            case UUID:
                return TypeFacility.RUNTIME_NAMES.OBJECT;
            case VARCHAR:
                return TypeFacility.RUNTIME_NAMES.STRING;
            case VARINT:
                return TypeFacility.RUNTIME_NAMES.BIG_INTEGER;
            case LIST:
                return TypeFacility.RUNTIME_NAMES.OBJECT;
            case SET:
                return TypeFacility.RUNTIME_NAMES.OBJECT;
            case MAP:
                return TypeFacility.RUNTIME_NAMES.OBJECT;
            case CUSTOM:
                return TypeFacility.RUNTIME_NAMES.BLOB;
            case UDT:
                return TypeFacility.RUNTIME_NAMES.OBJECT;
            case TUPLE:
                return TypeFacility.RUNTIME_NAMES.OBJECT;
            case SMALLINT:
                return TypeFacility.RUNTIME_NAMES.SHORT;
            case TINYINT:
                return TypeFacility.RUNTIME_NAMES.BYTE;
            case TIMEUUID:
                return TypeFacility.RUNTIME_NAMES.OBJECT;
            case TIMESTAMP:
                return TypeFacility.RUNTIME_NAMES.TIMESTAMP;
            case DATE:
                return TypeFacility.RUNTIME_NAMES.DATE;
            case TIME:
                //due to nano-second precision
                return TypeFacility.RUNTIME_NAMES.TIMESTAMP;
            default:
                return TypeFacility.RUNTIME_NAMES.OBJECT;
        }
    }
}
