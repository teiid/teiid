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
package org.teiid.translator.accumulo;

import java.util.Arrays;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.teiid.metadata.Column;
import org.teiid.metadata.Column.SearchType;
import org.teiid.metadata.ExtensionMetadataProperty;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.TranslatorProperty.PropertyType;
import org.teiid.translator.TypeFacility;

public class AccumuloMetadataProcessor implements MetadataProcessor<AccumuloConnection> {
    @ExtensionMetadataProperty(applicable=Column.class, datatype=String.class, display="Column Family", description="Column Familiy from the Key", required=true)
    public static final String CF = MetadataFactory.ACCUMULO_PREFIX+"CF"; //$NON-NLS-1$

    @ExtensionMetadataProperty(applicable=Column.class, datatype=String.class, display="Column Qualifier", description="If Column Qualifier from key makes the key value unique, then this is required")
    public static final String CQ = MetadataFactory.ACCUMULO_PREFIX+"CQ"; //$NON-NLS-1$

    @ExtensionMetadataProperty(applicable=Column.class, datatype=String.class, display="Value In", description="The value of key exists in Column Qualifier or Value slot; Default is VALUE, if value is in CQ then this property is required", allowed= "CQ,VALUE")
    public static final String VALUE_IN = MetadataFactory.ACCUMULO_PREFIX+"VALUE-IN"; //$NON-NLS-1$

    // allowed patterns {CF}, {CQ}, {VALUE}, {ROWID}
    public static final String DEFAULT_COLUMN_NAME_PATTERN = "{CF}_{CQ}"; //$NON-NLS-1$
    public static final String DEFAULT_VALUE_PATTERN = "{VALUE}"; //$NON-NLS-1$
    public static final String ROWID = "rowid"; //$NON-NLS-1$
    public enum ValueIn{CQ,VALUE};

    private String columnNamePattern = DEFAULT_COLUMN_NAME_PATTERN;
    private String valueIn = DEFAULT_VALUE_PATTERN;

    public void process(MetadataFactory mf, AccumuloConnection conn) {
        Connector connector = conn.getInstance();

        Set<String> tableNames = connector.tableOperations().list();
        for (String tableName:tableNames) {
            try {

                if (tableName.equals("!METADATA") //$NON-NLS-1$
                        || tableName.equals("trace") //$NON-NLS-1$
                        || tableName.startsWith("accumulo.")) { //$NON-NLS-1$
                    continue;
                }

                Text previousRow = null;
                Table table = null;
                Scanner scanner = connector.createScanner(tableName, conn.getAuthorizations());
                for (Entry<Key, Value> entry : scanner) {
                    Key key = entry.getKey();
                    Text cf = key.getColumnFamily();
                    Text cq = key.getColumnQualifier();
                    Text row = key.getRow();
                    if (previousRow == null || previousRow.equals(row)) {
                        previousRow = row;
                        if (mf.getSchema().getTable(tableName) == null) {
                            table = mf.addTable(tableName);
                            Column column = mf.addColumn(AccumuloMetadataProcessor.ROWID, TypeFacility.RUNTIME_NAMES.STRING, table);
                            column.setSearchType(SearchType.All_Except_Like);
                            mf.addPrimaryKey("PK0", Arrays.asList(AccumuloMetadataProcessor.ROWID), table); //$NON-NLS-1$
                            column.setUpdatable(false);
                        }
                        else {
                            table = mf.getSchema().getTable(tableName);
                        }
                        Column column = mf.addColumn(buildColumnName(cf, cq, row), TypeFacility.RUNTIME_NAMES.STRING, table);
                        column.setSearchType(SearchType.All_Except_Like);
                        column.setProperty(CF, cf.toString());
                        column.setProperty(CQ, cq.toString());
                        column.setProperty(VALUE_IN, getValueIn());
                        column.setUpdatable(true);
                    }
                    else {
                        break;
                    }
                }
                scanner.close();
                if (table != null) {
                    table.setSupportsUpdate(true);
                }
            } catch (TableNotFoundException e) {
                continue;
            }
        }
    }

    private String buildColumnName(Text cf, Text cq, Text rowid) {
        String pattern = getColumnNamePattern();
        pattern = pattern.replace("{CF}", cf.toString()); //$NON-NLS-1$
        pattern = pattern.replace("{CQ}", cq.toString()); //$NON-NLS-1$
        pattern = pattern.replace("{ROWID}", rowid.toString()); //$NON-NLS-1$
        return pattern;
    }

    @TranslatorProperty(display="Column Name Pattern", category=PropertyType.IMPORT, description="Pattern to derive column names from, available expressions to use({CF}, {CQ}, {ROW_ID}")
    public String getColumnNamePattern() {
        return columnNamePattern;
    }

    public void setColumnNamePattern(String columnNamePattern) {
        this.columnNamePattern = columnNamePattern;
    }

    @TranslatorProperty(display="Value In", category=PropertyType.IMPORT, description="Defines where the data value of property is in {VALUE} or {CQ}")
    public String getValueIn() {
        return valueIn;
    }

    public void setValueIn(String valueIn) {
        this.valueIn = valueIn;
    }
}
