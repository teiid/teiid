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
package org.teiid.translator.infinispan.hotrod;

import static org.teiid.language.SQLConstants.Tokens.*;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.teiid.infinispan.api.InfinispanConnection;
import org.teiid.infinispan.api.ProtobufDataManager;
import org.teiid.infinispan.api.ProtobufResource;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;

public class SchemaToProtobufProcessor {
    private static final String NL = "\n";//$NON-NLS-1$
    private static final String OPEN_CURLY = "{";//$NON-NLS-1$
    private static final String CLOSE_CURLY = "}";//$NON-NLS-1$
    private static final String TAB = "    "; //$NON-NLS-1$

    private StringBuilder buffer = new StringBuilder();
    private TreeMap<String, List<Column>> processLater = new TreeMap<>();
    private Schema schema;
    private boolean indexMessages = false;

    public ProtobufResource process(MetadataFactory metadataFactory, InfinispanConnection connection)
            throws TranslatorException {

        this.schema = metadataFactory.getSchema();
        buffer.append("package ").append(schema.getName()).append(";");
        buffer.append(NL);
        buffer.append(NL);

        for (Table table : schema.getTables().values()) {
            visit(table);
            buffer.append(NL);
            buffer.append(NL);
        }

        for (String name : this.processLater.keySet()) {
            visitTable(name, this.processLater.get(name));
        }

        return new ProtobufResource(schema.getName() + ".proto", buffer.toString());
    }

    private void addTab() {
        buffer.append(TAB);
    }

    private void visit(Table table) {
        if (table.getAnnotation() != null) {
            buffer.append("/* ").append(table.getAnnotation()).append(" */").append(NL);
        } else if (isIndexMessages()) {
            buffer.append("/* @Indexed */").append(NL);
        }
        buffer.append("message ").append(table.getName()).append(SPACE).append(OPEN_CURLY).append(NL);
        for (Column column : table.getColumns()) {
            visit(column);
        }

        processFKTable(table);

        buffer.append(CLOSE_CURLY);
    }

    private void visitTable(String name, List<Column> columns) {
        if (isIndexMessages()) {
            buffer.append("/* @Indexed */").append(NL);
        }
        buffer.append("message ").append(name).append(SPACE).append(OPEN_CURLY).append(NL);
        for (Column column : columns) {
            visitColumn(column);
        }
        buffer.append(CLOSE_CURLY);
    }

    private void visit(Column column) {
        String messageName = ProtobufMetadataProcessor.getMessageName(column);
        if (messageName != null) {
            if (this.processLater.get(messageName) == null) {
                addTab();
                int tag = ProtobufMetadataProcessor.getParentTag(column);
                String name = ProtobufMetadataProcessor.getParentColumnName(column);
                buffer.append("optional ");
                buffer.append(messageName.substring(messageName.lastIndexOf('.') + 1)).append(SPACE).append(name);
                buffer.append(" = ").append(tag).append(SEMICOLON).append(NL);
            }

            processLater(column);

            return;
        }

        visitColumn(column);
    }

    private void visitColumn(Column column) {
        addTab();
        if (column.getAnnotation() != null) {
            buffer.append("/* ").append(column.getAnnotation().replace('\n', ' ')).append(" */").append(NL);
            addTab();
        } else {
            if (isPartOfPrimaryKey(column) || isPartOfUniqueKey(column)) {
                buffer.append("/* @Id */").append(NL);
                addTab();
            }
        }

        boolean array = column.getJavaType().isArray();

        if (column.getNullType().equals(NullType.No_Nulls)) {
            buffer.append("required ");
        } else if (array) {
            buffer.append("repeated ");
        } else {
            buffer.append("optional ");
        }

        if (column.getNativeType() != null) {
            buffer.append(column.getNativeType()).append(SPACE);
        } else {
            Class<?> clazz = column.getJavaType();
            if (array) {
                clazz = clazz.getComponentType();
            }
            String type = ProtobufDataManager.getCompatibleProtobufType(clazz).toString().toLowerCase();
            buffer.append(type).append(SPACE);
        }

        if (column.getNameInSource() != null) {
            buffer.append(column.getNameInSource()).append(SPACE);
        } else {
            buffer.append(column.getName()).append(SPACE);
        }

        int tag = ProtobufMetadataProcessor.getTag(column);
        if (tag == -1) {
            tag = column.getPosition();
        }

        buffer.append("= ").append(tag).append(SEMICOLON).append(NL);
    }

    private void processLater(Column column) {
        String messageName = ProtobufMetadataProcessor.getMessageName(column);
        List<Column> columns = this.processLater.get(messageName);
        if (columns == null) {
            columns = new ArrayList<>();
            this.processLater.put(messageName, columns);
        }
        columns.add(column);
    }

    public boolean isPartOfPrimaryKey(Column column) {
        KeyRecord pk = ((Table) column.getParent()).getPrimaryKey();
        if (pk != null) {
            for (Column c : pk.getColumns()) {
                if (c.getName().equals(column.getName())) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean isPartOfUniqueKey(Column column) {
        List<KeyRecord> list = ((Table) column.getParent()).getUniqueKeys();
        for (KeyRecord uk : list) {
            for (Column c : uk.getColumns()) {
                if (c.getName().equals(column.getName())) {
                    return true;
                }
            }
        }
        return false;
    }

    // Find FK to this table, and add the column
    private void processFKTable(Table table) {
        int increment = 1;
        for (Table t : schema.getTables().values()) {
            if (table == t) {
                continue;
            }

            if (!t.getForeignKeys().isEmpty()) {
                List<ForeignKey> fks = t.getForeignKeys();
                for (ForeignKey fk : fks) {
                    if (fk.getReferenceTableName().equals(table.getName())) {
                        addTab();
                        String messageName = ProtobufMetadataProcessor.getMessageName(t);
                        if (messageName == null) {
                            messageName = table.getName();
                        } else {
                            messageName = messageName.substring(messageName.lastIndexOf('.') + 1);
                        }
                        String columnName = ProtobufMetadataProcessor.getParentColumnName(t);
                        if (columnName == null) {
                            columnName = t.getName().toLowerCase();
                        }
                        int tag = ProtobufMetadataProcessor.getParentTag(t);
                        if (tag == -1) {
                            tag = table.getColumns().size()+increment;
                            increment++;
                        }

                        buffer.append("repeated ");
                        buffer.append(messageName).append(SPACE).append(columnName);
                        buffer.append(" = ").append(tag).append(SEMICOLON).append(NL);
                    }
                }
            }
        }
    }

    boolean isIndexMessages() {
        return indexMessages;
    }

    void setIndexMessages(boolean indexMessages) {
        this.indexMessages = indexMessages;
    }
}
