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
package org.teiid.infinispan.api;

import java.util.TreeMap;

import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;

/**
 * This is the bridge between the Marshaller class and the Teiid metadata scheme.
 */
public class MarshallerBuilder {

    public static TreeMap<Integer, TableWireFormat> getWireMap(Table parentTbl, RuntimeMetadata metadata)
            throws TranslatorException {
        TreeMap<Integer, TableWireFormat> wireMap = buildWireMap(parentTbl, false, metadata);
        Schema schema = parentTbl.getParent();
        for (Table table:schema.getTables().values()) {
            if (table.equals(parentTbl)) {
                continue;
            }
            String mergeName = ProtobufMetadataProcessor.getMerge(table);
            if (mergeName != null && mergeName.equals(parentTbl.getFullName())) {
                // one 2 many relation
                int parentTag = ProtobufMetadataProcessor.getParentTag(table);
                String childName = ProtobufMetadataProcessor.getMessageName(table);
                TableWireFormat child = new TableWireFormat(childName, parentTag);
                wireMap.put(child.getReadTag(), child);
                TreeMap<Integer, TableWireFormat> childWireMap = buildWireMap(table, true, metadata);
                for (TableWireFormat twf : childWireMap.values()) {
                    child.addNested(twf);
                }
            }
        }
        return wireMap;
    }

    private static TreeMap<Integer, TableWireFormat> buildWireMap(Table table, boolean nested, RuntimeMetadata metadata)
            throws TranslatorException {
        TreeMap<Integer, TableWireFormat> wireMap = new TreeMap<>();
        for (Column column: table.getColumns()) {
            if (ProtobufMetadataProcessor.getPseudo(column) != null) {
                continue;
            }
            int parentTag = ProtobufMetadataProcessor.getParentTag(column);
            if ( parentTag == -1) {
                // normal columns
                int tag = ProtobufMetadataProcessor.getTag(column);
                String name = getDocumentAttributeName(column, nested, metadata);
                TableWireFormat twf = new TableWireFormat(name, tag, column);
                wireMap.put(twf.getReadTag(), twf);
            } else {
                // columns from one 2 one relation
                int tag = ProtobufMetadataProcessor.getTag(column);
                String name = getDocumentAttributeName(column, true, metadata);
                TableWireFormat child = new TableWireFormat(name, tag, column);

                String parentName = ProtobufMetadataProcessor.getMessageName(column);
                TableWireFormat parent = null;
                TableWireFormat existing = wireMap.get(TableWireFormat.buildNestedTag(parentTag));
                if (existing == null) {
                    parent = new TableWireFormat(parentName, parentTag);
                    wireMap.put(parent.getReadTag(), parent);
                } else {
                    parent = existing;
                }
                parent.addNested(child);
            }
        }
        return wireMap;
    }

    private static String getName(AbstractMetadataRecord record) {
        if (record.getNameInSource() != null && record.getNameInSource().length() > 0) {
            return record.getNameInSource();
        }
        return record.getName();
    }

    public static String getDocumentAttributeName(Column column, boolean nested, RuntimeMetadata metadata)
            throws TranslatorException {
        String nis = getName(column);

        // when a message name is present on the column, then this column is embedded from 1 to 1 relation
        // or if this column's table is merged, then that table is from 1 to many relation. in each case attribute
        // name is a fully qualified name.
        String parentMessageName = ProtobufMetadataProcessor.getMessageName((Table)column.getParent());
        String messageName = ProtobufMetadataProcessor.getMessageName(column);
        if (messageName == null) {
            String mergeName =  ProtobufMetadataProcessor.getMerge((Table)column.getParent());
            if (mergeName != null) {
                // this is 1-2-many case. The message-name on table properties
                messageName = ProtobufMetadataProcessor.getMessageName((Table)column.getParent());
                parentMessageName = ProtobufMetadataProcessor.getMessageName(metadata.getTable(mergeName));
            }
        }
        if (messageName != null) {
            nis = messageName + "/" + nis;
        }
        if (nested) {
            return parentMessageName+"/"+nis;
        }
        return nis;
    }

    public static TeiidTableMarshaller getMarshaller(Table table, RuntimeMetadata metadata) throws TranslatorException {
        return new TeiidTableMarshaller(ProtobufMetadataProcessor.getMessageName(table), getWireMap(table, metadata));
    }
}
