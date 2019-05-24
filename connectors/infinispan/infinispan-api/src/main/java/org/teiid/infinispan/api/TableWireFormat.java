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

import java.io.Serializable;
import java.util.TreeMap;

import org.infinispan.protostream.descriptors.Type;
import org.infinispan.protostream.impl.WireFormat;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.metadata.Column;

public class TableWireFormat implements Serializable{
    private static final long serialVersionUID = -9204011747402762948L;
    private int readTag;
    private int writeTag;
    private Type type;
    private Column column;
    private TreeMap<Integer, TableWireFormat> nested;
    private String attributeName;

    public String getAttributeName() {
        return this.attributeName;
    }

    public String getColumnName() {
        int idx = this.attributeName.lastIndexOf('/');
        if (idx != -1) {
            return this.attributeName.substring(idx+1);
        }
        return this.attributeName;
    }

    public int getReadTag() {
        return readTag;
    }

    public int getWriteTag() {
        return writeTag;
    }

    public Type getProtobufType() {
        return type;
    }

    public boolean isArrayType() {
        return (column.getJavaType().getComponentType() != null);
    }

    public TableWireFormat(String name, int tag, Column column) {
        this.writeTag = tag;
        Class<?> columnType = column.getJavaType();
        Type protobufType = buildProbufType(column.getNativeType(), columnType);
        this.readTag = WireFormat.makeTag(tag, protobufType.getWireType());;
        this.type = protobufType;
        this.column = column;
        this.attributeName = name;
    }

    public TableWireFormat(String name, int parentTag) {
        this.writeTag = parentTag;
        this.readTag = buildNestedTag(parentTag);
        this.attributeName = name;
    }

    public static int buildNestedTag(int tag) {
        return WireFormat.makeTag(tag, WireFormat.WIRETYPE_LENGTH_DELIMITED);
    }

    private Type buildProbufType(String nativeType, Class<?> columnType) {
        Type hotrodType;
        if (columnType.isArray()) {
            hotrodType = getProtobufType(nativeType, columnType.getComponentType());
        } else {
            hotrodType = getProtobufType(nativeType, columnType);
        }
        return hotrodType;
    }

    private Type getProtobufType(String nativeType, Class<?> columnType) {
        Type hotrodType;
        if (nativeType != null) {
            try {
                hotrodType = ProtobufDataManager.parseProtobufType(nativeType);
            } catch (TeiidRuntimeException e) {
                hotrodType = ProtobufDataManager.getCompatibleProtobufType(columnType);
            }
        } else {
            hotrodType = ProtobufDataManager.getCompatibleProtobufType(columnType);
        }
        return hotrodType;
    }

    public boolean isNested() {
        return this.nested != null && !this.nested.isEmpty();
    }

    public void addNested(TableWireFormat childFormat) {
        if (this.nested == null) {
            this.nested = new TreeMap<>();
        }
        this.nested.put(childFormat.getReadTag(), childFormat);
    }

    public TreeMap<Integer, TableWireFormat> getNestedWireMap() {
        return this.nested;
    }

    public Class<?> expectedType() {
        return this.column.getJavaType();
    }

    @Override
    public String toString() {
        return "TableWireFormat [expectedTag=" + readTag + ", attributeName=" + attributeName
                + ", nested=" + nested + "]";
    }
}