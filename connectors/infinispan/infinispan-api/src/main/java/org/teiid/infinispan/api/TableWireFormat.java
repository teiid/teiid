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
package org.teiid.infinispan.api;

import java.util.TreeMap;

import org.infinispan.protostream.descriptors.Type;
import org.infinispan.protostream.impl.WireFormat;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.metadata.Column;

public class TableWireFormat {
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