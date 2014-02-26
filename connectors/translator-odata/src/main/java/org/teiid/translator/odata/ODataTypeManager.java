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
package org.teiid.translator.odata;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;

import org.joda.time.LocalDateTime;
import org.joda.time.LocalTime;
import org.odata4j.core.Guid;
import org.odata4j.edm.EdmCollectionType;
import org.odata4j.edm.EdmProperty.CollectionKind;
import org.odata4j.edm.EdmSimpleType;
import org.odata4j.edm.EdmType;
import org.teiid.core.types.DataTypeManager;

public class ODataTypeManager {

	private static HashMap<String, EdmSimpleType> teiidkeyed = new HashMap<String, EdmSimpleType>();
	private static HashMap<String, String> odatakeyed = new HashMap<String, String>();
	
	static {
		teiidkeyed.put(DataTypeManager.DefaultDataTypes.STRING, EdmSimpleType.STRING);
		teiidkeyed.put(DataTypeManager.DefaultDataTypes.BOOLEAN, EdmSimpleType.BOOLEAN);
		teiidkeyed.put(DataTypeManager.DefaultDataTypes.BYTE, EdmSimpleType.SBYTE);
		teiidkeyed.put(DataTypeManager.DefaultDataTypes.SHORT, EdmSimpleType.INT16);
		teiidkeyed.put(DataTypeManager.DefaultDataTypes.CHAR, EdmSimpleType.STRING);
		teiidkeyed.put(DataTypeManager.DefaultDataTypes.INTEGER, EdmSimpleType.INT32);
		teiidkeyed.put(DataTypeManager.DefaultDataTypes.LONG, EdmSimpleType.INT64);
		teiidkeyed.put(DataTypeManager.DefaultDataTypes.BIG_INTEGER, EdmSimpleType.INT64);
		teiidkeyed.put(DataTypeManager.DefaultDataTypes.FLOAT, EdmSimpleType.SINGLE);
		teiidkeyed.put(DataTypeManager.DefaultDataTypes.DOUBLE, EdmSimpleType.DOUBLE);
		teiidkeyed.put(DataTypeManager.DefaultDataTypes.BIG_DECIMAL, EdmSimpleType.DECIMAL);
		teiidkeyed.put(DataTypeManager.DefaultDataTypes.DATE, EdmSimpleType.DATETIME);
		teiidkeyed.put(DataTypeManager.DefaultDataTypes.TIME,  EdmSimpleType.TIME);
		teiidkeyed.put(DataTypeManager.DefaultDataTypes.TIMESTAMP,  EdmSimpleType.DATETIME);
		teiidkeyed.put(DataTypeManager.DefaultDataTypes.OBJECT,  EdmSimpleType.BINARY); //currently problematic
		teiidkeyed.put(DataTypeManager.DefaultDataTypes.BLOB, EdmSimpleType.BINARY);
		teiidkeyed.put(DataTypeManager.DefaultDataTypes.CLOB, EdmSimpleType.BINARY);
		teiidkeyed.put(DataTypeManager.DefaultDataTypes.XML, EdmSimpleType.BINARY);
		teiidkeyed.put(DataTypeManager.DefaultDataTypes.VARBINARY, EdmSimpleType.BINARY);
		
		odatakeyed.put(EdmSimpleType.STRING.getFullyQualifiedTypeName(), DataTypeManager.DefaultDataTypes.STRING);
		odatakeyed.put(EdmSimpleType.BOOLEAN.getFullyQualifiedTypeName(), DataTypeManager.DefaultDataTypes.BOOLEAN);
		odatakeyed.put(EdmSimpleType.BYTE.getFullyQualifiedTypeName(), DataTypeManager.DefaultDataTypes.SHORT);
		odatakeyed.put(EdmSimpleType.SBYTE.getFullyQualifiedTypeName(), DataTypeManager.DefaultDataTypes.BYTE);
		odatakeyed.put(EdmSimpleType.INT16.getFullyQualifiedTypeName(), DataTypeManager.DefaultDataTypes.SHORT);
		odatakeyed.put(EdmSimpleType.INT32.getFullyQualifiedTypeName(), DataTypeManager.DefaultDataTypes.INTEGER);
		odatakeyed.put(EdmSimpleType.INT64.getFullyQualifiedTypeName(), DataTypeManager.DefaultDataTypes.LONG);
		odatakeyed.put(EdmSimpleType.SINGLE.getFullyQualifiedTypeName(), DataTypeManager.DefaultDataTypes.FLOAT);
		odatakeyed.put(EdmSimpleType.DOUBLE.getFullyQualifiedTypeName(), DataTypeManager.DefaultDataTypes.DOUBLE);
		odatakeyed.put(EdmSimpleType.DECIMAL.getFullyQualifiedTypeName(), DataTypeManager.DefaultDataTypes.BIG_DECIMAL);
		odatakeyed.put(EdmSimpleType.TIME.getFullyQualifiedTypeName(), DataTypeManager.DefaultDataTypes.TIME);
		odatakeyed.put(EdmSimpleType.DATETIME.getFullyQualifiedTypeName(), DataTypeManager.DefaultDataTypes.TIMESTAMP);
		odatakeyed.put(EdmSimpleType.DATETIMEOFFSET.getFullyQualifiedTypeName(), DataTypeManager.DefaultDataTypes.TIMESTAMP);
		odatakeyed.put(EdmSimpleType.BINARY.getFullyQualifiedTypeName(), DataTypeManager.DefaultDataTypes.BLOB);
		odatakeyed.put(EdmSimpleType.GUID.getFullyQualifiedTypeName(), DataTypeManager.DefaultDataTypes.STRING);
	}
	
	public static String teiidType(String odataType) {
		return odatakeyed.get(odataType);
	}
	
	public static EdmType odataType(String teiidType) {
		if (DataTypeManager.isArrayType(teiidType)) {
			return new EdmCollectionType(CollectionKind.List, odataType(DataTypeManager.getComponentType(teiidType)));
		}
		return teiidkeyed.get(teiidType);
	}
	
	public static Object convertToTeiidRuntimeType(Object value) {
		if (value == null) {
			return null;
		}
		if (DataTypeManager.getAllDataTypeClasses().contains(value.getClass())) {
			return value;
		}
		if (value instanceof LocalDateTime) {
			return new Timestamp(((LocalDateTime)value).toDateTime().getMillis());
		} else if (value instanceof Guid) {
			return value.toString();
		} else if (value instanceof LocalTime) {
			return new Time(((LocalTime)value).toDateTimeToday().getMillis());
		}
		return value;
	}
}
