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
package org.teiid.olingo;

import java.util.HashMap;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.teiid.core.types.DataTypeManager;

public class ODataTypeManager {

	private static HashMap<String, EdmPrimitiveTypeKind> teiidkeyed = new HashMap<String, EdmPrimitiveTypeKind>();
	private static HashMap<FullQualifiedName, String> odatakeyed = new HashMap<FullQualifiedName, String>();
	
	static {
		teiidkeyed.put(DataTypeManager.DefaultDataTypes.STRING, EdmPrimitiveTypeKind.String);
		teiidkeyed.put(DataTypeManager.DefaultDataTypes.BOOLEAN, EdmPrimitiveTypeKind.Boolean);
		teiidkeyed.put(DataTypeManager.DefaultDataTypes.BYTE, EdmPrimitiveTypeKind.Byte);
		teiidkeyed.put(DataTypeManager.DefaultDataTypes.SHORT, EdmPrimitiveTypeKind.Int16);
		teiidkeyed.put(DataTypeManager.DefaultDataTypes.CHAR, EdmPrimitiveTypeKind.String);
		teiidkeyed.put(DataTypeManager.DefaultDataTypes.INTEGER, EdmPrimitiveTypeKind.Int32);
		teiidkeyed.put(DataTypeManager.DefaultDataTypes.LONG, EdmPrimitiveTypeKind.Int64);
		teiidkeyed.put(DataTypeManager.DefaultDataTypes.BIG_INTEGER, EdmPrimitiveTypeKind.Int64);
		teiidkeyed.put(DataTypeManager.DefaultDataTypes.FLOAT, EdmPrimitiveTypeKind.Single);
		teiidkeyed.put(DataTypeManager.DefaultDataTypes.DOUBLE, EdmPrimitiveTypeKind.Double);
		teiidkeyed.put(DataTypeManager.DefaultDataTypes.BIG_DECIMAL, EdmPrimitiveTypeKind.Decimal);
		teiidkeyed.put(DataTypeManager.DefaultDataTypes.DATE, EdmPrimitiveTypeKind.Date);
		teiidkeyed.put(DataTypeManager.DefaultDataTypes.TIME,  EdmPrimitiveTypeKind.Time);
		teiidkeyed.put(DataTypeManager.DefaultDataTypes.TIMESTAMP,  EdmPrimitiveTypeKind.DateTime);
		teiidkeyed.put(DataTypeManager.DefaultDataTypes.OBJECT,  EdmPrimitiveTypeKind.Stream); //currently problematic
		teiidkeyed.put(DataTypeManager.DefaultDataTypes.BLOB, EdmPrimitiveTypeKind.Stream);
		teiidkeyed.put(DataTypeManager.DefaultDataTypes.CLOB, EdmPrimitiveTypeKind.Stream);
		teiidkeyed.put(DataTypeManager.DefaultDataTypes.XML, EdmPrimitiveTypeKind.Stream);
		teiidkeyed.put(DataTypeManager.DefaultDataTypes.VARBINARY, EdmPrimitiveTypeKind.Stream);
		
		odatakeyed.put(EdmPrimitiveTypeKind.String.getFullQualifiedName(), DataTypeManager.DefaultDataTypes.STRING);
		odatakeyed.put(EdmPrimitiveTypeKind.Boolean.getFullQualifiedName(), DataTypeManager.DefaultDataTypes.BOOLEAN);
		odatakeyed.put(EdmPrimitiveTypeKind.Byte.getFullQualifiedName(), DataTypeManager.DefaultDataTypes.SHORT);
		odatakeyed.put(EdmPrimitiveTypeKind.SByte.getFullQualifiedName(), DataTypeManager.DefaultDataTypes.BYTE);
		odatakeyed.put(EdmPrimitiveTypeKind.Int16.getFullQualifiedName(), DataTypeManager.DefaultDataTypes.SHORT);
		odatakeyed.put(EdmPrimitiveTypeKind.Int32.getFullQualifiedName(), DataTypeManager.DefaultDataTypes.INTEGER);
		odatakeyed.put(EdmPrimitiveTypeKind.Int64.getFullQualifiedName(), DataTypeManager.DefaultDataTypes.LONG);
		odatakeyed.put(EdmPrimitiveTypeKind.Single.getFullQualifiedName(), DataTypeManager.DefaultDataTypes.FLOAT);
		odatakeyed.put(EdmPrimitiveTypeKind.Double.getFullQualifiedName(), DataTypeManager.DefaultDataTypes.DOUBLE);
		odatakeyed.put(EdmPrimitiveTypeKind.Decimal.getFullQualifiedName(), DataTypeManager.DefaultDataTypes.BIG_DECIMAL);
		odatakeyed.put(EdmPrimitiveTypeKind.Time.getFullQualifiedName(), DataTypeManager.DefaultDataTypes.TIME);
		odatakeyed.put(EdmPrimitiveTypeKind.DateTime.getFullQualifiedName(), DataTypeManager.DefaultDataTypes.TIMESTAMP);
		odatakeyed.put(EdmPrimitiveTypeKind.Stream.getFullQualifiedName(), DataTypeManager.DefaultDataTypes.BLOB);
		odatakeyed.put(EdmPrimitiveTypeKind.Guid.getFullQualifiedName(), DataTypeManager.DefaultDataTypes.STRING);
	}
	
	public static String teiidType(String odataType) {
		return odatakeyed.get(odataType);
	}
	
	public static EdmPrimitiveTypeKind odataType(String teiidType) {
		if (DataTypeManager.isArrayType(teiidType)) {
			return  odataType(DataTypeManager.getComponentType(teiidType));
		}
		return teiidkeyed.get(teiidType);
	}
	
}
