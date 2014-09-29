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
import org.apache.olingo.commons.core.edm.primitivetype.EdmBinary;
import org.apache.olingo.commons.core.edm.primitivetype.EdmBoolean;
import org.apache.olingo.commons.core.edm.primitivetype.EdmByte;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDate;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDateTime;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDateTimeOffset;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDecimal;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDouble;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDuration;
import org.apache.olingo.commons.core.edm.primitivetype.EdmGuid;
import org.apache.olingo.commons.core.edm.primitivetype.EdmInt16;
import org.apache.olingo.commons.core.edm.primitivetype.EdmInt32;
import org.apache.olingo.commons.core.edm.primitivetype.EdmInt64;
import org.apache.olingo.commons.core.edm.primitivetype.EdmSByte;
import org.apache.olingo.commons.core.edm.primitivetype.EdmSingle;
import org.apache.olingo.commons.core.edm.primitivetype.EdmStream;
import org.apache.olingo.commons.core.edm.primitivetype.EdmString;
import org.apache.olingo.commons.core.edm.primitivetype.EdmTimeOfDay;
import org.apache.olingo.commons.core.edm.primitivetype.SingletonPrimitiveType;
import org.teiid.core.types.DataTypeManager;

public class ODataTypeManager {

    private static HashMap<String, EdmPrimitiveTypeKind> teiidkeyed = new HashMap<String, EdmPrimitiveTypeKind>();
    private static HashMap<EdmPrimitiveTypeKind, String> odatakeyed = new HashMap<EdmPrimitiveTypeKind, String>();

    static {
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.STRING,
                EdmPrimitiveTypeKind.String);
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.BOOLEAN,
                EdmPrimitiveTypeKind.Boolean);
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.BYTE,
                EdmPrimitiveTypeKind.Byte);
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.SHORT,
                EdmPrimitiveTypeKind.Int16);
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.CHAR,
                EdmPrimitiveTypeKind.String);
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.INTEGER,
                EdmPrimitiveTypeKind.Int32);
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.LONG,
                EdmPrimitiveTypeKind.Int64);
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.BIG_INTEGER,
                EdmPrimitiveTypeKind.Int64);
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.FLOAT,
                EdmPrimitiveTypeKind.Single);
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.DOUBLE,
                EdmPrimitiveTypeKind.Double);
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.BIG_DECIMAL,
                EdmPrimitiveTypeKind.Decimal);
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.DATE,
                EdmPrimitiveTypeKind.Date);
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.TIME,
                EdmPrimitiveTypeKind.Time);
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.TIMESTAMP,
                EdmPrimitiveTypeKind.DateTime);
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.OBJECT,
                EdmPrimitiveTypeKind.Stream); // currently problematic
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.BLOB,
                EdmPrimitiveTypeKind.Stream);
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.CLOB,
                EdmPrimitiveTypeKind.Stream);
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.XML,
                EdmPrimitiveTypeKind.Stream);
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.VARBINARY,
                EdmPrimitiveTypeKind.Stream);

        odatakeyed.put(EdmPrimitiveTypeKind.String,
                DataTypeManager.DefaultDataTypes.STRING);
        odatakeyed.put(EdmPrimitiveTypeKind.Boolean,
                DataTypeManager.DefaultDataTypes.BOOLEAN);
        odatakeyed.put(EdmPrimitiveTypeKind.Byte,
                DataTypeManager.DefaultDataTypes.SHORT);
        odatakeyed.put(EdmPrimitiveTypeKind.SByte,
                DataTypeManager.DefaultDataTypes.BYTE);
        odatakeyed.put(EdmPrimitiveTypeKind.Int16,
                DataTypeManager.DefaultDataTypes.SHORT);
        odatakeyed.put(EdmPrimitiveTypeKind.Int32,
                DataTypeManager.DefaultDataTypes.INTEGER);
        odatakeyed.put(EdmPrimitiveTypeKind.Int64,
                DataTypeManager.DefaultDataTypes.LONG);
        odatakeyed.put(EdmPrimitiveTypeKind.Single,
                DataTypeManager.DefaultDataTypes.FLOAT);
        odatakeyed.put(EdmPrimitiveTypeKind.Double,
                DataTypeManager.DefaultDataTypes.DOUBLE);
        odatakeyed.put(EdmPrimitiveTypeKind.Decimal,
                DataTypeManager.DefaultDataTypes.BIG_DECIMAL);
        odatakeyed.put(EdmPrimitiveTypeKind.Date,
                DataTypeManager.DefaultDataTypes.DATE);
        odatakeyed.put(EdmPrimitiveTypeKind.Time,
                DataTypeManager.DefaultDataTypes.TIME);
        odatakeyed.put(EdmPrimitiveTypeKind.DateTime,
                DataTypeManager.DefaultDataTypes.TIMESTAMP);
        odatakeyed.put(EdmPrimitiveTypeKind.Stream,
                DataTypeManager.DefaultDataTypes.BLOB);
        odatakeyed.put(EdmPrimitiveTypeKind.Guid,
                DataTypeManager.DefaultDataTypes.STRING);
        odatakeyed.put(EdmPrimitiveTypeKind.Binary,
                DataTypeManager.DefaultDataTypes.BYTE + "[]"); //$NON-NLS-1$
    }

    public static String teiidType(EdmPrimitiveTypeKind odataType) {
        return odatakeyed.get(odataType);
    }

    public static EdmPrimitiveTypeKind odataType(String teiidType) {
        if (DataTypeManager.isArrayType(teiidType)) {
            return odataType(DataTypeManager.getComponentType(teiidType));
        }
        return teiidkeyed.get(teiidType);
    }

    public static String teiidType(SingletonPrimitiveType type) {
        if (type instanceof EdmBinary) {
            return odatakeyed.get(EdmPrimitiveTypeKind.Binary);
        } else if (type instanceof EdmBoolean) {
            return odatakeyed.get(EdmPrimitiveTypeKind.Boolean);
        } else if (type instanceof EdmByte) {
            return odatakeyed.get(EdmPrimitiveTypeKind.Byte);
        } else if (type instanceof EdmDate) {
            return odatakeyed.get(EdmPrimitiveTypeKind.Date);
        } else if (type instanceof EdmDateTime) {
            return odatakeyed.get(EdmPrimitiveTypeKind.DateTime);
        } else if (type instanceof EdmDateTimeOffset) {
            return odatakeyed.get(EdmPrimitiveTypeKind.DateTime);
        } else if (type instanceof EdmDecimal) {
            return odatakeyed.get(EdmPrimitiveTypeKind.Decimal);
        } else if (type instanceof EdmDouble) {
            return odatakeyed.get(EdmPrimitiveTypeKind.Double);
        } else if (type instanceof EdmDuration) {
            return odatakeyed.get(EdmPrimitiveTypeKind.Int32);
        } else if (type instanceof EdmGuid) {
            return odatakeyed.get(EdmPrimitiveTypeKind.String);
        } else if (type instanceof EdmInt16) {
            return odatakeyed.get(EdmPrimitiveTypeKind.Int16);
        } else if (type instanceof EdmInt32) {
            return odatakeyed.get(EdmPrimitiveTypeKind.Int32);
        } else if (type instanceof EdmInt64) {
            return odatakeyed.get(EdmPrimitiveTypeKind.Int64);
        } else if (type instanceof EdmSByte) {
            return odatakeyed.get(EdmPrimitiveTypeKind.SByte);
        } else if (type instanceof EdmSingle) {
            return odatakeyed.get(EdmPrimitiveTypeKind.Single);
        } else if (type instanceof EdmStream) {
            return odatakeyed.get(EdmPrimitiveTypeKind.Stream);
        } else if (type instanceof EdmString) {
            return odatakeyed.get(EdmPrimitiveTypeKind.String);
        } else if (type instanceof EdmTimeOfDay) {
            return odatakeyed.get(EdmPrimitiveTypeKind.Time);
        }
        return null;
    }

}
