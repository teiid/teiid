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
package org.teiid.olingo.api;

import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.apache.olingo.commons.core.edm.primitivetype.EdmBinary;
import org.apache.olingo.commons.core.edm.primitivetype.EdmBoolean;
import org.apache.olingo.commons.core.edm.primitivetype.EdmByte;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDate;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDateTimeOffset;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDecimal;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDouble;
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

    private static HashMap<String, SingletonPrimitiveType> teiidkeyed = new HashMap<String, SingletonPrimitiveType>();
    private static HashMap<SingletonPrimitiveType, String> odatakeyed = new HashMap<SingletonPrimitiveType, String>();

    static {
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.STRING, EdmString.getInstance());
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.BOOLEAN, EdmBoolean.getInstance());
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.BYTE, EdmByte.getInstance());
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.SHORT, EdmInt16.getInstance());
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.CHAR, EdmString.getInstance());
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.INTEGER, EdmInt32.getInstance());
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.LONG, EdmInt64.getInstance());
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.BIG_INTEGER, EdmInt64.getInstance());
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.FLOAT, EdmSingle.getInstance());
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.DOUBLE, EdmDouble.getInstance());
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.BIG_DECIMAL, EdmDecimal.getInstance());
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.DATE, EdmDate.getInstance());
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.TIME, EdmTimeOfDay.getInstance());
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.TIMESTAMP, EdmDateTimeOffset.getInstance());
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.OBJECT, EdmStream.getInstance()); // currently
                                                                                              // problematic
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.BLOB, EdmStream.getInstance());
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.CLOB, EdmStream.getInstance());
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.XML, EdmStream.getInstance());
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.VARBINARY, EdmBinary.getInstance());

        odatakeyed.put(EdmString.getInstance(), DataTypeManager.DefaultDataTypes.STRING);
        odatakeyed.put(EdmBoolean.getInstance(), DataTypeManager.DefaultDataTypes.BOOLEAN);
        odatakeyed.put(EdmByte.getInstance(), DataTypeManager.DefaultDataTypes.SHORT);
        odatakeyed.put(EdmSByte.getInstance(), DataTypeManager.DefaultDataTypes.BYTE);
        odatakeyed.put(EdmInt16.getInstance(), DataTypeManager.DefaultDataTypes.SHORT);
        odatakeyed.put(EdmInt32.getInstance(), DataTypeManager.DefaultDataTypes.INTEGER);
        odatakeyed.put(EdmInt64.getInstance(), DataTypeManager.DefaultDataTypes.LONG);
        odatakeyed.put(EdmSingle.getInstance(), DataTypeManager.DefaultDataTypes.FLOAT);
        odatakeyed.put(EdmDouble.getInstance(), DataTypeManager.DefaultDataTypes.DOUBLE);
        odatakeyed.put(EdmDecimal.getInstance(), DataTypeManager.DefaultDataTypes.BIG_DECIMAL);
        odatakeyed.put(EdmDate.getInstance(), DataTypeManager.DefaultDataTypes.DATE);
        odatakeyed.put(EdmTimeOfDay.getInstance(), DataTypeManager.DefaultDataTypes.TIME);
        odatakeyed.put(EdmDateTimeOffset.getInstance(), DataTypeManager.DefaultDataTypes.TIMESTAMP);
        odatakeyed.put(EdmStream.getInstance(), DataTypeManager.DefaultDataTypes.BLOB);
        odatakeyed.put(EdmGuid.getInstance(), DataTypeManager.DefaultDataTypes.STRING);
        odatakeyed.put(EdmBinary.getInstance(), DataTypeManager.DefaultDataTypes.VARBINARY); //$NON-NLS-1$
    }

    public static String teiidType(SingletonPrimitiveType odataType, boolean array) {
        String type =  odatakeyed.get(odataType);
        if (array) {
           type +="[]";
        }
        return type;
    }

    public static SingletonPrimitiveType odataType(String teiidType) {
        if (DataTypeManager.isArrayType(teiidType)) {
            return odataType(DataTypeManager.getComponentType(teiidType));
        }
        return teiidkeyed.get(teiidType);
    }

    public static Object convertToTeiidRuntimeType(Class<?> type, Object value) {
        if (value == null) {
            return null;
        }
        if (DataTypeManager.getAllDataTypeClasses().contains(value.getClass())) {
            return value;
        }
        if (value instanceof UUID) {
            return value.toString();
        }
        if (value instanceof List<?>) {
            List<?> list = (List<?>)value;
            Object array = Array.newInstance(type.getComponentType(), list.size());
            for (int i = 0; i < list.size(); i++) {
                Array.set(array, i, list.get(i));
            }
            return array;
        }
        return value;
    }
}
