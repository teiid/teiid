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

import java.lang.reflect.Array;
import java.sql.Date;
import java.sql.Time;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.EdmParameter;
import org.apache.olingo.commons.api.edm.EdmPrimitiveType;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.core.edm.primitivetype.EdmBinary;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDate;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDecimal;
import org.apache.olingo.commons.core.edm.primitivetype.EdmPrimitiveTypeFactory;
import org.apache.olingo.commons.core.edm.primitivetype.EdmStream;
import org.apache.olingo.commons.core.edm.primitivetype.EdmTimeOfDay;
import org.apache.olingo.commons.core.edm.primitivetype.SingletonPrimitiveType;
import org.teiid.core.TeiidException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.Transform;
import org.teiid.core.types.TransformationException;

public class ODataTypeManager {

    private static HashMap<String, String> odataTypes = new HashMap<String, String>();
    private static HashMap<String, String> teiidTypes = new HashMap<String, String>();

    static {
        odataTypes.put("Edm.String", DataTypeManager.DefaultDataTypes.STRING);
        odataTypes.put("Edm.Boolean", DataTypeManager.DefaultDataTypes.BOOLEAN);
        odataTypes.put("Edm.Byte", DataTypeManager.DefaultDataTypes.SHORT);
        odataTypes.put("Edm.SByte", DataTypeManager.DefaultDataTypes.BYTE);
        odataTypes.put("Edm.Int16", DataTypeManager.DefaultDataTypes.SHORT);
        odataTypes.put("Edm.Int32", DataTypeManager.DefaultDataTypes.INTEGER);
        odataTypes.put("Edm.Int64", DataTypeManager.DefaultDataTypes.LONG);
        odataTypes.put("Edm.Single", DataTypeManager.DefaultDataTypes.FLOAT);
        odataTypes.put("Edm.Double", DataTypeManager.DefaultDataTypes.DOUBLE);
        odataTypes.put("Edm.Decimal", DataTypeManager.DefaultDataTypes.BIG_DECIMAL);
        odataTypes.put("Edm.Date", DataTypeManager.DefaultDataTypes.DATE);
        odataTypes.put("Edm.TimeOfDay", DataTypeManager.DefaultDataTypes.TIME);
        odataTypes.put("Edm.DateTimeOffset", DataTypeManager.DefaultDataTypes.TIMESTAMP);
        odataTypes.put("Edm.Stream", DataTypeManager.DefaultDataTypes.BLOB);
        odataTypes.put("Edm.Guid", DataTypeManager.DefaultDataTypes.STRING);
        odataTypes.put("Edm.Binary", DataTypeManager.DefaultDataTypes.VARBINARY); //$NON-NLS-1$
        
        teiidTypes.put(DataTypeManager.DefaultDataTypes.STRING, "Edm.String");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.BOOLEAN, "Edm.Boolean");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.SHORT, "Edm.Byte");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.BYTE, "Edm.SByte");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.SHORT, "Edm.Int16");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.INTEGER, "Edm.Int32");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.LONG, "Edm.Int64");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.FLOAT, "Edm.Single");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.DOUBLE, "Edm.Double");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.BIG_DECIMAL, "Edm.Decimal");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.DATE, "Edm.Date");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.TIME, "Edm.TimeOfDay");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.TIMESTAMP, "Edm.DateTimeOffset");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.BLOB, "Edm.Stream");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.CLOB, "Edm.Stream");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.XML, "Edm.Stream");
        teiidTypes.put(DataTypeManager.DefaultDataTypes.VARBINARY, "Edm.Binary"); //$NON-NLS-1$
        teiidTypes.put(DataTypeManager.DefaultDataTypes.OBJECT, "Edm.Binary"); //$NON-NLS-1$
    }
    
    public static String teiidType(SingletonPrimitiveType odataType, boolean array) {
        String type =  odataType.getFullQualifiedName().getFullQualifiedNameAsString();
        return teiidType(type, array);
    }
    
    public static String teiidType(String odataType, boolean array) {
        String type =  odataTypes.get(odataType);
        if (type == null) {
            type = DataTypeManager.DefaultDataTypes.STRING; // special case for enum type
        }
        if (array) {
           type +="[]";
        }
        return type;
    }
    
    public static EdmPrimitiveTypeKind odataType(Class<?> teiidRuntimeTypeClass) {
        String dataType = DataTypeManager.getDataTypeName(teiidRuntimeTypeClass);
        return odataType(dataType);
    } 
    
    public static EdmPrimitiveTypeKind odataType(String teiidRuntimeType) {
        if (teiidRuntimeType.endsWith("[]")) {
            teiidRuntimeType = teiidRuntimeType.substring(0, teiidRuntimeType.length()-2);
        }
        String type =  teiidTypes.get(teiidRuntimeType);
        if (type == null) {
            type = "Edm.String";
        }
        return EdmPrimitiveTypeKind.valueOfFQN(type);
    }    
    
    public static Object convertToTeiidRuntimeType(Class<?> type, Object value) throws TeiidException {
        if (value == null) {
            return null;
        }
        if (type.isAssignableFrom(value.getClass())) {
            return value;
        }
        if (value instanceof UUID) {
            return value.toString();
        }
        if (type.isArray() && value instanceof List<?>) {
            List<?> list = (List<?>)value;
            Class<?> expectedArrayComponentType = type.getComponentType();
            Object array = Array.newInstance(type.getComponentType(), list.size());
            for (int i = 0; i < list.size(); i++) {
                Object arrayItem = convertToTeiidRuntimeType(expectedArrayComponentType, list.get(i));
                Array.set(array, i, arrayItem);
            }
            return array;
        }
        
        Transform transform = DataTypeManager.getTransform(value.getClass(), type);
        if (transform != null) {
            try {
                value = transform.transform(value, type);
            } catch (TransformationException e) {
                throw new TeiidException(e);
            }
        }        
        return value;
    }
    
    public static Object parseLiteral(EdmParameter edmParameter, Class<?> runtimeType, String value)
            throws TeiidException {
        EdmPrimitiveType primitiveType = EdmPrimitiveTypeFactory.getInstance(EdmPrimitiveTypeKind
                .valueOf(edmParameter.getType()
                        .getFullQualifiedName()
                        .getFullQualifiedNameAsString().substring(4)));
        
        try {
            if (value.startsWith("'") && value.endsWith("'")) {
                value = value.substring(1, value.length()-1);
            }
            Object converted =  primitiveType.valueOfString(value, 
                    edmParameter.isNullable(), 
                    edmParameter.getMaxLength(), 
                    edmParameter.getPrecision(), 
                    edmParameter.getScale(), 
                    true, 
                    runtimeType);        
            return converted;
        } catch (EdmPrimitiveTypeException e) {
            throw new TeiidException(e);
        }
    }
    
    public static Object parseLiteral(EdmProperty edmProperty, Class<?> runtimeType, String value)
            throws TeiidException {
        EdmPrimitiveType primitiveType = EdmPrimitiveTypeFactory.getInstance(EdmPrimitiveTypeKind
                .valueOf(edmProperty.getType()
                        .getFullQualifiedName()
                        .getFullQualifiedNameAsString().substring(4)));
        
        try {
            if (value.startsWith("'") && value.endsWith("'")) {
                value = value.substring(1, value.length()-1);
            }
            Object converted =  primitiveType.valueOfString(value, 
                    edmProperty.isNullable(), 
                    edmProperty.getMaxLength(), 
                    edmProperty.getPrecision(), 
                    edmProperty.getScale(), 
                    true, 
                    runtimeType);        
            return converted;
        } catch (EdmPrimitiveTypeException e) {
            throw new TeiidException(e);
        }
    }
    
    public static Object parseLiteral(String odataType, String value)
            throws TeiidException {
        EdmPrimitiveType primitiveType = EdmPrimitiveTypeFactory.getInstance(EdmPrimitiveTypeKind
                .valueOf(odataType.substring(4)));
        
        int maxLength = DataTypeManager.MAX_STRING_LENGTH;
        if (primitiveType instanceof EdmBinary ||primitiveType instanceof EdmStream) {
            maxLength = DataTypeManager.MAX_VARBINARY_BYTES;
        }
        
        int precision = 4;
        int scale = 3;
        if (primitiveType instanceof EdmDecimal) {
            precision = 38;
            scale = 9;
        }
        
        Class<?> expectedClass = primitiveType.getDefaultType();
        
        try {
            if (value.startsWith("'") && value.endsWith("'")) {
                value = value.substring(1, value.length()-1);
            }
            Object converted =  primitiveType.valueOfString(value,
                    false,
                    maxLength, 
                    precision, 
                    scale, 
                    true, 
                    expectedClass);
            
            if (primitiveType instanceof EdmTimeOfDay) {
                Calendar ts =  (Calendar)converted;
                return new Time(ts.getTimeInMillis());
            } else if (primitiveType instanceof EdmDate) {
                Calendar ts =  (Calendar)converted;
                return new Date(ts.getTimeInMillis());
            }
            return converted;
        } catch (EdmPrimitiveTypeException e) {
            throw new TeiidException(e);
        }
    }    
}
