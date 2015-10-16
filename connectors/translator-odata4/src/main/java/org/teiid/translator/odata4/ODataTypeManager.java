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
package org.teiid.translator.odata4;

import java.lang.reflect.Array;
import java.sql.Date;
import java.sql.Time;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;

import org.apache.olingo.commons.api.edm.EdmPrimitiveType;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.core.edm.primitivetype.EdmBinary;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDate;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDecimal;
import org.apache.olingo.commons.core.edm.primitivetype.EdmPrimitiveTypeFactory;
import org.apache.olingo.commons.core.edm.primitivetype.EdmStream;
import org.apache.olingo.commons.core.edm.primitivetype.EdmString;
import org.apache.olingo.commons.core.edm.primitivetype.EdmTimeOfDay;
import org.teiid.core.TeiidException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.Transform;
import org.teiid.core.types.TransformationException;
import org.teiid.language.Literal;
import org.teiid.translator.TranslatorException;

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
        teiidTypes.put(DataTypeManager.DefaultDataTypes.VARBINARY, "Edm.Binary"); //$NON-NLS-1$
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
    
    public static EdmPrimitiveTypeKind odataType(Class<?> typeClass) {
        String dataType = DataTypeManager.getDataTypeName(typeClass);
        return odataType(dataType);
    } 
    
    public static EdmPrimitiveTypeKind odataType(String dataType) {
        if (dataType.endsWith("[]")) {
            dataType = dataType.substring(0, dataType.length()-2);
        }        
        String type =  teiidTypes.get(dataType);
        if (type == null) {
            type = "Edm.String";
        }
        return EdmPrimitiveTypeKind.valueOfFQN(type);
    }    
    
    public static String convertToODataInput(Literal obj, String odataType) throws TranslatorException {
        if (obj.getValue() == null) {
            return "null"; // is this correct?
        } else {
            try {
                Object val = obj.getValue();
                if(odataType.startsWith("Edm.")) {
                    odataType = odataType.substring(4);
                }
                EdmPrimitiveTypeKind kind = EdmPrimitiveTypeKind.valueOf(odataType);
                String value =  EdmPrimitiveTypeFactory.getInstance(kind).valueToString(
                        val, true, 4000, 0, 0, true);
                if (kind == EdmPrimitiveTypeKind.String) {
                    return EdmString.getInstance().toUriLiteral(value);
                }
                return value;
            } catch (EdmPrimitiveTypeException e) {
                throw new TranslatorException(e);
            }
        }
    }   
    
    public static Object convertTeiidRuntimeType(Object value, String odataType,
            Class<?> expectedType) throws TranslatorException {
        if (value == null) {
            return null;
        }
        
        if (expectedType.isArray() && value instanceof List) {
            List<?> values = (List<?>)value;
            Class<?> expectedArrayComponentType = expectedType.getComponentType();
            Object array = Array.newInstance(expectedArrayComponentType, values.size());                
            for (int i = 0; i < values.size(); i++) {
                Object arrayItem = convertTeiidRuntimeType(values.get(i), odataType, expectedArrayComponentType);
                Array.set(array, i, arrayItem);
            }               
            return array;
        }
        
        if (expectedType.isAssignableFrom(value.getClass())) {
            return value;
        } else {
            
            if (value instanceof String) {
                return parseLiteral(odataType, (String)value);
            }

            Transform transform = DataTypeManager.getTransform(value.getClass(), expectedType);
            if (transform != null) {
                try {
                    value = transform.transform(value, expectedType);
                } catch (TransformationException e) {
                    throw new TranslatorException(e);
                }
            }
        }
        return value;
    }
    
    public static Object parseLiteral(String odataType, String value)
            throws TranslatorException {
        EdmPrimitiveType primitiveType = EdmPrimitiveTypeFactory.getInstance(EdmPrimitiveTypeKind
                .valueOf(odataType.substring(4)));
        
        int maxLength = DataTypeManager.MAX_STRING_LENGTH;
        if (primitiveType instanceof EdmBinary ||primitiveType instanceof EdmStream) {
            maxLength = DataTypeManager.MAX_VARBINARY_BYTES;
        }
        
        int precision = 9;
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
            
            if(primitiveType instanceof EdmTimeOfDay) {
                Calendar ts =  (Calendar)converted;
                return new Time(ts.getTimeInMillis());
            } else if (primitiveType instanceof EdmDate) {
                Calendar ts =  (Calendar)converted;
                return new Date(ts.getTimeInMillis());
            }
            return converted;
        } catch (EdmPrimitiveTypeException e) {
            throw new TranslatorException(e);
        }
    }    
}
