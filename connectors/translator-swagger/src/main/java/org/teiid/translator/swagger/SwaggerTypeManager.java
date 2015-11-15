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
package org.teiid.translator.swagger;

import static org.teiid.translator.swagger.SwaggerMetadataProcessor.typeFormat;

import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.List;

import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.Transform;
import org.teiid.core.types.TransformationException;
import org.teiid.translator.TranslatorException;

public class SwaggerTypeManager {
    
    private static final String INTEGER = "integer";
    private static final String INTEGER_ = typeFormat("integer", "int32");
    
    private static final String LONG = "long";
    private static final String LONG_ = typeFormat("integer", "int64");
    
    private static final String FLOAT = "float";  
    private static final String FLOAT_ = typeFormat("number", "float");
    
    private static final String DOUBLE = "double";
    private static final String DOUBLE_ = typeFormat("number", "double");
    
    private static final String STRING = "string";
    private static final String STRING_ = typeFormat("string", "");
    
    private static final String BYTE = "byte";
    private static final String BYTE_ = typeFormat("string", "byte");
    
    private static final String BINARY = "binary";
    private static final String BINARY_ = typeFormat("string", "binary");
    
    private static final String BOOLEAN = "boolean";
    private static final String BOOLEAN_ = typeFormat("boolean", "");
    
    private static final String DATE = "date";
    private static final String DATE_ = typeFormat("string", "date");
    
    private static final String DATETIME = "dateTime";
    private static final String DATETIME_ = typeFormat("string", "date-time");
    
    private static final String PASSWORD = "password";
    private static final String PASSWORD_ = typeFormat("string", "password");
    
    // this no swagger definition
    private static final String BLOB = typeFormat("array", "");
    private static final String BLOB_ = typeFormat("object", "");
    
    private static HashMap<String, String> swaggerTypes = new HashMap<String, String>();    
    
    static {
        swaggerTypes.put(INTEGER, DataTypeManager.DefaultDataTypes.INTEGER);
        swaggerTypes.put(INTEGER_, DataTypeManager.DefaultDataTypes.INTEGER);
        swaggerTypes.put(LONG, DataTypeManager.DefaultDataTypes.LONG);
        swaggerTypes.put(LONG_, DataTypeManager.DefaultDataTypes.LONG);
        swaggerTypes.put(FLOAT, DataTypeManager.DefaultDataTypes.FLOAT);
        swaggerTypes.put(FLOAT_, DataTypeManager.DefaultDataTypes.FLOAT);
        swaggerTypes.put(DOUBLE, DataTypeManager.DefaultDataTypes.DOUBLE);
        swaggerTypes.put(DOUBLE_, DataTypeManager.DefaultDataTypes.DOUBLE);
        swaggerTypes.put(STRING, DataTypeManager.DefaultDataTypes.STRING);
        swaggerTypes.put(STRING_, DataTypeManager.DefaultDataTypes.STRING);
        swaggerTypes.put(BYTE, DataTypeManager.DefaultDataTypes.BYTE);
        swaggerTypes.put(BYTE_, DataTypeManager.DefaultDataTypes.BYTE);
        swaggerTypes.put(BINARY, DataTypeManager.DefaultDataTypes.BLOB);
        swaggerTypes.put(BINARY_, DataTypeManager.DefaultDataTypes.BLOB);
        swaggerTypes.put(BOOLEAN, DataTypeManager.DefaultDataTypes.BOOLEAN);
        swaggerTypes.put(BOOLEAN_, DataTypeManager.DefaultDataTypes.BOOLEAN);
        swaggerTypes.put(DATE, DataTypeManager.DefaultDataTypes.DATE);
        swaggerTypes.put(DATE_, DataTypeManager.DefaultDataTypes.DATE);
        swaggerTypes.put(DATETIME, DataTypeManager.DefaultDataTypes.TIME);
        swaggerTypes.put(DATETIME_, DataTypeManager.DefaultDataTypes.TIME);
        swaggerTypes.put(PASSWORD, DataTypeManager.DefaultDataTypes.STRING);
        swaggerTypes.put(PASSWORD_, DataTypeManager.DefaultDataTypes.STRING);
        
        swaggerTypes.put(BLOB, DataTypeManager.DefaultDataTypes.OBJECT);
        swaggerTypes.put(BLOB_, DataTypeManager.DefaultDataTypes.OBJECT);
        
    }
    
    static String teiidType(String name) {
        String type = swaggerTypes.get(name);
        if (type == null) {
            type = DataTypeManager.DefaultDataTypes.STRING; // special case for enum type
        }
        return type ;
    }
    
    static String teiidType(String type, String format) {
        if(null == format) {
            format = "";
        }
        String returnType = swaggerTypes.get(typeFormat(type, format));
        if(null == returnType) {
            returnType = DataTypeManager.DefaultDataTypes.STRING; 
        }
        return returnType;
    }

    public static Object convertTeiidRuntimeType(Object value, Class<?> expectedType) throws TranslatorException {
        if (value == null) {
            return null;
        }
        
        if (expectedType.isArray() && value instanceof List) {
            List<?> values = (List<?>)value;
            Class<?> expectedArrayComponentType = expectedType.getComponentType();
            Object array = Array.newInstance(expectedArrayComponentType, values.size());                
            for (int i = 0; i < values.size(); i++) {
                Object arrayItem = convertTeiidRuntimeType(values.get(i), expectedArrayComponentType);
                Array.set(array, i, arrayItem);
            }               
            return array;
        }
        
        if (expectedType.isAssignableFrom(value.getClass())) {
            return value;
        } else {
            
            if (value instanceof String) {
                return (String)value;
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
    

}
