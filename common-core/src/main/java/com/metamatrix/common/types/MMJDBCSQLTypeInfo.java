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

package com.metamatrix.common.types;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

/**
 * <p> This is a helper class used to obtain SQL type information for java types.
 * The SQL type infomation is obtained from java.sql.Types class. The integers and
 * strings retuned by methods in this class are based on constants in java.sql.Types.
 */

public final class MMJDBCSQLTypeInfo {


    // Prevent instantiation
    private MMJDBCSQLTypeInfo() {}
    
    // metamatrix types    
    public static final String STRING = DataTypeManager.DefaultDataTypes.STRING;
    public static final String BOOLEAN = DataTypeManager.DefaultDataTypes.BOOLEAN;
    public static final String TIME = DataTypeManager.DefaultDataTypes.TIME;
    public static final String DATE = DataTypeManager.DefaultDataTypes.DATE;
    public static final String TIMESTAMP = DataTypeManager.DefaultDataTypes.TIMESTAMP;
    public static final String INTEGER = DataTypeManager.DefaultDataTypes.INTEGER;
    public static final String FLOAT = DataTypeManager.DefaultDataTypes.FLOAT;
    public static final String DOUBLE = DataTypeManager.DefaultDataTypes.DOUBLE;
    public static final String BIGDECIMAL = DataTypeManager.DefaultDataTypes.BIG_DECIMAL;
    public static final String BIGINTEGER = DataTypeManager.DefaultDataTypes.BIG_INTEGER;
    public static final String BYTE = DataTypeManager.DefaultDataTypes.BYTE;
    public static final String SHORT = DataTypeManager.DefaultDataTypes.SHORT;
    public static final String LONG = DataTypeManager.DefaultDataTypes.LONG;
    public static final String CHAR = DataTypeManager.DefaultDataTypes.CHAR;
    public static final String OBJECT = DataTypeManager.DefaultDataTypes.OBJECT;
    public static final String CLOB = DataTypeManager.DefaultDataTypes.CLOB;
    public static final String BLOB = DataTypeManager.DefaultDataTypes.BLOB;
    public static final String XML = DataTypeManager.DefaultDataTypes.XML;
    public static final String NULL = DataTypeManager.DefaultDataTypes.NULL;
    
    private static Map NAME_TO_TYPE_MAP = new HashMap();
    
    static {
        NAME_TO_TYPE_MAP.put(STRING, new Integer(Types.VARCHAR));
        NAME_TO_TYPE_MAP.put(BOOLEAN, new Integer(Types.BIT));
        NAME_TO_TYPE_MAP.put(TIME, new Integer(Types.TIME));
        NAME_TO_TYPE_MAP.put(DATE, new Integer(Types.DATE));
        NAME_TO_TYPE_MAP.put(TIMESTAMP, new Integer(Types.TIMESTAMP));
        NAME_TO_TYPE_MAP.put(INTEGER, new Integer(Types.INTEGER));
        NAME_TO_TYPE_MAP.put(FLOAT, new Integer(Types.REAL));
        NAME_TO_TYPE_MAP.put(DOUBLE, new Integer(Types.DOUBLE));
        NAME_TO_TYPE_MAP.put(BIGDECIMAL, new Integer(Types.NUMERIC));
        NAME_TO_TYPE_MAP.put(BIGINTEGER, new Integer(Types.NUMERIC));
        NAME_TO_TYPE_MAP.put(BYTE, new Integer(Types.TINYINT));
        NAME_TO_TYPE_MAP.put(SHORT, new Integer(Types.SMALLINT));
        NAME_TO_TYPE_MAP.put(LONG, new Integer(Types.BIGINT));
        NAME_TO_TYPE_MAP.put(CHAR, new Integer(Types.CHAR));
        NAME_TO_TYPE_MAP.put(OBJECT, new Integer(Types.JAVA_OBJECT));
        NAME_TO_TYPE_MAP.put(CLOB, new Integer(Types.CLOB));
        NAME_TO_TYPE_MAP.put(BLOB, new Integer(Types.BLOB));
        NAME_TO_TYPE_MAP.put(XML, new Integer(Types.JAVA_OBJECT));
        NAME_TO_TYPE_MAP.put(NULL, new Integer(Types.NULL));
    }
    
    //java class names
    public static final String STRING_CLASS = DataTypeManager.DefaultDataClasses.STRING.getName();
    public static final String BOOLEAN_CLASS = DataTypeManager.DefaultDataClasses.BOOLEAN.getName();
    public static final String TIME_CLASS = DataTypeManager.DefaultDataClasses.TIME.getName();
    public static final String DATE_CLASS = DataTypeManager.DefaultDataClasses.DATE.getName();
    public static final String TIMESTAMP_CLASS = DataTypeManager.DefaultDataClasses.TIMESTAMP.getName();
    public static final String INTEGER_CLASS = DataTypeManager.DefaultDataClasses.INTEGER.getName();
    public static final String FLOAT_CLASS = DataTypeManager.DefaultDataClasses.FLOAT.getName();
    public static final String DOUBLE_CLASS = DataTypeManager.DefaultDataClasses.DOUBLE.getName();
    public static final String BIGDECIMAL_CLASS = DataTypeManager.DefaultDataClasses.BIG_DECIMAL.getName();
    public static final String BYTE_CLASS = DataTypeManager.DefaultDataClasses.BYTE.getName();
    public static final String SHORT_CLASS = DataTypeManager.DefaultDataClasses.SHORT.getName();
    public static final String LONG_CLASS = DataTypeManager.DefaultDataClasses.LONG.getName();
    public static final String CHAR_CLASS = DataTypeManager.DefaultDataClasses.CHAR.getName();
    public static final String BIGINTEGER_CLASS = DataTypeManager.DefaultDataClasses.BIG_INTEGER.getName();
    public static final String OBJECT_CLASS = DataTypeManager.DefaultDataClasses.OBJECT.getName();
    public static final String CLOB_CLASS = "java.sql.Clob"; //$NON-NLS-1$
    public static final String BLOB_CLASS = "java.sql.Blob"; //$NON-NLS-1$
    public static final String XML_CLASS = "com.metamatrix.core.jdbc.SQLXML"; //$NON-NLS-1$

    private static Map CLASSNAME_TO_TYPE_MAP = new HashMap();
    
    static {
        CLASSNAME_TO_TYPE_MAP.put(STRING_CLASS.toLowerCase(), new Integer(Types.VARCHAR));
        CLASSNAME_TO_TYPE_MAP.put(BOOLEAN_CLASS.toLowerCase(), new Integer(Types.BIT));
        CLASSNAME_TO_TYPE_MAP.put(TIME_CLASS.toLowerCase(), new Integer(Types.TIME));
        CLASSNAME_TO_TYPE_MAP.put(DATE_CLASS.toLowerCase(), new Integer(Types.DATE));
        CLASSNAME_TO_TYPE_MAP.put(TIMESTAMP_CLASS.toLowerCase(), new Integer(Types.TIMESTAMP));
        CLASSNAME_TO_TYPE_MAP.put(INTEGER_CLASS.toLowerCase(), new Integer(Types.INTEGER));
        CLASSNAME_TO_TYPE_MAP.put(FLOAT_CLASS.toLowerCase(), new Integer(Types.REAL));
        CLASSNAME_TO_TYPE_MAP.put(DOUBLE_CLASS.toLowerCase(), new Integer(Types.DOUBLE));
        CLASSNAME_TO_TYPE_MAP.put(BIGDECIMAL_CLASS.toLowerCase(), new Integer(Types.NUMERIC));
        CLASSNAME_TO_TYPE_MAP.put(BIGINTEGER_CLASS.toLowerCase(), new Integer(Types.NUMERIC));
        CLASSNAME_TO_TYPE_MAP.put(BYTE_CLASS.toLowerCase(), new Integer(Types.TINYINT));
        CLASSNAME_TO_TYPE_MAP.put(SHORT_CLASS.toLowerCase(), new Integer(Types.SMALLINT));
        CLASSNAME_TO_TYPE_MAP.put(LONG_CLASS.toLowerCase(), new Integer(Types.BIGINT));
        CLASSNAME_TO_TYPE_MAP.put(CHAR_CLASS.toLowerCase(), new Integer(Types.CHAR));
        CLASSNAME_TO_TYPE_MAP.put(OBJECT_CLASS.toLowerCase(), new Integer(Types.JAVA_OBJECT));
        CLASSNAME_TO_TYPE_MAP.put(CLOB_CLASS.toLowerCase(), new Integer(Types.CLOB));
        CLASSNAME_TO_TYPE_MAP.put(BLOB_CLASS.toLowerCase(), new Integer(Types.BLOB));
        CLASSNAME_TO_TYPE_MAP.put(XML_CLASS.toLowerCase(), new Integer(Types.JAVA_OBJECT));
    }
    
    /**
     * This method is used to obtain a short indicating JDBC SQL type for any object.
     * The short values that give the type info are from java.sql.Types.
     * @param Name of the metamatrix type.
     * @return A short value representing SQL Type for the given java type.
     */
    public static final int getSQLType(String typeName) {

        if (typeName == null) {
            return Types.NULL;
        }
        
        Integer sqlType = (Integer)NAME_TO_TYPE_MAP.get(typeName.toLowerCase());
        
        if (sqlType == null) {
            return Types.JAVA_OBJECT;
        }
        
        return sqlType.intValue();
    }    

    /**
     * Get sql Type from java class type name.  This should not be called with runtime types
     * as Clob and Blob are represented by ClobType and BlobType respectively.
     * @param typeName
     * @return int
     */
    public static final int getSQLTypeFromClass(String typeName) {

        if (typeName == null) {
            return Types.NULL;
        }
        
        Integer sqlType = (Integer)CLASSNAME_TO_TYPE_MAP.get(typeName.toLowerCase());
        
        if (sqlType == null) {
            return Types.JAVA_OBJECT;
        }
        
        return sqlType.intValue();
    } 
    
    /**
     * Get the sql type from the given runtime type 
     * @param type
     * @return
     */
    public static final int getSQLTypeFromRuntimeType(Class type) {
        String name = DataTypeManager.getDataTypeName(type);
        
        return getSQLType(name);
    }
    
    /**
     * This method is used to obtain a the java class name given an int value
     * indicating JDBC SQL type. The int values that give the type info are from
     * java.sql.Types.
     * @param int value giving the SQL type code.
     * @return A String representing the java class name for the given SQL Type.
     */
    public static final String getJavaClassName(int jdbcSQLType) {

        String javaClassName;

        switch(jdbcSQLType) {
            case Types.VARCHAR:
                javaClassName = STRING_CLASS;
                break;
            case Types.LONGVARCHAR:
                javaClassName = STRING_CLASS;
                break;
            case Types.CHAR:
                javaClassName = CHAR_CLASS;
                break;
            case Types.BIT:
                javaClassName = BOOLEAN_CLASS;
                break;
            case Types.DATE:
                javaClassName = DATE_CLASS;
                break;
            case Types.TIME:
                javaClassName = TIME_CLASS;
                break;                
            case Types.TIMESTAMP:
                javaClassName = TIMESTAMP_CLASS;
                break;
            case Types.INTEGER:
                javaClassName = INTEGER_CLASS;
                break;
           case Types.REAL:
           		javaClassName = FLOAT_CLASS;
                break;
            case Types.FLOAT:
                javaClassName = DOUBLE_CLASS;
                break; 
            case Types.DOUBLE:
                javaClassName = DOUBLE_CLASS;
                break;
            case Types.NUMERIC:
                javaClassName = BIGDECIMAL_CLASS;
                break;
            case Types.DECIMAL:
                javaClassName = BIGDECIMAL_CLASS;
                break;
            case Types.BIGINT:
                javaClassName = LONG_CLASS;
                break;
            case Types.TINYINT:
                javaClassName = BYTE_CLASS;
                break;
            case Types.SMALLINT:
                javaClassName = SHORT_CLASS;
                break;
            case Types.JAVA_OBJECT:
                 javaClassName = OBJECT_CLASS;
                 break;
            case Types.CLOB:
                 javaClassName = CLOB_CLASS;
                 break;
            case Types.BLOB:
                 javaClassName = BLOB_CLASS;
                 break;
            default:
                javaClassName = null;
                break;
        }

        return javaClassName; // return java class name
    }

    public static String[] getMMTypeNames() {
        return new String[] {
            STRING,
            BOOLEAN,
            TIME,
            DATE,
            TIMESTAMP,
            INTEGER,
            FLOAT,
            DOUBLE,
            BIGDECIMAL,
            BIGINTEGER,
            BYTE,
            SHORT,
            LONG,
            CHAR,
            OBJECT,
            CLOB,
            BLOB,
            XML
        };
    }

}
