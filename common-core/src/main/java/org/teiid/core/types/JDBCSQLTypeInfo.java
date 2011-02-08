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

package org.teiid.core.types;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLXML;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

/**
 * <p> This is a helper class used to obtain SQL type information for java types.
 * The SQL type information is obtained from java.sql.Types class. The integers and
 * strings returned by methods in this class are based on constants in java.sql.Types.
 */

public final class JDBCSQLTypeInfo {


    // Prevent instantiation
    private JDBCSQLTypeInfo() {}
    
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
    public static final String CLOB_CLASS = Clob.class.getName();
    public static final String BLOB_CLASS = Blob.class.getName();
    public static final String XML_CLASS = SQLXML.class.getName();
    
    private static Map<String, Integer> NAME_TO_TYPE_MAP = new HashMap<String, Integer>();
    private static Map<Integer, String> TYPE_TO_NAME_MAP = new HashMap<Integer, String>();
    private static Map<String, String> NAME_TO_CLASSNAME = new HashMap<String, String>();
    private static Map<String, String> CLASSNAME_TO_NAME = new HashMap<String, String>();
    
    static {
        addTypeMapping(STRING, STRING_CLASS, Types.VARCHAR, Types.CHAR);
        addTypeMapping(CHAR, CHAR_CLASS, Types.CHAR, false);
        addTypeMapping(BOOLEAN, BOOLEAN_CLASS, Types.BIT, Types.BOOLEAN);
        addTypeMapping(TIME, TIME_CLASS, Types.TIME);
        addTypeMapping(DATE, DATE_CLASS, Types.DATE);
        addTypeMapping(TIMESTAMP, TIMESTAMP_CLASS, Types.TIMESTAMP);
        addTypeMapping(INTEGER, INTEGER_CLASS, Types.INTEGER);
        addTypeMapping(FLOAT, FLOAT_CLASS, Types.REAL);
        addTypeMapping(DOUBLE, DOUBLE_CLASS, Types.DOUBLE, Types.FLOAT);
        addTypeMapping(BIGDECIMAL, BIGDECIMAL_CLASS, Types.NUMERIC, Types.DECIMAL);
        addTypeMapping(BIGINTEGER, BIGINTEGER_CLASS, Types.NUMERIC, false);
        addTypeMapping(BYTE, BYTE_CLASS, Types.TINYINT);
        addTypeMapping(SHORT, SHORT_CLASS, Types.SMALLINT);
        addTypeMapping(LONG, LONG_CLASS, Types.BIGINT);
        addTypeMapping(OBJECT, OBJECT_CLASS, Types.JAVA_OBJECT);
        addTypeMapping(CLOB, CLOB_CLASS, Types.CLOB, Types.LONGVARCHAR);
        addTypeMapping(BLOB, BLOB_CLASS, Types.BLOB, Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY);
        
        addTypeMapping(NULL, null, Types.NULL);
        
        addTypeMapping(XML, XML_CLASS, Types.SQLXML);
		TYPE_TO_NAME_MAP.put(Types.NVARCHAR, STRING);
        TYPE_TO_NAME_MAP.put(Types.LONGNVARCHAR, STRING);
		TYPE_TO_NAME_MAP.put(Types.NCHAR, CHAR);
		TYPE_TO_NAME_MAP.put(Types.NCLOB, CLOB);
    }

	private static void addTypeMapping(String typeName, String javaClass, int sqlType, int ... secondaryTypes) {
		addTypeMapping(typeName, javaClass, sqlType, true);
		for (int type : secondaryTypes) {
			TYPE_TO_NAME_MAP.put(type, typeName);
		}
	}

	private static void addTypeMapping(String typeName, String javaClass, int sqlType, boolean preferedType) {
		NAME_TO_TYPE_MAP.put(typeName, sqlType);
		if (preferedType) {
			TYPE_TO_NAME_MAP.put(sqlType, typeName);
		}
		if (javaClass != null) {
			NAME_TO_CLASSNAME.put(typeName, javaClass);
			CLASSNAME_TO_NAME.put(javaClass, typeName);
		}
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
        
        Integer sqlType = NAME_TO_TYPE_MAP.get(typeName);
        
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
    public static final int getSQLTypeFromClass(String className) {

        if (className == null) {
            return Types.NULL;
        }
        
        String name = CLASSNAME_TO_NAME.get(className);
        
        if (name == null) {
            return Types.JAVA_OBJECT;
        }
        
        return getSQLType(name);
    } 
    
    /**
     * Get the sql type from the given runtime type 
     * @param type
     * @return
     */
    public static final int getSQLTypeFromRuntimeType(Class<?> type) {
    	if (type == null) {
    		return Types.NULL;
    	}
    	
        String name = DataTypeManager.getDataTypeName(type);
        
        if (name == null) {
            return Types.JAVA_OBJECT;
        }
        
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
    	String className = NAME_TO_CLASSNAME.get(getTypeName(jdbcSQLType));
    	
    	if (className == null) {
    		return OBJECT_CLASS;
    	}
    	
    	return className;
    }
    
    public static final String getTypeName(int sqlType) {
    	String name = TYPE_TO_NAME_MAP.get(sqlType);
    	
    	if (name == null) {
    		return OBJECT;
    	}
    	
    	return name;
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
