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

package org.teiid.client.metadata;

import java.util.HashMap;
import java.util.Map;

import org.teiid.core.types.DataTypeManager;



/** 
 * @since 4.2
 */
public class ResultsMetadataDefaults {
    public static final Integer DEFAULT_RADIX = new Integer(10);
    public static final Integer DEFAULT_SCALE = new Integer(0);

    // XML column constants
    public final static String XML_COLUMN_NAME = "xml"; //$NON-NLS-1$
    public final static Integer XML_COLUMN_LENGTH = new Integer(Integer.MAX_VALUE);

    // Update constants
    public static final String UPDATE_COLUMN = "count"; //$NON-NLS-1$

    /** Maximum display size for the data type (Class -> Integer) */
    private static final Map<String, Integer> MAX_DISPLAY_SIZE = new HashMap<String, Integer>();
    /** Default precision for a data type (String -> Integer) */
    private static final Map<String, Integer> DEFAULT_PRECISION = new HashMap<String, Integer>();

    static {
        MAX_DISPLAY_SIZE.put(DataTypeManager.DefaultDataTypes.BIG_DECIMAL, new Integer(22));
        MAX_DISPLAY_SIZE.put(DataTypeManager.DefaultDataTypes.BIG_INTEGER, new Integer(20));
        MAX_DISPLAY_SIZE.put(DataTypeManager.DefaultDataTypes.BLOB, new Integer(Integer.MAX_VALUE));
        MAX_DISPLAY_SIZE.put(DataTypeManager.DefaultDataTypes.BOOLEAN, new Integer(5));
        MAX_DISPLAY_SIZE.put(DataTypeManager.DefaultDataTypes.BYTE, new Integer(4));
        MAX_DISPLAY_SIZE.put(DataTypeManager.DefaultDataTypes.CHAR, new Integer(1));
        MAX_DISPLAY_SIZE.put(DataTypeManager.DefaultDataTypes.CLOB, new Integer(Integer.MAX_VALUE));
        MAX_DISPLAY_SIZE.put(DataTypeManager.DefaultDataTypes.DATE, new Integer(10));
        MAX_DISPLAY_SIZE.put(DataTypeManager.DefaultDataTypes.DOUBLE, new Integer(22));
        MAX_DISPLAY_SIZE.put(DataTypeManager.DefaultDataTypes.FLOAT, new Integer(22));
        MAX_DISPLAY_SIZE.put(DataTypeManager.DefaultDataTypes.INTEGER, new Integer(11));
        MAX_DISPLAY_SIZE.put(DataTypeManager.DefaultDataTypes.LONG, new Integer(20));
        MAX_DISPLAY_SIZE.put(DataTypeManager.DefaultDataTypes.NULL, new Integer(0));
        MAX_DISPLAY_SIZE.put(DataTypeManager.DefaultDataTypes.OBJECT, new Integer(Integer.MAX_VALUE));
        MAX_DISPLAY_SIZE.put(DataTypeManager.DefaultDataTypes.SHORT, new Integer(6));
        MAX_DISPLAY_SIZE.put(DataTypeManager.DefaultDataTypes.STRING, new Integer(DataTypeManager.MAX_STRING_LENGTH));
        MAX_DISPLAY_SIZE.put(DataTypeManager.DefaultDataTypes.TIME, new Integer(8));
        MAX_DISPLAY_SIZE.put(DataTypeManager.DefaultDataTypes.TIMESTAMP, new Integer(29));
        MAX_DISPLAY_SIZE.put(DataTypeManager.DefaultDataTypes.XML, new Integer(Integer.MAX_VALUE));
        MAX_DISPLAY_SIZE.put(DataTypeManager.DefaultDataTypes.NULL, 4);

        /* NOTE1
         * For non-numeric columns (BLOB, BOOLEAN, CHAR, CLOB, DATE, OBJECT, STRING, TIME, TIMESTAMP, XML),
         * the default precision should be the max allowed length of the column
         */
        DEFAULT_PRECISION.put(DataTypeManager.DefaultDataTypes.BIG_DECIMAL, new Integer(20));
        DEFAULT_PRECISION.put(DataTypeManager.DefaultDataTypes.BIG_INTEGER, new Integer(19));
        DEFAULT_PRECISION.put(DataTypeManager.DefaultDataTypes.BLOB, new Integer(Integer.MAX_VALUE));
        DEFAULT_PRECISION.put(DataTypeManager.DefaultDataTypes.BOOLEAN, new Integer(1));
        DEFAULT_PRECISION.put(DataTypeManager.DefaultDataTypes.BYTE, new Integer(3));
        DEFAULT_PRECISION.put(DataTypeManager.DefaultDataTypes.CHAR, new Integer(1));
        DEFAULT_PRECISION.put(DataTypeManager.DefaultDataTypes.CLOB, new Integer(Integer.MAX_VALUE));
        DEFAULT_PRECISION.put(DataTypeManager.DefaultDataTypes.DATE, new Integer(10));
        DEFAULT_PRECISION.put(DataTypeManager.DefaultDataTypes.DOUBLE, new Integer(20));
        DEFAULT_PRECISION.put(DataTypeManager.DefaultDataTypes.FLOAT, new Integer(20));
        DEFAULT_PRECISION.put(DataTypeManager.DefaultDataTypes.INTEGER, new Integer(10));
        DEFAULT_PRECISION.put(DataTypeManager.DefaultDataTypes.LONG, new Integer(19));
        DEFAULT_PRECISION.put(DataTypeManager.DefaultDataTypes.NULL, new Integer(0));
        DEFAULT_PRECISION.put(DataTypeManager.DefaultDataTypes.OBJECT, new Integer(Integer.MAX_VALUE));
        DEFAULT_PRECISION.put(DataTypeManager.DefaultDataTypes.SHORT, new Integer(5));
        DEFAULT_PRECISION.put(DataTypeManager.DefaultDataTypes.STRING, new Integer(DataTypeManager.MAX_STRING_LENGTH));
        DEFAULT_PRECISION.put(DataTypeManager.DefaultDataTypes.TIME, new Integer(8));
        DEFAULT_PRECISION.put(DataTypeManager.DefaultDataTypes.TIMESTAMP, new Integer(29));
        DEFAULT_PRECISION.put(DataTypeManager.DefaultDataTypes.XML, new Integer(Integer.MAX_VALUE));
        DEFAULT_PRECISION.put(DataTypeManager.DefaultDataTypes.NULL, 1);
    }
    
    public static Integer getMaxDisplaySize(Class<?> dataTypeClass) {
        return MAX_DISPLAY_SIZE.get(DataTypeManager.getDataTypeName(dataTypeClass));
    }
    
    public static Integer getMaxDisplaySize(String typeName) {
        return MAX_DISPLAY_SIZE.get(typeName);
    }
    
    public static Integer getDefaultPrecision(Class<?> dataTypeClass) {
        return DEFAULT_PRECISION.get(DataTypeManager.getDataTypeName(dataTypeClass));
    }
    
    public static Integer getDefaultPrecision(String typeName) {
        return DEFAULT_PRECISION.get(typeName);
    }
    
    /** Uninstantiable */
    private ResultsMetadataDefaults() {}

}
