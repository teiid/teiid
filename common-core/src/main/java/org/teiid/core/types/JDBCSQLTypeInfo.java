/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.core.types;

import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLXML;
import java.sql.Types;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * <p> This is a helper class used to obtain SQL type information for java types.
 * The SQL type information is obtained from java.sql.Types class. The integers and
 * strings returned by methods in this class are based on constants in java.sql.Types.
 */

public final class JDBCSQLTypeInfo {

    public static class TypeInfo {
        String name;
        int maxDisplaySize;
        int defaultPrecision;
        String javaClassName;
        int[] jdbcTypes;

        public TypeInfo(int maxDisplaySize, int precision, String name,
                String javaClassName, int[] jdbcTypes) {
            super();
            this.maxDisplaySize = maxDisplaySize;
            this.defaultPrecision = precision;
            this.name = name;
            this.javaClassName = javaClassName;
            this.jdbcTypes = jdbcTypes;
        }

    }

    // Prevent instantiation
    private JDBCSQLTypeInfo() {}

    public static final Integer DEFAULT_RADIX = 10;
    public static final Integer DEFAULT_SCALE = 0;

    // XML column constants
    public final static Integer XML_COLUMN_LENGTH = Integer.MAX_VALUE;

    private static Map<String, TypeInfo> NAME_TO_TYPEINFO = new LinkedHashMap<String, TypeInfo>();
    private static Map<Integer, TypeInfo> TYPE_TO_TYPEINFO = new HashMap<Integer, TypeInfo>();
    private static Map<String, TypeInfo> CLASSNAME_TO_TYPEINFO = new HashMap<String, TypeInfo>();

    static {
        //note the order in which these are added matters.  if there are multiple sql type mappings (e.g. biginteger and bigdecimal to numeric), the latter will be the primary
        addType(DataTypeManager.DefaultDataTypes.BIG_INTEGER, 20, 19, DataTypeManager.DefaultDataClasses.BIG_INTEGER.getName(), Types.NUMERIC);
        addType(new String[] {DataTypeManager.DefaultDataTypes.BIG_DECIMAL, "decimal"}, 22, 20, DataTypeManager.DefaultDataClasses.BIG_DECIMAL.getName(), Types.NUMERIC, Types.DECIMAL); //$NON-NLS-1$
        addType(DataTypeManager.DefaultDataTypes.GEOMETRY, Integer.MAX_VALUE, Integer.MAX_VALUE, GeometryType.class.getName(), Types.BLOB, Types.LONGVARBINARY);
        addType(DataTypeManager.DefaultDataTypes.GEOGRAPHY, Integer.MAX_VALUE, Integer.MAX_VALUE, GeographyType.class.getName(), Types.BLOB, Types.LONGVARBINARY);
        addType(DataTypeManager.DefaultDataTypes.BLOB, Integer.MAX_VALUE, Integer.MAX_VALUE, Blob.class.getName(), Types.BLOB, Types.LONGVARBINARY);
        addType(DataTypeManager.DefaultDataTypes.BOOLEAN, 5, 1, DataTypeManager.DefaultDataClasses.BOOLEAN.getName(), Types.BIT, Types.BOOLEAN);
        addType(new String[] {DataTypeManager.DefaultDataTypes.BYTE, "tinyint"}, 4, 3, DataTypeManager.DefaultDataClasses.BYTE.getName(), Types.TINYINT); //$NON-NLS-1$
        addType(DataTypeManager.DefaultDataTypes.CHAR, 1, 1, DataTypeManager.DefaultDataClasses.CHAR.getName(), Types.CHAR);
        addType(DataTypeManager.DefaultDataTypes.JSON, Integer.MAX_VALUE, Integer.MAX_VALUE, Clob.class.getName(), Types.CLOB, Types.NCLOB, Types.LONGNVARCHAR, Types.LONGVARCHAR);
        addType(DataTypeManager.DefaultDataTypes.CLOB, Integer.MAX_VALUE, Integer.MAX_VALUE, Clob.class.getName(), Types.CLOB, Types.NCLOB, Types.LONGNVARCHAR, Types.LONGVARCHAR);
        addType(DataTypeManager.DefaultDataTypes.DATE, 10, 10, DataTypeManager.DefaultDataClasses.DATE.getName(), Types.DATE);
        addType(DataTypeManager.DefaultDataTypes.DOUBLE, 22, 20, DataTypeManager.DefaultDataClasses.DOUBLE.getName(), Types.DOUBLE, Types.FLOAT);
        addType(new String[] {DataTypeManager.DefaultDataTypes.FLOAT, "real"}, 22, 20, DataTypeManager.DefaultDataClasses.FLOAT.getName(), Types.REAL); //$NON-NLS-1$
        addType(DataTypeManager.DefaultDataTypes.INTEGER, 11, 10, DataTypeManager.DefaultDataClasses.INTEGER.getName(), Types.INTEGER);
        addType(new String[] {DataTypeManager.DefaultDataTypes.LONG, "bigint"}, 20, 19, DataTypeManager.DefaultDataClasses.LONG.getName(), Types.BIGINT); //$NON-NLS-1$
        addType(DataTypeManager.DefaultDataTypes.OBJECT, Integer.MAX_VALUE, Integer.MAX_VALUE, DataTypeManager.DefaultDataClasses.OBJECT.getName(), Types.JAVA_OBJECT);
        addType(new String[] {DataTypeManager.DefaultDataTypes.SHORT, "smallint"}, 6, 5, DataTypeManager.DefaultDataClasses.SHORT.getName(), Types.SMALLINT); //$NON-NLS-1$
        addType(new String[] {DataTypeManager.DefaultDataTypes.STRING, "varchar"}, DataTypeManager.MAX_STRING_LENGTH, DataTypeManager.MAX_STRING_LENGTH, DataTypeManager.DefaultDataClasses.STRING.getName(), Types.VARCHAR, Types.NVARCHAR, Types.CHAR, Types.NCHAR); //$NON-NLS-1$
        addType(DataTypeManager.DefaultDataTypes.TIME, 8, 8, DataTypeManager.DefaultDataClasses.TIME.getName(), Types.TIME);
        addType(DataTypeManager.DefaultDataTypes.TIMESTAMP, 29, 29, DataTypeManager.DefaultDataClasses.TIMESTAMP.getName(), Types.TIMESTAMP);
        addType(DataTypeManager.DefaultDataTypes.XML, Integer.MAX_VALUE, Integer.MAX_VALUE, SQLXML.class.getName(), Types.SQLXML);
        addType(DataTypeManager.DefaultDataTypes.NULL, 4, 1, null, Types.NULL);
        addType(DataTypeManager.DefaultDataTypes.VARBINARY, DataTypeManager.MAX_VARBINARY_BYTES, DataTypeManager.MAX_VARBINARY_BYTES, byte[].class.getName(), Types.VARBINARY, Types.BINARY);

        TypeInfo typeInfo = new TypeInfo(Integer.MAX_VALUE, 0, "ARRAY", Array.class.getName(), new int[Types.ARRAY]); //$NON-NLS-1$
        CLASSNAME_TO_TYPEINFO.put(Array.class.getName(), typeInfo);
        TYPE_TO_TYPEINFO.put(Types.ARRAY, typeInfo);
    }

    private static TypeInfo addType(String typeName, int maxDisplaySize, int precision, String javaClassName, int... sqlTypes) {
        TypeInfo ti = new TypeInfo(maxDisplaySize, precision, typeName, javaClassName, sqlTypes);
        NAME_TO_TYPEINFO.put(typeName, ti);
        if (javaClassName != null) {
            CLASSNAME_TO_TYPEINFO.put(javaClassName, ti);
        }
        for (int i : sqlTypes) {
            TYPE_TO_TYPEINFO.put(i, ti);
        }
        return ti;
    }

    private static void addType(String[] typeNames, int maxDisplaySize, int precision, String javaClassName, int... sqlTypes) {
        TypeInfo ti = addType(typeNames[0], maxDisplaySize, precision, javaClassName, sqlTypes);
        for (int i = 1; i < typeNames.length; i++) {
            NAME_TO_TYPEINFO.put(typeNames[i], ti);
        }
    }

    /**
     * This method is used to obtain a short indicating JDBC SQL type for any object.
     * The short values that give the type info are from java.sql.Types.
     * @param typeName of the teiid type.
     * @return A short value representing SQL Type for the given java type.
     */
    public static final int getSQLType(String typeName) {

        if (typeName == null) {
            return Types.NULL;
        }

        TypeInfo sqlType = NAME_TO_TYPEINFO.get(typeName);

        if (sqlType == null) {
            if (DataTypeManager.isArrayType(typeName)) {
                return Types.ARRAY;
            }
            return Types.JAVA_OBJECT;
        }

        return sqlType.jdbcTypes[0];
    }

    /**
     * Get sql Type from java class type name.  This should not be called with runtime types
     * as Clob and Blob are represented by ClobType and BlobType respectively.
     * @param className
     * @return int
     */
    public static final int getSQLTypeFromClass(String className) {

        if (className == null) {
            return Types.NULL;
        }

        TypeInfo sqlType = CLASSNAME_TO_TYPEINFO.get(className);

        if (sqlType == null) {
            return Types.JAVA_OBJECT;
        }

        return sqlType.jdbcTypes[0];
    }

    /**
     * Get the sql type from the given runtime type
     * @param type
     * @return the SQL type code
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
     * @param jdbcSQLType value giving the SQL type code.
     * @return A String representing the java class name for the given SQL Type.
     */
    public static final String getJavaClassName(int jdbcSQLType) {
        TypeInfo typeInfo = TYPE_TO_TYPEINFO.get(jdbcSQLType);

        if (typeInfo == null) {
            return DataTypeManager.DefaultDataClasses.OBJECT.getName();
        }

        return typeInfo.javaClassName;
    }

    public static final String getTypeName(int sqlType) {
        TypeInfo typeInfo = TYPE_TO_TYPEINFO.get(sqlType);

        if (typeInfo == null) {
            return DataTypeManager.DefaultDataTypes.OBJECT;
        }

        return typeInfo.name;
    }

    public static Set<String> getMMTypeNames() {
        return NAME_TO_TYPEINFO.keySet();
    }

    public static Integer getMaxDisplaySize(Class<?> dataTypeClass) {
        return getMaxDisplaySize(DataTypeManager.getDataTypeName(dataTypeClass));
    }

    public static Integer getMaxDisplaySize(String typeName) {
        TypeInfo ti = NAME_TO_TYPEINFO.get(typeName);
        if (ti == null) {
            return null;
        }
        return ti.maxDisplaySize;
    }

    public static Integer getDefaultPrecision(Class<?> dataTypeClass) {
        return getDefaultPrecision(DataTypeManager.getDataTypeName(dataTypeClass));
    }

    public static Integer getDefaultPrecision(String typeName) {
        TypeInfo ti = NAME_TO_TYPEINFO.get(typeName);
        if (ti == null) {
            return null;
        }
        return ti.defaultPrecision;
    }

}
