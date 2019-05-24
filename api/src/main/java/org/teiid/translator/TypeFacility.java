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

package org.teiid.translator;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.teiid.core.types.*;
import org.teiid.core.types.basic.ObjectToAnyTransform;
import org.teiid.core.util.TimestampWithTimezone;

/**
 */
public class TypeFacility {

    public static final class RUNTIME_CODES {
        public static final int STRING = DataTypeManager.DefaultTypeCodes.STRING;
        public static final int CHAR = DataTypeManager.DefaultTypeCodes.CHAR;
        public static final int BOOLEAN = DataTypeManager.DefaultTypeCodes.BOOLEAN;
        public static final int BYTE = DataTypeManager.DefaultTypeCodes.BYTE;
        public static final int SHORT = DataTypeManager.DefaultTypeCodes.SHORT;
        public static final int INTEGER = DataTypeManager.DefaultTypeCodes.INTEGER;
        public static final int LONG = DataTypeManager.DefaultTypeCodes.LONG;
        public static final int BIG_INTEGER = DataTypeManager.DefaultTypeCodes.BIGINTEGER;
        public static final int FLOAT = DataTypeManager.DefaultTypeCodes.FLOAT;
        public static final int DOUBLE = DataTypeManager.DefaultTypeCodes.DOUBLE;
        public static final int BIG_DECIMAL = DataTypeManager.DefaultTypeCodes.BIGDECIMAL;
        public static final int DATE = DataTypeManager.DefaultTypeCodes.DATE;
        public static final int TIME = DataTypeManager.DefaultTypeCodes.TIME;
        public static final int TIMESTAMP = DataTypeManager.DefaultTypeCodes.TIMESTAMP;
        public static final int OBJECT = DataTypeManager.DefaultTypeCodes.OBJECT;
        public static final int BLOB = DataTypeManager.DefaultTypeCodes.BLOB;
        public static final int CLOB = DataTypeManager.DefaultTypeCodes.CLOB;
        public static final int XML = DataTypeManager.DefaultTypeCodes.XML;
        public static final int NULL = DataTypeManager.DefaultTypeCodes.NULL;
        public static final int VARBINARY = DataTypeManager.DefaultTypeCodes.VARBINARY;
        public static final int GEOMETRY = DataTypeManager.DefaultTypeCodes.GEOMETRY;
        public static final int GEOGRAPHY = DataTypeManager.DefaultTypeCodes.GEOGRAPHY;
        public static final int JSON = DataTypeManager.DefaultTypeCodes.JSON;
    }

    public interface RUNTIME_TYPES {
        public static final Class<String> STRING        = DataTypeManager.DefaultDataClasses.STRING;
        public static final Class<Boolean> BOOLEAN       = DataTypeManager.DefaultDataClasses.BOOLEAN;
        public static final Class<Byte> BYTE          = DataTypeManager.DefaultDataClasses.BYTE;
        public static final Class<Short> SHORT         = DataTypeManager.DefaultDataClasses.SHORT;
        public static final Class<Character> CHAR          = DataTypeManager.DefaultDataClasses.CHAR;
        public static final Class<Integer> INTEGER       = DataTypeManager.DefaultDataClasses.INTEGER;
        public static final Class<Long> LONG          = DataTypeManager.DefaultDataClasses.LONG;
        public static final Class<BigInteger> BIG_INTEGER   = DataTypeManager.DefaultDataClasses.BIG_INTEGER;
        public static final Class<Float> FLOAT         = DataTypeManager.DefaultDataClasses.FLOAT;
        public static final Class<Double> DOUBLE        = DataTypeManager.DefaultDataClasses.DOUBLE;
        public static final Class<? extends BigDecimal> BIG_DECIMAL   = DataTypeManager.DefaultDataClasses.BIG_DECIMAL;
        public static final Class<java.sql.Date> DATE          = DataTypeManager.DefaultDataClasses.DATE;
        public static final Class<Time> TIME          = DataTypeManager.DefaultDataClasses.TIME;
        public static final Class<Timestamp> TIMESTAMP     = DataTypeManager.DefaultDataClasses.TIMESTAMP;
        public static final Class<Object> OBJECT        = DataTypeManager.DefaultDataClasses.OBJECT;
        public static final Class<BlobType> BLOB          = DataTypeManager.DefaultDataClasses.BLOB;
        public static final Class<ClobType> CLOB          = DataTypeManager.DefaultDataClasses.CLOB;
        public static final Class<XMLType> XML           = DataTypeManager.DefaultDataClasses.XML;
        public static final Class<NullType> NULL         = DataTypeManager.DefaultDataClasses.NULL;
        public static final Class<BinaryType> VARBINARY         = DataTypeManager.DefaultDataClasses.VARBINARY;
        public static final Class<GeometryType> GEOMETRY         = DataTypeManager.DefaultDataClasses.GEOMETRY;
        public static final Class<GeographyType> GEOGRAPHY         = DataTypeManager.DefaultDataClasses.GEOGRAPHY;
        public static final Class<JsonType> JSON         = DataTypeManager.DefaultDataClasses.JSON;
    }

    public static final class RUNTIME_NAMES {
        public static final String STRING       = DataTypeManager.DefaultDataTypes.STRING;
        public static final String BOOLEAN      = DataTypeManager.DefaultDataTypes.BOOLEAN;
        public static final String BYTE         = DataTypeManager.DefaultDataTypes.BYTE;
        public static final String SHORT        = DataTypeManager.DefaultDataTypes.SHORT;
        public static final String CHAR         = DataTypeManager.DefaultDataTypes.CHAR;
        public static final String INTEGER      = DataTypeManager.DefaultDataTypes.INTEGER;
        public static final String LONG         = DataTypeManager.DefaultDataTypes.LONG;
        public static final String BIG_INTEGER  = DataTypeManager.DefaultDataTypes.BIG_INTEGER;
        public static final String FLOAT        = DataTypeManager.DefaultDataTypes.FLOAT;
        public static final String DOUBLE       = DataTypeManager.DefaultDataTypes.DOUBLE;
        public static final String BIG_DECIMAL  = DataTypeManager.DefaultDataTypes.BIG_DECIMAL;
        public static final String DATE         = DataTypeManager.DefaultDataTypes.DATE;
        public static final String TIME         = DataTypeManager.DefaultDataTypes.TIME;
        public static final String TIMESTAMP    = DataTypeManager.DefaultDataTypes.TIMESTAMP;
        public static final String OBJECT       = DataTypeManager.DefaultDataTypes.OBJECT;
        public static final String NULL         = DataTypeManager.DefaultDataTypes.NULL;
        public static final String BLOB         = DataTypeManager.DefaultDataTypes.BLOB;
        public static final String CLOB         = DataTypeManager.DefaultDataTypes.CLOB;
        public static final String XML          = DataTypeManager.DefaultDataTypes.XML;
        public static final String VARBINARY    = DataTypeManager.DefaultDataTypes.VARBINARY;
        public static final String GEOMETRY     = DataTypeManager.DefaultDataTypes.GEOMETRY;
        public static final String GEOGRAPHY     = DataTypeManager.DefaultDataTypes.GEOGRAPHY;
        public static final String JSON         = DataTypeManager.DefaultDataTypes.JSON;
    }

    /**
     * Get the Class constant for the given String runtime type name
     * <br>IMPORTANT: only considered the default runtime types
     */
    public static Class<?> getDataTypeClass(String type) {
        return DataTypeManager.getDataTypeClass(type);
    }

    /**
     * Get the String constant for the given runtime type class
     */
    public static String getDataTypeName(Class<?> type) {
        return DataTypeManager.getDataTypeName(type);
    }

    /**
     * Get the closest runtime type for the given class
     */
    public static Class<?> getRuntimeType(Class<?> type) {
        if (type.isPrimitive()) {
            return convertPrimitiveToObject(type);
        }
        return DataTypeManager.getRuntimeType(type);
    }

    /**
     * Get the SQL type for the given runtime type Class constant
     * @param type
     * @return
     */
    public static final int getSQLTypeFromRuntimeType(Class<?> type) {
        return JDBCSQLTypeInfo.getSQLTypeFromRuntimeType(type);
    }

    /**
     * Get the runtime type name for the given SQL type
     * @param sqlType
     * @return
     */
    public static final String getDataTypeNameFromSQLType(int sqlType) {
        if (sqlType == Types.ARRAY) {
            return RUNTIME_NAMES.OBJECT;
        }
        return JDBCSQLTypeInfo.getTypeName(sqlType);
    }

    /**
     * Convert the given value to the closest runtime type see {@link RUNTIME_TYPES}
     * @param value
     * @return
     */
    public Object convertToRuntimeType(Object value) {
        return DataTypeManager.convertToRuntimeType(value, true);
    }

    /**
     * Convert the given date to a target type, optionally adjusting its display
     * for a given target Calendar.
     * @param date
     * @param initial
     * @param target
     * @param targetType
     * @return
     */
    public Object convertDate(Date date, TimeZone initial, Calendar target,
            Class<?> targetType) {
        return TimestampWithTimezone.create(date, initial, target, targetType);
    }

    /**
     * Convert a primitive class to the corresponding object class
     * @param clazz
     * @return
     */
    public static Class<?> convertPrimitiveToObject(Class<?> clazz) {
        return ObjectToAnyTransform.convertPrimitiveToObject(clazz);
    }

}
