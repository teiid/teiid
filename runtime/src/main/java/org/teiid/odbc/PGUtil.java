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
package org.teiid.odbc;

import java.sql.Types;

public class PGUtil {

    public static final int PG_TYPE_UNSPECIFIED = 0;

    public static final int PG_TYPE_VARCHAR = 1043;

    public static final int PG_TYPE_BOOL = 16;
    public static final int PG_TYPE_BYTEA = 17;
    public static final int PG_TYPE_CHAR = 18;
    public static final int PG_TYPE_BPCHAR = 1042;
    public static final int PG_TYPE_INT8 = 20;
    public static final int PG_TYPE_INT2 = 21;
    public static final int PG_TYPE_INT4 = 23;
    public static final int PG_TYPE_TEXT = 25;
    public static final int PG_TYPE_XML = 142;
    //private static final int PG_TYPE_OID = 26;
    public static final int PG_TYPE_FLOAT4 = 700;
    public static final int PG_TYPE_FLOAT8 = 701;
    public static final int PG_TYPE_UNKNOWN = 705;

    public static final int PG_TYPE_GEOMETRY = 32816;
    public static final int PG_TYPE_GEOMETRYARRAY = 32824;

    public static final int PG_TYPE_GEOGRAPHY = 33454;
    public static final int PG_TYPE_GEOGRAPHYARRAY = 33462;

    public static final int PG_TYPE_JSON = 3803;
    public static final int PG_TYPE_JSONARRAY = 3811;

    public static final int PG_TYPE_OIDVECTOR = 30;
    public static final int PG_TYPE_INT2VECTOR = 22;
    public static final int PG_TYPE_OIDARRAY = 1028;
    public static final int PG_TYPE_CHARARRAY = 1002;
    public static final int PG_TYPE_TEXTARRAY = 1009;

    public static final int PG_TYPE_DATE = 1082;
    public static final int PG_TYPE_TIME = 1083;
    public static final int PG_TYPE_TIMESTAMP_NO_TMZONE = 1114;
    public static final int PG_TYPE_NUMERIC = 1700;

    public static final int PG_TYPE_BOOLARRAY = 1000;
    public static final int PG_TYPE_BYTEAARRAY = 1001;
    public static final int PG_TYPE_INT8ARRAY = 1026;
    public static final int PG_TYPE_INT2ARRAY = 1005;
    public static final int PG_TYPE_INT4ARRAY = 1007;
    public static final int PG_TYPE_FLOAT4ARRAY = 1021;
    public static final int PG_TYPE_FLOAT8ARRAY = 1022;
    public static final int PG_TYPE_DATEARRAY = 1182;
    public static final int PG_TYPE_TIMEARRAY = 1183;
    public static final int PG_TYPE_TIMESTAMP_NO_TMZONEARRAY = 1115;
    public static final int PG_TYPE_NUMERICARRAY = 1031;
    public static final int PG_TYPE_XMLARRAY = 143;

    public static class PgColInfo {
        public String name;
        public int reloid;
        public short attnum;
        public int type;
        public int precision;
        public int mod = -1;
    }

    /**
     * Types.ARRAY is not supported
     */
    public static int convertType(final int type, final String typeName) {
        switch (type) {
        case Types.BIT:
        case Types.BOOLEAN:
            return PG_TYPE_BOOL;
        case Types.VARCHAR:
            return PG_TYPE_VARCHAR;
        case Types.CHAR:
            return PG_TYPE_BPCHAR;
        case Types.TINYINT:
        case Types.SMALLINT:
            return PG_TYPE_INT2;
        case Types.INTEGER:
            return PG_TYPE_INT4;
        case Types.BIGINT:
            return PG_TYPE_INT8;
        case Types.NUMERIC:
        case Types.DECIMAL:
            return PG_TYPE_NUMERIC;
        case Types.FLOAT:
        case Types.REAL:
            return PG_TYPE_FLOAT4;
        case Types.DOUBLE:
            return PG_TYPE_FLOAT8;
        case Types.TIME:
            return PG_TYPE_TIME;
        case Types.DATE:
            return PG_TYPE_DATE;
        case Types.TIMESTAMP:
            return PG_TYPE_TIMESTAMP_NO_TMZONE;

        case Types.BLOB:
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
            if (typeName.equalsIgnoreCase("geometry")) { //$NON-NLS-1$
                return PG_TYPE_GEOMETRY;
            }
            if (typeName.equalsIgnoreCase("geography")) { //$NON-NLS-1$
                return PG_TYPE_GEOGRAPHY;
            }
            return PG_TYPE_BYTEA;

        case Types.SQLXML:
            return PG_TYPE_XML;

        case Types.LONGVARCHAR:
        case Types.CLOB:
            if (typeName.equalsIgnoreCase("json")) { //$NON-NLS-1$
                return PG_TYPE_JSON;
            }
            return PG_TYPE_TEXT;

        case Types.ARRAY:
            switch (typeName) {
            case "boolean[]": //$NON-NLS-1$
                return PG_TYPE_BOOLARRAY;
            case "byte[]": //$NON-NLS-1$
            case "short[]": //$NON-NLS-1$
                return PG_TYPE_INT2ARRAY;
            case "integer[]": //$NON-NLS-1$
                return PG_TYPE_INT4ARRAY;
            case "long[]": //$NON-NLS-1$
                return PG_TYPE_INT8ARRAY;
            case "float[]": //$NON-NLS-1$
                return PG_TYPE_FLOAT4ARRAY;
            case "double[]": //$NON-NLS-1$
                return PG_TYPE_FLOAT8ARRAY;
            case "biginiteger[]": //$NON-NLS-1$
            case "bigdecimal[]": //$NON-NLS-1$
                return PG_TYPE_NUMERICARRAY;
            case "date[]": //$NON-NLS-1$
                return PG_TYPE_DATEARRAY;
            case "time[]": //$NON-NLS-1$
                return PG_TYPE_TIMEARRAY;
            case "timestamp[]": //$NON-NLS-1$
                return PG_TYPE_TIMESTAMP_NO_TMZONEARRAY;
            case "geometry[]": //$NON-NLS-1$
                return PG_TYPE_GEOMETRYARRAY;
            case "geography[]": //$NON-NLS-1$
                return PG_TYPE_GEOGRAPHYARRAY;
            case "xml[]": //$NON-NLS-1$
                return PG_TYPE_XMLARRAY;
            case "json[]": //$NON-NLS-1$
                return PG_TYPE_JSONARRAY;
            default:
                return PG_TYPE_TEXTARRAY;
            }

        default:
            return PG_TYPE_UNKNOWN;
        }
    }
}
