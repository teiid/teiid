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
package org.teiid.test.client.ctc;

import java.util.Map;
import java.util.HashMap;

/**
 * Constants used in XML messaging.
 */
public final class TagNames {

  private static final String DATA_ELE_TYPE_INT = "integer"; //$NON-NLS-1$
  private static final String DATA_ELE_TYPE_STRING = "string"; //$NON-NLS-1$
  private static final String DATA_ELE_TYPE_FLOAT = "float"; //$NON-NLS-1$
  private static final String DATA_ELE_TYPE_LONG = "long"; //$NON-NLS-1$
  private static final String DATA_ELE_TYPE_DOUBLE = "double"; //$NON-NLS-1$
  private static final String DATA_ELE_TYPE_BYTE = "byte"; //$NON-NLS-1$
  private static final String DATA_ELE_TYPE_DATE = "date"; //$NON-NLS-1$
  private static final String DATA_ELE_TYPE_TIME = "time"; //$NON-NLS-1$
  private static final String DATA_ELE_TYPE_TIMESTAMP = "timestamp"; //$NON-NLS-1$
  private static final String DATA_ELE_TYPE_BOOLEAN = "boolean"; //$NON-NLS-1$
  private static final String DATA_ELE_TYPE_CHAR = "char"; //$NON-NLS-1$
  private static final String DATA_ELE_TYPE_SHORT = "short"; //$NON-NLS-1$
  private static final String DATA_ELE_TYPE_BIG_INT = "biginteger"; //$NON-NLS-1$
  private static final String DATA_ELE_TYPE_BIG_DEC = "bigdecimal"; //$NON-NLS-1$
  private static final String DATA_ELE_TYPE_OBJECT = "object"; //$NON-NLS-1$
  private static final String DATA_ELE_TYPE_XML = "xml"; //$NON-NLS-1$
  private static final String DATA_ELE_TYPE_CLOB = "clob"; //$NON-NLS-1$
  private static final String DATA_ELE_TYPE_BLOB = "blob"; //$NON-NLS-1$

    public static Map TYPE_MAP;

    static {
        TYPE_MAP = new HashMap();

        TYPE_MAP.put(DATA_ELE_TYPE_OBJECT,      java.lang.Object.class);
        TYPE_MAP.put(DATA_ELE_TYPE_INT,         java.lang.Integer.class);
        TYPE_MAP.put(DATA_ELE_TYPE_STRING,      java.lang.String.class);
        TYPE_MAP.put(DATA_ELE_TYPE_FLOAT,       java.lang.Float.class);
        TYPE_MAP.put(DATA_ELE_TYPE_LONG,        java.lang.Long.class);
        TYPE_MAP.put(DATA_ELE_TYPE_DOUBLE,      java.lang.Double.class);
        TYPE_MAP.put(DATA_ELE_TYPE_BYTE,        java.lang.Byte.class);
        TYPE_MAP.put(DATA_ELE_TYPE_BOOLEAN,     java.lang.Boolean.class);
        TYPE_MAP.put(DATA_ELE_TYPE_CHAR,        java.lang.Character.class);
        TYPE_MAP.put(DATA_ELE_TYPE_SHORT,       java.lang.Short.class);
        TYPE_MAP.put(DATA_ELE_TYPE_BIG_INT,     java.math.BigInteger.class);
        TYPE_MAP.put(DATA_ELE_TYPE_BIG_DEC,     java.math.BigDecimal.class);
        TYPE_MAP.put(DATA_ELE_TYPE_DATE,        java.sql.Date.class);
        TYPE_MAP.put(DATA_ELE_TYPE_TIME,        java.sql.Time.class);
        TYPE_MAP.put(DATA_ELE_TYPE_TIMESTAMP,   java.sql.Timestamp.class);
        TYPE_MAP.put(DATA_ELE_TYPE_XML,         java.lang.String.class);
        TYPE_MAP.put(DATA_ELE_TYPE_CLOB,        java.lang.String.class);
        TYPE_MAP.put(DATA_ELE_TYPE_BLOB,        java.lang.String.class);
    }


    /**
     * Enumeration of XML element tag constants.
     */
    public static final class Elements {
        public static final String STRING = "string"; //$NON-NLS-1$
        public static final String BOOLEAN = "boolean"; //$NON-NLS-1$
        public static final String BYTE = "byte"; //$NON-NLS-1$
        public static final String SHORT = "short"; //$NON-NLS-1$
        public static final String CHAR = "char"; //$NON-NLS-1$
        public static final String INTEGER = "integer"; //$NON-NLS-1$
        public static final String LONG = "long"; //$NON-NLS-1$
        public static final String BIGINTEGER = "biginteger"; //$NON-NLS-1$
        public static final String FLOAT = "float"; //$NON-NLS-1$
        public static final String DOUBLE = "double"; //$NON-NLS-1$
        public static final String BIGDECIMAL = "bigdecimal"; //$NON-NLS-1$
        public static final String DATE = "date"; //$NON-NLS-1$
        public static final String TIME = "time"; //$NON-NLS-1$
        public static final String TIMESTAMP = "timestamp"; //$NON-NLS-1$
        public static final String OBJECT = "object"; //$NON-NLS-1$

        public static final String CLASS = "class"; //$NON-NLS-1$
        public static final String TABLE = "table"; //$NON-NLS-1$
        public static final String TABLE_ROW = "tableRow"; //$NON-NLS-1$
        public static final String TABLE_CELL = "tableCell"; //$NON-NLS-1$
        public static final String NULL = "null"; //$NON-NLS-1$
        public static final String EXCEPTION = "exception"; //$NON-NLS-1$
        public static final String ACTUAL_EXCEPTION = "actual_exception"; //$NON-NLS-1$
        public static final String EXPECTED_EXCEPTION = "expected_exception"; //$NON-NLS-1$
        public static final String EXCEPTION_TYPE = "exceptionType"; //$NON-NLS-1$
        public static final String MESSAGE = "message"; //$NON-NLS-1$
        public static final String DATA_ELEMENT = "dataElement"; //$NON-NLS-1$
        public static final String QUERY_RESULTS = "queryResults"; //$NON-NLS-1$
        public static final String ACTUAL_QUERY_RESULTS = "actual_queryResults"; //$NON-NLS-1$
        public static final String EXPECTED_QUERY_RESULTS = "expected_queryResults"; //$NON-NLS-1$
        public static final String QUERY = "query"; //$NON-NLS-1$
        public static final String SELECT = "select"; //$NON-NLS-1$
        public static final String ROOT_ELEMENT = "root"; //$NON-NLS-1$
        public static final String SQL = "sql"; //$NON-NLS-1$
        public static final String PARM = "parm"; //$NON-NLS-1$
    }

    /**
     * Enumeration of XML element attribute names.
     */
    public static final class Attributes {
        public static final String TYPE = "type"; //$NON-NLS-1$
        public static final String SIZE = "size"; //$NON-NLS-1$
        public static final String TABLE_ROW_COUNT = "rowCount"; //$NON-NLS-1$
        public static final String TABLE_COLUMN_COUNT = "columnCount"; //$NON-NLS-1$
        public static final String NAME = "name"; //$NON-NLS-1$
        public static final String VALUE = "value"; //$NON-NLS-1$
        public static final String DISTINCT = "distinct"; //$NON-NLS-1$
        public static final String STAR = "star"; //$NON-NLS-1$
        public static final String UPDATE_CNT = "updatecnt"; //$NON-NLS-1$
    }

    /**
     * Enumeration of XML element attribute values.
     */
    public static final class Values {
        public static final String TRUE = "true"; //$NON-NLS-1$
        public static final String FALSE = "false"; //$NON-NLS-1$
    }
}
