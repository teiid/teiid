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
package org.teiid.translator.swagger;

import java.lang.reflect.Array;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.Transform;
import org.teiid.core.types.TransformationException;
import org.teiid.translator.TranslatorException;

public class SwaggerTypeManager {
    private static Pattern timestampPattern = Pattern.compile("(\\d{4})-(\\d{2})-(\\d{2})" //date //$NON-NLS-1$
            + "[T|t](\\d{2}):(\\d{2}):(\\d{2})" // time //$NON-NLS-1$
            + "(\\.\\d{1})?" // optional fractions //$NON-NLS-1$
            + "([Z|z]|([+|-](\\d{2}):(\\d{2})))");  //$NON-NLS-1$ // timezone

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
    private static final String OBJECT = typeFormat("array", "");
//    private static final String OBJECT_ = typeFormat("object", "");

    static String typeFormat(String type, String format){
        return type + "/" + format;
    }

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
        swaggerTypes.put(DATETIME, DataTypeManager.DefaultDataTypes.TIMESTAMP);
        swaggerTypes.put(DATETIME_, DataTypeManager.DefaultDataTypes.TIMESTAMP);
        swaggerTypes.put(PASSWORD, DataTypeManager.DefaultDataTypes.STRING);
        swaggerTypes.put(PASSWORD_, DataTypeManager.DefaultDataTypes.STRING);

        swaggerTypes.put(OBJECT, DataTypeManager.DefaultDataTypes.OBJECT);

    }

    static String teiidType(String name) {
        String type = swaggerTypes.get(name);
        if (type == null) {
            type = DataTypeManager.DefaultDataTypes.STRING; // special case for enum type
        }
        return type ;
    }

    public static String teiidType(String type, String format, boolean array) {
        if(null == format) {
            format = "";
        }
        String returnType = swaggerTypes.get(typeFormat(type, format));
        if(null == returnType) {
            returnType = DataTypeManager.DefaultDataTypes.STRING;
        }
        if (array) {
            returnType +="[]";
        }
        return returnType;
    }

    public static Object convertTeiidRuntimeType(Object value, Class<?> expectedType) throws TranslatorException {

        if (value == null) {
            return null;
        }

        if (expectedType.isAssignableFrom(value.getClass())) {
            return value;
        } else {

            if(expectedType.isAssignableFrom(Timestamp.class) && value instanceof Long) {
                return new Timestamp((Long)value);
            } else if (expectedType.isAssignableFrom(java.sql.Timestamp.class) && value instanceof String){
                return formTimestamp((String)value);
            } else if (expectedType.isAssignableFrom(java.util.Date.class) && value instanceof Long) {
                return new java.util.Date((Long)value);
            }  else if (expectedType.isAssignableFrom(java.util.Date.class) && value instanceof String) {
                return formDate((String)value);
            } else if (expectedType.isArray() && value instanceof List) {
                List<?> values = (List<?>)value;
                Class<?> expectedArrayComponentType = expectedType.getComponentType();
                Object array = Array.newInstance(expectedArrayComponentType, values.size());
                for (int i = 0; i < values.size(); i++) {
                    Object arrayItem = convertTeiidRuntimeType(values.get(i), expectedArrayComponentType);
                    Array.set(array, i, arrayItem);
                }
                return array;
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

    static Date formDate(String value) throws TranslatorException {
        try {
            DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
            return new Date(formatter.parse(value).getTime());
        } catch (ParseException e) {
            throw new TranslatorException(e, SwaggerPlugin.Util.gs(SwaggerPlugin.Event.TEIID28012));
        }
    }

    static Timestamp formTimestamp(String value) throws TranslatorException {
        Matcher m = timestampPattern.matcher(value);
        if (m.matches()) {
            Calendar cal = null;
            String timeZone = m.group(8);
            if (timeZone.equalsIgnoreCase("Z")) {
                cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
            } else {
                cal = Calendar.getInstance(TimeZone.getTimeZone("GMT" + m.group(9)));
            }

            cal.set(Integer.valueOf(m.group(1)), Integer.valueOf(m.group(2))-1,
                    Integer.valueOf(m.group(3)), Integer.valueOf(m.group(4)),
                    Integer.valueOf(m.group(5)), Integer.valueOf(m.group(6)));

            Timestamp ts = new Timestamp(cal.getTime().getTime());
            if (m.group(7) != null) {
                String fraction = m.group(7).substring(1);
                ts.setNanos(Integer.parseInt(fraction));
            } else {
                ts.setNanos(0);
            }
            return ts;
        } else {
            throw new TranslatorException(SwaggerPlugin.Util
                    .gs(SwaggerPlugin.Event.TEIID28011, timestampPattern));
        }
    }

    static String timestampToString(Timestamp ts) {
        SimpleDateFormat timestampSDF = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SZ");
        if (ts == null) {
            return null;
        }
        String str =  timestampSDF.format(ts);
        if(str.endsWith("+0000")) {
            str = str.replace("+0000", "Z");
        }
        return str;
    }

    static String dateToString(Date date) {
        SimpleDateFormat dateSDF = new SimpleDateFormat("yyyy-MM-dd");
        if (date == null) {
            return null;
        }
        return dateSDF.format(date);
    }
}
