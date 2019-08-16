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

package org.teiid.jdbc;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;

import org.teiid.core.types.BinaryType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.ReaderInputStream;


/**
 * <p>This class is used to transform objects into desired data types. The static
 * method on this class are used by Metadatresults, ResultsWrapper and
 * MMCallableStatement classes.
 */
final class DataTypeTransformer {

    // Prevent instantiation
    private DataTypeTransformer() {}

    /**
     * Gets an object value and transforms it into a java.math.BigDecimal object.
     * @param value the object to be transformed
     * @return a BigDecimal object
     * @throws SQLException if failed to transform to the desired datatype
     */
    static final BigDecimal getBigDecimal(Object value) throws SQLException {
        return transform(value, BigDecimal.class);
    }

    static final <T> T transform(Object value, Class<T> targetType) throws SQLException {
        return transform(value, targetType, getRuntimeType(targetType));
    }

    static final <T> T transform(Object value, Class<T> targetType, Class<?> runtimeType) throws SQLException {
        if (value == null || targetType.isAssignableFrom(value.getClass())) {
            return targetType.cast(value);
        }
        if (targetType == byte[].class) {
            if (value instanceof Blob) {
                Blob blob = (Blob)value;
                long length = blob.length();
                if (length > Integer.MAX_VALUE) {
                    throw new TeiidSQLException(JDBCPlugin.Util.getString("DataTypeTransformer.blob_too_big")); //$NON-NLS-1$
                }
                return targetType.cast(blob.getBytes(1, (int)length));
            } else if (value instanceof String) {
                return targetType.cast(((String)value).getBytes());
            } else if (value instanceof BinaryType) {
                return targetType.cast(((BinaryType)value).getBytesDirect());
            }
        } else if (targetType == String.class) {
            if (value instanceof SQLXML) {
                return targetType.cast(((SQLXML)value).getString());
            } else if (value instanceof Clob) {
                Clob c = (Clob)value;
                long length = c.length();
                if (length == 0) {
                    //there is a bug in SerialClob with 0 length
                    return targetType.cast(""); //$NON-NLS-1$
                }
                return targetType.cast(c.getSubString(1, length>Integer.MAX_VALUE?Integer.MAX_VALUE:(int)length));
            }
        }
        try {
            return (T)DataTypeManager.transformValue(DataTypeManager.convertToRuntimeType(value, true), runtimeType);
        } catch (Exception e) {
            String valueStr = value.toString();
            if (valueStr.length() > 20) {
                valueStr = valueStr.substring(0, 20) + "..."; //$NON-NLS-1$
            }
            String msg = JDBCPlugin.Util.getString("DataTypeTransformer.Err_converting", valueStr, targetType.getSimpleName()); //$NON-NLS-1$
            throw TeiidSQLException.create(e, msg);
        }
    }

    static final <T> Class<?> getRuntimeType(Class<T> type) {
        Class<?> runtimeType = type;
        if (!DataTypeManager.getAllDataTypeClasses().contains(type)) {
            if (type == Clob.class) {
                runtimeType = DataTypeManager.DefaultDataClasses.CLOB;
            } else if (type == Blob.class) {
                runtimeType = DataTypeManager.DefaultDataClasses.BLOB;
            } else if (type == SQLXML.class) {
                runtimeType = DataTypeManager.DefaultDataClasses.XML;
            } else if (type == byte[].class) {
                runtimeType = DataTypeManager.DefaultDataClasses.VARBINARY;
            } else {
                runtimeType = DataTypeManager.DefaultDataClasses.OBJECT;
            }
        }
        return runtimeType;
    }

    /**
     * Gets an object value and transforms it into a boolean
     * @param  value the object to be transformed
     * @return a Boolean object
     * @throws SQLException if failed to transform to the desired datatype
     */
    static final boolean getBoolean(Object value) throws SQLException {
        if (value == null) {
            return false;
        }
        return transform(value, Boolean.class);
    }

    /**
     * Gets an object value and transforms it into a byte
     * @param value the object to be transformed
     * @return a Byte object
     * @throws SQLException if failed to transform to the desired datatype
     */
    static final byte getByte(Object value) throws SQLException {
        if (value == null) {
            return 0;
        }
        return transform(value, Byte.class);
    }

    static final byte[] getBytes(Object value) throws SQLException {
        return transform(value, byte[].class);
    }

    static final Character getCharacter(Object value) throws SQLException {
        return transform(value, Character.class);
    }

    /**
     * Gets an object value and transforms it into a java.sql.Date object.
     * @param value the object to be transformed
     * @throws SQLException if failed to transform to the desired datatype
     */
    static final Date getDate(Object value) throws SQLException {
        return transform(value, Date.class);
    }

    /**
     * Gets an object value and transforms it into a double
     * @param value the object to be transformed
     * @return a Double object
     * @throws SQLException if failed to transform to the desired datatype
     */
    static final double getDouble(Object value) throws SQLException {
        if (value == null) {
            return 0;
        }
        return transform(value, Double.class);
    }

    /**
     * Gets an object value and transforms it into a float
     * @param value the object to be transformed
     * @return a Float object
     * @throws SQLException if failed to transform to the desired datatype
     */
    static final float getFloat(Object value) throws SQLException {
        if (value == null) {
            return 0;
        }
        return transform(value, Float.class);
    }

    /**
     * Gets an object value and transforms it into a integer
     * @param value the object to be transformed
     * @return a Integer object
     * @throws SQLException if failed to transform to the desired datatype
     */
    static final int getInteger(Object value) throws SQLException {
        if (value == null) {
            return 0;
        }
        return transform(value, Integer.class);
    }

    /**
     * Gets an object value and transforms it into a long
     * @param value the object to be transformed
     * @return a Long object
     * @throws SQLException if failed to transform to the desired datatype
     */
    static final long getLong(Object value) throws SQLException {
        if (value == null) {
            return 0;
        }
        return transform(value, Long.class);
    }

    /**
     * Gets an object value and transforms it into a short
     * @param value the object to be transformed
     * @return a Short object
     * @throws SQLException if failed to transform to the desired datatype
     */
    static final short getShort(Object value) throws SQLException {
        if (value == null) {
            return 0;
        }
        return transform(value, Short.class);
    }

    /**
     * Gets an object value and transforms it into a java.sql.Time object.
     * @param value the object to be transformed
     * @return a Time object
     * @throws SQLException if failed to transform to the desired datatype
     */
    static final Time getTime(Object value) throws SQLException {
        return transform(value, Time.class);
    }

    /**
     * Gets an object value and transforms it into a java.sql.Timestamp object.
     * @param value the object to be transformed
     * @return a Timestamp object
     * @throws SQLException if failed to transform to the desired datatype
     */
    static final Timestamp getTimestamp(Object value) throws SQLException {
        return transform(value, Timestamp.class);
    }

    static final String getString(Object value) throws SQLException {
        return transform(value, String.class);
    }

    /**
     * Gets an object value and transforms it into a java.sql.Timestamp object.
     * @param value the object to be transformed
     * @return a Timestamp object
     * @throws SQLException if failed to transform to the desired datatype
     */
    static final Blob getBlob(Object value) throws SQLException {
        return transform(value, Blob.class);
    }

    /**
     * Gets an object value and transforms it into a java.sql.Timestamp object.
     * @param value the object to be transformed
     * @return a Timestamp object
     * @throws SQLException if failed to transform to the desired datatype
     */
    static final Clob getClob(Object value) throws SQLException {
        return transform(value, Clob.class);
    }

    /**
     * Gets an object value and transforms it into a SQLXML object.
     * @param value the object to be transformed
     * @return a SQLXML object
     * @throws SQLException if failed to transform to the desired datatype
     */
    static final SQLXML getSQLXML(Object value) throws SQLException {
        return transform(value, SQLXML.class);
    }

    static final Reader getCharacterStream(Object value) throws SQLException {
        if (value == null) {
            return null;
        }

        if (value instanceof Clob) {
            return ((Clob) value).getCharacterStream();
        }

        if (value instanceof SQLXML) {
            return ((SQLXML)value).getCharacterStream();
        }

        return new StringReader(getString(value));
    }

    static final InputStream getAsciiStream(Object value) throws SQLException {
        if (value == null) {
            return null;
        }

        if (value instanceof Clob) {
            return ((Clob) value).getAsciiStream();
        }

        if (value instanceof SQLXML) {
            //TODO: could check the SQLXML encoding
            return new ReaderInputStream(((SQLXML)value).getCharacterStream(), Charset.forName("ASCII")); //$NON-NLS-1$
        }

        return new ByteArrayInputStream(getString(value).getBytes(Charset.forName("ASCII"))); //$NON-NLS-1$
    }

    static final NClob getNClob(Object value) throws SQLException {
        final Clob clob = getClob(value);
        if (clob == null) {
            return null;
        }
        if (clob instanceof NClob) {
            return (NClob)clob;
        }
        return (NClob) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[] {NClob.class}, new InvocationHandler() {

            @Override
            public Object invoke(Object proxy, Method method, Object[] args)
                    throws Throwable {
                try {
                    return method.invoke(clob, args);
                } catch (InvocationTargetException e) {
                    throw e.getCause();
                }
            }
        });
    }

    static final Array getArray(Object obj) throws SQLException {
        //TODO: type primitive arrays more closely
        return transform(obj, Array.class, Object[].class);
    }

}