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

package org.teiid.jdbc;

import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;

import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.TransformationException;
import org.teiid.core.types.DataTypeManager.DefaultDataClasses;


/**
 * <p>This class is used to transform objects into desired data types. The static
 * method on this class are used by Metadatresults, ResultsWrapper and
 * MMCallableStatement classes.</p>
 */
final class DataTypeTransformer {

    // Prevent instantiation
    private DataTypeTransformer() {}

    /**
     * Gets an object value and transforms it into a java.math.BigDecimal object.
     * @param value, the object to be transformed
     * @return a BigDecimal object
     * @throws SQLException if failed to transform to the desired datatype
     */
    static final BigDecimal getBigDecimal(Object value) throws SQLException {
    	return transform(value, BigDecimal.class, "BigDecimal"); //$NON-NLS-1$
    }
    
    private static final <T> T transform(Object value, Class<T> type, String typeName) throws SQLException {
    	return transform(value, type, type, typeName);
    }
    
    private static final <T> T transform(Object value, Class<T> targetType, Class<?> runtimeType, String typeName) throws SQLException {
    	if (value == null || targetType.isAssignableFrom(value.getClass())) {
    		return targetType.cast(value);
    	}
    	try {
    		return targetType.cast(DataTypeManager.transformValue(DataTypeManager.convertToRuntimeType(value), runtimeType));
    	} catch (TransformationException e) {
    		String valueStr = value.toString();
    		if (valueStr.length() > 20) {
    			valueStr = valueStr.substring(0, 20) + "..."; //$NON-NLS-1$
    		}
    		String msg = JDBCPlugin.Util.getString("DataTypeTransformer.Err_converting", valueStr, typeName); //$NON-NLS-1$
            throw TeiidSQLException.create(e, msg);
    	} 
    }
    
    /**
     * Gets an object value and transforms it into a boolean
     * @param value, the object to be transformed
     * @return a Boolean object
     * @throws SQLException if failed to transform to the desired datatype
     */
    static final boolean getBoolean(Object value) throws SQLException {
    	if (value == null) {
    		return false;
    	}
    	return transform(value, Boolean.class, "Boolean"); //$NON-NLS-1$
    }

    /**
     * Gets an object value and transforms it into a byte
     * @param value, the object to be transformed
     * @return a Byte object
     * @throws SQLException if failed to transform to the desired datatype
     */
    static final byte getByte(Object value) throws SQLException {
    	if (value == null) {
    		return 0;
    	}
    	return transform(value, Byte.class, "Byte"); //$NON-NLS-1$
    }
    
    static final byte[] getBytes(Object value) throws SQLException {
        if (value == null) {
            return null;
        } else if (value instanceof byte[]) {
        	return (byte[])value;
        } else if (value instanceof Blob) {
            Blob blob = (Blob)value;
            long length = blob.length();
            if (length > Integer.MAX_VALUE) {
                throw new TeiidSQLException(JDBCPlugin.Util.getString("DataTypeTransformer.blob_too_big")); //$NON-NLS-1$
            }
            return blob.getBytes(1, (int)length);
        } else if (value instanceof String) {
        	return ((String)value).getBytes();
        }
        throw new TeiidSQLException(JDBCPlugin.Util.getString("DataTypeTransformer.cannot_get_bytes")); //$NON-NLS-1$
    }
    
    static final Character getCharacter(Object value) throws SQLException {
    	return transform(value, Character.class, "Character"); //$NON-NLS-1$
    }

    /**
     * Gets an object value and transforms it into a java.sql.Date object.
     * @param value, the object to be transformed
     * @param Calendar object to be used to construct the Date object.
     * @return a Date object
     * @throws SQLException if failed to transform to the desired datatype
     */
    static final Date getDate(Object value) throws SQLException {
    	return transform(value, Date.class, "Date"); //$NON-NLS-1$
    }

    /**
     * Gets an object value and transforms it into a double
     * @param value, the object to be transformed
     * @return a Double object
     * @throws SQLException if failed to transform to the desired datatype
     */
    static final double getDouble(Object value) throws SQLException {
    	if (value == null) {
    		return 0;
    	}
    	return transform(value, Double.class, "Double"); //$NON-NLS-1$
    }

    /**
     * Gets an object value and transforms it into a float
     * @param value, the object to be transformed
     * @return a Float object
     * @throws SQLException if failed to transform to the desired datatype
     */
    static final float getFloat(Object value) throws SQLException {
    	if (value == null) {
    		return 0;
    	}
    	return transform(value, Float.class, "Float"); //$NON-NLS-1$
    }

    /**
     * Gets an object value and transforms it into a integer
     * @param value, the object to be transformed
     * @return a Integer object
     * @throws SQLException if failed to transform to the desired datatype
     */
    static final int getInteger(Object value) throws SQLException {
    	if (value == null) {
    		return 0;
    	}
    	return transform(value, Integer.class, "Integer"); //$NON-NLS-1$
    }

    /**
     * Gets an object value and transforms it into a long
     * @param value, the object to be transformed
     * @return a Long object
     * @throws SQLException if failed to transform to the desired datatype
     */
    static final long getLong(Object value) throws SQLException {
    	if (value == null) {
    		return 0;
    	}
    	return transform(value, Long.class, "Long"); //$NON-NLS-1$
    }

    /**
     * Gets an object value and transforms it into a short
     * @param value, the object to be transformed
     * @return a Short object
     * @throws SQLException if failed to transform to the desired datatype
     */
    static final short getShort(Object value) throws SQLException {
    	if (value == null) {
    		return 0;
    	}
    	return transform(value, Short.class, "Short"); //$NON-NLS-1$
    }

    /**
     * Gets an object value and transforms it into a java.sql.Time object.
     * @param value, the object to be transformed
     * @param Calendar object to be used to construct the Time object.
     * @return a Time object
     * @throws SQLException if failed to transform to the desired datatype
     */
    static final Time getTime(Object value) throws SQLException {
    	return transform(value, Time.class, "Time"); //$NON-NLS-1$
    }

    /**
     * Gets an object value and transforms it into a java.sql.Timestamp object.
     * @param value, the object to be transformed
     * @param Calendar object to be used to construct the Timestamp object.
     * @return a Timestamp object
     * @throws SQLException if failed to transform to the desired datatype
     */
    static final Timestamp getTimestamp(Object value) throws SQLException {
    	return transform(value, Timestamp.class, "Timestamp"); //$NON-NLS-1$
    }
    
    static final String getString(Object value) throws SQLException {
    	if (value instanceof SQLXML) {
    		return ((SQLXML)value).getString();
    	} else if (value instanceof Clob) {
    		Clob c = (Clob)value;
    		long length = c.length();
    		if (length == 0) {
    			//there is a bug in SerialClob with 0 length
    			return ""; //$NON-NLS-1$ 
    		}
    		return c.getSubString(1, length>Integer.MAX_VALUE?Integer.MAX_VALUE:(int)length);
    	}
    	return transform(value, String.class, "String"); //$NON-NLS-1$
    }

    /**
     * Gets an object value and transforms it into a java.sql.Timestamp object.
     * @param value, the object to be transformed
     * @param Calendar object to be used to construct the Timestamp object.
     * @return a Timestamp object
     * @throws SQLException if failed to transform to the desired datatype
     */
    static final Blob getBlob(Object value) throws SQLException {
    	return transform(value, Blob.class, DefaultDataClasses.BLOB, "Blob"); //$NON-NLS-1$
    }

    /**
     * Gets an object value and transforms it into a java.sql.Timestamp object.
     * @param value, the object to be transformed
     * @param Calendar object to be used to construct the Timestamp object.
     * @return a Timestamp object
     * @throws SQLException if failed to transform to the desired datatype
     */
    static final Clob getClob(Object value) throws SQLException {
    	return transform(value, Clob.class, DefaultDataClasses.CLOB, "Clob"); //$NON-NLS-1$
    }

    /**
     * Gets an object value and transforms it into a SQLXML object.
     * @param value, the object to be transformed
     * @return a SQLXML object
     * @throws SQLException if failed to transform to the desired datatype
     */    
    static final SQLXML getSQLXML(Object value) throws SQLException {
    	return transform(value, SQLXML.class, DefaultDataClasses.XML, "SQLXML"); //$NON-NLS-1$
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
}