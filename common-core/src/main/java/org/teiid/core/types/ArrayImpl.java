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

import java.io.Serializable;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;

import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.HashCodeUtil;

/**
 * Provides a serializable {@link Array} implementation with minimal JDBC functionality. 
 */
public final class ArrayImpl implements Comparable<ArrayImpl>, Serializable, Array {
	private static final long serialVersionUID = 517794153664734815L;
	/**
	 * a regrettable hack for pg compatibility since we want to avoid adding a vector type
	 */
	private boolean zeroBased;
	private Object[] values;
	
	@SuppressWarnings("serial")
	public final static class NullException extends RuntimeException {};
	private final static NullException ex = new NullException();
	
	public ArrayImpl(Object[] values) {
		this.values = values;
	}

	@Override
	public int compareTo(ArrayImpl o) {
		return compareTo(o, false, null);
	}
		
	public int compareTo(ArrayImpl o, boolean noNulls, Comparator<Object> comparator) {
		if (zeroBased != o.zeroBased) {
			throw new TeiidRuntimeException("Incompatible types"); //$NON-NLS-1$
		}
		int len1 = values.length;
		int len2 = o.values.length;
	    int lim = Math.min(len1, len2);
	    for (int k = 0; k < lim; k++) {
	    	Object object1 = values[k];
			Object object2 = o.values[k];
			if (object1 == null) {
				if (noNulls) {
					throw ex;
				}
	    		if (object2 != null) {
	    			return -1;
	    		}
	    		continue;
	    	} else if (object2 == null) {
	    		if (noNulls) {
					throw ex;
				}
	    		return 1;
	    	}
			int comp = 0;
			if (comparator != null) {
				comp = comparator.compare(object1, object2);				
			} else {
				comp = ((Comparable)object1).compareTo(object2);
			}
			if (comp != 0) {
			    return comp;
			}
	    }
	    return len1 - len2;
	}
	
	@Override
	public int hashCode() {
		return HashCodeUtil.expHashCode(0, this.values);
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (!(obj instanceof ArrayImpl)) {
			return false;
		}
		ArrayImpl other = (ArrayImpl)obj;
		return zeroBased == other.zeroBased && Arrays.equals(values, other.values);
	}
	
	public Object[] getValues() {
		return values;
	}
	
	@Override
	public String toString() {
		return Arrays.toString(this.values);
	}
	
	public boolean isZeroBased() {
		return zeroBased;
	}
	
	public void setZeroBased(boolean zeroBased) {
		this.zeroBased = zeroBased;
	}

	@Override
	public void free() throws SQLException {
		
	}

	@Override
	public Object getArray() throws SQLException {
		return values;
	}

	@Override
	public Object getArray(Map<String, Class<?>> map) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Object getArray(long index, int count) throws SQLException {
		if (index > Integer.MAX_VALUE) {
			throw new ArrayIndexOutOfBoundsException(String.valueOf(index));
		}
		int offset = zeroBased?0:1;
		int iIndex = (int)index - offset;
		if (iIndex >= values.length || iIndex < 0) {
			throw new ArrayIndexOutOfBoundsException(iIndex);
		}
		return Arrays.copyOfRange(values, iIndex, Math.min(iIndex + count, values.length));
	}

	@Override
	public Object getArray(long index, int count, Map<String, Class<?>> map)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getBaseType() throws SQLException {
		return JDBCSQLTypeInfo.getSQLType(DataTypeManager.getDataTypeName(values.getClass().getComponentType()));
	}

	@Override
	public String getBaseTypeName() throws SQLException {
		return DataTypeManager.getDataTypeName(values.getClass().getComponentType());
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getResultSet(Map<String, Class<?>> map)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getResultSet(long index, int count) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getResultSet(long index, int count,
			Map<String, Class<?>> map) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}
	
}
