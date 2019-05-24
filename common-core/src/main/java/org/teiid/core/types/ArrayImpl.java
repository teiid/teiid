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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;

import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.ExternalizeUtil;
import org.teiid.core.util.HashCodeUtil;

/**
 * Provides a serializable {@link Array} implementation with minimal JDBC functionality.
 */
public final class ArrayImpl implements Comparable<ArrayImpl>, Externalizable, Array {
    private static final String INVALID = ""; //$NON-NLS-1$
    private static final long serialVersionUID = 517794153664734815L;
    /**
     * a regrettable hack for pg compatibility since we want to avoid adding a vector type
     */
    private boolean zeroBased;
    private Object[] values;

    @SuppressWarnings("serial")
    public final static class NullException extends RuntimeException {};
    private final static NullException ex = new NullException();

    public ArrayImpl(Object... values) {
        this.values = values;
    }

    public ArrayImpl() {

    }

    private void checkValues() throws SQLException {
        if (values == null) {
            throw new SQLException("Already freed or invalid"); //$NON-NLS-1$
        }
    }

    @Override
    public int compareTo(ArrayImpl o) {
        return compareTo(o, false, null);
    }

    public int compareTo(ArrayImpl o, boolean noNulls, Comparator<Object> comparator) {
        if (zeroBased != o.zeroBased) {
            throw new TeiidRuntimeException("Incompatible types"); //$NON-NLS-1$
        }
        try {
            checkValues();
            o.checkValues();
        } catch (SQLException e) {
            throw new TeiidRuntimeException(e);
        }
        Object[] values2 = o.values;
        return compare(noNulls, comparator, values, values2);
    }

    private static int compare(boolean noNulls, Comparator<Object> comparator,
            Object[] values, Object[] values2) {
        int len1 = values.length;
        int len2 = values2.length;
        int lim = Math.min(len1, len2);
        for (int k = 0; k < lim; k++) {
            Object object1 = values[k];
            Object object2 = values2[k];
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
            if (object1 instanceof Object[] && object2 instanceof Object[]) {
                comp = compare(noNulls, comparator, (Object[])object1, (Object[])object2);
            } else if (comparator != null) {
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
        try {
            return zeroBased == other.zeroBased && compareTo(other) == 0;
        } catch (ClassCastException e) {
            return false;
        }
    }

    public Object[] getValues() {
        if (values == null) {
            throw new TeiidRuntimeException("Already freed or invalid"); //$NON-NLS-1$
        }
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
        this.values = null;
    }

    @Override
    public Object getArray() throws SQLException {
        checkValues();
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
        checkValues();
        return Arrays.copyOfRange(values, iIndex, Math.min(iIndex + count, values.length));
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map)
            throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getBaseType() throws SQLException {
        checkValues();
        return JDBCSQLTypeInfo.getSQLType(DataTypeManager.getDataTypeName(values.getClass().getComponentType()));
    }

    @Override
    public String getBaseTypeName() throws SQLException {
        checkValues();
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

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        String componentType = in.readUTF();
        if (INVALID.equalsIgnoreCase(componentType)) {
            return;
        }
        this.values = ExternalizeUtil.readArray(in, DataTypeManager.getDataTypeClass(componentType));
        zeroBased = in.readBoolean();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        if (values == null) {
            out.writeUTF(INVALID);
            return;
        }
        out.writeUTF(DataTypeManager.getDataTypeName(this.values.getClass().getComponentType()));
        ExternalizeUtil.writeArray(out, values);
        out.writeBoolean(zeroBased);
    }

}
