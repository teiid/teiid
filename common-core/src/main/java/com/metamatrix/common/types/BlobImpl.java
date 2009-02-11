/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.common.types;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import com.metamatrix.core.CorePlugin;

/**
 * This object holds a chunk of binary data and implements the JDBC Blob interface.
 * This object presents a streaming interface but actually encapsulates the entire
 * blob object.  Connectors can construct this object when dealing with large
 * objects.
 */
public class BlobImpl implements Blob, Serializable {

    //if no default is set, this default size is used by the connector.
    public static final int DEFAULT_MAX_SIZE = 5000000;
    private byte[] data;

    /**
     * Creates a MMBlob object by calling <code>getBytes</code> on the 
     * <code>Blob</code> argument.
     * @param blob the Blob object to get the data from.
     */
    public BlobImpl(Blob blob) throws SQLException {
        int length = (int) blob.length();
        data = blob.getBytes(1, length);
    }

    /**
     * Creates a MMBlob object by copying the data in <code>originalData</code>
     * @param originalData the array of bytes to copy into this MMBlob object.
     */
    public BlobImpl(byte[] originalData) {
        data = new byte[originalData.length];
        int originalDataStartPosition = 0;
        int dataStartPosition = 0;
        int bytesToCopy = originalData.length;
        System.arraycopy(originalData, originalDataStartPosition, data, dataStartPosition, bytesToCopy);
    }

    public BlobImpl(InputStream in, int length) throws SQLException{
    	data = new byte[length];
    	try {
			in.read(data, 0, length);
		} catch (IOException e) {
			throw new SQLException(e.getMessage());
		}
    }
    
    /**
     * Retrieves the <code>BLOB</code> designated by this
     * <code>Blob</code> instance as a stream.
     * @return a stream containing the <code>BLOB</code> data
     * @exception SQLException if there is an error accessing the
     * <code>BLOB</code>
     */
    public InputStream getBinaryStream() throws SQLException {
        return new ByteArrayInputStream(data);
    }

    /**
     * Returns as an array of bytes, part or all of the <code>BLOB</code>
     * value that this <code>Blob</code> object designates.  The byte
     * array contains up to <code>length</code> consecutive bytes
     * starting at position <code>pos</code>.
     * @param pos the ordinal position of the first byte in the 
     * <code>BLOB</code> value to be extracted; the first byte is at
     * position 1
     * @param length the number of consecutive bytes to be copied
     * @return a byte array containing up to <code>length</code> 
     * consecutive bytes from the <code>BLOB</code> value designated
     * by this <code>Blob</code> object, starting with the
     * byte at position <code>pos</code>
     * @exception SQLException if there is an error accessing the
     * <code>BLOB</code>
     */
    public byte[] getBytes(long pos, int length) throws SQLException {
        if (pos < 1) {
            throw new SQLException(CorePlugin.Util.getString("BlobImpl.Invalid_byte_position",  new Long(pos))); //$NON-NLS-1$
        }

        if (pos > data.length) {
            return null;
        }

        if (length < 0) {
            throw new SQLException(CorePlugin.Util.getString("BlobImpl.Invalid_bytes_length",  new Long(length))); //$NON-NLS-1$
        }

        if (length > data.length) {
            length = data.length;
        }

        byte[] dataCopy = new byte[length];
        int startingDataPosition = (int) pos - 1; //pos is the ordinal index (starts at 1) so we need pos - 1
        int startingCopyPosition = 0;
        System.arraycopy(data, startingDataPosition, dataCopy, startingCopyPosition, length);
        return dataCopy;
    }

    /**
     * Returns the number of bytes in the <code>BLOB</code> value
     * designated by this <code>Blob</code> object.
     * @return length of the <code>BLOB</code> in bytes
     * @exception SQLException if there is an error accessing the
     * length of the <code>BLOB</code>
     */
    public long length() throws SQLException {
        return data.length;
    }

    /** 
     * Determines the byte position in the <code>BLOB</code> value
     * designated by this <code>Blob</code> object at which 
     * <code>pattern</code> begins.  The search begins at position
     * <code>start</code>.
     * @param pattern the <code>Blob</code> object designating
     * the <code>BLOB</code> value for which to search
     * @param start the position in the <code>BLOB</code> value
     *        at which to begin searching; the first position is 1
     * @return the position at which the pattern begins, else -1
     * @exception SQLException if there is an error accessing the
     * <code>BLOB</code>
     */
    public long position(Blob pattern, long start) throws SQLException {
        if (pattern == null || start > data.length) {
            return -1;
        }
        int length = (int) pattern.length();
        byte[] patternBytes = pattern.getBytes(1, length);
        return position(patternBytes, start);
    }

    /** 
     * Determines the byte position at which the specified byte 
     * <code>pattern</code> begins within the <code>BLOB</code>
     * value that this <code>Blob</code> object represents.  The
     * search for <code>pattern</code> begins at position
     * <code>start</code>.  
     * @param pattern the byte array for which to search
     * @param start the position at which to begin searching; the
     *        first position is 1
     * @return the position at which the pattern appears, else -1
     * @exception SQLException if there is an error accessing the 
     * <code>BLOB</code>
     */
    public long position(byte[] pattern, long start) throws SQLException {
        if (start < 1) {
            throw new SQLException(CorePlugin.Util.getString("BlobImpl.Invalid_start_position",  new Long(start))); //$NON-NLS-1$
        }
        if (pattern == null || start > data.length) {
            return -1;
        }
        String patternString = new String(pattern);
        String byteString = new String(data);
        start--;
        int position = byteString.indexOf(patternString, (int) start);
        if (position != -1)
            position++;
        return position;
    }

    /**
     * Compares two MMBlob objects for equality.
     * @return True if equal
     * @see java.lang.Object#equals(Object)
     */
    public boolean equals(Object obj) {
        try {
            if (obj == null || data == null) {
                return false;
            }

            if (obj == this) {
                return true;
            }

            if (!(obj instanceof BlobImpl)) {
                return false;
            }

            BlobImpl blob = (BlobImpl) obj;

            if (blob.length() != length()) {
                return false;
            }

            byte[] bytes = blob.getBytes(1, (int) blob.length());

            for (int i = 0; i < bytes.length; i++) {
                if (bytes[i] != data[i]) {
                    return false;
                }
            }
        } catch (SQLException e) {
            return false;
        }
        return true;
    }

    /** 
     * Returns a String that is a comma delimited list of up to the first 5 and 
     * last 5 bytes of the Blob in Hexadecimal.
     * @return the String object convenient for displaying this Blob object
     */
    public String toString() {
        String pre = "{"; //$NON-NLS-1$
        String post = ""; //$NON-NLS-1$

        int lastByte = data.length - 1;
        int frontTruncate = 5;
        int endTruncate = lastByte - 5;

        if (lastByte < 10) {
            frontTruncate = lastByte;
            endTruncate = lastByte;
        }

        for (int i = 0; i < frontTruncate; i++) {
            pre += Integer.toString(data[i], 16) + ","; //$NON-NLS-1$
        }

        for (int i = endTruncate; i < lastByte; i++) {
            post += "," + Integer.toString(data[i], 16); //$NON-NLS-1$
        }

        post += "}"; //$NON-NLS-1$

        return "Length:" + data.length + ":" + pre + "..." + post; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

	/**
	 * @see java.sql.Blob#setBytes(long, byte[])
	 */
	public int setBytes(long pos, byte[] bytes) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/**
	 * @see java.sql.Blob#setBytes(long, byte[], int, int)
	 */
	public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/**
	 * @see java.sql.Blob#setBinaryStream(long)
	 */
	public OutputStream setBinaryStream(long pos) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/**
	 * @see java.sql.Blob#truncate(long)
	 */
	public void truncate(long len) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void free() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public InputStream getBinaryStream(long arg0, long arg1)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}
}


