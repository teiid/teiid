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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;

import javax.sql.rowset.serial.SerialBlob;

import org.teiid.core.CorePlugin;
import org.teiid.core.types.LobSearchUtil.StreamProvider;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.SqlUtil;


/**
 * This object holds a chunk of binary data and implements the JDBC Blob interface.
 * It presents a streaming interface and provides a way to access partial
 * of the blob data. Connectors can use this object when dealing with large
 * objects.
 */
public class BlobImpl extends BaseLob implements Blob, StreamProvider {
    
	public BlobImpl() {
		
	}
	
    /**
     * Creates a MMBlob object with the <code>valueID</code>.
     * @param valueID reference to value chunk in data source.
     */
    public BlobImpl(InputStreamFactory streamFactory) {
    	super(streamFactory);
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
            Object[] params = new Object[] {new Long(pos)};
            throw new SQLException(CorePlugin.Util.getString("MMClob_MMBlob.0", params)); //$NON-NLS-1$
        }
        else if (pos > length()) {
            return null;
        }
        pos = pos - 1;
        
        if (length < 0) {
            Object[] params = new Object[] {new Integer( length)};
            throw new SQLException(CorePlugin.Util.getString("MMClob_MMBlob.3", params)); //$NON-NLS-1$
        }
        else if (pos + length > length()) {
            length = (int)(length() - pos);
        }
        InputStream in = getBinaryStream();
        try {
        	long skipped = 0;
        	while (pos > 0) {
        		skipped = in.skip(pos);
        		pos -= skipped;
        	}
        	return ObjectConverterUtil.convertToByteArray(in, length);
        } catch (IOException e) {
        	throw new SQLException(e);
        } finally {
        	try {
				in.close();
			} catch (IOException e) {
			}
        }
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
    public long position(final Blob pattern, long start) throws SQLException {
        if (pattern == null) {
            return -1;
        }
        
        return LobSearchUtil.position(new LobSearchUtil.StreamProvider() {
        	public InputStream getBinaryStream() throws SQLException {
        		return pattern.getBinaryStream();
        	}
        }, pattern.length(), this, this.length(), start, 1);
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
    	if (pattern == null) {
    		return -1;
    	}
        return position(new SerialBlob(pattern), start);
    }
        
	public InputStream getBinaryStream(long arg0, long arg1)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}


	/**
	 * @see java.sql.Blob#setBytes(long, byte[])
	 */
	public int setBytes(long pos, byte[] bytes) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	/**
	 * @see java.sql.Blob#setBytes(long, byte[], int, int)
	 */
	public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	/**
	 * @see java.sql.Blob#setBinaryStream(long)
	 */
	public OutputStream setBinaryStream(long pos) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	/**
	 * @see java.sql.Blob#truncate(long)
	 */
	public void truncate(long len) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}
}
