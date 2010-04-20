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

package com.metamatrix.common.types;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;

import javax.sql.rowset.serial.SerialBlob;

import com.metamatrix.core.MetaMatrixRuntimeException;

/**
 * Represent a value of type "blob", which can be streamable from client
 */
public final class BlobType extends Streamable<Blob> implements Blob {

	private static final long serialVersionUID = 1294191629070433450L;
    
    public BlobType() {
    }

    public BlobType(Blob blob) {
    	super(blob);
    }
    
    /** 
     * @see java.sql.Blob#getBinaryStream()
     */
    public InputStream getBinaryStream() throws SQLException {
        return this.reference.getBinaryStream();
    }

    /** 
     * @see java.sql.Blob#getBytes(long, int)
     */
    public byte[] getBytes(long pos, int length) throws SQLException {
        return this.reference.getBytes(pos, length);
    }

    /** 
     * @see java.sql.Blob#length()
     */
    public long length() throws SQLException {
        //caching the length
        if (this.length != -1) {
            return this.length;
        }
        // if did not find before then do it again.
        this.length = this.reference.length();
        return length;
    }
    
    /** 
     * @see java.sql.Blob#position(java.sql.Blob, long)
     */
    public long position(Blob pattern, long start) throws SQLException {
        return this.reference.position(pattern, start);
    }

    /** 
     * @see java.sql.Blob#position(byte[], long)
     */
    public long position(byte[] pattern, long start) throws SQLException {
        return this.reference.position(pattern, start);
    }

    /** 
     * @see java.sql.Blob#setBinaryStream(long)
     */
    public OutputStream setBinaryStream(long pos) throws SQLException {
        return this.reference.setBinaryStream(pos);
    }

    /** 
     * @see java.sql.Blob#setBytes(long, byte[], int, int)
     * @since 4.3
     */
    public int setBytes(long pos,
                        byte[] bytes,
                        int offset,
                        int len) throws SQLException {
        return this.reference.setBytes(pos, bytes, offset, len);
    }

    /** 
     * @see java.sql.Blob#setBytes(long, byte[])
     */
    public int setBytes(long pos, byte[] bytes) throws SQLException {
        return this.reference.setBytes(pos, bytes);
    }

    /** 
     * @see java.sql.Blob#truncate(long)
     */
    public void truncate(long len) throws SQLException {
        this.reference.truncate(len);
    }
    
    //## JDBC4.0-begin ##
	public void free() throws SQLException {
		this.reference.free();
	}

	public InputStream getBinaryStream(long pos, long length)
			throws SQLException {
		return this.reference.getBinaryStream(pos, length);
	}
	//## JDBC4.0-end ##
	
	public static SerialBlob createBlob(byte[] bytes) {
		if (bytes == null) {
			return null;
		}
		try {
			return new SerialBlob(bytes);
		} catch (SQLException e) {
			throw new MetaMatrixRuntimeException(e);
		}
	}
	
	private void writeObject(java.io.ObjectOutputStream out) throws IOException {
		try {
			length();
		} catch (SQLException e) {
		}
		out.defaultWriteObject();
	}
	
}
