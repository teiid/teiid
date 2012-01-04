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

import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;

import javax.sql.rowset.serial.SerialBlob;

import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.ObjectConverterUtil;


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
    
    @Override
    long computeLength() throws SQLException {
        return this.reference.length();
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
    
	public void free() throws SQLException {
		this.reference.free();
	}

	public InputStream getBinaryStream(long pos, long length)
			throws SQLException {
		return this.reference.getBinaryStream(pos, length);
	}
	
	public static SerialBlob createBlob(byte[] bytes) {
		if (bytes == null) {
			return null;
		}
		try {
			return new SerialBlob(bytes);
		} catch (SQLException e) {
			throw new TeiidRuntimeException(e);
		}
	}
	
	@Override
	protected void readReference(ObjectInput in) throws IOException {
		byte[] bytes = new byte[(int)getLength()];
		in.readFully(bytes);
		try {
			this.reference = new SerialBlob(bytes);
		} catch (SQLException e) {
			throw new IOException(e);
		}
	}
	
	@Override
	protected void writeReference(final DataOutput out) throws IOException {
		try {
			writeBinary(out, getBinaryStream(), (int)length);
		} catch (SQLException e) {
			throw new IOException();
		}
	}

	static void writeBinary(final DataOutput out, InputStream is, int length) throws IOException {
		OutputStream os = new OutputStream() {
			
			@Override
			public void write(int b) throws IOException {
				out.write(b);
			}
		};
		try {
			ObjectConverterUtil.write(os, is, length, false);
		} finally {
			is.close();
		}
	}
	
}
