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
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;

import javax.sql.rowset.serial.SerialBlob;

import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.ObjectConverterUtil;


/**
 * Represent a value of type "blob", which can be streamable from client
 */
public final class BlobType extends Streamable<Blob> implements Blob, Comparable<BlobType> {

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
    public byte[] getBytes(long pos, int len) throws SQLException {
        return this.reference.getBytes(pos, len);
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

	public InputStream getBinaryStream(long pos, long len)
			throws SQLException {
		return this.reference.getBinaryStream(pos, len);
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
			throw new IOException(e);
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
			int bytes = ObjectConverterUtil.write(os, is, length, false);
			if (bytes != length) {
				throw new IOException("Expected length " + length + " but was " + bytes); //$NON-NLS-1$ //$NON-NLS-2$
			}
		} finally {
			is.close();
		}
	}
	
	@Override
	public int compareTo(BlobType o) {
		try {
    		InputStream is1 = this.getBinaryStream();
    		InputStream is2 = o.getBinaryStream();
    		long len1 = this.length();
    		long len2 = o.length();
    		long n = Math.min(len1, len2);
		    for (long i = 0; i < n; i++) {
				int b1 = is1.read();
				int b2 = is2.read();
				if (b1 != b2) {
				    return b1 - b2;
				}
		    }
    		return Long.signum(len1 - len2);
		} catch (SQLException e) {
			throw new TeiidRuntimeException(e);
		} catch (IOException e) {
			throw new TeiidRuntimeException(e);
		}
	}
	
}
