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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialException;

import com.metamatrix.core.CorePlugin;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.ArgCheck;

/**
 * Represent a value of type "blob", which can be streamable from client
 */
public final class BlobType implements Streamable, Blob {

	private transient Blob srcBlob;
	private String streamId;
	private String persistentId;
	private long length = -1;
    
    /**
     * Can't construct
     */
    BlobType() {
        super();
    }

    public BlobType(Blob blob) {
    	ArgCheck.isNotNull(blob);
        this.srcBlob = blob;
        try {
            this.length = blob.length();
        } catch (SQLException e) {
            // ignore.
        }
    }
    
    public Blob getSourceBlob() {
    	return srcBlob;
    }
    
    /** 
     * @see com.metamatrix.common.types.Streamable#getReferenceStreamId()
     */
    public String getReferenceStreamId() {
        return this.streamId;
    }
    
    /** 
     * @see com.metamatrix.common.types.Streamable#setReferenceStreamId(java.lang.String)
     */
    public void setReferenceStreamId(String id) {
        this.streamId = id;
    }
    
    /** 
     * @see com.metamatrix.common.types.Streamable#getPersistenceStreamId()
     */
    public String getPersistenceStreamId() {
        return persistentId;
    }

    /** 
     * @see com.metamatrix.common.types.Streamable#setPersistenceStreamId(java.lang.String)
     */
    public void setPersistenceStreamId(String id) {
        this.persistentId = id;
    }    
    
    /** 
     * @see java.sql.Blob#getBinaryStream()
     */
    public InputStream getBinaryStream() throws SQLException {
        checkReference();
        return this.srcBlob.getBinaryStream();
    }

    /** 
     * @see java.sql.Blob#getBytes(long, int)
     */
    public byte[] getBytes(long pos, int length) throws SQLException {
        checkReference();
        return this.srcBlob.getBytes(pos, length);
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
        checkReference();
        return this.srcBlob.length();
    }

    /** 
     * @see java.sql.Blob#position(java.sql.Blob, long)
     */
    public long position(Blob pattern, long start) throws SQLException {
        checkReference();
        return this.srcBlob.position(pattern, start);
    }

    /** 
     * @see java.sql.Blob#position(byte[], long)
     */
    public long position(byte[] pattern, long start) throws SQLException {
        checkReference();
        return this.srcBlob.position(pattern, start);
    }

    /** 
     * @see java.sql.Blob#setBinaryStream(long)
     */
    public OutputStream setBinaryStream(long pos) throws SQLException {
        checkReference();
        return this.srcBlob.setBinaryStream(pos);
    }

    /** 
     * @see java.sql.Blob#setBytes(long, byte[], int, int)
     * @since 4.3
     */
    public int setBytes(long pos,
                        byte[] bytes,
                        int offset,
                        int len) throws SQLException {
        checkReference();
        return this.srcBlob.setBytes(pos, bytes, offset, len);
    }

    /** 
     * @see java.sql.Blob#setBytes(long, byte[])
     */
    public int setBytes(long pos, byte[] bytes) throws SQLException {
        checkReference();
        return this.srcBlob.setBytes(pos, bytes);
    }

    /** 
     * @see java.sql.Blob#truncate(long)
     */
    public void truncate(long len) throws SQLException {
        checkReference();
        this.srcBlob.truncate(len);
    }
    
    /** 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    public boolean equals(Object o) {
    	if (this == o) {
    		return true;
    	}
    	
    	if (!(o instanceof BlobType)) {
    		return false;
    	}
    	
    	BlobType other = (BlobType)o;
    	
    	if (this.srcBlob != null) {
    		return this.srcBlob.equals(other.srcBlob);
    	}
    	
    	return this.persistentId == other.persistentId
				&& this.streamId == other.streamId;

    }

    /** 
     * @see java.lang.Object#toString()
     */
    public String toString() {
        checkReference();
        return srcBlob.toString();
    }    
        
    private void checkReference() {
        if (this.srcBlob == null) {
            throw new InvalidReferenceException(CorePlugin.Util.getString("BlobValue.InvalidReference")); //$NON-NLS-1$
        }
    }
    
    /**
     * Utility Method to convert blob into byte array  
     * @param blob
     * @return byte array
     */
    public static byte[] getByteArray(Blob blob) throws SQLException, IOException {
        InputStream reader = blob.getBinaryStream();
        ByteArrayOutputStream writer = new ByteArrayOutputStream();
        int c = reader.read();
        while (c != -1) {
            writer.write((byte)c);
            c = reader.read();
        }
        reader.close();
        byte[] data = writer.toByteArray();
        writer.close();
        return data;        
    }
    //## JDBC4.0-begin ##
	public void free() throws SQLException {
		checkReference();
		this.srcBlob.free();
	}

	public InputStream getBinaryStream(long pos, long length)
			throws SQLException {
		checkReference();
		return this.srcBlob.getBinaryStream(pos, length);
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
	
}
