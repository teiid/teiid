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
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.Clob;
import java.sql.SQLException;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;
import javax.sql.rowset.serial.SerialException;

import com.metamatrix.core.CorePlugin;
import com.metamatrix.core.MetaMatrixRuntimeException;

/**
 * This is wrapper on top of a "clob" object, which implements the "java.sql.Clob"
 * interface. This class also implements the Streamable interface
 */
public final class ClobType implements Streamable, Clob, Sequencable {

	private transient Clob srcClob;    
	private String streamId;
	private String persistentId;
	private long length = -1;
    
    /**
     * Can't construct
     */
    ClobType() {
        super();
    }
    
    public Clob getSourceClob() {
    	return this.srcClob;
    }
    
    public ClobType(Clob clob) {
        if (clob == null) {
            throw new IllegalArgumentException(CorePlugin.Util.getString("ClobValue.isNUll")); //$NON-NLS-1$
        }
        // this will serve as the in VM reference
        this.srcClob = clob;
        
        try {
            this.length = clob.length();
        } catch (SQLException e) {
            // ignore.
        }
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
     * @see java.sql.Clob#getAsciiStream()
     */
    public InputStream getAsciiStream() throws SQLException {
        checkReference();
        return this.srcClob.getAsciiStream();
    }

    /** 
     * @see java.sql.Clob#getCharacterStream()
     */
    public Reader getCharacterStream() throws SQLException {
        checkReference();
        return this.srcClob.getCharacterStream();
    }

    /** 
     * @see java.sql.Clob#getSubString(long, int)
     */
    public String getSubString(long pos, int length) throws SQLException {
        checkReference();
        return this.srcClob.getSubString(pos, length);
    }

    /** 
     * @see java.sql.Clob#length()
     */
    public long length() throws SQLException {
        if (this.length != -1) {
            return this.length;
        }
        
        checkReference();
        return this.srcClob.length();
    }

    /** 
     * @see java.sql.Clob#position(java.sql.Clob, long)
     */
    public long position(Clob searchstr, long start) throws SQLException {
        checkReference();
        return this.srcClob.position(searchstr, start);
    }

    /** 
     * @see java.sql.Clob#position(java.lang.String, long)
     */
    public long position(String searchstr, long start) throws SQLException {
        checkReference();
        return this.srcClob.position(searchstr, start);
    }

    /** 
     * @see java.sql.Clob#setAsciiStream(long)
     */
    public OutputStream setAsciiStream(long pos) throws SQLException {
        checkReference();
        return this.srcClob.setAsciiStream(pos);
    }

    /** 
     * @see java.sql.Clob#setCharacterStream(long)
     */
    public Writer setCharacterStream(long pos) throws SQLException {
        checkReference();
        return this.srcClob.setCharacterStream(pos);
    }

    /** 
     * @see java.sql.Clob#setString(long, java.lang.String, int, int)
     */
    public int setString(long pos,
                         String str,
                         int offset,
                         int len) throws SQLException {
        checkReference();
        return this.srcClob.setString(pos, str, offset, len);
    }

    /** 
     * @see java.sql.Clob#setString(long, java.lang.String)
     */
    public int setString(long pos, String str) throws SQLException {
        checkReference();
        return this.srcClob.setString(pos, str);
    }

    /** 
     * @see java.sql.Clob#truncate(long)
     */
    public void truncate(long len) throws SQLException {
        checkReference();
        this.srcClob.truncate(len);
    }    

    /** 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    public boolean equals(Object o) {
    	if (this == o) {
    		return true;
    	}
    	
    	if (!(o instanceof ClobType)) {
    		return false;
    	}
    	
    	ClobType other = (ClobType)o;
    	
    	if (this.srcClob != null) {
    		return this.srcClob.equals(other.srcClob);
    	}
    	
    	return this.persistentId == other.persistentId
				&& this.streamId == other.streamId;
    }

    /** 
     * @see java.lang.Object#toString()
     */
    public String toString() {
        checkReference();
        return srcClob.toString();
    }
            
    private void checkReference() {
        if (this.srcClob == null) {
            throw new InvalidReferenceException(CorePlugin.Util.getString("ClobValue.InvalidReference")); //$NON-NLS-1$
        }
    }     
    
    /**
     * Utility method to convert to String  
     * @param clob
     * @return string form of the clob passed.
     */
    public static String getString(Clob clob) throws SQLException, IOException {
        Reader reader = clob.getCharacterStream();
        StringWriter writer = new StringWriter();
        int c = reader.read();
        while (c != -1) {
            writer.write((char)c);
            c = reader.read();
        }
        reader.close();
        String data = writer.toString();
        writer.close();
        return data;        
    }
    
    private final static int CHAR_SEQUENCE_BUFFER_SIZE = 2 << 12;
    
    public CharSequence getCharSequence() {
        checkReference();
        return new CharSequence() {

        	private String buffer;
        	private int beginPosition;
        	        	
            public int length() {
                long result;
                try {
                    result = ClobType.this.length();
                } catch (SQLException err) {
                    throw new MetaMatrixRuntimeException(err);
                } 
                if (((int)result) != result) {
                    throw new MetaMatrixRuntimeException("Clob value is not representable by CharSequence"); //$NON-NLS-1$                    
                }
                return (int)result;
            }

            public char charAt(int index) {
                try {
                	if (buffer == null || index < beginPosition || index >= beginPosition + buffer.length()) {
                		buffer = ClobType.this.getSubString(index + 1, CHAR_SEQUENCE_BUFFER_SIZE);
                		beginPosition = index;
                	}
                	return buffer.charAt(index - beginPosition);
                } catch (SQLException err) {
                    throw new MetaMatrixRuntimeException(err);
                } 
            }

            public CharSequence subSequence(int start,
                                            int end) {
                try {
                    return ClobType.this.getSubString(start + 1, end - start);
                } catch (SQLException err) {
                    throw new MetaMatrixRuntimeException(err);
                }
            }
            
        };
    }
    //## JDBC4.0-begin ##
	public void free() throws SQLException {
		checkReference();
		this.srcClob.free();
	}

	public Reader getCharacterStream(long pos, long length) throws SQLException {
		checkReference();
		return this.srcClob.getCharacterStream(pos, length);
	}
	//## JDBC4.0-end ##
	
	public static SerialClob createClob(char[] chars) {
		try {
			return new SerialClob(chars);
		} catch (SQLException e) {
			throw new MetaMatrixRuntimeException(e);
		}
	}
}
