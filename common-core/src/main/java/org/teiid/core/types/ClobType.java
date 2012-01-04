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
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.Clob;
import java.sql.SQLException;

import javax.sql.rowset.serial.SerialClob;

import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.ObjectConverterUtil;


/**
 * This is wrapper on top of a "clob" object, which implements the "java.sql.Clob"
 * interface. This class also implements the Streamable interface
 */
public final class ClobType extends Streamable<Clob> implements Clob, Sequencable {

	private static final long serialVersionUID = 2753412502127824104L;
    
    public ClobType() {
    }
    
    public ClobType(Clob clob) {
    	super(clob);
    }
    
    /** 
     * @see java.sql.Clob#getAsciiStream()
     */
    public InputStream getAsciiStream() throws SQLException {
        return this.reference.getAsciiStream();
    }

    /** 
     * @see java.sql.Clob#getCharacterStream()
     */
    public Reader getCharacterStream() throws SQLException {
        return this.reference.getCharacterStream();
    }

    /** 
     * @see java.sql.Clob#getSubString(long, int)
     */
    public String getSubString(long pos, int length) throws SQLException {
        return this.reference.getSubString(pos, length);
    }
    
    @Override
    long computeLength() throws SQLException {
        return this.reference.length();
    }

    /** 
     * @see java.sql.Clob#position(java.sql.Clob, long)
     */
    public long position(Clob searchstr, long start) throws SQLException {
        return this.reference.position(searchstr, start);
    }

    /** 
     * @see java.sql.Clob#position(java.lang.String, long)
     */
    public long position(String searchstr, long start) throws SQLException {
        return this.reference.position(searchstr, start);
    }

    /** 
     * @see java.sql.Clob#setAsciiStream(long)
     */
    public OutputStream setAsciiStream(long pos) throws SQLException {
        return this.reference.setAsciiStream(pos);
    }

    /** 
     * @see java.sql.Clob#setCharacterStream(long)
     */
    public Writer setCharacterStream(long pos) throws SQLException {
        return this.reference.setCharacterStream(pos);
    }

    /** 
     * @see java.sql.Clob#setString(long, java.lang.String, int, int)
     */
    public int setString(long pos,
                         String str,
                         int offset,
                         int len) throws SQLException {
        return this.reference.setString(pos, str, offset, len);
    }

    /** 
     * @see java.sql.Clob#setString(long, java.lang.String)
     */
    public int setString(long pos, String str) throws SQLException {
        return this.reference.setString(pos, str);
    }

    /** 
     * @see java.sql.Clob#truncate(long)
     */
    public void truncate(long len) throws SQLException {
        this.reference.truncate(len);
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
    
    private final static int CHAR_SEQUENCE_BUFFER_SIZE = 1 << 12;
    
    public CharSequence getCharSequence() {
        return new CharSequence() {

        	private String buffer;
        	private int beginPosition;
        	        	
            public int length() {
                long result;
                try {
                    result = ClobType.this.length();
                } catch (SQLException err) {
                    throw new TeiidRuntimeException(err);
                } 
                if (((int)result) != result) {
                    throw new TeiidRuntimeException("Clob value is not representable by CharSequence"); //$NON-NLS-1$                    
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
                    throw new TeiidRuntimeException(err);
                } 
            }

            public CharSequence subSequence(int start,
                                            int end) {
                try {
                    return ClobType.this.getSubString(start + 1, end - start);
                } catch (SQLException err) {
                    throw new TeiidRuntimeException(err);
                }
            }
            
        };
    }

    public void free() throws SQLException {
		this.reference.free();
	}

	public Reader getCharacterStream(long pos, long length) throws SQLException {
		return this.reference.getCharacterStream(pos, length);
	}
	
	public static SerialClob createClob(char[] chars) {
		try {
			return new SerialClob(chars);
		} catch (SQLException e) {
			throw new TeiidRuntimeException(e);
		}
	}
	
	@Override
	protected void readReference(ObjectInput in) throws IOException {
		char[] chars = new char[(int)length];
		for (int i = 0; i < chars.length; i++) {
			chars[i] = in.readChar();
		}
		try {
			this.reference = new SerialClob(chars);
		} catch (SQLException e) {
			throw new IOException(e);
		}
	}
	
	/**
	 * Since we have the length in chars we'll just write out in double byte format.
	 * These clobs should be small, so the wasted space should be minimal.
	 */
	@Override
	protected void writeReference(final DataOutput out) throws IOException {
		Writer w = new Writer() {
			
			@Override
			public void write(char[] cbuf, int off, int len) throws IOException {
				for (int i = off; i < len; i++) {
					out.writeShort(cbuf[i]);
				}
			}
			
			@Override
			public void flush() throws IOException {
			}
			
			@Override
			public void close() throws IOException {
			}
		};
		Reader r;
		try {
			r = getCharacterStream();
		} catch (SQLException e) {
			throw new IOException(e);
		}
		try {
			ObjectConverterUtil.write(w, r, (int)length, false);
		} finally {
			r.close();
		}
	}

}
