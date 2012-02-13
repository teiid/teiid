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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.nio.charset.Charset;
import java.sql.Clob;
import java.sql.SQLException;

import org.teiid.core.CorePlugin;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.ReaderInputStream;
import org.teiid.core.util.SqlUtil;


/**
 * This object holds a chunk of char data and implements the JDBC Clob interface.
 * This object presents a streaming interface and provides a way to access partial
 * of the Clob data. Connectors can use this object when dealing with large
 * objects.
 */
public class ClobImpl extends BaseLob implements Clob {
    
	private final static class ClobStreamProvider implements
			LobSearchUtil.StreamProvider {
		private final Clob searchstr;

		private ClobStreamProvider(Clob searchstr) {
			this.searchstr = searchstr;
		}

		public InputStream getBinaryStream() throws SQLException {
			ReaderInputStream ris = new ReaderInputStream(searchstr.getCharacterStream(), Charset.forName("UTF-16")); //$NON-NLS-1$
			try {
				ris.skip(2);
				return ris;
			} catch (IOException e) {
				throw new SQLException(e);
			}
		}
	}

	private long len = -1;
	
	public ClobImpl() {
		
	}
	
	/**
	 * Creates a new ClobImpl.  Note that the length is not taken from the {@link InputStreamFactory} since
	 * it refers to bytes and not chars.
	 * @param streamFactory
	 * @param length
	 */
    public ClobImpl(InputStreamFactory streamFactory, long length) {
		super(streamFactory);
		this.len = length;
	}
    
    public ClobImpl(final char[] chars) {
    	this(new InputStreamFactory() {
    		
    		String str = new String(chars);

			@Override
			public InputStream getInputStream() throws IOException {
				return new ByteArrayInputStream(str.getBytes());
			}
			
			@Override
			public Reader getCharacterStream() throws IOException {
				return new StringReader(str);
			}
			
			@Override
			public StorageMode getStorageMode() {
				return StorageMode.MEMORY;
			}
    		
    	}, chars.length);
    }

	/**
     * Gets the <code>CLOB</code> value designated by this <code>Clob</code>
     * object as a stream of Ascii bytes.
     * @return an ascii stream containing the <code>CLOB</code> data
     * @exception SQLException if there is an error accessing the
     * <code>CLOB</code> value
     */
    public InputStream getAsciiStream() throws SQLException {
    	return new ReaderInputStream(getCharacterStream(), Charset.forName("US-ASCII")); //$NON-NLS-1$
    }

    /**
     * Returns a copy of the specified substring
     * in the <code>CLOB</code> value
     * designated by this <code>Clob</code> object.
     * The substring begins at position
     * <code>pos</code> and has up to <code>length</code> consecutive
     * characters.
     * @param pos the first character of the substring to be extracted.
     *            The first character is at position 1.
     * @param length the number of consecutive characters to be copied
     * @return a <code>String</code> that is the specified substring in
     *         the <code>CLOB</code> value designated by this <code>Clob</code> object
     * @exception SQLException if there is an error accessing the <code>CLOB</code>
     */
    public String getSubString(long pos, int length) throws SQLException {
        if (pos < 1) {
            Object[] params = new Object[] {new Long(pos)};
            throw new SQLException(CorePlugin.Util.getString("MMClob_MMBlob.0", params)); //$NON-NLS-1$
        } else if (pos > length()) {
            return null;
        }
        pos = pos - 1;
        if (length < 0) {
            Object[] params = new Object[] {new Integer( length)};
            throw new SQLException(CorePlugin.Util.getString("MMClob_MMBlob.1", params)); //$NON-NLS-1$
        } else if ((pos+length) > length()) {
            length = (int)(length()-pos);
        }
        Reader in = getCharacterStream();
        try {
	        try {
	        	long skipped = 0;
	        	while (pos > 0) {
	        		skipped = in.skip(pos);
	        		pos -= skipped;
	        	}
	        	return new String(ObjectConverterUtil.convertToCharArray(in, length));
	        } finally {
	        	in.close();
	        } 
        } catch (IOException e) {
        	throw new SQLException(e);
        }
    }

    /**
     * Returns the number of characters in the <code>CLOB</code> value
     * designated by this <code>Clob</code> object.
     * @return length of the <code>CLOB</code> in characters
     */
    public long length() throws SQLException {
    	if (len == -1) {
    		long length = 0;
    		Reader r = new BufferedReader(getCharacterStream());
    		try {
				while (r.read() != -1) {
					length++;
				}
			} catch (IOException e) {
				throw new SQLException(e);
			} finally {
				try {
					r.close();
				} catch (IOException e) {
				}
			}
    		this.len = length;
    	}
        return len;
    }

    /**
     * Determines the character position at which the specified
     * <code>Clob</code> object <code>searchstr</code> appears in this
     * <code>Clob</code> object.  The search begins at position
     * <code>start</code>.
     * @param searchstr the <code>Clob</code> object for which to search
     * @param start the position at which to begin searching; the first
     *              position is 1
     * @return the position at which the <code>Clob</code> object appears,
     * else -1; the first position is 1
     */
    public long position(final Clob searchstr, long start) throws SQLException {
        if (searchstr == null) {
            return -1;
        }
        
        return LobSearchUtil.position(new ClobStreamProvider(searchstr), searchstr.length(), new ClobStreamProvider(this), this.length(), start, 2);
    }
    
    /**
    * Determines the character position at which the specified substring
    * <code>searchstr</code> appears in the SQL <code>CLOB</code> value
    * represented by this <code>Clob</code> object.  The search
    * begins at position <code>start</code>.
    * @param searchstr the substring for which to search
    * @param start the position at which to begin searching; the first position
    *              is 1
    * @return the position at which the substring appears, else -1; the first
    *         position is 1
    * @exception SQLException if there is an error accessing the
    * <code>CLOB</code> value
    */
    public long position(String searchstr, long start) throws SQLException {
    	if (searchstr == null) {
            return -1;
        }
    	return position(new ClobImpl(searchstr.toCharArray()), start);
    }
    	    
	public Reader getCharacterStream(long arg0, long arg1) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public OutputStream setAsciiStream(long arg0) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public Writer setCharacterStream(long arg0) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public int setString(long arg0, String arg1) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public int setString(long arg0, String arg1, int arg2, int arg3)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void truncate(long arg0) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public static Clob createClob(char[] chars) {
		return new ClobImpl(chars);
	}

}
