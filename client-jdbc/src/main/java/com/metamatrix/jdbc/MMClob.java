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

package com.metamatrix.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.Charset;
import java.sql.Clob;
import java.sql.SQLException;

import javax.sql.rowset.serial.SerialClob;

import com.metamatrix.common.lob.LobChunkInputStream;
import com.metamatrix.common.lob.ReaderInputStream;
import com.metamatrix.common.types.ClobType;
import com.metamatrix.common.types.Streamable;
import com.metamatrix.common.util.SqlUtil;
import com.metamatrix.dqp.client.impl.StreamingLobChunckProducer;

/**
 * This object holds a chunk of char data and implements the JDBC Clob interface.
 * This object presents a streaming interface and provides a way to access partial
 * of the Clob data. Connectors can use this object when dealing with large
 * objects.
 */
public class MMClob implements Clob {
    
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
				throw MMSQLException.create(e);
			}
		}
	}

	private final StreamingLobChunckProducer.Factory lobChunckFactory;
	private final ClobType clob;
	
    public static Clob newInstance(StreamingLobChunckProducer.Factory lobChunckFactory, ClobType clob) throws SQLException {
    	if (!Boolean.getBoolean(Streamable.FORCE_STREAMING)) {
            Clob sourceClob = clob.getSourceClob();
            if (sourceClob != null) {
            	return sourceClob;
            }
        }
        return new MMClob(lobChunckFactory, clob);        
    }
    
    public MMClob(StreamingLobChunckProducer.Factory lobChunkFactory, ClobType clob) throws SQLException {
    	this.lobChunckFactory = lobChunkFactory;
    	this.clob = clob;
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
     * Gets the <code>CLOB</code> value designated by this <code>Clob</code>
     * object as a Unicode stream.
     * @return a Unicode stream containing the <code>CLOB</code> data
     * @exception SQLException if there is an error accessing the
     * <code>CLOB</code> value
     */
    public Reader getCharacterStream() throws SQLException {
        return new LobChunkInputStream(lobChunckFactory.getLobChunkProducer()).getUTF16Reader();
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
            throw new SQLException(JDBCPlugin.Util.getString("MMClob_MMBlob.0", params)); //$NON-NLS-1$
        } else if (pos > length()) {
            return null;
        }
        pos = pos - 1;
        if (length < 0) {
            Object[] params = new Object[] {new Integer( length)};
            throw new SQLException(JDBCPlugin.Util.getString("MMClob_MMBlob.1", params)); //$NON-NLS-1$
        } else if ((pos+length) > length()) {
            length = (int)(length()-pos);
        }
        char[] dataCopy = new char[length];
        Reader in = getCharacterStream();
        try {
	        try {
	        	in.skip(pos);
	        	in.read(dataCopy);
	        } finally {
	        	in.close();
	        } 
        } catch (IOException e) {
        	throw MMSQLException.create(e);
        }
        return new String(dataCopy);
    }

    /**
     * Returns the number of characters in the <code>CLOB</code> value
     * designated by this <code>Clob</code> object.
     * @return length of the <code>CLOB</code> in characters
     */
    public long length() throws SQLException {
        return clob.length();
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
    	return position(new SerialClob(searchstr.toCharArray()), start);
    }
    	    
	public void free() throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
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

}
