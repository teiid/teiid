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
import java.io.CharArrayReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Serializable;
import java.io.Writer;
import java.nio.charset.Charset;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;

import com.metamatrix.core.CorePlugin;

/**
 * This object holds a chunk of character data and implements the JDBC Clob interface. 
 * This object presents a streaming interface but actually encapsulates the entire clob 
 * object.  Connectors can construct this object when dealing with large objects.
 */
public class ClobImpl implements Clob, Serializable {

    public static final int DEFAULT_MAX_SIZE = 5000000;

    private char[] data;

    /**
     * Creates a MMClob object by using the CharacterStream of the 
     * <code>Clob</code> argument.
     * @param clob the Clob object to get the characters from.
     */
    public ClobImpl(Clob clob) throws SQLException {
        try {
            int length = (int) clob.length();
            data = new char[length];
            Reader reader = clob.getCharacterStream();
            int charsRead = 0;
            int start = 0;
            int bytesToRead = length;
            while ((charsRead = reader.read(data, start, bytesToRead)) != -1 && bytesToRead != 0) {
                start += charsRead;
                bytesToRead -= charsRead;
            }

        } catch (IOException ioe) {
            throw new SQLException(CorePlugin.Util.getString("ClobImpl.Failed_copy_clob",  ioe.getMessage())); //$NON-NLS-1$
        }
    }

    /**
     * Creates a MMClob object by copying the chars in <code>originalData</code>
     * @param originalData the array of chars to copy into this MMClob object.
     */
    public ClobImpl(char[] originalData) {
        data = new char[originalData.length];
        int originalDataStartPosition = 0;
        int dataStartPosition = 0;
        int charsToCopy = originalData.length;
        System.arraycopy(originalData, originalDataStartPosition, data, dataStartPosition, charsToCopy);
    }

    public ClobImpl(InputStream in, Charset charSet, int length) throws SQLException{
    	data = new char[length];
    	try {
			new InputStreamReader(in, charSet).read(data, 0, length);
		} catch (IOException e) {
			throw new SQLException(e.getMessage());
		}
    }
    
    public ClobImpl(Reader reader, int length) throws SQLException{
    	data = new char[length];
    	try {
    		reader.read(data, 0, length);
		} catch (IOException e) {
			throw new SQLException(e.getMessage());
		}
    }
    
    /**
     * Gets the <code>CLOB</code> value designated by this <code>Clob</code>
     * object as a stream of Ascii bytes.
     * @return an ascii stream containing the <code>CLOB</code> data
     * @exception SQLException if there is an error accessing the 
     * <code>CLOB</code> value
     */
    public InputStream getAsciiStream() throws SQLException {
        byte[] bytes = new String(data).getBytes(Charset.forName("US-ASCII")); //$NON-NLS-1$
        return new ByteArrayInputStream(bytes);
    }

    /**
     * Gets the <code>CLOB</code> value designated by this <code>Clob</code>
     * object as a Unicode stream.
     * @return a Unicode stream containing the <code>CLOB</code> data
     * @exception SQLException if there is an error accessing the 
     * <code>CLOB</code> value
     */
    public Reader getCharacterStream() throws SQLException {
        return new CharArrayReader(data);
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
     * @exception SQLException if there is an error accessing the
     * <code>CLOB</code> 
     */
    public String getSubString(long pos, int length) throws SQLException {

        if (pos < 1) {
            throw new SQLException(CorePlugin.Util.getString("ClobImpl.Invalid_substring_position",  new Long(pos))); //$NON-NLS-1$
        }

        if (pos > data.length) {
            return null;
        }

        if (length < 0) {
            throw new SQLException(CorePlugin.Util.getString("ClobImpl.Invalid_substring_length",  new Long(length))); //$NON-NLS-1$
        }

        if (length > data.length) {
            length = data.length;
        }

        pos--; //the API specifies that the first char is at pos = 1
        return new String(data, (int) pos, length);
    }

    /**
     * Returns the number of characters 
     * in the <code>CLOB</code> value
     * designated by this <code>Clob</code> object.
     * @return length of the <code>CLOB</code> in characters
     * @exception SQLException if there is an error accessing the
     * length of the <code>CLOB</code>
     */
    public long length() throws SQLException {
        return data.length;
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
     * @exception SQLException if there is an error accessing the 
     * <code>CLOB</code> value
     */
    public long position(Clob searchstr, long start) throws SQLException {

        if (searchstr == null || start > data.length) {
            return -1;
        }
        int length = (int) searchstr.length();
        String searchString = searchstr.getSubString(1, length);
        return position(searchString, start);
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
        if (start < 1) {
            throw new SQLException(CorePlugin.Util.getString("ClobImpl.Invalid_start_position",  new Long(start))); //$NON-NLS-1$
        }
        if (searchstr == null || start > data.length) {
            return -1;
        }
        String str = new String(data);
        start--;
        int position = str.indexOf(searchstr, (int) start);
        if (position != -1)
            position++;
        return position;
    }

    /**
     * Compares two MMClob objects for equality.
     * @return True if equal
     * @see java.lang.Object#equals(Object)
     */
    public boolean equals(Object obj) {
        if (obj == null || data == null) {
            return false;
        }

        if (obj == this) {
            return true;
        }

        if (!(obj instanceof ClobImpl)) {
            return false;
        }

        ClobImpl clob = (ClobImpl) obj;
        
        return Arrays.equals(data, clob.data);
    }

    /** 
     * Returns a String that is a coma delimited list of up to
     * the first 5 and last 5 chars of the Clob.
     * @return the String object convenient for displaying this Clob object
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
            pre += data[i] + ","; //$NON-NLS-1$
        }

        for (int i = endTruncate; i < lastByte; i++) {
            post += "," + data[i]; //$NON-NLS-1$
        }

        post += "}"; //$NON-NLS-1$

        return "Length:" + data.length + ":" + pre + "..." + post; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

	/**
	 * @see java.sql.Clob#setString(long, java.lang.String)
	 */
	public int setString(long pos, String str) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/**
	 * @see java.sql.Clob#setString(long, java.lang.String, int, int)
	 */
	public int setString(long pos, String str, int offset, int len) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/**
	 * @see java.sql.Clob#setAsciiStream(long)
	 */
	public OutputStream setAsciiStream(long pos) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/**
	 * @see java.sql.Clob#setCharacterStream(long)
	 */
	public Writer setCharacterStream(long pos) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/**
	 * @see java.sql.Clob#truncate(long)
	 */
	public void truncate(long len) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void free() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Reader getCharacterStream(long arg0, long arg1) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

}
