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

package com.metamatrix.connector.xml.base;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.Long;

import com.metamatrix.connector.api.ConnectorException;

public class FileBackedValueReference implements LargeTextValueReference {
	private RandomAccessFile file;
	private long length;
    private FileLifeManager fileLifeManager;

	protected FileBackedValueReference(FileLifeManager fileLifeManager) throws IOException
	{
        this.fileLifeManager = fileLifeManager;
		this.file = fileLifeManager.createRandomAccessFile();
        length = charCountFromByteCount(this.file.length());
	}

	public Object getValue()
	{
    	return file;
	}
	public long getSize()
	{
		return length;
	}
	
	public String getContentAsString() throws ConnectorException
	{
		try {
			Long len = new Long(this.file.length());
			byte[] bytes = new byte[len.intValue()];
			int count = file.read(bytes);
			String str = new String(bytes, 0, count, getEncoding());
			return str;
		}
        catch (IOException e) {
            throw new ConnectorException(e);
        }
        catch (RuntimeException e) {
            throw new ConnectorException(e);
        }
		
	}
	
	
	public boolean isBinary()
	{
		return false;
	}

	/**
	 * @return Returns the encoding.
	 */
	public static String getEncoding() {
		return encoding;
	}

	// The encoding used to store the text in a file. A fix width encoding is used to
	// allow for easier random access at the expense of disk space (and potentially
	// disk access time)
	private static final String encoding = "UTF-16BE";
	private static final long charCountFromByteCount(long byteCount) throws IOException {
		return byteCount / 2;
	}
	private static final long byteCountFromCharCount(long charCount) {
		return charCount * 2;
	}
	private static final int charCountFromByteCount(int byteCount) throws IOException {
		return (int)charCountFromByteCount((long)byteCount);
	}
	private static final int byteCountFromCharCount(int charCount) {
		return (int)byteCountFromCharCount((long)charCount);
	}
}
