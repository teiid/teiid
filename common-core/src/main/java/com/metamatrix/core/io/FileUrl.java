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

package com.metamatrix.core.io;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import com.metamatrix.core.util.StringUtil;

/**
 * This class allows for the original url of a 
 * URL object that was used to create a File object to be saved.
 * The File object is created from the URL by saving the InputStream
 * from the url off to a local file.
 */
public class FileUrl extends File {
	
	/*
	 * This is the original url of the InputStream that was
	 * used to create this File object. 
	 */
	private String originalUrlString = StringUtil.Constants.EMPTY_STRING;

	public FileUrl(String parent, String child) {
		super(parent, child);
	}

	public FileUrl(String pathname) {
		super(pathname);
	}

	public FileUrl(URI uri) {
		super(uri);
	}

	/**
	 * @return originalUrlString The original url used to create this File object
	 */
	public String getOriginalUrlString() {
		return originalUrlString;
	}

	/**
	 * @param originalUrlString
	 */
	public void setOriginalUrlString(String originalUrlString) {
		this.originalUrlString = originalUrlString;
	}

	/* (non-Javadoc)
	 * @see java.io.File#createTempFile(java.lang.String, java.lang.String, java.io.File)
	 */
	public static File createTempFile(String prefix, String suffix)
        throws IOException
    {
		FileUrl fileUrl = null;
		File file = File.createTempFile(prefix, suffix);
		
		fileUrl = new FileUrl(file.toURI());
		
		file = null;
		
		return fileUrl;
    }
	
}
