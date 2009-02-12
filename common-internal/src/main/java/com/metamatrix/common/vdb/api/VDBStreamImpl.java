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

package com.metamatrix.common.vdb.api;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import com.metamatrix.common.CommonPlugin;

public class VDBStreamImpl implements VDBStream {

	File file;
	byte[] contents;
	
	public VDBStreamImpl(File file){
		this.file = file;
	}
	
	public VDBStreamImpl(byte[] contents) {
		this.contents = contents;
	}
	
	public File getFile() {
		if (this.file != null && file.exists()) {
			return file;
		}
		throw new UnsupportedOperationException(CommonPlugin.Util.getString("VDBArchiveSotRef.File_doesnt_exist", file)); //$NON-NLS-1$
	}

	public InputStream getInputStream() {
		if (this.file != null && file.exists()) {
			try {
				return new FileInputStream(this.file);
			} catch (FileNotFoundException e) {
				//ignore.
			}
		}
		
		if (contents != null && contents.length > 0) {
			return new ByteArrayInputStream(this.contents);
		}		
		return null;
	}

	public byte[] toByteArray() {
		if (contents != null && contents.length > 0) {
			return this.contents;
		}
		throw new UnsupportedOperationException("Contents not found"); //$NON-NLS-1$
	}

}
