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
package org.teiid.translator;

import java.io.File;

import javax.resource.ResourceException;
import javax.resource.cci.Connection;

import org.teiid.core.util.FileUtils;

/**
 * Simple {@link Connection} interface for the filesystem
 */
public interface FileConnection extends Connection {
	
	/**
	 * Gets the file at the given path.  This file may not exist, but can be used to create/save a new file.
	 * @param path
	 * @return
	 */
	File getFile(String path) throws ResourceException;
	
	public static class Util {
		
		/**
		 * Gets the file or files, if the path is a directory, at the given path.  The path may include
		 * a trailing extension wildcard, such as foo/bar/*.txt to return only txt files at the given path.
		 * Note the path can only refer to a single directory - directories are not recursively scanned.
		 * @param path
		 * @return
		 */
		public static File[] getFiles(String location, FileConnection fc) throws ResourceException {
			File datafile = fc.getFile(location);
	        
	        if (datafile.isDirectory()) {
	        	return datafile.listFiles();
	        }
	        
	        String fname = datafile.getName();
	        String ext = FileUtils.getExtension(fname);
	        File parentDir = datafile.getParentFile();
	        
	        // determine if the wild card is used to indicate all files
	        // of the specified extension
	        if (ext != null && "*".equals(FileUtils.getBaseFileNameWithoutExtension(fname))) { //$NON-NLS-1$            
	            return FileUtils.findAllFilesInDirectoryHavingExtension(parentDir.getAbsolutePath(), "." + ext); //$NON-NLS-1$
	        }
	        if (!datafile.exists()) {
	        	return null;
	        }
	        return new File[] {datafile};
		}
		
	}
	
}
