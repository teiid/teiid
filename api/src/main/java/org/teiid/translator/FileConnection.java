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
import java.io.FilenameFilter;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;

import javax.resource.ResourceException;
import javax.resource.cci.Connection;

import org.teiid.connector.DataPlugin;

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
		 * Gets the file or files, if the path is a directory, at the given path.  
		 * Note the path can only refer to a single directory - directories are not recursively scanned.
		 * @param exceptionIfFileNotFound 
		 * @param path
		 * @return
		 */
		public static File[] getFiles(String location, FileConnection fc, boolean exceptionIfFileNotFound) throws ResourceException {
			File datafile = fc.getFile(location);
	        
	        if (datafile.isDirectory()) {
	        	return datafile.listFiles();
	        }
	        
	        if (datafile.exists()) {
	        	return new File[] {datafile};
	        }
	        
	        File parentDir = datafile.getParentFile();
	        
	        if (parentDir == null || !parentDir.exists()) {
	        	if (exceptionIfFileNotFound) {
					throw new ResourceException(DataPlugin.Util.gs("file_not_found", location)); //$NON-NLS-1$
	        	}
	        	return null;
	        }
	        
	        if (location.contains("*")) { //$NON-NLS-1$
	        	//for backwards compatibility support any wildcard, but no escapes or other glob searches
	        	location = location.replaceAll("\\\\", "\\\\\\\\"); //$NON-NLS-1$ //$NON-NLS-2$ 
	        	location = location.replaceAll("\\?", "\\\\?"); //$NON-NLS-1$ //$NON-NLS-2$
	        	location = location.replaceAll("\\[", "\\\\["); //$NON-NLS-1$ //$NON-NLS-2$
	        	location = location.replaceAll("\\{", "\\\\{"); //$NON-NLS-1$ //$NON-NLS-2$
	        	
		        final PathMatcher matcher =
		        	    FileSystems.getDefault().getPathMatcher("glob:" + location); //$NON-NLS-1$

		        FilenameFilter fileFilter = new FilenameFilter() {
		        	
		        	@Override
		        	public boolean accept(File dir, String name) {
		        		return matcher.matches(FileSystems.getDefault().getPath(name));
		        	}
		        };

		        return parentDir.listFiles(fileFilter);
	        }
	        
	        if (exceptionIfFileNotFound) {
				throw new ResourceException(DataPlugin.Util.gs("file_not_found", location)); //$NON-NLS-1$
	        }
	        
        	return null;
		}
		
	}
	
}
