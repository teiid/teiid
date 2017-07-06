/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.translator;

import java.io.File;
import java.io.FileFilter;
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

		        FileFilter fileFilter = new FileFilter() {
		        	
		        	@Override
		        	public boolean accept(File pathname) {
		        		return pathname.isFile() && matcher.matches(FileSystems.getDefault().getPath(pathname.getName()));
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
