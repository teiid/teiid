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

package org.teiid.resource.adapter.file;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.regex.Pattern;

import javax.resource.ResourceException;

import org.teiid.resource.spi.BasicConnection;
import org.teiid.translator.FileConnection;


/**
 * TODO: consider using VFS 
 */
public class FileConnectionImpl extends BasicConnection implements FileConnection {
	
	private File parentDirectory;
	private Map<String, String> fileMapping;
	private boolean allowParentPaths;
	private static final Pattern parentRef = Pattern.compile("(^\\.\\.(\\\\{2}|/)?.*)|((\\\\{2}|/)\\.\\.)"); //$NON-NLS-1$
	
	public FileConnectionImpl(String parentDirectory, Map<String, String> fileMapping, boolean allowParentPaths) {
		this.parentDirectory = new File(parentDirectory);
		if (fileMapping == null) {
			fileMapping = Collections.emptyMap();
		}
		this.fileMapping = fileMapping;
		this.allowParentPaths = allowParentPaths;
	}
	
	@Override
	public File getFile(String path) throws ResourceException {
    	if (path == null) {
    		return this.parentDirectory;
        }
		String altPath = fileMapping.get(path);
		if (altPath != null) {
			path = altPath;
		}
    	if (!allowParentPaths && parentRef.matcher(path).matches()) {	
			throw new ResourceException(FileManagedConnectionFactory.UTIL.getString("parentpath_not_allowed", path)); //$NON-NLS-1$
		}
		return new File(parentDirectory, path);	
    }

	@Override
	public void close() throws ResourceException {
		
	}
	
}
