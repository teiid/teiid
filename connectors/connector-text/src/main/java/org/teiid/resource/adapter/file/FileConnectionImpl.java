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

import javax.resource.ResourceException;

import org.teiid.core.util.FileUtils;
import org.teiid.resource.adapter.FileConnection;
import org.teiid.resource.spi.BasicConnection;


/**
 * TODO: consider using VFS 
 */
public class FileConnectionImpl extends BasicConnection implements FileConnection {
	
	private File parentDirectory;
	
	public FileConnectionImpl(String parentDirectory) {
		this.parentDirectory = new File(parentDirectory);
	}

    public File[] getFiles(String location) {
        if (location == null) return null;
        
        File datafile = new File(parentDirectory, location);
        
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

	@Override
	public void close() throws ResourceException {
		
	}
	
}
