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

package com.metamatrix.admin.objects;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.metamatrix.admin.AdminPlugin;
import com.metamatrix.admin.api.exception.AdminComponentException;
import com.metamatrix.admin.api.exception.AdminException;
import com.metamatrix.admin.api.exception.AdminProcessingException;
import com.metamatrix.admin.api.objects.AdminOptions;
import com.metamatrix.admin.api.objects.ScriptsContainer;
import com.metamatrix.core.util.FileUtils;


/** 
 * @since 4.3
 */
public class MMScriptsContainer implements
                               ScriptsContainer {
    
    // <String> fileName => <byte[]> file contents
    private Map fileMap;
    
    /** 
     * 
     * @since 4.3
     */
    public MMScriptsContainer() {
        super();
        fileMap = new HashMap(10);
    }

    /** 
     * @see com.metamatrix.admin.api.objects.ScriptsContainer#getFileNames()
     * @since 4.3
     */
    public Collection getFileNames() {
        return new ArrayList(this.fileMap.keySet());
    }

    /** 
     * @see com.metamatrix.admin.api.objects.ScriptsContainer#saveAllToDirectory(java.lang.String, AdminOptions)
     * @since 4.3
     */
    public void saveAllToDirectory(String directoryLocation, AdminOptions options) throws AdminException {
        String fileSeparator = File.separator;
        String path = (directoryLocation.endsWith(fileSeparator) ? directoryLocation : directoryLocation + fileSeparator);
        
        // Check that the directory exists and is writable
        File directory = new File(path);
        if ( ! directory.isDirectory() ) {
            Object[] params = new Object[] {directoryLocation};
            throw new AdminProcessingException(AdminPlugin.Util.getString("MMScriptsContainer.unable_to_locate_directory", params)); //$NON-NLS-1$
        }
        if ( ! directory.canWrite() ) {
            Object[] params = new Object[] {directoryLocation};
            throw new AdminProcessingException(AdminPlugin.Util.getString("MMScriptsContainer.unable_to_write_to_directory", params)); //$NON-NLS-1$
        }
        
        // First check that we will not overwrite any of the files
        Collection existingFiles = null;
        for ( Iterator fileItr = fileMap.keySet().iterator(); fileItr.hasNext();) {
            String fileName = path + (String) fileItr.next();
            File aFile = new File(fileName);
            
            if ( aFile.exists() ) {
                if ( existingFiles == null ) {
                    existingFiles = new ArrayList();
                }
                existingFiles.add(fileName);
            }
        }
        if ( existingFiles != null && (options == null || options.containsOption(AdminOptions.OnConflict.EXCEPTION)) ) {
            Object[] params = new Object[] {existingFiles.toString()};
            throw new AdminProcessingException(AdminPlugin.Util.getString("MMScriptsContainer.files_exist", params)); //$NON-NLS-1$
        }
        
        if ( existingFiles == null || options.containsOption(AdminOptions.OnConflict.OVERWRITE) ) {
            // Now write each file
            for (Iterator fileItr = fileMap.keySet().iterator(); fileItr.hasNext();) {
                String fileName = (String)fileItr.next();
                File target = new File(path + fileName);

                try {
                    target.createNewFile();
                    byte[] fileContents = (byte[])fileMap.get(fileName);
                    FileUtils.write(fileContents, target);
                } catch (IOException err) {
                    Object[] params = new Object[] {
                        fileName
                    };
                    throw new AdminProcessingException(AdminPlugin.Util.getString("MMScriptsContainer.error_writing_file", params)); //$NON-NLS-1$
                }
            }
        } // if
    }

    
//=================================================================================================
//  SETTERS ARE NOT IN THE PUBLIIC INTERFACE
//=================================================================================================
    
    /**
     * Add file contents by name.
     * 
     * @param fileName  - required
     * @param fileContents - required
     * @throws AdminComponentException if one of the required args are null or empty.
     */
    public void addFile(String fileName, byte[] fileContents) throws AdminComponentException {
        if ( fileName == null || fileName.length() == 0 ) {
            throw new AdminComponentException(AdminPlugin.Util.getString("MMScriptsContainer.fileName_was_null")); //$NON-NLS-1$
        }
        if ( fileContents == null || fileContents.length == 0 ) {
            throw new AdminComponentException(AdminPlugin.Util.getString("MMScriptsContainer.fileContents_was_null")); //$NON-NLS-1$
        }
        this.fileMap.put(fileName, fileContents);
    }
}
