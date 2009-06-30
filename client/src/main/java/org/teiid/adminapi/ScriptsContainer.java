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

package org.teiid.adminapi;

import java.io.Serializable;
import java.util.Collection;




/** 
 * Contains all information nessecary to save the scripts that will be run
 * by a MetaMatrix utility to load or refresh the data resident in a
 * Materialized View. 
 * <p>
 * Users can get the contents of each file and save each with the
 * filename associated with it or, more easily, users can save all files
 * to a local folder.  (See {@link #saveAllToDirectory})</p>
 * <p>
 * @since 4.3
 */
public interface ScriptsContainer extends Serializable {
    
    /**
     * Get the file names of all scripts in this container.
     *  
     * @return The <code>Collection</code> of <code>String</code>
     * file names of all scripts in this container.  This
     * collection may be empty if an error has occured.
     * @since 4.3
     */
    Collection getFileNames();
    
    /**
     * Save all of the scripts contained to the specified local
     * directory.
     *  
     * @param directoryLocation the directory location to save
     * the contained scripts.  Must be accessable from where
     * this code is running.
     * @param options Specify whether to {@link AdminOptions.OnConflict#OVERWRITE}
     * or {@link AdminOptions.OnConflict#EXCEPTION} (default) when script files
     * exist in the <code>directoryLocation</code>.
     * @throws AdminException if there's an error saving the files.
     * @since 4.3
     */
    void saveAllToDirectory(String directoryLocation, AdminOptions options) throws AdminException;
}
