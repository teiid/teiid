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

package com.metamatrix.modeler.core.index;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import com.metamatrix.internal.core.index.Index;

/**
 * IndexSelector
 */
public interface IndexSelector {

    /**
     * Return the array of {@link com.metamatrix.internal.core.index.IIndex} 
     * instances to use
     * @return array of indexes
     * @throws IOException if errors are encountered obtaining the index file
     */
    Index[] getIndexes() throws IOException;

    /**
     * Get the relative paths to files in the vdb.
     * @return The array of paths to files in the vdb
     * @since 4.2
     */
    String[] getFilePaths();

    /**
     * Return the collection of strings that represent contents of the files at 
     * paths relative to the directory location of the index files.
     * @param paths collection of paths for files
     * @return collection of contents of files at given locations as strings
     */
    List getFileContentsAsString(List paths);

    /**
     * Return a strings that represent contents of the file at 
     * paths relative to the directory location of the index files.
     * @param path The path to the file
     * @return Contents of the file at the given location as a string 
     */
    String getFileContentAsString(String path);

    /**
     * Return a InputStream the contents of the file at 
     * path relative to the directory location of the index files.
     * @param path The path to the file
     * @param tokens The tokens found in the file that need to be replaced
     * @param tokenReplacements The strings used to replace tokens in the file
     * @return contents of file at given location as InputStream
     */
    InputStream getFileContent(String path, String[] tokens, String[] tokenRelacements);

    /**
     * Return the file at the given relative path in the vdb. 
     * @param path The path to the file
     */    
    File getFile(String path);

    /**
     * Get the length of the file after token replacement if any is compleate. 
     * @param path The path to the file
     * @return length of file at given location
     */
    long getFileSize(String path);

    /**
     * Return a InputStream the contents of the file at 
     * path relative to the directory location of the index files.
     * @param path The path to the file
     * @return contents of file at given location as InputStream
     */
    InputStream getFileContent(String path);
    
    /**
     * Return boolean indicating if indexes on this selector are good to use. Applications
     * using the selector should mark it invalid of files backing the indexes are somehow
     * getting deleted or currupted.  
     * @return true if the indexes on the selector 
     * @since 4.2
     */
    boolean isValid();
    
    /**
     * Set boolean indicating if indexes on this selector are good to use. Applications
     * using the selector should mark it invalid of files backing the indexes are somehow
     * getting deleted or currupted.  
     * @param true if the indexes on the selector 
     * @since 4.2
     */
    void setValid(boolean valid);    

}