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
package com.metamatrix.installer.anttask.extensions;

import java.io.File;

public interface ExtensionConstants {

/**
* The extension directory to install from
*/
  public static final String EXTENSION_DIR = "extensions";

  /**
  * Provides the relative location to the extension files
  */
	public static final String RELATIVE_DIR_PATH = EXTENSION_DIR + File.separator;

    /**
    * The path location of the extension jars to load.
    */
    public static final String EXTENSION_DIR_PATH = "extension.jar.path";
    
    /**
    * The file type of the file
    */
    public static final String EXTENSION_FILE_TYPE= "extension.jar.filetype";
    
    /**
    * The file name to assign to the file
    */
    public static final String EXTENSION_FILE_NAME= "extension.jar.filename";


    /**
    * An OPTIONAL property for setting the position the file should be added
    */
    public static final String EXTENSION_FILE_POS = "extension.jar.position";

    /**
    * An OPTIONAL property for setting the description
    */
    public static final String EXTENSION_FILE_DESC = "extension.jar.description";
    
    /**
    * The name to assign to the extension
    */
    public static final String EXTENSION_NAME= "extension.name";
    
} 
