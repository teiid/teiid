/*
 * Copyright © 2000-2005 MetaMatrix, Inc.  All rights reserved.
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
