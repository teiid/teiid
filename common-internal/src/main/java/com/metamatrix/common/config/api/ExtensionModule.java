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

package com.metamatrix.common.config.api;


/** 
 * Defines an extension module as an object
 * @since 4.3
 */
public interface ExtensionModule extends ComponentDefn {
    /**
     * The name of the JAR file type of extension
     * module - this is the only type of
     * extension module that can be searched
     * for Class objects
     */
    public static final String JAR_FILE_TYPE = "JAR File"; //$NON-NLS-1$

    /**
     * The name of the Metadata Keyword type of
     * extension module.
     */
    public static final String METADATA_KEYWORD_TYPE = "Metadata Keyword"; //$NON-NLS-1$

    /**
     * The name of the Metamodel Extension type of
     * extension module.
     */
    public static final String METAMODEL_EXTENSION_TYPE = "Metamodel Extension"; //$NON-NLS-1$

    /**
     * The name of the Function Definition type of
     * extension module.
     */
    public static final String FUNCTION_DEFINITION_TYPE = "Function Definition"; //$NON-NLS-1$
    
    /**
     * The name of the Configuration Model type of
     * extension module.
     */
    public static final String CONFIGURATION_MODEL_TYPE = "Configuration Model"; //$NON-NLS-1$
    
    /**
     * The name of the VDB File type of extension module.
     */
    public static final String VDB_FILE_TYPE = "VDB File"; //$NON-NLS-1$

    /**
     * The name of the Miscellaneous File type of extension module.
     */
    public static final String MISC_FILE_TYPE = "Miscellaneous Type"; //$NON-NLS-1$
    
    
    /**
     * @return description
     */
    public String getDescription();

    /**
     * @return byte array of file contents
     */
    public byte[] getFileContents();
    
    /**
     * @return String of the Module Type for this Extension Module
     */
    public String getModuleType();
        
}
