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

package com.metamatrix.common.extensionmodule;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import com.metamatrix.common.extensionmodule.exception.InvalidExtensionModuleTypeException;

/**
 * <p>This names of extension module types which are known and supported.</p>
 *
 * <p>If all that is needed is the Collection of all known module types,
 * without needing to programatically distinguish between them, use the
 * {@link ExtensionModuleManager#getSourceTypes getSourceTypes} method of
 * {@link ExtensionModuleManager}:
 * <pre><code>
 * ExtensionModuleManager.{@link ExtensionModuleManager#getInstance getInstance()}.{@link ExtensionModuleManager#getSourceTypes getSourceTypes}
 * </code></pre></p>
 */
public class ExtensionModuleTypes{

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
     * The name of the miscellaneous File type of extension module.
     */
    public static final String MISC_FILE_TYPE = "Miscellaneous Type"; //$NON-NLS-1$
    

    /**
     * The Collection of all known extension module
     * type names
     */
    static final Collection ALL_TYPES;

    static {
        ArrayList types = new ArrayList(5);
        types.add(JAR_FILE_TYPE);
        types.add(METADATA_KEYWORD_TYPE);
        types.add(METAMODEL_EXTENSION_TYPE);
        types.add(FUNCTION_DEFINITION_TYPE);
        types.add(CONFIGURATION_MODEL_TYPE);        
        types.add(VDB_FILE_TYPE);   
        types.add(MISC_FILE_TYPE);                
        ALL_TYPES = Collections.unmodifiableList(types);
    }

    /**
     * Indicates whether the type name in question is a valid
     * type
     * @param typeName name of type in question
     * @return whether the parameter represents a valid type
     */
    public static boolean isValidType(String typeName){
        return ALL_TYPES.contains(typeName);
    }

    /**
     * Calls to {@link #isValidType} and throws an
     * InvalidExtensionTypeException if that method returns
     * false.
     * @param typeName name of type in question
     * @throws InvalidExtensionTypeException if typeName is not a valid
     * type
     */
    public static void checkTypeIsValid(String typeName)
    throws InvalidExtensionModuleTypeException{
        if (!isValidType(typeName)){
            throw new InvalidExtensionModuleTypeException(typeName);
        }
    }
}

