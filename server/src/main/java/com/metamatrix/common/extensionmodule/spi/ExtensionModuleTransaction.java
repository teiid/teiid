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

package com.metamatrix.common.extensionmodule.spi;

import java.util.Collection;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.connection.TransactionInterface;
import com.metamatrix.common.extensionmodule.ExtensionModuleDescriptor;
import com.metamatrix.common.extensionmodule.exception.*;

public interface ExtensionModuleTransaction extends TransactionInterface {

    /**
     * Adds an extension module to the end of the list of modules.  The
     * type will be checked to be a known and supported type.
     * @param principalName name of principal requesting this addition
     * @param type one of the known types of extension file
     * @param sourceName name (e.g. filename) of extension module
     * @param source actual contents of module
	 * @param checksum Checksum of file contents
     * @param description (optional) description of the extension module
     * @param enabled indicates whether each extension module is enabled for
     * being searched or not (for convenience, a module can be disabled
     * without being removed)
     * @return ExtensionModuleDescriptor describing the newly-added
     * extension module
     * @throws DuplicateExtensionModuleException if an extension module
     * with the same sourceName already exists
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    ExtensionModuleDescriptor addSource(String principalName, String type, String sourceName, byte[] source, long checksum, String description, boolean enabled)
    throws DuplicateExtensionModuleException, MetaMatrixComponentException;

    /**
     * Deletes a module from the list of modules
     * @param principalName name of principal requesting this addition
     * @param sourceName name (e.g. filename) of extension module
     * @throws ExtensionModuleNotFoundException if no extension module with
     * name sourceName can be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    void removeSource(String principalName, String sourceName)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException;

    /**
     * Returns List (of Strings) of all extension module names, in order of
     * their search ordering (empty List if there are none)
     * @return List (of Strings) of all extension module names, in order of
     * their search ordering (empty List if there are none)
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    List getSourceNames() throws MetaMatrixComponentException;

    /**
     * Returns List of ExtensionModuleDescriptor objects, in order
     * of their search ordering, or empty List if there are none
     * @return List of ExtensionModuleDescriptor objects, in order
     * of their search ordering, or empty List if there are none
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    List getSourceDescriptors() throws MetaMatrixComponentException;

    /**
     * Returns List of ExtensionModuleDescriptor objects of indicated type,
     * in order of their search ordering.  The type will be checked to be
     * a known and supported type. (return empty List if there are none)
     * @param type one of the known types of extension file
     * @param includeDisabled if "false", only descriptors for <i>enabled</i>
     * extension modules will be returned; otherwise all modules will be.
     * @return List of ExtensionModuleDescriptor objects of indicated type,
     * in order of their search ordering, or empty List if there are none
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    List getSourceDescriptors(String type, boolean includeDisabled) throws MetaMatrixComponentException;

    /**
     * Returns the ExtensionModuleDescriptor object for the extension
     * module indicated by sourceName
     * @param sourceName name (e.g. filename) of extension module
     * @return the ExtensionModuleDescriptor object for the extension
     * module indicated by sourceName
     * @throws ExtensionModuleNotFoundException if no extension module with
     * name sourceName can be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    ExtensionModuleDescriptor getSourceDescriptor(String sourceName)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException;

    /**
     * Sets the positions in the search order of all modules (all modules
     * must be included or an ExtensionModuleOrderingException will be thrown)
     * The sourceNames List parameter should indicate the new desired order.
     * @param principalName name of principal requesting this addition
     * @param sourceNames Collection of String names of existing
     * extension modules whose search position is to be set
     * @throws ExtensionModuleOrderingException if the extension files could
     * not be ordered as requested because another administrator had
     * concurrently added or removed an extension file or files, or because
     * an indicated position is out of bounds.
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    void setSearchOrder(String principalName, List sourceNames)
    throws ExtensionModuleOrderingException, MetaMatrixComponentException;

    /**
     * Sets the "enabled" (for searching) property of all of the indicated
     * extension modules.
     * @param principalName name of principal requesting this addition
     * @param sourceNames Collection of String names of existing
     * extension modules whose "enabled" status is to be set
     * @param enabled indicates whether each extension module is enabled for
     * being searched or not (for convenience, a module can be disabled
     * without being removed)
     * @return updated List of ExtensionModuleDescriptor objects, in order
     * of their search ordering
     * @throws ExtensionModuleNotFoundException if no extension module with
     * name sourceName can be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    void setEnabled(String principalName, Collection sourceNames, boolean enabled)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException;

    /**
     * Retrieves an extension module in byte[] form
     * @param sourceName name (e.g. filename) of extension module
     * @return actual contents of module in byte[] array form
     * @throws ExtensionModuleNotFoundException if no extension module with
     * name sourceName can be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    byte[] getSource(String sourceName)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException;

    /**
     * Updates the indicated extension module
     * @param principalName name of principal requesting this addition
     * @param sourceName name (e.g. filename) of extension module
     * @param source actual contents of module
	 * @param checksum Checksum of file contents
     * @return ExtensionModuleDescriptor describing the newly-updated
     * extension module
     * @throws ExtensionModuleNotFoundException if no extension module with
     * name sourceName can be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    ExtensionModuleDescriptor setSource(String principalName, String sourceName, byte[] source, long checksum)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException;

    /**
     * Updates the indicated extension module's module name
     * @param principalName name of principal requesting this addition
     * @param sourceName name (e.g. filename) of extension module
     * @param newName new name for the module
     * @return ExtensionModuleDescriptor describing the newly-updated
     * extension module
     * @throws ExtensionModuleNotFoundException if no extension module with
     * name sourceName can be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    ExtensionModuleDescriptor setSourceName(String principalName, String sourceName, String newName)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException;

    /**
     * Updates the indicated extension module's description
     * @param principalName name of principal requesting this addition
     * @param sourceName name (e.g. filename) of extension module
     * @param description (optional) description of the extension module.
     * <code>null</code> can be passed in to indicate no description.
     * @return ExtensionModuleDescriptor describing the newly-updated
     * extension module
     * @throws ExtensionModuleNotFoundException if no extension module with
     * name sourceName can be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    ExtensionModuleDescriptor setSourceDescription(String principalName, String sourceName, String description)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException;
    
    /**
     * Indicates if an extension module name is already in used.
     * The method will return <code>true</code> if the source name is already used,
     * @param sourceName is the name of the extension module to check
     */
    boolean isNameInUse(String sourceName) throws MetaMatrixComponentException;
    
}

