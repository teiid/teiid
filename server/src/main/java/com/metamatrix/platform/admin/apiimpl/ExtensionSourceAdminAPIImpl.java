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

package com.metamatrix.platform.admin.apiimpl;

import java.util.Collection;
import java.util.List;

import org.teiid.adminapi.AdminRoles;

import com.metamatrix.admin.RolesAllowed;
import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.common.extensionmodule.ExtensionModuleDescriptor;
import com.metamatrix.common.extensionmodule.ExtensionModuleManager;
import com.metamatrix.common.extensionmodule.exception.DuplicateExtensionModuleException;
import com.metamatrix.common.extensionmodule.exception.ExtensionModuleNotFoundException;
import com.metamatrix.common.extensionmodule.exception.ExtensionModuleOrderingException;
import com.metamatrix.common.extensionmodule.exception.InvalidExtensionModuleTypeException;
import com.metamatrix.platform.admin.api.ExtensionSourceAdminAPI;
import com.metamatrix.platform.security.api.SessionToken;

@RolesAllowed(value=AdminRoles.RoleName.ADMIN_READONLY)
public class ExtensionSourceAdminAPIImpl implements ExtensionSourceAdminAPI {

    private ExtensionModuleManager extensionSourceManager;
    private static ExtensionSourceAdminAPI extensionSourceAdminAPI;
    
    /**
     * ctor
     */
    private ExtensionSourceAdminAPIImpl() {
        
    }

    public synchronized static ExtensionSourceAdminAPI getInstance() {
        if (extensionSourceAdminAPI == null) {
            extensionSourceAdminAPI = new ExtensionSourceAdminAPIImpl();
        }
        return extensionSourceAdminAPI;
    }
    
    /**
     * <p>Lazy getter for ExtensionSourceManager singleton instance - if this
     * object's ExtensionSourceManager is null, it will call
     * ExtensionSourceManager.getInstance()</p>
     *
     * <p>Keeping the instance reference here avoids the synchronization overhead of
     * repeatedly calling ExtensionSourceManager.getInstance()</p>
     */
    private ExtensionModuleManager getExtensionSourceManager(){
        if (extensionSourceManager == null){
            extensionSourceManager = ExtensionModuleManager.getInstance();
        }
        return extensionSourceManager;
    }

    /**
     * Adds an extension source to the end of the list of sources
     * @param type one of the known types of extension file
     * @param sourceName name (e.g. filename) of extension source
     * @param source actual contents of source
     * @param description (optional) description of the extension source
     * @param enabled indicates whether each extension source is enabled for
     * being searched or not (for convenience, a source can be disabled
     * without being removed)
     * @return ExtensionSourceDescriptor describing the newly-added
     * extension source
     * @throws InvalidSessionException if there is not a valid administrative session
     * @throws AuthorizationException if the administrator does not have privileges to use this method
     * @throws DuplicateExtensionSourceException if an extension source
     * with the same sourceName already exists
     * @throws InvalidExtensionTypeException if the indicated type is not one
     * of the currently-supported extension source types
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    @RolesAllowed(value=AdminRoles.RoleName.ADMIN_SYSTEM)
    public ExtensionModuleDescriptor addSource(String type, String sourceName, byte[] source, String description, boolean enabled)
    throws InvalidSessionException, AuthorizationException, DuplicateExtensionModuleException, InvalidExtensionModuleTypeException, MetaMatrixComponentException{
        SessionToken token = AdminAPIHelper.validateSession();
        return getExtensionSourceManager().addSource(token.getUsername(), type, sourceName, source, description, enabled);
    }

    /**
     * Deletes a source from the list of sources
     * @param sourceName name (e.g. filename) of extension source
     * @throws InvalidSessionException if there is not a valid administrative session
     * @throws AuthorizationException if the administrator does not have privileges to use this method
     * @throws ExtensionSourceNotFoundException if no extension source with
     * name sourceName can be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    @RolesAllowed(value=AdminRoles.RoleName.ADMIN_SYSTEM)
    public void removeSource(String sourceName)
    throws InvalidSessionException, AuthorizationException, ExtensionModuleNotFoundException, MetaMatrixComponentException{
        SessionToken token = AdminAPIHelper.validateSession();
        getExtensionSourceManager().removeSource(token.getUsername(), sourceName);
    }

    /**
     * Returns List (of Strings) of all extension source types currently
     * supported.
     * @return List of the String names of the currently-supported extension
     * source types
     * @throws InvalidSessionException if there is not a valid administrative session
     * @throws AuthorizationException if the administrator does not have privileges to use this method
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    public Collection getSourceTypes()
    throws InvalidSessionException, AuthorizationException, MetaMatrixComponentException{
        return getExtensionSourceManager().getSourceTypes();
    }

    /**
     * Returns List (of Strings) of all extension source names, in order of
     * their search ordering
     * @return List (of Strings) of all extension source names, in order of
     * their search ordering
     * @throws InvalidSessionException if there is not a valid administrative session
     * @throws AuthorizationException if the administrator does not have privileges to use this method
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    public List getSourceNames()
    throws InvalidSessionException, AuthorizationException, MetaMatrixComponentException{
        return getExtensionSourceManager().getSourceNames();
    }

    /**
     * Returns List of ExtensionSourceDescriptor objects, in order
     * of their search ordering
     * @return List of ExtensionSourceDescriptor objects, in order
     * of their search ordering
     * @throws InvalidSessionException if there is not a valid administrative session
     * @throws AuthorizationException if the administrator does not have privileges to use this method
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    public List getSourceDescriptors()
    throws InvalidSessionException, AuthorizationException, MetaMatrixComponentException{
        return getExtensionSourceManager().getSourceDescriptors();
    }

    /**
     * Returns the ExtensionSourceDescriptor object for the extension
     * source indicated by sourceName
     * @param sourceName name (e.g. filename) of extension source
     * @return the ExtensionSourceDescriptor object for the extension
     * source indicated by sourceName
     * @throws InvalidSessionException if there is not a valid administrative session
     * @throws AuthorizationException if the administrator does not have privileges to use this method
     * @throws ExtensionSourceNotFoundException if no extension source with
     * name sourceName can be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    public ExtensionModuleDescriptor getSourceDescriptor(String sourceName)
    throws InvalidSessionException, AuthorizationException, ExtensionModuleNotFoundException, MetaMatrixComponentException{
//        SessionToken token =
        AdminAPIHelper.validateSession();
        return getExtensionSourceManager().getSourceDescriptor(sourceName);
    }

    /**
     * Sets the positions in the search order of all sources (all sources
     * must be included or an ExtensionSourceOrderingException will be thrown)
     * The sourceNames List parameter should indicate the new desired order.
     * @param sourceNames Collection of String names of existing
     * extension sources whose search position is to be set
     * @return updated List of ExtensionSourceDescriptor objects, in order
     * of their search ordering
     * @throws InvalidSessionException if there is not a valid administrative session
     * @throws AuthorizationException if the administrator does not have privileges to use this method
     * @throws ExtensionSourceOrderingException if the extension files could
     * not be ordered as requested because another administrator had
     * concurrently added or removed an extension file or files, or because
     * an indicated position is out of bounds.
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    @RolesAllowed(value=AdminRoles.RoleName.ADMIN_SYSTEM)
    public List setSearchOrder(List sourceNames)
    throws InvalidSessionException, AuthorizationException, ExtensionModuleOrderingException, MetaMatrixComponentException{
        SessionToken token = AdminAPIHelper.validateSession();
        return getExtensionSourceManager().setSearchOrder(token.getUsername(), sourceNames);
    }

    /**
     * Sets the "enabled" (for searching) property of all of the indicated
     * extension sources.
     * @param sourceNames Collection of String names of existing
     * extension sources whose "enabled" status is to be set
     * @param enabled indicates whether each extension source is enabled for
     * being searched or not (for convenience, a source can be disabled
     * without being removed)
     * @return updated List of ExtensionSourceDescriptor objects, in order
     * of their search ordering
     * @throws InvalidSessionException if there is not a valid administrative session
     * @throws AuthorizationException if the administrator does not have privileges to use this method
     * @throws ExtensionSourceNotFoundException if no extension source with
     * name sourceName can be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    @RolesAllowed(value=AdminRoles.RoleName.ADMIN_SYSTEM)
    public List setEnabled(Collection sourceNames, boolean enabled)
    throws InvalidSessionException, AuthorizationException, ExtensionModuleNotFoundException, MetaMatrixComponentException{
        SessionToken token = AdminAPIHelper.validateSession();
        return getExtensionSourceManager().setEnabled(token.getUsername(), sourceNames, enabled);
    }

    /**
     * Updates the indicated extension source
     * @param sourceName name (e.g. filename) of extension source
     * @param source actual contents of source
     * @return ExtensionSourceDescriptor describing the newly-updated
     * extension source
     * @throws InvalidSessionException if there is not a valid administrative session
     * @throws AuthorizationException if the administrator does not have privileges to use this method
     * @throws ExtensionSourceNotFoundException if no extension source with
     * name sourceName can be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    @RolesAllowed(value=AdminRoles.RoleName.ADMIN_SYSTEM)
    public ExtensionModuleDescriptor setSource(String sourceName, byte[] source)
    throws InvalidSessionException, AuthorizationException, ExtensionModuleNotFoundException, MetaMatrixComponentException{
        SessionToken token = AdminAPIHelper.validateSession();
        return getExtensionSourceManager().setSource(token.getUsername(), sourceName, source);
    }

    /**
     * Updates the indicated extension source's source name
     * @param sourceName name (e.g. filename) of extension source
     * @param newName new name for the source
     * @return ExtensionSourceDescriptor describing the newly-updated
     * extension source
     * @throws ExtensionSourceNotFoundException if no extension source with
     * name sourceName can be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    @RolesAllowed(value=AdminRoles.RoleName.ADMIN_SYSTEM)
    public ExtensionModuleDescriptor setSourceName(String sourceName, String newName)
    throws InvalidSessionException, AuthorizationException, ExtensionModuleNotFoundException, MetaMatrixComponentException{
        SessionToken token = AdminAPIHelper.validateSession();
        return getExtensionSourceManager().setSourceName(token.getUsername(), sourceName, newName);
    }

    /**
     * Updates the indicated extension source's description
     * @param sourceName name (e.g. filename) of extension source
     * @param description (optional) description of the extension source.
     * <code>null</code> can be passed in to indicate no description.
     * @return ExtensionSourceDescriptor describing the newly-updated
     * extension source
     * @throws ExtensionSourceNotFoundException if no extension source with
     * name sourceName can be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    @RolesAllowed(value=AdminRoles.RoleName.ADMIN_SYSTEM)
    public ExtensionModuleDescriptor setSourceDescription(String sourceName, String description)
    throws InvalidSessionException, AuthorizationException, ExtensionModuleNotFoundException, MetaMatrixComponentException{
        SessionToken token = AdminAPIHelper.validateSession();
        return getExtensionSourceManager().setSourceDescription(token.getUsername(), sourceName, description);
    }

    /** 
     * @see com.metamatrix.platform.admin.api.ExtensionSourceAdminAPI#isSourceExists()
     */
    public boolean isSourceExists(String sourceName) 
        throws InvalidSessionException, AuthorizationException, MetaMatrixComponentException {
        try {
            getSourceDescriptor(sourceName);
            return true;
        } catch (ExtensionModuleNotFoundException e) {
            // if we get into this exception we need to return the exception
        }
        return false;
    }
}

