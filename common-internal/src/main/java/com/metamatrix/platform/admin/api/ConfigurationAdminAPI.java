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

package com.metamatrix.platform.admin.api;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.common.actions.ActionDefinition;
import com.metamatrix.common.actions.ModificationException;
import com.metamatrix.common.config.api.ComponentDefn;
import com.metamatrix.common.config.api.ComponentDefnID;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.ConfigurationObjectEditor;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.HostID;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;


public interface ConfigurationAdminAPI extends SubSystemAdminAPI {

    /**
     *  Returns a <code>ConfigurationObjectEditor</code> to perform editing operations
     *  on a configuration type object.  The editing process will create actions for
     *  each specific type of editing operation.  Those actions are what need to be
     *  submitted to the <code>ConfigurationService</code> for actual updates to occur.
     *  @return ConfigurationObjectEditor
     */
    ConfigurationObjectEditor createEditor()
    throws ConfigurationException, InvalidSessionException, AuthorizationException, MetaMatrixComponentException;

    /**
     * Returns the ID of the next startup <code>Configuration</code>, which should reflect
     * the desired runtime state of the system.
     * @return ID of next startup configuration
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     * @throws InvalidSessionException if there is not a valid administrative session
     * @throws AuthorizationException if the administrator does not have privileges to use this method
     * @throws MetaMatrixComponentException if a general remote system problem occurred
     */
    ConfigurationID getNextStartupConfigurationID()
    throws ConfigurationException, InvalidSessionException, AuthorizationException, MetaMatrixComponentException;

    /**
     * Returns the current deployed <code>Configuration</code>.  Note, this configuration
     * may not match the actual configuration the system is currently executing under due
     * to administrative task that can be done to tune the system.  Those administrative
     * task <b>do not</b> change the actual <code>Configuration</code> stored in the
     * <code>ConfigurationService</code>.
     * @return Configuration that is currently in use
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    Configuration getCurrentConfiguration()
    throws ConfigurationException, InvalidSessionException, AuthorizationException, MetaMatrixComponentException;

    /**
     * Returns the current deployed <code>Configuration</code>.  Note, this configuration
     * may not match the actual configuration the system is currently executing under due
     * to administrative task that can be done to tune the system.  Those administrative
     * task <b>do not</b> change the actual <code>Configuration</code> stored in the
     * <code>ConfigurationService</code>.
     * @return Configuration that is currently in use
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    Configuration getNextStartupConfiguration()
    throws ConfigurationException, InvalidSessionException, AuthorizationException, MetaMatrixComponentException;

    /**
     * Returns the current <code>ConfigurationModelContainer</code>.  Note, this configuration
     * may not match the actual configuration the system is currently executing under due
     * to administrative task that can be done to tune the system.  Those administrative
     * task <b>do not</b> change the actual <code>Configuration</code> stored in the
     * <code>ConfigurationService</code>.
     * @param configName is the name of the Configuration model to obtain
     * @return Configuration that is currently in use
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    ConfigurationModelContainer getConfigurationModel(String configName)
    throws ConfigurationException, InvalidSessionException, AuthorizationException, MetaMatrixComponentException;



    /**
     * <p>Returns a Collection containing the Configuration object for the specified
     * ConfigurationID id, and also any dependant objects needed to fully
     * define this configuration, such as Host objects, ComponentType
     * objects, and ComponentTypeDefn objects.</p>
     *
     * <p>A Configuration instance contains all of the
     * <code>ComponentDefn</code> objects that "belong" to just that
     * Configuration: VM component definitions, service
     * component definitions, product service configurations, and
     * deployed components.  Objects such as Host objects,
     * ComponentType objects, and ComponentTypeDefn objects describe
     * ComponentDefns, but are not contained by a Configuration.  Therefore,
     * they are included in this Collection for convenience.</p>
     *
     * <p>The Collection will contain instances of
     * {@link com.metamatrix.common.namedobject.BaseObject}.
     * Specifically, this Map should contain the objects for:
     * one configuration object, one or more Host objects, one or more
     * ComponentType objects, and one or more ComponentTypeDefn objects.</p>
     *
     * <p>This method is intended to facilitate exporting a configuration
     * to XML.</p>
     *
     * <p>Here is what the Collection would contain at runtime:
     * <pre>
     * Configuration instance
     * Host instance1
     * Host instance2
     * ...
     * ComponentType instance1
     * ComponentType instance2
     * ...
     * ComponentTypeDefn instance1
     * ComponentTypeDefn instance2
     * </pre></p>
     *
     * @param configID ID Of a Configuration
     * @return Collection of BaseObject instances
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     * @throws InvalidSessionException if there is not a valid administrative session
     * @throws AuthorizationException if the administrator does not have privileges to use this method
     * @throws MetaMatrixComponentException if a general remote system problem occurred
     */
    Collection getConfigurationAndDependents(ConfigurationID configID)
    throws ConfigurationException, InvalidSessionException, AuthorizationException, MetaMatrixComponentException;

    /**
     *   Returns the component type definitions for the specified <code>ComponentTypeID</code>.
     *   This does not return the dependent definitions for service type components.
     *   @param componentTypeID is a ComponentTypeID
     *   @return Collection of ComponentTypeDefns
     *
     *   @see getDependentComponentTypeDefinitions(ComponentTypeID)
     */
    Collection getComponentTypeDefinitions(ComponentTypeID componentTypeID)
    throws ConfigurationException, InvalidSessionException, AuthorizationException, MetaMatrixComponentException;


    /**
     *   Returns the all component type definitions for the specified <code>ComponentTypeID</code>.
     *   This includes the dependent definitions for service type components.
     *   @param componentTypeID is a ComponentTypeID
     *   @return Collection of ComponentTypeDefns
     *
     *   @see getDependentComponentTypeDefinitions(ComponentTypeID)
     */
    Collection getAllComponentTypeDefinitions(ComponentTypeID componentTypeID)
    throws ConfigurationException, InvalidSessionException, AuthorizationException, MetaMatrixComponentException;

    /**
     *  Returns a <code>ComponentType</code> for the specified <code>ComponentTypeID</code>
     *  @param id is for the requested component type.
     *  @return ComponentType based on the id
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    ComponentType getComponentType(ComponentTypeID id)
    throws ConfigurationException, InvalidSessionException, AuthorizationException, MetaMatrixComponentException;

    /**
     * Returns a <code>List</code> of type <code>ComponentType</code> that represents
     * all the ComponentTypes defined.  Note that this will include objects of
     * type ProductType (a subclass of ComponentType).
     * @param includeDeprecated true if class names that have been deprecated should be
     *    included in the returned list, or false if only non-deprecated constants should be returned.
     * @return Collection of type <code>ComponentType</code>
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     * @see ComponentType
     * @see #getAllProductTypes
     */
    Collection getAllComponentTypes(boolean includeDeprecated)
    throws ConfigurationException, InvalidSessionException, AuthorizationException, MetaMatrixComponentException;

    /**
     * Returns a <code>Host</code> for the specified <code>HostID</code>.
     * </br>
     * @return Host
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    Host getHost(HostID hostID)
    throws ConfigurationException, InvalidSessionException, AuthorizationException, MetaMatrixComponentException;

    /**
     * Returns a Collection of {@link com.metamatrix.common.config.api.ResourceDescriptor ResourceDescriptor}
     * for all internal resources defined to the system.  The internal resources are not managed with
     * the other configuration related information.  They are not dictated based on which configuration
     * they will operate (i.e., next startup or operational);
     * @throws AuthorizationException if caller is not authorized to perform this method.
     * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException if an error occurred in communicating with a component.
     */
    Collection getResources()
    throws ConfigurationException, InvalidSessionException, AuthorizationException, MetaMatrixComponentException;

    /**
     * Save the resource changes based on each {@link com.metamatrix.common.config.api.ResourceDescriptor ResourceDescriptor}
     * in the collection.
     * @param resourceDescriptors for the resources to be changed          * 
     * @throws AuthorizationException if caller is not authorized to perform this method.
     * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException if an error occurred in communicating with a component.
     */
    void saveResources(Collection resourceDescriptors)
    throws ConfigurationException, InvalidSessionException, AuthorizationException, MetaMatrixComponentException;


   

    /**
     * Returns a <code>ComponentDefn</code> for the specified <code>ComponentDefnID</code>.
     * </br>
     * @return ComponentDefn
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    ComponentDefn getComponentDefn(ConfigurationID configurationID, ComponentDefnID componentDefnID)
    throws ConfigurationException, InvalidSessionException, AuthorizationException, MetaMatrixComponentException;

   

    // --------------------------------------------------------------
    //                A C T I O N     M E T H O D S
    // --------------------------------------------------------------

    /**
     * Execute as a single transaction the specified action, and optionally
     * return the set of objects or object IDs that were affected/modified by the action.
     * @param action the definition of the action to be performed on data within
     * the repository.
     * @return the set of objects that were affected by this transaction.
     * @throws ModificationException if the target of the action is invalid, or
     * if the target object is not a supported class of targets.
     * @throws IllegalArgumentException if the action is null
     * or if the result specification is invalid
     * @throws ConfigurationException if an error occurred within or during
     * communication with the Metadata Service.
     */
    Set executeTransaction(ActionDefinition action)
    throws ModificationException, ConfigurationException, InvalidSessionException, AuthorizationException, MetaMatrixComponentException;

    /**
     * Execute a list of actions, and optionally
     * return the set of objects or object IDs that were affected/modified by the action.
     * @param actions the ordered list of actions that are to be performed on data within
     * the repository.
     * @return the set of objects that were affected by this transaction.
     * @throws ModificationException if the target of any of the actions is invalid, or
     * if the target object is not a supported class of targets.
     * @throws IllegalArgumentException if the action is null
     * or if the result specification is invalid
     * @throws ConfigurationException if an error occurred within or during
     * communication with the Metadata Service.
     */
    Set executeTransaction(List actions)
    throws ModificationException, ConfigurationException, InvalidSessionException, AuthorizationException, MetaMatrixComponentException;

    /**
     * Check whether the encrypted properties for the specified ComponentDefns can be decrypted.
     * @param defns List<ComponentDefn>
     * @return List<Boolean> in the same order as the paramater <code>defns</code>.
     * For each, true if the properties could be decrypted for that defn.
     * @throws ConfigurationException
     * @throws InvalidSessionException
     * @throws AuthorizationException
     * @throws MetaMatrixComponentException
     * @since 4.3
     */
    List checkPropertiesDecryptable(List defns) 
    throws ConfigurationException, InvalidSessionException, AuthorizationException, MetaMatrixComponentException;

}

