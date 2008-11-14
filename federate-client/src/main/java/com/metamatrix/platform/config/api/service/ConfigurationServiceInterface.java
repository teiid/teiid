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

package com.metamatrix.platform.config.api.service;

import java.io.InputStream;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.api.exception.security.InvalidSessionException;
import com.metamatrix.common.actions.ActionDefinition;
import com.metamatrix.common.actions.ModificationException;
import com.metamatrix.common.config.api.ComponentDefn;
import com.metamatrix.common.config.api.ComponentDefnID;
import com.metamatrix.common.config.api.ComponentObject;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.ConfigurationObjectEditor;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.HostID;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.api.exceptions.ConfigurationLockException;
import com.metamatrix.common.config.api.exceptions.InvalidConfigurationException;
import com.metamatrix.platform.security.api.service.SecureService;
import com.metamatrix.platform.service.api.ServiceInterface;
import com.metamatrix.platform.service.api.exception.ServiceException;


public interface ConfigurationServiceInterface extends Remote, ServiceInterface, SecureService {

    public static String NAME ="ConfigurationService"; //$NON-NLS-1$

    /**
     * Returns a <code>ConfigurationObjectEditor</code> to perform editing operations on a configuration type object. The
     * editing process will create actions for each specific type of editing operation. Those actions are what need to be
     * submitted to the <code>ConfigurationService</code> for actual updates to occur.
     * 
     * @return ConfigurationObjectEditor
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @throws ServiceException
     *             if a general service problem occurred
     * @throws RemoteException
     *             if there is a communication error
     */
    ConfigurationObjectEditor createEditor() throws ConfigurationException, ServiceException, RemoteException;

    /**
     * Returns a <code>List</code>, of type <code>ConfigurationInfo</code>, representing all the current configurations that
     * are known to the <code>ConfigurationService</code>. A null value should never be returned.
     * 
     * @return List of <code>ConfigurationInfo</code>s
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @throws ServiceException
     *             if a general service problem occurred
     * @throws RemoteException
     *             if there is a communication error
     */
    // List getConfigurationInfos() throws ConfigurationException, RemoteException;

    /**
     * Returns the <code>ConfigurationID</code> for the operational configuration.
     * 
     * @return ConfigurationID for current configuration
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @throws ServiceException
     *             if a general service problem occurred
     * @throws RemoteException
     *             if there is a communication error
     */
    ConfigurationID getCurrentConfigurationID() throws ConfigurationException, ServiceException, RemoteException;

    /**
     * Returns the ID of the next startup <code>Configuration</code>, which should reflect the desired runtime state of the
     * system.
     * 
     * @return ID of next startup configuration
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @throws ServiceException
     *             if a general service problem occurred
     * @throws RemoteException
     *             if there is a communication error
     */
    ConfigurationID getNextStartupConfigurationID() throws ConfigurationException, ServiceException, RemoteException;

    /**
     * Returns the ID of the startup <code>Configuration</code>, which should reflect the desired runtime state of the system.
     * 
     * @return ID of startup configuration
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @throws ServiceException
     *             if a general service problem occurred
     * @throws RemoteException
     *             if there is a communication error
     */
    ConfigurationID getStartupConfigurationID() throws ConfigurationException, ServiceException, RemoteException;

    /**
     * Baselines the realtime portion of the current (operational) configuration into the next-startup configuration.
     * 
     * @param principalName
     *            the name of the principal that is requesting the baselining
     */
    void baselineCurrentConfiguration(String principalName) throws ConfigurationException, ServiceException, RemoteException;

    /**
     * Returns the current deployed <code>Configuration</code>. Note, this configuration may not match the actual configuration
     * the system is currently executing under due to administrative task that can be done to tune the system. Those
     * administrative task <b>do not</b> change the actual <code>Configuration</code> stored in the
     * <code>ConfigurationService</code>.
     * 
     * @return Configuration that is currently in use
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @throws ServiceException
     *             if a general service problem occurred
     * @throws RemoteException
     *             if there is a communication error
     */
    Configuration getCurrentConfiguration() throws ConfigurationException, ServiceException, RemoteException;

    /**
     * Returns the next startup <code>Configuration</code>, the Configuration that the system will next boot up with (once it
     * is entirely shut down).
     * 
     * @return Configuration that the system will next start up with.
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @throws ServiceException
     *             if a general service problem occurred
     * @throws RemoteException
     *             if there is a communication error
     */
    Configuration getNextStartupConfiguration() throws ConfigurationException, ServiceException, RemoteException;

    /**
     * Returns the startup <code>Configuration</code>, the Configuration that the system booted up with.
     * 
     * @return Configuration that the system booted up with.
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @throws ServiceException
     *             if a general service problem occurred
     * @throws RemoteException
     *             if there is a communication error
     */
    Configuration getStartupConfiguration() throws ConfigurationException, ServiceException, RemoteException;

    /**
     * Returns the named <code>Configuration</code>.
     * 
     * @param configName
     *            is the name of the Configuration to obtain
     * @return Configuration
     * @throws InvalidConfigurationException
     *             if the specified name does not exist
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @throws ServiceException
     *             if a general service problem occurred
     * @throws RemoteException
     *             if there is a communication error
     */
    Configuration getConfiguration(String configName) throws InvalidConfigurationException, ConfigurationException, ServiceException, RemoteException;

    /**
     * Returns the <code>ConfigurationModelContainer</code> that contains everything (i.e., ComponentTypes, Shared Resources and
     * ComponentDefns) that the server needs to start.
     * 
     * @param configName
     *            if the name of the configuration model to obtain
     * @return ConfigurationModelContainer
     */
    ConfigurationModelContainer getConfigurationModel(String configName) throws InvalidConfigurationException, ConfigurationException, ServiceException, RemoteException;

    /**
     * <p>
     * Returns a Collection containing the Configuration object for the specified ConfigurationID id, and also any dependant
     * objects needed to fully define this configuration, such as Host objects, ComponentType objects, and ComponentTypeDefn
     * objects.
     * </p>
     * <p>
     * A Configuration instance contains all of the <code>ComponentDefn</code> objects that "belong" to just that Configuration:
     * VM component definitions, service component definitions, product service configurations, and deployed components. Objects
     * such as Host objects, ComponentType objects, and ComponentTypeDefn objects describe ComponentDefns, but are not contained
     * by a Configuration. Therefore, they are included in this Collection for convenience.
     * </p>
     * <p>
     * The Collection will contain instances of {@link com.metamatrix.common.namedobject.BaseObject}. Specifically, this Map
     * should contain the objects for: one configuration object, one or more Host objects, one or more ComponentType objects, and
     * one or more ComponentTypeDefn objects.
     * </p>
     * <p>
     * This method is intended to facilitate exporting a configuration to XML.
     * </p>
     * <p>
     * Here is what the Collection would contain at runtime:
     * 
     * <pre>
     * 
     *  Configuration instance
     *  Host instance1
     *  Host instance2
     *  ...
     *  ComponentType instance1
     *  ComponentType instance2
     *  ...
     *  ComponentTypeDefn instance1
     *  ComponentTypeDefn instance2
     *  
     * </pre>
     * 
     * </p>
     * 
     * @param configID
     *            ID Of a Configuration
     * @return Collection of BaseObject instances
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @throws ServiceException
     *             if a general service problem occurred
     * @throws RemoteException
     *             if there is a communication error
     */
    Collection getConfigurationAndDependents(ConfigurationID configID) throws ConfigurationException, ServiceException, RemoteException;

    /**
     * <p>
     * This method will return a Collection of objects that represent the set of global configuration objects currently
     * represented in the configuration database. This method will generally be used when attempting to import a configuration
     * into the database as the 'Next Startup' configuration. This information is important when importing a new configuration so
     * that any global type configuration objects that are to be imported can be resolved against the global objects that
     * currently exist in the database.
     * </p>
     * 
     * <pre>
     * 
     *  The Collection of objects will contain the following configuration 
     *  object types:
     *  
     *  ComponentTypes
     *  ProductTypes
     *  Hosts
     *  
     * </pre>
     * 
     * @return a Collection of all of the global configuration objects as they exist in the database.
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @throws InvalidSessionException
     *             if there is not a valid administrative session
     * @throws AuthorizationException
     *             if the administrator does not have privileges to use this method
     * @throws MetaMatrixComponentException
     *             if a general remote system problem occurred
     */
    public Collection getAllGlobalConfigObjects()
    throws ConfigurationException, ServiceException, RemoteException;

    /**
     * Returns a Map of component type definitions for each <code>ComponentTypeID</code> that is contained in the passed
     * <code>Collection</code>. This does not return the dependent definitions for service type components.
     * 
     * @param componentIDs
     *            is a Collection
     * @return Map of a Map of component type difinitions keyed by <code>ComponentTypeID</code>
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @throws ServiceException
     *             if a general service problem occurred
     * @throws RemoteException
     *             if there is a communication error
     * @see getDependentComponentTypeDefintions(Collection)
     */
    Map getComponentTypeDefinitions(Collection componentIDs) throws ConfigurationException, ServiceException, RemoteException;

    /**
     * Returns the component type definitions for the specified <code>ComponentTypeID</code>. This does not return the
     * dependent definitions for service type components.
     * 
     * @param componentTypeID
     *            is a ComponentTypeID
     * @return Collection of ComponentTypeDefns
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @throws ServiceException
     *             if a general service problem occurred
     * @throws RemoteException
     *             if there is a communication error
     * @see getDependentComponentTypeDefinitions(ComponentTypeID)
     */
    Collection getComponentTypeDefinitions(ComponentTypeID componentTypeID) throws ConfigurationException, ServiceException, RemoteException;


    /**
     * Returns the all component type definitions for the specified <code>ComponentTypeID</code>. This includes the dependent
     * definitions for service type components.
     * 
     * @param componentTypeID
     *            is a ComponentTypeID
     * @return Collection of ComponentTypeDefns
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @throws ServiceException
     *             if a general service problem occurred
     * @throws RemoteException
     *             if there is a communication error
     * @see getDependentComponentTypeDefinitions(ComponentTypeID)
     */
    Collection getAllComponentTypeDefinitions(ComponentTypeID componentTypeID) throws ConfigurationException, ServiceException, RemoteException;

    /**
     * Returns a Map of all component type definitions for each <code>ComponentTypeID</code> that is contained in the passed
     * <code>Collection</code>. This only returns the dependent definitions for service type components where the component
     * type is defined as having a super component type.
     * 
     * @param componentIDs
     *            is a Collection
     * @return Map of component type difinitions keyed by <code>ComponentTypeID</code>
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @throws ServiceException
     *             if a general service problem occurred
     * @throws RemoteException
     *             if there is a communication error
     * @see getComponentTypeDefinitions(Collection)
     * @see getDependentComponentTypeDefinitions(ComponentType)
     */
    Map getDependentComponentTypeDefinitions(Collection componentIDs) throws ConfigurationException, ServiceException, RemoteException;

    /**
     * Returns the component type definitions for the specified <code>ComponentTypeID</code>. This only returns the dependent
     * definitions for service type components where the component type is defined as having a super component type.
     * 
     * @param componentTypeID
     *            is a ComponentTypeID
     * @return Collection of ComponentTypeDefns
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @throws ServiceException
     *             if a general service problem occurred
     * @throws RemoteException
     *             if there is a communication error
     * @see getComponentTypeDefinitions(ComponentTypeID)
     */
    Collection getDependentComponentTypeDefinitions(ComponentTypeID componentTypeID) throws ConfigurationException, ServiceException, RemoteException;


    /**
     * Returns a <code>List</code> of type <code>ComponentType</code> . that are flagged as being monitored. A component of
     * this type is considered to be available for monitoring statistics.
     * 
     * @param includeDeprecated
     *            true if class names that have been deprecated should be included in the returned list, or false if only
     *            non-deprecated constants should be returned.
     * @return Collection of type <code>ComponentType</code>
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @throws ServiceException
     *             if a general service problem occurred
     * @throws RemoteException
     *             if there is a communication error
     * @see #ComponentType
     */
    Collection getMonitoredComponentTypes(boolean includeDeprecated) throws ConfigurationException, ServiceException, RemoteException;

    /**
     * Returns a <code>ComponentType</code> for the specified <code>ComponentTypeID</code>
     * 
     * @param id
     *            is for the requested component type.
     * @return ComponentType based on the id
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @throws ServiceException
     *             if a general service problem occurred
     * @throws RemoteException
     *             if there is a communication error
     */
    ComponentType getComponentType(ComponentTypeID id) throws ConfigurationException, ServiceException, RemoteException;

    /**
     * Returns a <code>List</code> of type <code>ComponentType</code> that represents all the ComponentTypes defined.
     * 
     * @param includeDeprecated
     *            true if class names that have been deprecated should be included in the returned list, or false if only
     *            non-deprecated constants should be returned.
     * @return Collection of type <code>ComponentType</code>
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @throws ServiceException
     *             if a general service problem occurred
     * @throws RemoteException
     *             if there is a communication error
     * @see #ComponentType
     */
    Collection getAllComponentTypes(boolean includeDeprecated) throws ConfigurationException, ServiceException, RemoteException;

    /**
     * Call to save a configuration under a new name. </br>
     * 
     * @param name
     *            is the current name of the configuration
     * @param newName
     *            is the new name the current configuration will be save under
     * @throws InvalidConfigurationException
     *             if the specified name does not exist
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @throws ServiceException
     *             if a general service problem occurred
     * @throws RemoteException
     *             if there is a communication error
     */
    // void saveConfigurationAs(String name, String newName) throws InvalidConfigurationException, ConfigurationException,
    // RemoteException;

    /**
     * Call to lock a specific configuration.
     * 
     * @param configurationID
     *            for the <code>Configuration</code> to lock.
     * @return boolean indicating if the lock was obtained
     * @throws MetadataLockException
     *             if the appropriate locks required to perform this operation are not held by this editor.
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @throws ServiceException
     *             if a general service problem occurred
     * @throws RemoteException
     *             if there is a communication error
     */
    // void lockConfiguration(ConfigurationID configurationID) throws ConfigurationLockException, ConfigurationException,
    // RemoteException;

    /**
     * Call to unlock a specific configuration.
     * 
     * @param configurationID
     *            for the <code>Configuration</code> to unlock.
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @throws ServiceException
     *             if a general service problem occurred
     * @throws RemoteException
     *             if there is a communication error
     */
    // void unlockConfiguration(ConfigurationID configurationID) throws ConfigurationLockException, ConfigurationException,
    // RemoteException;

    /**
     * Returns a <code>Host</code> for the specified <code>HostID</code>. </br>
     * 
     * @return Host
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @throws ServiceException
     *             if a general service problem occurred
     * @throws RemoteException
     *             if there is a communication error
     */
    Host getHost(HostID hostID) throws ConfigurationException, ServiceException, RemoteException;

    /**
     * Returns a <code>Collection</code> of currently defined hosts. This method does not cache, it reretrieves the data
     * everytime. </br>
     * 
     * @return Collection of type Host
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @throws ServiceException
     *             if a general service problem occurred
     * @throws RemoteException
     *             if there is a communication error
     */
    Collection getHosts() throws ConfigurationException, ServiceException, RemoteException;


    /**
     * Returns a <code>ComponentDefn</code> for the specified <code>ComponentDefnID</code>. </br>
     * 
     * @param configurationID
     *            is the configuration for which the component exist
     * @param componentDefnID
     *            is the component being requested
     * @return ComponentDefn
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     */
    ComponentDefn getComponentDefn(ConfigurationID configurationID, ComponentDefnID componentDefnID)
    throws ConfigurationException, ServiceException, RemoteException;
    
    /**
     * Returns a collection of <code>ComponentDefn</code>s for the specified collection of <code>ComponentDefnID</code>s and
     * <code>ConfigurationID</code>. If the configuration is null the parent name from the componentID will be used. </br> The
     * reason for adding the option to specify the configurationID is so that the same collection of componentIDs can be used to
     * obtain the componentDefns from the different configurations. Otherwise, the requestor would have to create a new set of
     * componetDefnIDs for each configuration. <br>
     * 
     * @param componentDefnIDs
     *            contains all the ids for which componet defns to be returned
     * @param configurationID
     *            is the configuration from which the component defns are to be derived; optional, nullalble
     * @return Collection of ComponentDefn objects
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     */
    Collection getComponentDefns(Collection componentDefnIDs, ConfigurationID configurationID)
    throws ConfigurationException, ServiceException, RemoteException;
    

    /**
     * Returns a Collection of {@link com.metamatrix.common.config.api.ResourceDescriptor ResourceDescriptor} for all resource
     * pools defined to the system.
     * 
     * @param configurationID
     *            is the configuration from which the component defns are to be derived
     * @throws ServiceException
     *             if there is a problem with the service infrastructure
     * @throws RemoteException
     *             if there is a communication error
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     */
    Collection getResourcePools(ConfigurationID configurationID)
    throws ConfigurationException, ServiceException, RemoteException;
    
    
  /**
     * Returns a Collection of {@link com.metamatrix.common.config.api.ComponentType ComponentType} for all resource pool types of
     * a specified configuration.
     * 
     * @param configurationID
     *            is the configuration from which the component types are to be derived
     * @throws ServiceException
     *             if there is a problem with the service infrastructure
     * @throws RemoteException
     *             if there is a communication error
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     */    
   Collection getResourcePoolTypes(ConfigurationID configurationID)
    throws ConfigurationException, ServiceException, RemoteException;
 
 
    /**
     * Returns a Collection of {@link com.metamatrix.common.config.api.ComponentType ComponentType} that represent the pool types
     * for which new {@link ResourceDescriptor ResourcePools} of these types can be created. This means only these types have
     * logic implemented to make use of the resource pool.
     * 
     * @param configurationID
     *            is the configuration from which the component types are to be derived
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    Collection getPoolableResourcePoolTypes(ConfigurationID configurationID)
   throws ConfigurationException, ServiceException, RemoteException;
        

   /**
     * Returns a Collection of {@link com.metamatrix.common.config.api.ResourceDescriptor ResourceDescriptor} for all resource
     * pools of a specified type defined in a configuration.
     * 
     * @param callerSessionID
     *            ID of the caller's current session.
     * @param configurationID
     *            is the configuration from which the component defns are to be derived
     * @param componentTypeID
     *            indicates the type of pools in the configuration to return
     * @throws ServiceException
     *             if there is a problem with the service infrastructure
     * @throws RemoteException
     *             if there is a communication error
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     */
    Collection getResourcePools(ConfigurationID configurationID, ComponentTypeID componentTypeID)
    throws ConfigurationException, ServiceException, RemoteException;
    
    /**
     * Returns a Collection of {@link com.metamatrix.common.config.api.ResourceDescriptor ResourceDescriptor} for all internal
     * resources defined to the system. The internal resources are not managed with the other configuration related information.
     * They are not dictated based on which configuration they will operate (i.e., next startup or operational);
     * 
     * @throws ServiceException
     *             if there is a problem with the service infrastructure
     * @throws RemoteException
     *             if there is a communication error
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     */
    Collection getResources()
    throws ConfigurationException, ServiceException, RemoteException;

    /**
     * Returns a Collection of {@link com.metamatrix.common.config.api.ResourceDescriptor ResourceDescriptor} that are of the
     * specified resource type.
     * 
     * @param componentTypeID
     *            that identifies the type of resources to be returned
     * @throws ServiceException
     *             if there is a problem with the service infrastructure
     * @throws RemoteException
     *             if there is a communication error
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     */
    Collection getResources(ComponentTypeID componentTypeID)
    throws ConfigurationException, ServiceException, RemoteException;


   /**
     * Save the resource changes based on each {@link com.metamatrix.common.config.api.ResourceDescriptor ResourceDescriptor} in
     * the collection.
     * 
     * @param resourceDescriptors
     *            for the resources to be changed *
     * @throws ServiceException
     *             if there is a problem with the service infrastructure
     * @throws RemoteException
     *             if there is a communication error
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     */
    void saveResources(Collection resourceDescriptors, String principalName)
    throws ConfigurationException, ServiceException, RemoteException;

    

    /**
     * Obtain the List of {@link com.metamatrix.common.config.api.ReleaseInfo} objects which represent the products of the system.
     * Each ReleaseInfo contains the name of the product, as well as release info.
     * 
     * @return Collection of ReleaseInfo objects of licensed products.
     * @throws ConfigurationException
     *             if an business error occurred within or during communication with the Configuration Service.
     * @throws ServiceException
     *             if there is a problem with the service infrastructure
     * @throws RemoteException
     *             if there is a communication error
     */
    Collection getProductReleaseInfos() throws ConfigurationException, ServiceException, RemoteException;

    // --------------------------------------------------------------
    // A C T I O N M E T H O D S
    // --------------------------------------------------------------

    /**
     * Execute as a single transaction the specified action, and optionally return the set of objects or object IDs that were
     * affected/modified by the action.
     * 
     * @param action
     *            the definition of the action to be performed on data within the repository.
     * @param principalName
     *            of the person executing the transaction
     * @return the set of objects that were affected by this transaction.
     * @throws ModificationException
     *             if the target of the action is invalid, or if the target object is not a supported class of targets.
     * @throws IllegalArgumentException
     *             if the action is null or if the result specification is invalid
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @throws ServiceException
     *             if a general service problem occurred
     * @throws RemoteException
     *             if there is a communication error
     */
    Set executeTransaction(ActionDefinition action, String principalName ) throws ModificationException, ConfigurationLockException, ConfigurationException, ServiceException, RemoteException;

    /**
     * Execute a list of actions, and optionally return the set of objects or object IDs that were affected/modified by the
     * action.
     * 
     * @param actions
     *            the ordered list of actions that are to be performed on data within the repository.
     * @param principalName
     *            of the person executing the transaction
     * @return the set of objects that were affected by this transaction.
     * @throws ModificationException
     *             if the target of any of the actions is invalid, or if the target object is not a supported class of targets.
     * @throws IllegalArgumentException
     *             if the action is null or if the result specification is invalid
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @throws ServiceException
     *             if a general service problem occurred
     * @throws RemoteException
     *             if there is a communication error
     */
    Set executeTransaction(List actions, String principalName) throws ModificationException, ConfigurationLockException, ConfigurationException, ServiceException, RemoteException;

    /**
     * Execute a list of actions, and optionally return the set of objects or object IDs that were affected/modified by the
     * action.
     * 
     * @param doAdjust
     *            flag to turn on/off the ConnectorBinding adjustments that are made at the SPI level.
     * @param actions
     *            the ordered list of actions that are to be performed on data within the repository.
     * @param principalName
     *            of the person executing the transaction
     * @return the set of objects that were affected by this transaction.
     * @throws ModificationException
     *             if the target of any of the actions is invalid, or if the target object is not a supported class of targets.
     * @throws IllegalArgumentException
     *             if the action is null or if the result specification is invalid
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @throws ServiceException
     *             if a general service problem occurred
     * @throws RemoteException
     *             if there is a communication error
     */
 // Set executeTransaction(boolean doAdjust, List actions, String principalName) throws ModificationException,
    // ConfigurationLockException, ConfigurationException, ServiceException, RemoteException;

    /**
     * Execute a list of insert actions and for actions on objects of type ComponentDefn or DeployedComponent object, it will have
     * its configuration id resassigned, and optionally return the set of objects or object IDs that were affected/modified by the
     * action. Only insert actions can be performed here because changing a configuration id on a modify action has larger
     * consiquences.
     * 
     * @param assignConfigurationID
     *            the configuration for which any action for a component object will have its configurationID set to this.
     * @param actions
     *            the ordered list of actions that are to be performed on data within the repository.
     * @param principalName
     *            of the person executing the transaction
     * @return the set of objects that were affected by this transaction.
     * @throws ModificationException
     *             if the target of any of the actions is invalid, or an action that is not an insert, or if the target object is
     *             not a supported class of targets.
     * @throws IllegalArgumentException
     *             if the action is null or if the result specification is invalid
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @throws ServiceException
     *             if a general service problem occurred
     * @throws RemoteException
     *             if there is a communication error
     */
    Set executeInsertTransaction(ConfigurationID assignConfigurationID, List actions, String principalName) throws ModificationException, ConfigurationLockException, ConfigurationException, ServiceException, RemoteException;

    /**
     * Undo the specified number of previously-committed transactions.
     * 
     * @param numberOfActions
     *            the number of actions in the history that are to be undone.
     * @param principalName
     *            of the person executing the transaction
     * @return the set of objects that were affected by undoing these actions.
     * @throws IllegalArgumentException
     *             if the number is negative.
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @throws ServiceException
     *             if a general service problem occurred
     * @throws RemoteException
     *             if there is a communication error
     */
    Set undoActionsAsTransaction(int numberOfActions, String principalName) throws ConfigurationException, ServiceException, RemoteException;

    /**
     * Get the history of actions executed in transactions by this editor. The actions at the front of the list will be those most
     * recently executed.
     * 
     * @return the ordered list of actions in the history.
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @throws ServiceException
     *             if a general service problem occurred
     * @throws RemoteException
     *             if there is a communication error
     */
    List getHistory() throws ConfigurationException, ServiceException, RemoteException;

    /**
     * Clear the history of all actions without undoing any of them.
     * 
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @throws ServiceException
     *             if a general service problem occurred
     * @throws RemoteException
     *             if there is a communication error
     */
    void clearHistory() throws ConfigurationException, ServiceException, RemoteException;

    /**
     * Get the number of actions that are currently in the history.
     * 
     * @return the number of actions in the history.
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @throws ServiceException
     *             if a general service problem occurred
     * @throws RemoteException
     *             if there is a communication error
     */
    int getHistorySize() throws ConfigurationException, ServiceException, RemoteException;

    /**
     * Set the limit on the number of actions in the history. Note that the history may at times be greater than this limit,
     * because when actions are removed from the history, all actions for a transactions are removed at the same time. If doing so
     * would make the history size smaller than the limit, no actions are removed.
     * 
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @throws ServiceException
     *             if a general service problem occurred
     * @throws RemoteException
     *             if there is a communication error
     */
    int getHistoryLimit() throws ConfigurationException, ServiceException, RemoteException;

    /**
     * Set the limit on the number of actions in the history. Note that the history may at times be greater than this limit,
     * because when actions are removed from the history, all actions for a transactions are removed at the same time. If doing so
     * would make the history size smaller than the limit, no actions are removed.
     * 
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @throws ServiceException
     *             if a general service problem occurred
     * @throws RemoteException
     *             if there is a communication error
     */
    void setHistoryLimit(int maximumHistoryCount) throws ConfigurationException, ServiceException, RemoteException;


    /**
     * Return the time the server was started. If the state of the server is not "Started" then a null is returned.
     * 
     * @return Date Time server was started.
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @throws ServiceException
     *             if a general service problem occurred
     * @throws RemoteException
     *             if there is a communication error
     */
    Date getServerStartupTime() throws ConfigurationException, ServiceException, RemoteException;
    
    /**
     * Add Host to Configuration Add a new Host to the System (MetaMatrix Cluster)
     * 
     * @param hostName
     *            Host Name of new Host being added to Configuration
     * @param principalName
     * @param properties
     * @return Host
     * @throws ConfigurationException
     * @throws ServiceException
     * @throws RemoteException
     * @since 4.3
     */
    Host addHost(String hostName, String principalName, Properties properties) throws ConfigurationException, ServiceException, RemoteException;
    
    /**
     * Add Process for the specified Host to Configuration Add a new Virtuial Machine to the System (MetaMatrix Cluster)
     * 
     * @param processName
     *            Name of the New Process being Added to Configuration
     * @param hostName
     *            Name of the Host that the new Process is being added
     * @param principalName
     *            User Name of user who is making the change
     * @param properties
     *            name, value need to setup a Host
     * @return VMComponentDefn
     * @throws ConfigurationException
     * @throws ServiceException
     * @throws RemoteException
     * @since 4.3
     */
    VMComponentDefn addProcess(String processName, String hostName, String principalName, Properties properties) throws ConfigurationException, ServiceException, RemoteException;
    
    /**
     * Set System Property Value in Configuration
     * 
     * @param propertyName
     *            Name of Property
     * @param propertyValue
     *            Value of Property
     * @param principalName
     *            User Name of user who is making the change
     * @throws ConfigurationException
     * @throws ServiceException
     * @throws RemoteException
     * @since 4.3
     */
    void setSystemPropertyValue(String propertyName,
                                       String propertyValue,
                                       String principalName) throws ConfigurationException,ServiceException, RemoteException;
    
    
    /**
     * Set System Property Values in Configuration.
     * Any properties not set will be left unchanged.
     * 
     * @param properties
     *            Properties to set
     * @param principalName
     *            User Name of user who is making the change
     * @throws ConfigurationException
     * @throws ServiceException
     * @throws RemoteException
     * @since 4.3
     */
    void updateSystemPropertyValues(Properties properties,
                                       String principalName) throws ConfigurationException,ServiceException, RemoteException;
    
    /**
     * Deploy a new Connector Binding into Configuration
     * 
     * @param connectorBindingName
     * @param connectorType
     *            Connector Type for this Connector Binding
     * @param pscName Name of the PSC to deploy the Connector Binding to.  
     *         If pscName is null, this method does not deploy the Connector Binding to a PSC.
     * @param principalName
     *            User Name of user who is making the change
     * @param properties
     * @return ConnectorBinding object
     * @throws ConfigurationException
     * @throws ServiceException
     * @throws RemoteException
     * @since 4.3
     */
    ConnectorBinding createConnectorBinding(String connectorBindingName,
                                                   String connectorType,
                                                   String pscName,
                                                   String principalName,
                                                   Properties properties) throws ConfigurationException, ServiceException, RemoteException;
    
    /**
     * Modify a Component in Configuration
     * 
     * @param theObject
     * @param theProperties
     * @param principalName
     * @return
     * @throws ModificationException
     * @throws Exception
     * @since 4.3
     */
    public Object modify(ComponentObject theObject,
                         Properties theProperties,
                         String principalName) throws ConfigurationException, ServiceException, RemoteException, ModificationException;
    
    
    /**
     * Import a Connector Type
     * 
     * @param inputStream
     * @param name
     *            Name of the Connector Type to import
     * @param principalName
     *            User Name of user who is making the change
     * @return ComponentType
     * @throws ConfigurationException
     * @throws ServiceException
     * @throws RemoteException
     * @since 4.3
     */
    public ComponentType importConnectorType(InputStream inputStream,
                                             String name,
                                             String principalName) throws ConfigurationException, ServiceException, RemoteException;
    
    /**
     * Import a connector Binding from InputStream, and deploy it to a PSC.
     *  
     * @param inputStream 
     * @param name Name of Connector Binding to import
     * @param pscName Name of the PSC to deploy the Connector Binding to.  
     * If pscName is null, this method does not deploy the Connector Binding to a PSC.
     * @param principalName
     * @return ConnectorBinding
     * @throws ConfigurationException
     * @throws ServiceException
     * @throws RemoteException
     * @since 4.3
     */
    public ConnectorBinding importConnectorBinding(InputStream inputStream,
                                                   String name,
                                                   String pscName,
                                                   String principalName) throws ConfigurationException,
                                                                        ServiceException, RemoteException;
        
    /**
     * Deletes a component object.
     * 
     * @param theObject
     *            the object being deleted
     * @param theDeleteDependenciesFlag
     *            boolean flag for deleting dependencies
     * @param principalName
     *            User Name of user who is making the change
     * @throws ConfigurationException
     * @throws ModificationException
     * @throws ServiceException
     * @throws RemoteException
     * @since 4.3
     */
    public void delete(ComponentObject theObject,
                        boolean theDeleteDependenciesFlag,
                        String principalName) throws ConfigurationException, ModificationException,
                        ServiceException, RemoteException; 
    
    /**
     * Delete a Component Type
     *  
     * @param componentType Component Type Object being deleted
     * @param principalName User Name who is making the change
     * @throws ConfigurationException
     * @throws ServiceException
     * @throws ModificationException
     * @throws RemoteException
     * @since 4.3
     */
    public void delete(ComponentType componentType,
                       String principalName) throws ConfigurationException,
                       ServiceException, ModificationException, RemoteException ;
    
    
    /**
     * Deploys the ServiceComponentDefns indicated by the ProductServiceConfig,
     * contained by the Configuration, onto the specified Host and VM.
     * 
     * @param theHost host on which the services will be deployed
     * @param theProcess VM on which the services will be deployed
     * @param pscName Name of the PSC
     * @param principalName User Name deploying the Services
     * 
     * @return Collection of DeployedComponent objects, each representing
     * one of the deployed ServiceComponentDefns 
     * 
     * @throws ConfigurationException
     * @throws ServiceException
     * @throws ModificationException
     * @throws RemoteException
     * @since 4.3
     */
    
    public Collection deployPSC(Host theHost,
                                VMComponentDefn theProcess,
                                String pscName,
                                String principalName) throws ConfigurationException,
                                                     ServiceException,
                                                     ModificationException,
                                                     RemoteException;

    
    
    /**
     * Check whether the encrypted properties for the specified ComponentDefns can be decrypted.
     * @param defns List<ComponentDefn>
     * @return List<Boolean> in the same order as the paramater <code>defns</code>.
     * For each, true if the properties could be decrypted for that defn.
     * @throws ConfigurationException
     * @throws ServiceException
     * @throws RemoteException
     * @since 4.3
     */
    public List checkPropertiesDecryptable(List defns) throws ConfigurationException,
                                                                   ServiceException,
                                                                   RemoteException;

    /**
     * Check whether the given properties pertainting to the given component (name and type)
     * contain at least one value that the server cannot decrypt with its current keystore. 
     * @param props component properties possibly containing encrypted values. 
     * @param componentTypeIdentifier The type identifier of the component to which the properties belong.
     * @return <code>true</code> iff all of the encrypted properties, if any, can be decrypted. 
     * @throws ConfigurationException
     * @throws ServiceException
     * @since 4.3
     */
    boolean checkPropertiesDecryptable(Properties props,
                                       String componentTypeIdentifier) throws ConfigurationException,
                                       ServiceException,
                                       RemoteException;
    
}
