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

package com.metamatrix.platform.admin.apiimpl;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.metamatrix.admin.api.server.AdminRoles;
import com.metamatrix.api.exception.ComponentCommunicationException;
import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.api.exception.security.InvalidSessionException;
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
import com.metamatrix.common.config.api.ProductType;
import com.metamatrix.common.config.api.ResourceDescriptor;
import com.metamatrix.common.config.api.ResourceDescriptorID;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.api.exceptions.ConfigurationLockException;
import com.metamatrix.common.config.api.exceptions.InvalidConfigurationException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.pooling.api.ResourcePoolMgr;
import com.metamatrix.common.pooling.api.exception.ResourcePoolException;
import com.metamatrix.platform.PlatformPlugin;
import com.metamatrix.platform.admin.api.ConfigurationAdminAPI;
import com.metamatrix.platform.admin.api.PlatformAdminLogConstants;
import com.metamatrix.platform.config.api.service.ConfigurationServiceInterface;
import com.metamatrix.platform.registry.ClusteredRegistryState;
import com.metamatrix.platform.registry.ResourcePoolMgrBinding;
import com.metamatrix.platform.security.api.SessionToken;
import com.metamatrix.platform.service.api.exception.ServiceException;
import com.metamatrix.platform.util.PlatformProxyHelper;

public class ConfigurationAdminAPIImpl extends SubSystemAdminAPIImpl implements ConfigurationAdminAPI {

    // Auth svc proxy
    private ConfigurationServiceInterface configAdmin;
    private ClusteredRegistryState registry;
    private static ConfigurationAdminAPI configAdminAPI;

    /**
     * ctor
     */
    private ConfigurationAdminAPIImpl(ClusteredRegistryState registry) throws MetaMatrixComponentException {
        configAdmin = PlatformProxyHelper.getConfigurationServiceProxy(PlatformProxyHelper.ROUND_ROBIN_LOCAL);
        this.registry = registry;
    }

    public synchronized static ConfigurationAdminAPI getInstance(ClusteredRegistryState registry) throws MetaMatrixComponentException {
        if (configAdminAPI == null) {
            configAdminAPI = new ConfigurationAdminAPIImpl(registry);
        }
        return configAdminAPI;
    }

    
    
    
    /**
     * Returns a <code>ConfigurationObjectEditor</code> to perform editing operations on a configuration type object. The
     * editing process will create actions for each specific type of editing operation. Those actions are what need to be
     * submitted to the <code>ConfigurationService</code> for actual updates to occur.
     * 
     * @return ConfigurationObjectEditor
     */
    public synchronized ConfigurationObjectEditor createEditor() throws ConfigurationException,
                                                                                                   InvalidSessionException,
                                                                                                   AuthorizationException,
                                                                                                   MetaMatrixComponentException {
        // Validate caller's session
        // SessionToken token =
        AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role

        try {
            return configAdmin.createEditor();
        }  catch (RemoteException err) {
            throw new MetaMatrixComponentException(err);
        }
    }

    /**
     * Returns the <code>ConfigurationID</code> for the current configuration.
     * 
     * @param sessionID
     *            ID of administrator's session
     * @return ConfigurationID for current configuration
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @throws InvalidSessionException
     *             if there is not a valid administrative session
     * @throws AuthorizationException
     *             if the administrator does not have privileges to use this method
     * @throws MetaMatrixComponentException
     *             if a general remote system problem occurred
     */
    public synchronized ConfigurationID getCurrentConfigurationID() throws ConfigurationException,
                                                                                                      InvalidSessionException,
                                                                                                      AuthorizationException,
                                                                                                      MetaMatrixComponentException {
        // Validate caller's session
        // SessionToken token =
        AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role

        try {
            return configAdmin.getCurrentConfigurationID();
        }  catch (RemoteException err) {
            throw new MetaMatrixComponentException(err);
        }
    }

    /**
     * Returns the ID of the next startup <code>Configuration</code>, which should reflect the desired runtime state of the
     * system.
     * 
     * @param sessionID
     *            ID of administrator's session
     * @return ID of next startup configuration
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @throws InvalidSessionException
     *             if there is not a valid administrative session
     * @throws AuthorizationException
     *             if the administrator does not have privileges to use this method
     * @throws MetaMatrixComponentException
     *             if a general remote system problem occurred
     */
    public synchronized ConfigurationID getNextStartupConfigurationID() throws ConfigurationException,
                                                                                                          InvalidSessionException,
                                                                                                          AuthorizationException,
                                                                                                          MetaMatrixComponentException {
        // Validate caller's session
        // SessionToken token =
        AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role
        try {
            return configAdmin.getNextStartupConfigurationID();
        }  catch (RemoteException err) {
            throw new MetaMatrixComponentException(err);
        }
    }

    /**
     * Returns the ID of the startup <code>Configuration</code>, which should reflect the desired runtime state of the system.
     * 
     * @param sessionID
     *            ID of administrator's session
     * @return ID of startup configuration
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @throws InvalidSessionException
     *             if there is not a valid administrative session
     * @throws AuthorizationException
     *             if the administrator does not have privileges to use this method
     * @throws MetaMatrixComponentException
     *             if a general remote system problem occurred
     */
    public synchronized ConfigurationID getStartupConfigurationID() throws ConfigurationException,
                                                                                                      InvalidSessionException,
                                                                                                      AuthorizationException,
                                                                                                      MetaMatrixComponentException {
        // Validate caller's session
        // SessionToken token =
        AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role
        try {
            return configAdmin.getStartupConfigurationID();
        }  catch (RemoteException err) {
            throw new MetaMatrixComponentException(err);
        }
    }

    /**
     * Returns the current deployed <code>Configuration</code>. Note, this configuration may not match the actual configuration
     * the system is currently executing under due to administrative task that can be done to tune the system. Those
     * administrative task <b>do not</b> change the actual <code>Configuration</code> stored in the
     * <code>ConfigurationService</code>.
     * 
     * @return Configuration that is currently in use
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     */
    public synchronized Configuration getCurrentConfiguration() throws ConfigurationException,
                                                                                                  InvalidSessionException,
                                                                                                  AuthorizationException,
                                                                                                  MetaMatrixComponentException {
        // Validate caller's session
        // SessionToken token =
        AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role
        try {
            return configAdmin.getCurrentConfiguration();
        }  catch (RemoteException err) {
            throw new MetaMatrixComponentException(err);
        }
    }

    /**
     * Returns the current deployed <code>Configuration</code>. Note, this configuration may not match the actual configuration
     * the system is currently executing under due to administrative task that can be done to tune the system. Those
     * administrative task <b>do not</b> change the actual <code>Configuration</code> stored in the
     * <code>ConfigurationService</code>.
     * 
     * @return Configuration that is currently in use
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     */
    public synchronized Configuration getNextStartupConfiguration() throws ConfigurationException,
                                                                                                      InvalidSessionException,
                                                                                                      AuthorizationException,
                                                                                                      MetaMatrixComponentException {
        // Validate caller's session
        // SessionToken token =
        AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role
        try {
            return configAdmin.getNextStartupConfiguration();
        }  catch (RemoteException err) {
            throw new MetaMatrixComponentException(err);
        }
    }

    /**
     * Returns the current deployed <code>Configuration</code>. Note, this configuration may not match the actual configuration
     * the system is currently executing under due to administrative task that can be done to tune the system. Those
     * administrative task <b>do not</b> change the actual <code>Configuration</code> stored in the
     * <code>ConfigurationService</code>.
     * 
     * @return Configuration that is currently in use
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     */
    public synchronized Configuration getStartupConfiguration() throws ConfigurationException,
                                                                                                  InvalidSessionException,
                                                                                                  AuthorizationException,
                                                                                                  MetaMatrixComponentException {
        // Validate caller's session
        // SessionToken token =
        AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role
        try {
            return configAdmin.getStartupConfiguration();
        }  catch (RemoteException err) {
            throw new MetaMatrixComponentException(err);
        }
    }

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
     */
    public synchronized Configuration getConfiguration(String configName) throws InvalidConfigurationException,
                                                                         ConfigurationException,
                                                                         InvalidSessionException,
                                                                         AuthorizationException,
                                                                         MetaMatrixComponentException {
        // Validate caller's session
        // SessionToken token =
        AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role
        try {
            return configAdmin.getConfiguration(configName);
        }  catch (RemoteException err) {
            throw new MetaMatrixComponentException(err);
        }
    }

    public synchronized ConfigurationModelContainer getConfigurationModel(String configName) throws ConfigurationException,
                                                                                            InvalidSessionException,
                                                                                            AuthorizationException,
                                                                                            MetaMatrixComponentException {
        AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role
        try {
            return configAdmin.getConfigurationModel(configName);
        }  catch (RemoteException err) {
            throw new MetaMatrixComponentException(err);
        }
    }

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
     * @param sessionID
     *            ID of administrator's session
     * @param configID
     *            ID Of a Configuration
     * @return Collection of BaseObject instances
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @throws InvalidSessionException
     *             if there is not a valid administrative session
     * @throws AuthorizationException
     *             if the administrator does not have privileges to use this method
     * @throws MetaMatrixComponentException
     *             if a general remote system problem occurred
     */
    public synchronized Collection getConfigurationAndDependents(ConfigurationID configID) throws ConfigurationException,
                                                                                          InvalidSessionException,
                                                                                          AuthorizationException,
                                                                                          MetaMatrixComponentException {
        // Validate caller's session
        // SessionToken token =
        AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role
        try {
            return configAdmin.getConfigurationAndDependents(configID);
        }  catch (RemoteException err) {
            throw new MetaMatrixComponentException(err);
        }
    }

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
    public synchronized Collection getAllGlobalConfigObjects() throws ConfigurationException,
                                                                                                 InvalidSessionException,
                                                                                                 AuthorizationException,
                                                                                                 MetaMatrixComponentException {
        // Validate caller's session
        // SessionToken token =
        AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role
        try {
            return configAdmin.getAllGlobalConfigObjects();
        }  catch (RemoteException err) {
            throw new MetaMatrixComponentException(err);
        }
    }

    /**
     * Baselines the realtime portion of the current (operational) configuration into the next-startup configuration.
     * 
     * @param principalName
     *            the name of the principal that is requesting the baselining
     */
    public synchronized void baselineCurrentConfiguration() throws ConfigurationException,
                                                                                              InvalidSessionException,
                                                                                              AuthorizationException,
                                                                                              MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(token, AdminRoles.RoleName.ADMIN_SYSTEM, "ConfigurationAdminAPIImpl.baselineCurrentConfiguration()"); //$NON-NLS-1$
        try {
            configAdmin.baselineCurrentConfiguration(token.getUsername());
        }  catch (RemoteException err) {
            throw new MetaMatrixComponentException(err);
        }
    }

    /**
     * Returns a Map of component type definitions for each <code>ComponentTypeID</code> that is contained in the passed
     * <code>Collection</code>. This does not return the dependent definitions for service type components.
     * 
     * @param componentIDs
     *            is a Collection
     * @return Map of a Map of component type difinitions keyed by <code>ComponentTypeID</code>
     * @see getDependentComponentTypeDefintions(Collection)
     */
    public synchronized Map getComponentTypeDefinitions(Collection componentIDs) throws ConfigurationException,
                                                                                InvalidSessionException,
                                                                                AuthorizationException,
                                                                                MetaMatrixComponentException {
        // Validate caller's session
        // SessionToken token =
        AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role
        try {
            return configAdmin.getComponentTypeDefinitions(componentIDs);
        }  catch (RemoteException err) {
            throw new MetaMatrixComponentException(err);
        }
    }

    /**
     * Returns the component type definitions for the specified <code>ComponentTypeID</code>. This does not return the
     * dependent definitions for service type components.
     * 
     * @param componentTypeID
     *            is a ComponentTypeID
     * @return Collection of ComponentTypeDefns
     * @see getDependentComponentTypeDefinitions(ComponentTypeID)
     */
    public synchronized Collection getComponentTypeDefinitions(ComponentTypeID componentTypeID) throws ConfigurationException,
                                                                                               InvalidSessionException,
                                                                                               AuthorizationException,
                                                                                               MetaMatrixComponentException {
        // Validate caller's session
        // SessionToken token =
        AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role
        try {
            return configAdmin.getComponentTypeDefinitions(componentTypeID);
        }  catch (RemoteException err) {
            throw new MetaMatrixComponentException(err);
        }
    }

    /**
     * Returns the all component type definitions for the specified <code>ComponentTypeID</code>. This includes the dependent
     * definitions for service type components.
     * 
     * @param componentTypeID
     *            is a ComponentTypeID
     * @return Collection of ComponentTypeDefns
     * @see getDependentComponentTypeDefinitions(ComponentTypeID)
     */
    public synchronized Collection getAllComponentTypeDefinitions(ComponentTypeID componentTypeID) throws ConfigurationException,
                                                                                                  InvalidSessionException,
                                                                                                  AuthorizationException,
                                                                                                  MetaMatrixComponentException {
        // Validate caller's session
        // SessionToken token =
        AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role
        try {
            return configAdmin.getAllComponentTypeDefinitions(componentTypeID);
        }  catch (RemoteException err) {
            throw new MetaMatrixComponentException(err);
        }
    }

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
     * @see #ComponentType
     */
    public synchronized Collection getMonitoredComponentTypes(boolean includeDeprecated) throws ConfigurationException,
                                                                                        InvalidSessionException,
                                                                                        AuthorizationException,
                                                                                        MetaMatrixComponentException {
        // Validate caller's session
        // SessionToken token =
        AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role
        try {
            return configAdmin.getMonitoredComponentTypes(includeDeprecated);
        }  catch (RemoteException err) {
            throw new MetaMatrixComponentException(err);
        }
    }

    /**
     * Returns a <code>ComponentType</code> for the specified <code>ComponentTypeID</code>
     * 
     * @param id
     *            is for the requested component type.
     * @return ComponentType based on the id
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     */
    public synchronized ComponentType getComponentType(ComponentTypeID id) throws ConfigurationException,
                                                                          InvalidSessionException,
                                                                          AuthorizationException,
                                                                          MetaMatrixComponentException {
        // Validate caller's session
        // SessionToken token =
        AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role
        try {
            return configAdmin.getComponentType(id);
        }  catch (RemoteException err) {
            throw new MetaMatrixComponentException(err);
        }
    }

    /**
     * Returns a <code>List</code> of type <code>ComponentType</code> that represents all the ComponentTypes defined.
     * 
     * @param includeDeprecated
     *            true if class names that have been deprecated should be included in the returned list, or false if only
     *            non-deprecated constants should be returned.
     * @return Collection of type <code>ComponentType</code>
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @see #ComponentType
     */
    public synchronized Collection getAllComponentTypes(boolean includeDeprecated) throws ConfigurationException,
                                                                                  InvalidSessionException,
                                                                                  AuthorizationException,
                                                                                  MetaMatrixComponentException {
        // Validate caller's session
        // SessionToken token =
        AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role
        try {
            return configAdmin.getAllComponentTypes(includeDeprecated);
        }  catch (RemoteException err) {
            throw new MetaMatrixComponentException(err);
        }
    }

    
    /**
     * Returns a <code>List</code> of type <code>ProductType</code> that represents all the ProductTypes defined.
     * 
     * @param includeDeprecated
     *            true if class names that have been deprecated should be included in the returned list, or false if only
     *            non-deprecated constants should be returned.
     * @return Collection of type <code>ProductType</code>
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @see #ProductType
     */
    public Collection getAllProductTypes(boolean includeDeprecated) throws ConfigurationException,
                                                                   InvalidSessionException,
                                                                   AuthorizationException,
                                                                   MetaMatrixComponentException {
        Iterator allTypes = getAllComponentTypes(includeDeprecated).iterator();
        ArrayList productTypes = new ArrayList();
        Object aType = null;
        while (allTypes.hasNext()) {
            aType = allTypes.next();
            if (aType instanceof ProductType) {
                productTypes.add(aType);
            }
        }
        return productTypes;
    }
    
    
    
    /**
     * Returns a <code>Host</code> for the specified <code>HostID</code>. </br>
     * 
     * @return Host
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     */
    public synchronized Host getHost(HostID hostID) throws ConfigurationException,
                                                   InvalidSessionException,
                                                   AuthorizationException,
                                                   MetaMatrixComponentException {
        // Validate caller's session
        // SessionToken token =
        AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role
        try {
            return configAdmin.getHost(hostID);
        }  catch (RemoteException err) {
            throw new MetaMatrixComponentException(err);
        }
    }

    /**
     * Returns a <code>Collection</code> of currently defined hosts. This method does not cache, it reretrieves the data
     * everytime. </br>
     * 
     * @return Collection of type Host
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     */
    public synchronized Collection getHosts() throws ConfigurationException,
                                                                                InvalidSessionException,
                                                                                AuthorizationException,
                                                                                MetaMatrixComponentException {
        // Validate caller's session
        // SessionToken token =
        AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role
        try {
            return configAdmin.getHosts();
        }  catch (RemoteException err) {
            throw new MetaMatrixComponentException(err);
        }
    }

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
    public synchronized Collection getComponentDefns(Collection componentDefnIDs,
                                                     ConfigurationID configurationID) throws ConfigurationException,
                                                                                     InvalidSessionException,
                                                                                     AuthorizationException,
                                                                                     MetaMatrixComponentException {
        // Validate caller's session
        // SessionToken token =
        AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role
        try {
            return configAdmin.getComponentDefns(componentDefnIDs, configurationID);
        }  catch (RemoteException err) {
            throw new MetaMatrixComponentException(err);
        }
    }

    public synchronized ComponentDefn getComponentDefn(ConfigurationID configurationID,
                                                       ComponentDefnID componentDefnID) throws ConfigurationException,
                                                                                       InvalidSessionException,
                                                                                       AuthorizationException,
                                                                                       MetaMatrixComponentException {

        // Validate caller's session
        // SessionToken token =
        AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role
        try {
            return configAdmin.getComponentDefn(configurationID, componentDefnID);
        }  catch (RemoteException err) {
            throw new MetaMatrixComponentException(err);
        }
    }

    /**
     * Returns a Collection of {@link com.metamatrix.common.config.api.ComponentType ComponentType} for all resource pool types of
     * a specified configuration.
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
    public synchronized Collection getResourcePoolTypes(ConfigurationID configurationID) throws ConfigurationException,
                                                                                        InvalidSessionException,
                                                                                        AuthorizationException,
                                                                                        MetaMatrixComponentException {

        // Validate caller's session
        // SessionToken token =
        AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role
        try {
            return configAdmin.getResourcePoolTypes(configurationID);
        }  catch (RemoteException err) {
            throw new MetaMatrixComponentException(err);
        }
    }

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
    public synchronized Collection getPoolableResourcePoolTypes(ConfigurationID configurationID) throws ConfigurationException,
                                                                                                InvalidSessionException,
                                                                                                AuthorizationException,
                                                                                                MetaMatrixComponentException {

        // Validate caller's session
        // SessionToken token =
        AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role
        try {
            return configAdmin.getPoolableResourcePoolTypes(configurationID);
        }  catch (RemoteException err) {
            throw new MetaMatrixComponentException(err);
        }
    }

    /**
     * Returns a Collection of {@link com.metamatrix.common.config.api.ResourceDescriptor ResourceDescriptor} for all resource
     * pools defined to the system.
     * 
     * @param configurationID
     *            is the configuration from which the component defns are to be derived
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    public synchronized Collection getResourcePools(ConfigurationID configurationID) throws ConfigurationException,
                                                                                    InvalidSessionException,
                                                                                    AuthorizationException,
                                                                                    MetaMatrixComponentException {

        // Validate caller's session
        // SessionToken token =
        AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role
        try {
            return configAdmin.getResourcePools(configurationID);
        }  catch (RemoteException err) {
            throw new MetaMatrixComponentException(err);
        }
    }

    /**
     * Returns a Collection of {@link com.metamatrix.common.config.api.ResourceDescriptor ResourceDescriptor} for all resource
     * pools of a specified type defined in a configuration.
     * 
     * @param configurationID
     *            is the configuration from which the component defns are to be derived
     * @param componentTypeID
     *            indicates the type of pools in the configuration to return
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    public synchronized Collection getResourcePools(ConfigurationID configurationID,
                                                    ComponentTypeID componentTypeID) throws ConfigurationException,
                                                                                    InvalidSessionException,
                                                                                    AuthorizationException,
                                                                                    MetaMatrixComponentException {

        // Validate caller's session
        // SessionToken token =
        AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role

        try {
            return configAdmin.getResourcePools(configurationID, componentTypeID);
        }  catch (RemoteException err) {
            throw new MetaMatrixComponentException(err);
        }
    }

    /**
     * Returns a Collection of {@link com.metamatrix.common.config.api.ResourceDescriptor ResourceDescriptor} for all internal
     * resources defined to the system. The internal resources are not managed with the other configuration related information.
     * They are not dictated based on which configuration they will operate (i.e., next startup or operational);
     * 
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    public synchronized Collection getResources() throws ConfigurationException,
                                                                                    InvalidSessionException,
                                                                                    AuthorizationException,
                                                                                    MetaMatrixComponentException {

        // Validate caller's session
        // SessionToken token =
        AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role
        try {
            return configAdmin.getResources();
        }  catch (RemoteException err) {
            throw new MetaMatrixComponentException(err);
        }
    }

    /**
     * Returns a Collection of {@link com.metamatrix.common.config.api.ResourceDescriptor ResourceDescriptor} that are of the
     * specified resource type.
     * 
     * @param componentTypeID
     *            that identifies the type of resources to be returned
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    public synchronized Collection getResources(ComponentTypeID componentTypeID) throws ConfigurationException,
                                                                                InvalidSessionException,
                                                                                AuthorizationException,
                                                                                MetaMatrixComponentException {
        // Validate caller's session
        // SessionToken token =
        AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role
        try {
            return configAdmin.getResources(componentTypeID);
        }  catch (RemoteException err) {
            throw new MetaMatrixComponentException(err);
        }
    }

    /**
     * Save the resource changes based on each {@link com.metamatrix.common.config.api.ResourceDescriptor ResourceDescriptor} in
     * the collection.
     * 
     * @param resourceDescriptors
     *            for the resources to be changed
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    public synchronized void saveResources(Collection resourceDescriptors) throws ConfigurationException,
                                                                          InvalidSessionException,
                                                                          AuthorizationException,
                                                                          MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role
        try {
            configAdmin.saveResources(resourceDescriptors, token.getUsername());
        }  catch (RemoteException err) {
            throw new MetaMatrixComponentException(err);
        }
    }
    

    

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
     *             if an error occurred within or during communication with the Metadata Service.
     */
    public synchronized Set executeTransaction(ActionDefinition action) throws ModificationException,
                                                                       ConfigurationLockException,
                                                                       ConfigurationException,
                                                                       InvalidSessionException,
                                                                       AuthorizationException,
                                                                       MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(token, AdminRoles.RoleName.ADMIN_SYSTEM, "ConfigurationAdminAPIImpl.executeTransaction(" + action + ")"); //$NON-NLS-1$ //$NON-NLS-2$
        try {
            return configAdmin.executeTransaction(action, token.getUsername());
        }  catch (RemoteException err) {
            throw new MetaMatrixComponentException(err);
        }
    }

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
     *             if an error occurred within or during communication with the Metadata Service.
     */
    public synchronized Set executeTransaction(List actions) throws ModificationException,
                                                            ConfigurationLockException,
                                                            ConfigurationException,
                                                            InvalidSessionException,
                                                            AuthorizationException,
                                                            MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(token, AdminRoles.RoleName.ADMIN_SYSTEM, "ConfigurationAdminAPIImpl.executeTransaction(" + actions + ")"); //$NON-NLS-1$ //$NON-NLS-2$
        try {
            return configAdmin.executeTransaction(actions, token.getUsername());
        }  catch (RemoteException err) {
            throw new MetaMatrixComponentException(err);
        }
    }

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
     * @return the set of objects that were affected by this transaction.
     * @throws ModificationException
     *             if the target of any of the actions is invalid, or an action that is not an insert, or if the target object is
     *             not a supported class of targets.
     * @throws IllegalArgumentException
     *             if the action is null or if the result specification is invalid
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Metadata Service.
     */
    public synchronized Set executeInsertTransaction(ConfigurationID assignConfigurationID,
                                                     List actions) throws ModificationException,
                                                                  ConfigurationLockException,
                                                                  ConfigurationException,
                                                                  InvalidSessionException,
                                                                  AuthorizationException,
                                                                  MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(token, AdminRoles.RoleName.ADMIN_SYSTEM, "ConfigurationAdminAPIImpl.executeInsertTransaction(" + assignConfigurationID + ", " + actions + ")"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        try {
            return configAdmin.executeInsertTransaction(assignConfigurationID, actions, token.getUsername());
        }  catch (RemoteException err) {
            throw new MetaMatrixComponentException(err);
        }
    }

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
     *             if an error occurred within or during communication with the Metadata Service.
     */
    public synchronized Set undoActionsAsTransaction(int numberOfActions) throws ConfigurationException,
                                                                         InvalidSessionException,
                                                                         AuthorizationException,
                                                                         MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(token, AdminRoles.RoleName.ADMIN_SYSTEM, "ConfigurationAdminAPIImpl.undoActionsAsTransaction(" + numberOfActions + ")"); //$NON-NLS-1$ //$NON-NLS-2$
        try {
            return configAdmin.undoActionsAsTransaction(numberOfActions, token.getUsername());
        }  catch (RemoteException err) {
            throw new MetaMatrixComponentException(err);
        }
    }

    /**
     * Get the history of actions executed in transactions by this editor. The actions at the front of the list will be those most
     * recently executed.
     * 
     * @return the ordered list of actions in the history.
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Metadata Service.
     */
    public synchronized List getHistory() throws ConfigurationException,
                                                                            InvalidSessionException,
                                                                            AuthorizationException,
                                                                            MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(token, AdminRoles.RoleName.ADMIN_SYSTEM, "ConfigurationAdminAPIImpl.getHistory()"); //$NON-NLS-1$
        try {
            return configAdmin.getHistory();
        }  catch (RemoteException err) {
            throw new MetaMatrixComponentException(err);
        }
    }

    /**
     * Clear the history of all actions without undoing any of them.
     * 
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Metadata Service.
     */
    public synchronized void clearHistory() throws ConfigurationException,
                                                                              InvalidSessionException,
                                                                              AuthorizationException,
                                                                              MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(token, AdminRoles.RoleName.ADMIN_SYSTEM, "ConfigurationAdminAPIImpl.clearHistory()"); //$NON-NLS-1$
        try {
            configAdmin.clearHistory();
        }  catch (RemoteException err) {
            throw new MetaMatrixComponentException(err);
        }
    }

    /**
     * Get the number of actions that are currently in the history.
     * 
     * @return the number of actions in the history.
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Metadata Service.
     */
    public synchronized int getHistorySize() throws ConfigurationException,
                                                                               InvalidSessionException,
                                                                               AuthorizationException,
                                                                               MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(token, AdminRoles.RoleName.ADMIN_SYSTEM, "ConfigurationAdminAPIImpl.getHistorySize()"); //$NON-NLS-1$
        try {
            return configAdmin.getHistorySize();
        }  catch (RemoteException err) {
            throw new MetaMatrixComponentException(err);
        }
    }

    /**
     * Set the limit on the number of actions in the history. Note that the history may at times be greater than this limit,
     * because when actions are removed from the history, all actions for a transactions are removed at the same time. If doing so
     * would make the history size smaller than the limit, no actions are removed.
     * 
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Metadata Service.
     */
    public synchronized int getHistoryLimit() throws ConfigurationException,
                                                                                InvalidSessionException,
                                                                                AuthorizationException,
                                                                                MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(token, AdminRoles.RoleName.ADMIN_SYSTEM, "ConfigurationAdminAPIImpl.getHistoryLimit()"); //$NON-NLS-1$
        try {
            return configAdmin.getHistoryLimit();
        }  catch (RemoteException err) {
            throw new MetaMatrixComponentException(err);
        }
    }

    /**
     * Set the limit on the number of actions in the history. Note that the history may at times be greater than this limit,
     * because when actions are removed from the history, all actions for a transactions are removed at the same time. If doing so
     * would make the history size smaller than the limit, no actions are removed.
     * 
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Metadata Service.
     */
    public synchronized void setHistoryLimit(int maximumHistoryCount) throws ConfigurationException,
                                                                     InvalidSessionException,
                                                                     AuthorizationException,
                                                                     MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(token, AdminRoles.RoleName.ADMIN_SYSTEM, "ConfigurationAdminAPIImpl.setHistoryLimit(" +maximumHistoryCount + ")"); //$NON-NLS-1$ //$NON-NLS-2$
        try {
            configAdmin.setHistoryLimit(maximumHistoryCount);
        }  catch (RemoteException err) {
            throw new MetaMatrixComponentException(err);
        }
    }

    /**
     * Execute an update to immediately apply the changes to the
     * {@link com.metamatrix.common.pooling.api.ResourcePool ResourcePool} identified by the
     * {@link com.metamatrix.common.config.api.ResourceDescriptorID ID}.
     * 
     * @param resourcePoolID
     *            identifies the resource pool for which the changes will be applied
     * @param properties
     *            are the changes to be applied to the resource pool
     * @throws ResourcePoolException
     *             if an error occurs applying the changes to the resource pool
     * @throws IllegalArgumentException
     *             if the action is null or if the result specification is invalid
     * @throws AuthorizationException
     *             if the user is not authorized to make the changes
     */
    public synchronized void executeUpdateTransaction(ResourceDescriptorID resourcePoolID,
                                                      Properties properties) throws ResourcePoolException,
                                                                            InvalidSessionException,
                                                                            AuthorizationException,
                                                                            MetaMatrixComponentException,
                                                                            RemoteException {

        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(token, AdminRoles.RoleName.ADMIN_SYSTEM, "ConfigurationAdminAPIImpl.executeUpdateTransaction(" + resourcePoolID + ", " + properties + ")"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

        Iterator poolIter = registry.getResourcePoolManagerBindings(null,null).iterator();
        while (poolIter.hasNext()) {
            ResourcePoolMgrBinding binding = (ResourcePoolMgrBinding)poolIter.next();
            ResourcePoolMgr mgr = binding.getResourcePoolMgr();
            mgr.updateResourcePool(resourcePoolID, properties);

        }

    }

    /**
     * @see com.metamatrix.platform.admin.apiimpl.ConfigurationAdminAPI#addHost(java.lang.String, java.util.Properties)
     * @since 4.3
     */
    public synchronized Host addHost(String hostName,
                                     Properties properties) throws ConfigurationException,
                                                           InvalidSessionException,
                                                           AuthorizationException,
                                                           MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(token, AdminRoles.RoleName.ADMIN_SYSTEM, "ConfigurationAdminAPIImpl.addHost(" + hostName + ", " + properties + ")"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        try {
            return configAdmin.addHost(hostName, token.getUsername(), properties);
        }  catch (RemoteException err) {
            throw new MetaMatrixComponentException(err);
        }
    }
    
    
    
    
    /**
     * @see com.metamatrix.platform.admin.api.ConfigurationAdminAPI#checkPropertiesDecryptable(java.util.List)
     * @since 4.3
     */
    public synchronized List checkPropertiesDecryptable(List defns) throws ConfigurationException,
                                                           InvalidSessionException,
                                                           AuthorizationException,
                                                           MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(token, AdminRoles.RoleName.ADMIN_SYSTEM, "ConfigurationAdminAPIImpl.checkPropertiesDecryptable(" + defns + ")"); //$NON-NLS-1$ //$NON-NLS-2$
        try {
            return configAdmin.checkPropertiesDecryptable(defns);
        }  catch (RemoteException err) {
            throw new MetaMatrixComponentException(err);
        }
    }
    
    
    
    /**
     * @see com.metamatrix.platform.admin.api.ConfigurationAdminAPI#getProducts(com.metamatrix.platform.security.api.MetaMatrixSessionID)
     * @since 4.2
     */
    public Collection getProducts() throws AuthorizationException,
                                                                InvalidSessionException,
                                                                MetaMatrixComponentException {
        Collection result = null;
        try {
            result = configAdmin.getProductReleaseInfos();
        } catch (ServiceException e) {
            LogManager.logError(PlatformAdminLogConstants.CTX_ADMIN_API, e, PlatformPlugin.Util.getString("ConfigurationAdminAPIImpl.Problem_getting_Product_Release_Infos", e.getMessage())); //$NON-NLS-1$
            throw new ComponentNotFoundException(e, e.getMessage());
        } catch (ConfigurationException e) {
            LogManager.logError(PlatformAdminLogConstants.CTX_ADMIN_API, e, PlatformPlugin.Util.getString("ConfigurationAdminAPIImpl.Problem_getting_Product_Release_Infos", e.getMessage())); //$NON-NLS-1$
            throw new ComponentCommunicationException(e, e.getMessage()); 
        } catch (RemoteException e) {
            LogManager.logError(PlatformAdminLogConstants.CTX_ADMIN_API, e, PlatformPlugin.Util.getString("ConfigurationAdminAPIImpl.Problem_getting_Product_Release_Infos", e.getMessage())); //$NON-NLS-1$
            throw new ComponentCommunicationException(e, e.getMessage());
        }
        return result;
    }
}
