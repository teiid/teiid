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

package com.metamatrix.platform.config.service;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.api.exception.security.InvalidSessionException;
import com.metamatrix.common.actions.ActionDefinition;
import com.metamatrix.common.actions.AddObject;
import com.metamatrix.common.actions.CreateObject;
import com.metamatrix.common.actions.ModificationException;
import com.metamatrix.common.config.ProductReleaseInfoUtil;
import com.metamatrix.common.config.api.ComponentDefn;
import com.metamatrix.common.config.api.ComponentDefnID;
import com.metamatrix.common.config.api.ComponentObject;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ComponentTypeDefn;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.ConfigurationObjectEditor;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.HostID;
import com.metamatrix.common.config.api.ProductServiceConfig;
import com.metamatrix.common.config.api.ProductServiceConfigID;
import com.metamatrix.common.config.api.ResourceDescriptor;
import com.metamatrix.common.config.api.ServiceComponentDefnID;
import com.metamatrix.common.config.api.SharedResource;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.config.api.VMComponentDefnID;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.api.exceptions.ConfigurationLockException;
import com.metamatrix.common.config.api.exceptions.InvalidArgumentException;
import com.metamatrix.common.config.api.exceptions.InvalidConfigurationException;
import com.metamatrix.common.config.model.BasicConfigurationObjectEditor;
import com.metamatrix.common.config.model.ComponentCryptoUtil;
import com.metamatrix.common.config.xml.XMLConfigurationImportExportUtility;
import com.metamatrix.common.connection.ManagedConnectionException;
import com.metamatrix.common.connection.TransactionMgr;
import com.metamatrix.common.log.I18nLogManager;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.namedobject.BaseID;
import com.metamatrix.common.util.LogCommonConstants;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.platform.PlatformPlugin;
import com.metamatrix.platform.config.api.service.ConfigurationServiceInterface;
import com.metamatrix.platform.config.api.service.ConfigurationServicePropertyNames;
import com.metamatrix.platform.config.spi.ConfigurationTransaction;
import com.metamatrix.platform.config.spi.SystemConfigurationNames;
import com.metamatrix.platform.service.api.exception.ServiceException;
import com.metamatrix.platform.service.controller.AbstractService;
import com.metamatrix.platform.util.ErrorMessageKeys;
import com.metamatrix.platform.util.LogMessageKeys;


/**
*
*  Caching only Hosts and ComponentTypes - but these will be updated by a scheduled thread
*/

public class ConfigurationServiceImpl extends AbstractService implements ConfigurationServiceInterface {

  /**
    * The transaction mgr for ManagedConnections.
    */
    private TransactionMgr transMgr;

//    private int sessionCount;
    private ActionHistory actionHistory = new ActionHistory();

    /**
     * Flag denoting whether this service is closed and may not accept new work.
     */
//    private boolean serviceIsClosed = false;

    private static final String CONTEXT = LogCommonConstants.CTX_CONFIG;

    private static BasicConfigurationObjectEditor editor = new BasicConfigurationObjectEditor(false);


    public ConfigurationServiceImpl() {
        super();
    }

   // -----------------------------------------------------------------------------------
    //                 S E R V I C E - R E L A T E D    M E T H O D S
    // -----------------------------------------------------------------------------------

    public void initializeForTesting(Properties env) throws Exception {
        this.initService(env);
    }

    /**
     * Perform initialization and commence processing. This method is called only once.
     */
    protected void initService(Properties env) throws Exception {

        try {

   /*
            // Create the properties for the connections ...
            Properties props = new Properties();
            this.addProperty(env, ConfigurationServicePropertyNames.CONNECTION_DRIVER,   props, ManagedConnection.DRIVER );
            this.addProperty(env, ConfigurationServicePropertyNames.CONNECTION_PROTOCOL, props, ManagedConnection.PROTOCOL );
            this.addProperty(env, ConfigurationServicePropertyNames.CONNECTION_DATABASE, props, ManagedConnection.DATABASE );
            this.addProperty(env, ConfigurationServicePropertyNames.CONNECTION_USERNAME, props, ManagedConnection.USERNAME );
            this.addProperty(env, ConfigurationServicePropertyNames.CONNECTION_PASSWORD, props, ManagedConnection.PASSWORD );
            spiConnectionProperties = new UnmodifiableProperties( props );

            // Create the properties for the connection pool ...
            props = new Properties();
            this.addProperty(env, ConfigurationServicePropertyNames.CONNECTION_FACTORY,                       props, ManagedConnectionPool.FACTORY );
            this.addProperty(env, ConfigurationServicePropertyNames.CONNECTION_POOL_MAXIMUM_AGE,              props, ManagedConnectionPool.MAXIMUM_AGE );
            this.addProperty(env, ConfigurationServicePropertyNames.CONNECTION_POOL_MAXIMUM_CONCURRENT_USERS, props, ManagedConnectionPool.MAXIMUM_CONCURRENT_USERS );
            Properties poolEnvironment = new UnmodifiableProperties( props );

            // Create the connection pool instance
            this.connectionPool = new ManagedConnectionPool(poolEnvironment,spiConnectionProperties);
*/
            env.setProperty(TransactionMgr.FACTORY, env.getProperty(ConfigurationServicePropertyNames.CONNECTION_FACTORY));
//            this.addProperty(env, ConfigurationServicePropertyNames.CONNECTION_FACTORY, env, TransactionMgr.FACTORY );

            transMgr = new TransactionMgr(env, this.getInstanceName());

            I18nLogManager.logInfo(CONTEXT, LogMessageKeys.CONFIG_0002, new Object[] { getInstanceName()});

        } catch ( ManagedConnectionException e ) {
            throw new ConfigurationException(e);
        }
    }

    /**
     * Close the service to new work if applicable. After this method is called
     * the service should no longer accept new work to perform but should continue
     * to process any outstanding work. This method is called by die().
     */
    protected void closeService() throws Exception {
        String instanceName = this.getInstanceName().toString();
        LogManager.logDetail(CONTEXT, instanceName + ": closing"); //$NON-NLS-1$
//        this.serviceIsClosed = true;
        I18nLogManager.logInfo(CONTEXT, LogMessageKeys.CONFIG_0003, new Object[]{ instanceName});
    }

    /**
     * Wait until the service has completed all outstanding work. This method
     * is called by die() just before calling dieNow().
     */
    protected void waitForServiceToClear() throws Exception {
    }

    /**
     * Terminate all processing and reclaim resources. This method is called by dieNow()
     * and is only called once.
     */
    protected void killService() {
//        this.sessionCount = 0;
    }


    public ConfigurationObjectEditor createEditor() throws ConfigurationException {
        return new BasicConfigurationObjectEditor(true);
    }

    /**
     * Returns the ID of the operational <code>Configuration</code>, which should reflect
     * the desired runtime state of the system.
     * @return ID of operational configuration
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    public ConfigurationID getCurrentConfigurationID() throws ConfigurationException {
        return this.getDesignatedConfigurationID(Configuration.NEXT_STARTUP);
    }

    /**
     * Returns the ID of the next startup <code>Configuration</code>, which should reflect
     * the desired runtime state of the system.
     * @return ID of next startup configuration
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    public ConfigurationID getNextStartupConfigurationID() throws ConfigurationException {
        return this.getDesignatedConfigurationID(Configuration.NEXT_STARTUP);
    }

    /**
     * Baselines the realtime portion of the current (operational) configuration into the
     * next-startup configuration.
     * @param principalName the name of the principal that is requesting the
     * baselining
     */
    public void baselineCurrentConfiguration(String principalName) throws ConfigurationException {
        throw new UnsupportedOperationException(PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0039));

    }

    /**
     * Returns the ID of the startup <code>Configuration</code>, which should reflect
     * the desired runtime state of the system.
     * @return ID of startup configuration
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    public ConfigurationID getStartupConfigurationID() throws ConfigurationException {
        return this.getDesignatedConfigurationID(SystemConfigurationNames.STARTUP);
    }

    /**
     * Returns the operational <code>Configuration</code>, which should reflect
     * the desired runtime state of the system.
     * @return Configuration that is currently in use
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    public Configuration getCurrentConfiguration() throws ConfigurationException {
        return this.getDesignatedConfiguration(SystemConfigurationNames.NEXT_STARTUP);
    }

    /**
     * Returns the next startup <code>Configuration</code>, the Configuration
     * that the system will next boot up with (once it is entirely shut down).
     * @return Configuration that the system will next start up with.
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    public Configuration getNextStartupConfiguration() throws ConfigurationException{
        return this.getDesignatedConfiguration(SystemConfigurationNames.NEXT_STARTUP);
    }

    /**
     * Returns the startup <code>Configuration</code>, the Configuration
     * that the system booted up with.
     * @return Configuration that the system booted up with.
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    public Configuration getStartupConfiguration() throws ConfigurationException{
        return this.getDesignatedConfiguration(SystemConfigurationNames.STARTUP);
    }

    private Configuration getDesignatedConfiguration(String designation) throws ConfigurationException {
        // Look in the cache ...
        Configuration config = null;

        ConfigurationTransaction transaction = null;
        try {
            transaction = getReadTransaction();
            config = transaction.getDesignatedConfiguration(designation);

            if (config != null) {
                LogManager.logDetail(CONTEXT, "Found " + designation + " configuration " + config.getName()); //$NON-NLS-1$ //$NON-NLS-2$
            } else {
                LogManager.logDetail(CONTEXT, "No " + designation + " configuration found "); //$NON-NLS-1$ //$NON-NLS-2$
                throw new ConfigurationException("No " + designation + " configuration was found"); //$NON-NLS-1$ //$NON-NLS-2$
            }

        } catch ( ManagedConnectionException e ) {
            throw new ConfigurationException(e,ErrorMessageKeys.CONFIG_0040, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0040, designation));
        } finally {
            if ( transaction != null ) {
                try {
                    transaction.close();
                } catch ( Exception txne ) {
					I18nLogManager.logError(CONTEXT,  ErrorMessageKeys.CONFIG_0041, txne);
                }
                transaction = null;
            }
        }

        return config;
    }

    private ConfigurationID getDesignatedConfigurationID(String designation) throws ConfigurationException {
        ConfigurationID configID = null;
        ConfigurationTransaction transaction = null;
        try {
            transaction = getReadTransaction();
            configID = transaction.getDesignatedConfigurationID(designation);

            if (configID != null) {
                LogManager.logTrace(CONTEXT, "Found " + designation + " configuration id " + configID); //$NON-NLS-1$ //$NON-NLS-2$
            } else {
                throw new ConfigurationException(ErrorMessageKeys.CONFIG_0042, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0042));
            }

        } catch ( ManagedConnectionException e ) {
            throw new ConfigurationException(e,ErrorMessageKeys.CONFIG_0042, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0042));
        } finally {
            if ( transaction != null ) {
                try {
                    transaction.close();
                } catch ( Exception txne ) {
					I18nLogManager.logError(CONTEXT, ErrorMessageKeys.CONFIG_0041, txne);
                }
                transaction = null;
            }
        }
        return configID;
    }

    public ConfigurationModelContainer getConfigurationModel(String configName) throws InvalidConfigurationException, ConfigurationException {
        // Look in the cache ...
        ConfigurationModelContainer config = null;

        ConfigurationTransaction transaction = null;
        try {
            transaction = getReadTransaction();
            config = transaction.getConfigurationModel(configName);

            if (config != null) {
            } else {
                LogManager.logTrace(CONTEXT, "No configuration model found"); //$NON-NLS-1$
            }

        }catch ( Exception e ) {
        	throw new ConfigurationException(e,ErrorMessageKeys.CONFIG_0043, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0043));
        }finally {
            if ( transaction != null ) {
                try {
                    transaction.close();
                } catch ( Exception txne ) {
                    I18nLogManager.logError(CONTEXT, ErrorMessageKeys.CONFIG_0041, txne);
                }
                transaction = null;
            }
        }
        return config;

	}

    public Configuration getConfiguration(String configName) throws InvalidConfigurationException, ConfigurationException  {
        if ( configName == null) {
            throw new IllegalArgumentException(PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0045, "configName")); //$NON-NLS-1$
        }

        // Look in the cache ...
        Configuration config = null;

        ConfigurationTransaction transaction = null;
        try {
            transaction = getReadTransaction();
            config = transaction.getConfiguration(configName);

            if (config != null) {
                LogManager.logDetail(CONTEXT, "Found current configuration " + configName); //$NON-NLS-1$
            } else {
                LogManager.logTrace(CONTEXT, "No current configuration found for " + configName); //$NON-NLS-1$
            }

        }catch ( ManagedConnectionException e ) {
            throw new ConfigurationException(e,ErrorMessageKeys.CONFIG_0044, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0044, configName));
        }finally {
            if ( transaction != null ) {
                try {
                    transaction.close();
                } catch ( Exception txne ) {
                    I18nLogManager.logError(CONTEXT, ErrorMessageKeys.CONFIG_0041, txne);
                }
                transaction = null;
            }
        }
        return config;
    }

    /**
     * <p>Returns a Collection containing the Configuration object for the specified
     * ConfigurationID id, and also any dependant objects needed to fully
     * define this configuration, such as Host objects, ComponentType
     * objects, and ComponentTypeDefn objects.</p>
     *
     * <p>A Configuration instance contains all of the
     * <code>ComponentDefn</code> objects that "belong" to just that
     * Configuration model: VM component definitions, service
     * component definitions, product service configurations, and
     * deployed components.  Objects such as Host objects,
     * ComponentType objects, ComponentTypeDefn, Resources, and
     * ConnectorBinding objects describe or support
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
     * ConnectorBinding instance1
     * ConnectorBinding instance2
     * ...
     * SharedResource intance1
     * SharedResource instance
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
     */
    public Collection getConfigurationAndDependents(ConfigurationID configID) throws ConfigurationException {
        if ( configID == null) {
            throw new IllegalArgumentException(PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0045, "configID")); //$NON-NLS-1$
        }

        // Look in the cache ...
        Collection result = null;

        ConfigurationTransaction transaction = null;
        try {
            transaction = getReadTransaction();
            result = transaction.getAllObjectsForConfigurationModel(configID);

			return result;

        }catch ( ManagedConnectionException e ) {
            throw new ConfigurationException(e,ErrorMessageKeys.CONFIG_0046, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0046, configID));
        }finally {
            if ( transaction != null ) {
                try {
                    transaction.close();
                } catch ( Exception txne ) {
                    I18nLogManager.logError(CONTEXT, ErrorMessageKeys.CONFIG_0041, txne);
                }
                transaction = null;
            }
        }

//        throw new ConfigurationException("Unable to find the configuration and dependent objects from " + configID);
    }

    /**
    * <p>This method will return a Collection of objects that represent the
    * set of global configuration objects currently represented in the
    * configuration database.  This method will generally be used when
    * attempting to import a configuration into the database as the 'Next Startup'
    * configuration.  This information is important when importing a new configuration
    * so that any global type configuration objects that are to be imported can
    * be resolved against the global objects that currently exist in the
    * database.</p>
    *
    * <pre>
    * The Collection of objects will contain the following configuration
    * object types:
    *
    * ComponentTypes
    * ProductTypes
    * Hosts
    * </pre>
    *
    * @return a Collection of all of the global configuration objects as they
    * exist in the database.
    * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
    * @throws InvalidSessionException if there is not a valid administrative session
    * @throws AuthorizationException if the administrator does not have privileges to use this method
    * @throws MetaMatrixComponentException if a general remote system problem occurred
    */
    public Collection getAllGlobalConfigObjects()
    throws ConfigurationException{
        Collection allObjects = new ArrayList();
        ConfigurationTransaction transaction = null;
        try {
            transaction = getReadTransaction();
            allObjects.addAll(transaction.getAllComponentTypes(true));
            allObjects.addAll(transaction.getProductTypes(true));
            allObjects.addAll(transaction.getHosts());
        } catch ( ManagedConnectionException e ) {
            throw new ConfigurationException(e,ErrorMessageKeys.CONFIG_0047, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0047));
        } finally {
            if ( transaction != null ) {
                try {
                    transaction.close();
                } catch ( Exception txne ) {
                    I18nLogManager.logError(CONTEXT, ErrorMessageKeys.CONFIG_0041, txne);
                }
                transaction = null;
            }
        }
        return allObjects;


    }
    public ComponentType getComponentType(ComponentTypeID id) throws ConfigurationException {
        if ( id == null) {
            throw new IllegalArgumentException(PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0045, "id")); //$NON-NLS-1$
        }

        ComponentType type = null;
        ConfigurationTransaction transaction = null;
        try {
            transaction = getReadTransaction();
            type = transaction.getComponentType(id);

            if (type != null) {
                LogManager.logDetail(CONTEXT, "Found component type " + id); //$NON-NLS-1$
            } else {
                LogManager.logDetail(CONTEXT, "No component type found for " + id); //$NON-NLS-1$
            }

        } catch ( ManagedConnectionException e ) {
            throw new ConfigurationException(e,ErrorMessageKeys.CONFIG_0048, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0048, "id")); //$NON-NLS-1$
        } finally {
            if ( transaction != null ) {
                try {
                    transaction.close();
                } catch ( Exception txne ) {
                    I18nLogManager.logError(CONTEXT, ErrorMessageKeys.CONFIG_0041, txne);
                }
                transaction = null;
            }
        }
        return type;

    }

    public Collection getAllComponentTypes(boolean includeDeprecated) throws ConfigurationException {
        ConfigurationTransaction transaction = null;
        Collection result = new LinkedList();
        try {
            transaction = getReadTransaction();
            result = transaction.getAllComponentTypes(includeDeprecated);

            if (result != null && result.size() > 0) {
               LogManager.logDetail(CONTEXT, "Found all component types"); //$NON-NLS-1$

            } else {
               throw new ConfigurationException(ErrorMessageKeys.CONFIG_0049, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0049));
            }

        } catch ( ManagedConnectionException e ) {
            throw new ConfigurationException(e,ErrorMessageKeys.CONFIG_0049, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0049));
        } finally {
           if ( transaction != null ) {
                try {
                    transaction.close();
                } catch ( Exception txne ) {
                    I18nLogManager.logError(CONTEXT, ErrorMessageKeys.CONFIG_0041, txne);
                }
                transaction = null;
            }
        }
        return result;

    }

    public Collection getMonitoredComponentTypes(boolean includeDeprecated) throws ConfigurationException {
        ConfigurationTransaction transaction = null;
        Collection result = null;
        try {
            transaction = getReadTransaction();
            result = transaction.getMonitoredComponentTypes(includeDeprecated);

            if (result != null && result.size() > 0) {
               LogManager.logDetail(CONTEXT, "Found monitored component types"); //$NON-NLS-1$

            } else {
               LogManager.logTrace(CONTEXT, "No monitored component types found"); //$NON-NLS-1$
               result = new ArrayList(1);
            }

        } catch ( ManagedConnectionException e ) {
            throw new ConfigurationException(e,ErrorMessageKeys.CONFIG_0050, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0050));
        } finally {
            if ( transaction != null ) {
                try {
                    transaction.close();
                } catch ( Exception txne ) {
                    I18nLogManager.logError(CONTEXT, ErrorMessageKeys.CONFIG_0041, txne);
                }
                transaction = null;
            }
        }
        return result;
    }

    public Collection getComponentTypeDefinitions(ComponentTypeID componentTypeID) throws ConfigurationException {
        ConfigurationTransaction transaction = null;
        Collection result=null;
        try {
            transaction = getReadTransaction();
            result = transaction.getComponentTypeDefinitions(componentTypeID);

            if (result != null && result.size() > 0) {
                LogManager.logDetail(CONTEXT, new Object[] {"Found component type definitions for ", componentTypeID} ); //$NON-NLS-1$

            } else {
                LogManager.logTrace(CONTEXT, new Object[] {"Couldn't find component type definitions for ", componentTypeID} ); //$NON-NLS-1$
            }

        } catch ( ManagedConnectionException e ) {
            throw new ConfigurationException(e,ErrorMessageKeys.CONFIG_0051, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0051, componentTypeID));
        } finally {
            if ( transaction != null ) {
                try {
                    transaction.close();
                } catch ( Exception txne ) {
                    I18nLogManager.logError(CONTEXT, ErrorMessageKeys.CONFIG_0041, txne);
                }
                transaction = null;
            }
        }

        if (result == null) {
            result = new ArrayList(1);
        }

        return result;

    }


    public Map getComponentTypeDefinitions(Collection componentIDs) throws ConfigurationException {
        Map map = new HashMap();
        Collection defns;
        BaseID id = null ;
        ComponentType type;
        Iterator it = componentIDs.iterator();
        while (it.hasNext()) {
            Object obj = it.next();
            if (obj instanceof ComponentTypeID) {
                id = (ComponentTypeID) obj;
            } else if (obj instanceof ComponentType) {
                type = (ComponentType) obj;
                id = type.getID();
            } else {
                continue;
            }

            defns = getComponentTypeDefinitions( (ComponentTypeID) id);
            map.put(id, defns);
        }

        return map;
    }

    public Map getDependentComponentTypeDefinitions(Collection componentIDs) throws ConfigurationException {
        ConfigurationTransaction transaction = null;
        Map map = new HashMap(componentIDs.size());
        BaseID id = null;
        ComponentType type;

        try {
            transaction = getReadTransaction();
            Iterator it = componentIDs.iterator();
            while (it.hasNext()) {

                Object obj = it.next();
                if (obj instanceof ComponentTypeID) {
                    id = (ComponentTypeID) obj;
                } else if (obj instanceof ComponentType) {
                    type = (ComponentType) obj;
                    id = type.getID();
                } else {
                    continue;
                }

                Collection defns = getDependentComponentTypeDefinitions(transaction, (ComponentTypeID) id);
                map.put(id, defns);
            }

        } catch ( ManagedConnectionException e ) {
            throw new ConfigurationException(e,ErrorMessageKeys.CONFIG_0052, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0052));
        } finally {
           if ( transaction != null ) {
                try {
                    transaction.close();
                } catch ( Exception txne ) {
                    I18nLogManager.logError(CONTEXT, ErrorMessageKeys.CONFIG_0041, txne);
                }
                transaction = null;
            }
        }

        return map;

    }

    public Collection getDependentComponentTypeDefinitions(ComponentTypeID componentTypeID) throws ConfigurationException {
        ConfigurationTransaction transaction = null;
        try {
            transaction = getReadTransaction();
            Collection defns = getDependentComponentTypeDefinitions(transaction, componentTypeID);
            return defns;

        } catch ( ManagedConnectionException e ) {
			throw new ConfigurationException(e,ErrorMessageKeys.CONFIG_0052, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0052));
        } finally {
           if ( transaction != null ) {
                try {
                    transaction.close();
                } catch ( Exception txne ) {
                    I18nLogManager.logError(CONTEXT, ErrorMessageKeys.CONFIG_0041, txne);
                }
                transaction = null;
           }
        }
    }

    private Collection getDependentComponentTypeDefinitions(ConfigurationTransaction transaction, ComponentTypeID componentTypeID) throws ConfigurationException {

        Collection result=null;

        Collection types = transaction.getAllComponentTypes(false);

        result = getSuperComponentTypeDefinitions(null, null, types, componentTypeID, transaction);

        if (result != null && result.size() > 0) {
            LogManager.logDetail(CONTEXT, new Object[] {"Found dependent component type definitions for ", componentTypeID} ); //$NON-NLS-1$
        } else {
            result = new ArrayList(1);
        }

        return result;

    }

    /**
     * This method calls itself recursively to return a Collection of all
     * ComponentTypeDefn objects defined for the super-type of the
     * componentTypeID parameter.  The equality of each PropertyDefn object contained
     * within each ComponentTypeDefn is the criteria to determine if a
     * defn exists in the sub-type already, or not.
     * @param defnMap Map of PropertyDefn object to the ComponentTypeDefn
     * object containing that PropertyDefn
     * @param defns return-by-reference Collection, built recursively
     * @param componentTypes Collection of all possible ComponentType
     * objects
     * @param componentTypeID the type for which super-type ComponentTypeDefn
     * objects are sought
     * @param transaction The transaction to operate within
     * @return Collection of all super-type ComponentTypeDefn objects (which
     * are <i>not</i> overridden by sup-types)
     */
    private Collection getSuperComponentTypeDefinitions(Map defnMap, Collection defns,
                                                Collection componentTypes,
                                                ComponentTypeID componentTypeID,
                                                ConfigurationTransaction transaction) throws ConfigurationException {
        if (defnMap == null) {
            defnMap = new HashMap();
        }

        if (defns == null) {
            defns = new ArrayList();
        }
        ComponentType type = null;
        for (Iterator it = componentTypes.iterator(); it.hasNext(); ) {
            ComponentType ct = (ComponentType) it.next();
            if (componentTypeID.equals(ct.getID())) {
                type = ct;
            }
        }

        if (type == null) {
            throw new ConfigurationException(ErrorMessageKeys.CONFIG_0053, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0053, componentTypeID));
        }

        if (type.getSuperComponentTypeID() == null) {
            return defns;
        }

        Collection superDefns = transaction.getComponentTypeDefinitions(type.getSuperComponentTypeID());
        // add the defns not already defined to the map
//            BaseID id;
        ComponentTypeDefn sDefn;
        if (superDefns != null && superDefns.size() > 0) {
            Iterator it = superDefns.iterator();
//                ComponentTypeDefn cdefn = null;
            while (it.hasNext()) {
                sDefn = (ComponentTypeDefn) it.next();
                //this map has been changed to be keyed
                //on the PropertyDefn object of a ComponentTypeDefn,
                //instead of the i.d. of the ComponentTypeDefn
                if (!defnMap.containsKey(sDefn.getPropertyDefinition())) {
                    defnMap.put(sDefn.getPropertyDefinition(), sDefn);
                    defns.add(sDefn);
                }
            }
        }

        return getSuperComponentTypeDefinitions(defnMap, defns, componentTypes, type.getSuperComponentTypeID(), transaction);
    }

    public Collection getAllComponentTypeDefinitions(ComponentTypeID typeID)  throws ConfigurationException {
        Collection defns = getComponentTypeDefinitions(typeID);
        Collection inheritedDefns = getDependentComponentTypeDefinitions(typeID);

        //We want the final, returned Collection to NOT have any
        //duplicate objects in it.  The two Collections above may have
        //duplicates - one in inheritedDefns which is a name and a default
        //value for a super-type, and one in defns which is a name AND A
        //DIFFERENT DEFAULT VALUE, from the specified type, which overrides
        //the default value of the supertype.  We want to only keep the
        //BasicComponentTypeDefn corresponding to the sub-type name and default
        //value.
        //For example, type "JDBCConnector" has a ComponentType for the
        //property called "ServiceClassName" and a default value equal to
        //"com.metamatrix.connector.jdbc.JDBCConnectorTranslator".  The
        //super type "Connector" also defines a "ServiceClassName" defn,
        //but defines no default values.  Or worse yet, it might define
        //in invalid default value.  So we only want to keep the right
        //defn and value.

        Iterator inheritedIter =  inheritedDefns.iterator();
        Iterator localIter = defns.iterator();

        ComponentTypeDefn inheritedDefn = null;
        ComponentTypeDefn localDefn = null;

        while (localIter.hasNext()){
            localDefn = (ComponentTypeDefn)localIter.next();
            while (inheritedIter.hasNext()){
                inheritedDefn = (ComponentTypeDefn)inheritedIter.next();
                if (localDefn.getPropertyDefinition().equals(inheritedDefn.getPropertyDefinition())){
                    inheritedIter.remove();
                }
            }
            inheritedIter = inheritedDefns.iterator();
        }

        defns.addAll(inheritedDefns);


        return defns;
    }


    public Host getHost(HostID hostID) throws ConfigurationException {
        if ( hostID == null ) {
            throw new IllegalArgumentException(PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0045, "hostID")); //$NON-NLS-1$
        }

        Collection hosts = getHosts();
        for (Iterator it = hosts.iterator(); it.hasNext(); ) {
             Host h = (Host) it.next();
             if (h.getID().equals(hostID)) {
                return h;
             }
        }

        return null;
    }


    public Collection getHosts() throws ConfigurationException {
        Collection hosts = null;
        ConfigurationTransaction transaction = null;
        try {
                transaction = getReadTransaction();
                hosts = transaction.getHosts();

        } catch ( ManagedConnectionException e ) {
                throw new ConfigurationException(e,ErrorMessageKeys.CONFIG_0055, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0055));
        } finally {
           if ( transaction != null ) {
                try {
                    transaction.close();
                } catch ( Exception txne ) {
                    I18nLogManager.logError(CONTEXT, ErrorMessageKeys.CONFIG_0041, txne);
                }
                transaction = null;
            }
        }

        if (hosts == null) {
            hosts = Collections.EMPTY_LIST;
        }

        return hosts;
    }


     public Collection getComponentDefns(Collection componentDefnIDs, ConfigurationID configurationID)
                        throws ConfigurationException {


        if (componentDefnIDs == null || componentDefnIDs.size() == 0) {
            return Collections.EMPTY_LIST;
        }
        Collection defns = new ArrayList(componentDefnIDs.size());

        ConfigurationTransaction transaction = null;
        try {
                transaction = getReadTransaction();
                ComponentDefnID id;
                ComponentDefn defn = null;

                for (Iterator it=componentDefnIDs.iterator(); it.hasNext(); ) {
                        id = (ComponentDefnID) it.next();
                        defn = transaction.getComponentDefinition(id, configurationID);
                        if (defn != null) {
                            defns.add(defn);
                        }
                }

        } catch ( ManagedConnectionException e ) {
            throw new ConfigurationException(e,ErrorMessageKeys.CONFIG_0056, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0056));
        } finally {
           if ( transaction != null ) {
                try {
                    transaction.close();
                } catch ( Exception txne ) {
                    I18nLogManager.logError(CONTEXT, ErrorMessageKeys.CONFIG_0057, txne);
                }
                transaction = null;
            }
        }

        return defns;

    }


    /**
     * Returns a <code>ComponentDefn</code> for the specified <code>ComponentDefnID</code>.
     * </br>
     * @param configurationID is the configuration for which the component exist
     * @param componentDefnID is the component being requested
     * @return ComponentDefn
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    public ComponentDefn getComponentDefn(ConfigurationID configurationID, ComponentDefnID componentDefnID)
    throws ConfigurationException{
        ConfigurationTransaction transaction = null;
        ComponentDefn defn = null;
        try {
                transaction = getReadTransaction();

                defn = transaction.getComponentDefinition(componentDefnID, configurationID);


        } catch ( ManagedConnectionException e ) {
                throw new ConfigurationException(e,ErrorMessageKeys.CONFIG_0058, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0058,componentDefnID.getName()));
        } finally {
           if ( transaction != null ) {
                try {
                    transaction.close();
                } catch ( Exception txne ) {
                    I18nLogManager.logError(CONTEXT, ErrorMessageKeys.CONFIG_0059, txne, new Object[] {componentDefnID.getName()});
                }
                transaction = null;
            }
        }

        return defn;

    }

  /**
     * Returns a Collection of {@link com.metamatrix.common.config.api.ComponentType ComponentType}
     * for all resource pool types of a specified configuration.
     * @param configurationID is the configuration from which the component types are to
     * be derived
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
   public Collection getResourcePoolTypes(ConfigurationID configurationID)
    throws ConfigurationException {

        if (configurationID == null || configurationID.size() == 0) {
            return Collections.EMPTY_LIST;
        }

        ConfigurationTransaction transaction = null;
        try {


            Collection types = getAllComponentTypes(false);
            Collection keepTypes = new ArrayList(5);

            for (Iterator it = types.iterator(); it.hasNext(); ) {
                    ComponentType type = (ComponentType) it.next();
                    if (type.getComponentTypeCode() == ComponentType.RESOURCE_COMPONENT_TYPE_CODE) {
                        if (type.getID().equals(SharedResource.JDBC_COMPONENT_TYPE_ID)) {
                            keepTypes.add(type);
                        } else if (type.getID().equals(SharedResource.JMS_COMPONENT_TYPE_ID)) {
                            keepTypes.add(type);
                        } else if (type.getID().equals(SharedResource.SEARCHBASE_COMPONENT_TYPE_ID)) {
                            keepTypes.add(type);
                        } else if (type.getID().equals(SharedResource.MISC_COMPONENT_TYPE_ID)) {
                            keepTypes.add(type);
                        }
                    }

            }
            return keepTypes;


        } catch ( ConfigurationException e ) {
                throw new ConfigurationException(e,ErrorMessageKeys.CONFIG_0060, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0060));
        } finally {
           if ( transaction != null ) {
                try {
                    transaction.close();
                } catch ( Exception txne ) {
                    I18nLogManager.logError(CONTEXT, ErrorMessageKeys.CONFIG_0061, txne);
                }
                transaction = null;
            }
        }

    }

    /**
     * Returns a Collection of {@link com.metamatrix.common.config.api.ComponentType ComponentType}
     * that represent the pool types for which new {@link ResourceDescriptor ResourcePools}
     * of these types can be created.  This means only these types have logic implemented to
     * make use of the resource pool.
     * @param configurationID is the configuration from which the component types are to
     * be derived
     * @throws AuthorizationException if caller is not authorized to perform this method.
     * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException if an error occurred in communicating with a component.
     */
    public Collection getPoolableResourcePoolTypes(ConfigurationID configurationID)
    throws ConfigurationException {

        if (configurationID == null || configurationID.size() == 0) {
            return Collections.EMPTY_LIST;
        }
        ConfigurationTransaction transaction = null;
        try {

            Collection types = getAllComponentTypes(false);
            Collection keepTypes = new ArrayList(5);

            for (Iterator it = types.iterator(); it.hasNext(); ) {
                    ComponentType type = (ComponentType) it.next();
                    if (type.getComponentTypeCode() == ComponentType.RESOURCE_COMPONENT_TYPE_CODE) {
                        if (type.getID().equals(SharedResource.JDBC_COMPONENT_TYPE_ID)) {
                            keepTypes.add(type);
                        } else if (type.getID().equals(SharedResource.SEARCHBASE_COMPONENT_TYPE_ID)) {
                            keepTypes.add(type);
                        }
                    }

            }
            return keepTypes;

        } catch ( ConfigurationException e ) {
			throw new ConfigurationException(e,ErrorMessageKeys.CONFIG_0060, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0060));
        } finally {
           if ( transaction != null ) {
                try {
                    transaction.close();
                } catch ( Exception txne ) {
                    I18nLogManager.logError(CONTEXT, ErrorMessageKeys.CONFIG_0061, txne);
                }
                transaction = null;
            }
        }

    }


     public Collection getResourcePools(ConfigurationID configurationID)
                        throws ConfigurationException {


        if (configurationID == null || configurationID.size() == 0) {
            return Collections.EMPTY_LIST;
        }

        ConfigurationTransaction transaction = null;
        try {
                transaction = getReadTransaction();

                return transaction.getConnectionPools(configurationID);

        } catch ( ManagedConnectionException e ) {
			throw new ConfigurationException(e,ErrorMessageKeys.CONFIG_0060, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0060));
        } finally {
           if ( transaction != null ) {
                try {
                    transaction.close();
                } catch ( Exception txne ) {
					I18nLogManager.logError(CONTEXT, ErrorMessageKeys.CONFIG_0061, txne);
                }
                transaction = null;
            }
        }

    }

   /**
     * Returns a Collection of {@link com.metamatrix.common.config.api.ResourceDescriptor ResourceDescriptor}
     * for all resource pools of a specified type defined in a configuration.
     * @param callerSessionID ID of the caller's current session.
     * @param configurationID is the configuration from which the component defns are to
     * be derived
     * @param componentTypeID indicates the type of pools in the configuration to return
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    public Collection getResourcePools(ConfigurationID configurationID, ComponentTypeID componentTypeID)
    throws ConfigurationException {

        if (configurationID == null || configurationID.size() == 0) {
            return Collections.EMPTY_LIST;
        }

        if (componentTypeID == null || componentTypeID.size() == 0) {
            return Collections.EMPTY_LIST;
        }

        Collection result = null;

        ConfigurationTransaction transaction = null;
        try {
                transaction = getReadTransaction();

                Collection pools = transaction.getConnectionPools(configurationID);
                result = new ArrayList(pools.size());

                for (Iterator it=pools.iterator(); it.hasNext(); ) {
                    ResourceDescriptor rd =(ResourceDescriptor) it.next();
                    if (rd.getComponentTypeID().equals(componentTypeID)) {
                        result.add(rd);
                    }

                }

                return result;

        } catch ( ManagedConnectionException e ) {
			throw new ConfigurationException(e,ErrorMessageKeys.CONFIG_0060, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0060));
        } finally {
           if ( transaction != null ) {
                try {
                    transaction.close();
                } catch ( Exception txne ) {
					I18nLogManager.logError(CONTEXT, ErrorMessageKeys.CONFIG_0061, txne);
                }
                transaction = null;
            }
        }

    }


    /**
     * Returns a Collection of {@link com.metamatrix.common.config.api.ResourceDescriptor ResourceDescriptor}
     * for all internal resources defined to the system.  The internal resources are not managed with
     * the other configuration related information.  They are not dictated based on which configuration
     * they will operate (i.e., next startup or operational);
     * @throws AuthorizationException if caller is not authorized to perform this method.
     * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException if an error occurred in communicating with a component.
     */
    public Collection getResources()
    throws ConfigurationException{
        ConfigurationTransaction transaction = null;
        try {
                transaction = getReadTransaction();

                return transaction.getResources();

        } catch ( ManagedConnectionException e ) {
			throw new ConfigurationException(e,ErrorMessageKeys.CONFIG_0060, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0060));
        } finally {
           if ( transaction != null ) {
                try {
                    transaction.close();
                } catch ( Exception txne ) {
					I18nLogManager.logError(CONTEXT, ErrorMessageKeys.CONFIG_0061, txne);
                }
                transaction = null;
            }
        }

    }

    /**
     * Returns a Collection of {@link com.metamatrix.common.config.api.ResourceDescriptor ResourceDescriptor}
     * that are of the specified resource type.
     * @param componentType that identifies the type of resources to be returned
     * @throws AuthorizationException if caller is not authorized to perform this method.
     * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException if an error occurred in communicating with a component.
     */
    public Collection getResources(ComponentTypeID componentTypeID)
    throws ConfigurationException {
        ConfigurationTransaction transaction = null;
        try {
                transaction = getReadTransaction();

                return transaction.getResources(componentTypeID);

        } catch ( ManagedConnectionException e ) {
			throw new ConfigurationException(e,ErrorMessageKeys.CONFIG_0060, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0060));
        } finally {
           if ( transaction != null ) {
                try {
                    transaction.close();
                } catch ( Exception txne ) {
					I18nLogManager.logError(CONTEXT, ErrorMessageKeys.CONFIG_0061, txne);
                }
                transaction = null;
            }
        }

    }

   /**
     * Save the resource changes based on each {@link com.metamatrix.common.config.api.ResourceDescriptor ResourceDescriptor}
     * in the collection.
     * @param resourceDescriptors for the resources to be changed          *
     * @throws AuthorizationException if caller is not authorized to perform this method.
     * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException if an error occurred in communicating with a component.
     */
    public void saveResources(Collection resourceDescriptors, String principalName)
    throws ConfigurationException {
        ConfigurationTransaction transaction = null;
        boolean success = false;
        try {

            transaction = this.getWriteTransaction();

            transaction.saveResources(resourceDescriptors, principalName);

            transaction.commit();                   // commit the transaction
            success = true;
        } catch ( ManagedConnectionException e ) {
            throw new ConfigurationException(e);
        } finally {
           if ( transaction != null ) {
        	   if (!success) {
	               try {
	                   transaction.rollback();         // rollback the transaction
	               } catch ( Exception e2 ) {
	   				I18nLogManager.logError(CONTEXT, ErrorMessageKeys.CONFIG_0063, e2);
	               }
        	   }

                try {
                    transaction.close();
                } catch ( Exception txne ) {
                    I18nLogManager.logError(CONTEXT, ErrorMessageKeys.CONFIG_0041, txne);
                }
            }
        }

    }

    /**
     * Obtain the List of {@link com.metamatrix.common.config.api.ReleaseInfo} objects
     * which represent the products of the system.  Each ReleaseInfo contains
     * the name of the product, as well as release info.
     * @return Collection of ReleaseInfo objects of licensed products.
     * @throws ConfigurationException if an business error occurred within or during communication with the Configuration Service.
     */
    public Collection getProductReleaseInfos() throws ConfigurationException {
        return ProductReleaseInfoUtil.getProductReleaseInfos();
    }


    // --------------------------------------------------------------
    //                A C T I O N     M E T H O D S
    // --------------------------------------------------------------

    /**
     * Execute as a single transaction the specified action, and optionally
     * return the set of objects or object IDs that were affected/modified by the action.
     * @param action the definition of the action to be performed on data within
     * the repository.
     * @param principalName of the person executing the transaction
     * @return the set of objects that were affected by this transaction.
     * @throws ModificationException if the target of the action is invalid, or
     * if the target object is not a supported class of targets.
     * @throws IllegalArgumentException if the action is null
     * or if the result specification is invalid
     * @throws ConfigurationException if an error occurred within or during
     * communication with the Configuration Service.
     */
    public Set executeTransaction(ActionDefinition action, String principalName) 
    	throws ModificationException, ConfigurationLockException, ConfigurationException{
        if ( action == null ) {
            throw new IllegalArgumentException(PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0045, "action")); //$NON-NLS-1$
        }
        List actions = new ArrayList(1);
        actions.add(action);
        return this.executeTransaction(actions, principalName);
    }

    /**
     * Execute as a single transaction the specified actions, and optionally
     * return the set of objects or object IDs that were affected/modified by the action.
     * @param actions the ordered list of actions that are to be performed on data within
     * the repository.
     * @param principalName of the person executing the transaction
     * @return the set of objects that were affected by this transaction.
     * @throws ModificationException if the target of any of the actions is invalid, or
     * if the target object is not a supported class of targets.
     * @throws IllegalArgumentException if the action is null
     * or if the result specification is invalid
     * @throws ConfigurationException if an error occurred within or during
     * communication with the Configuration Service.
     */
     public Set executeTransaction(List actions, String principalName) throws ModificationException, ConfigurationLockException, ConfigurationException {
        if ( actions == null ) {
            throw new IllegalArgumentException(PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0045, "actions")); //$NON-NLS-1$
        }
        LogManager.logDetail(CONTEXT, new Object[]{"Executing transaction for user ", principalName, " with ",new Integer(actions.size())," action(s)" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        Set result = new HashSet();
        if ( actions.isEmpty() ) {
            return result;
        }

        ConfigurationTransaction transaction = null;


        // Iterate through the actions, and apply all as a single transaction
        try {
            transaction = this.getWriteTransaction();
            result = transaction.executeActions(actions,principalName);
            transaction.commit();                   // commit the transaction

            // Add the actions to the history ...
            this.actionHistory.addActionsForTransaction(actions);
        } catch ( ConfigurationLockException e ) {
            try {
                if ( transaction != null ) {
                    transaction.rollback();         // rollback the transaction
                }
            } catch ( Exception e2 ) {
				I18nLogManager.logError(CONTEXT, ErrorMessageKeys.CONFIG_0065, e,new Object[]{ principalName, printActions(actions)});
            }
            throw e;
        } catch ( ConfigurationException e ) {
        // must increment by 1 because each actionList starts at zero
            //actionCounter += e.getActionIndex() + 1;
            //e.setActionIndex(actionCounter);

            try {
                if ( transaction != null ) {
                    transaction.rollback();         // rollback the transaction
                }
            } catch ( Exception e2 ) {
				I18nLogManager.logError(CONTEXT, ErrorMessageKeys.CONFIG_0065, e,new Object[]{ principalName, printActions(actions)});
            }
            throw e;
        } catch ( Exception e ) {
             try {
                if ( transaction != null ) {
                    transaction.rollback();         // rollback the transaction
                }
            } catch ( Exception e2 ) {
				I18nLogManager.logError(CONTEXT, ErrorMessageKeys.CONFIG_0065, e,new Object[]{ principalName, printActions(actions)});
            }
            throw new ConfigurationException(e);
        } finally {
           if ( transaction != null ) {
                try {
                    transaction.close();
                } catch ( Exception txne ) {
                    I18nLogManager.logError(CONTEXT, ErrorMessageKeys.CONFIG_0041, txne);
                }
                transaction = null;
            }
        }
        return result;
    }



    /**
     * Execute a list of insert actions and for actions on objects of type component object, it will have its
     * configuration id resassigned, and optionally
     * return the set of objects or object IDs that were affected/modified by the action.
     * @param assignConfigurationID the configuration for which any action for a component object will
     * have its configurationID set to this.
     * @param actions the ordered list of actions that are to be performed on data within
     * the repository.
     * @param principalName of the person executing the transaction
     * @return the set of objects that were affected by this transaction.
     * @throws ModificationException if the target of any of the actions is invalid, or
     * if the target object is not a supported class of targets.
     * @throws IllegalArgumentException if the action is null
     * or if the result specification is invalid
     * @throws ConfigurationException if an error occurred within or during
     * communication with the Metadata Service.
     */
    public Set executeInsertTransaction(ConfigurationID assignConfigurationID, List actions, String principalName) 
    	throws ModificationException, ConfigurationLockException, ConfigurationException {
        if ( actions == null ) {
            throw new IllegalArgumentException(PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0045, "actions")); //$NON-NLS-1$
        }
        // Iterate through the actions, and set the ConfigurationID on the
        // arg if necessary
        ActionDefinition currentAction = null;
        Iterator iter = actions.iterator();
        Object argObj;
        boolean chk = false;
        while ( iter.hasNext() ) {
            currentAction = (ActionDefinition) iter.next();
            if ( currentAction instanceof CreateObject) {
                chk = true;
            } else if (currentAction instanceof AddObject) {
                chk = false;
            } else {
                throw new ModificationException(ErrorMessageKeys.CONFIG_0066, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0066));
            }

            // only CreateObjects have arguments
            if (chk && assignConfigurationID != null)  {
                Object args[] = currentAction.getArguments();
                argObj = args[0];
                editor.assignConfigurationID(argObj,assignConfigurationID);
            }
        }

        //Pass this on through to the executeTransaction method
        return this.executeTransaction(actions, principalName);
    }

    /**
     * Undo the specified number of previously-committed transactions.
     * @param numberOfActions the number of actions in the history that are to be undone.
     * @return the set of objects that were affected by undoing these actions.
     * @throws IllegalArgumentException if the number is negative.
     * @throws ConfigurationException if an error occurred within or during
     * communication with the Configuration Service.
     */
    public Set undoActionsAsTransaction(int numberOfActions, String principalName) throws ConfigurationException {
        if ( numberOfActions < 0 ) {
            throw new IllegalArgumentException(PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0068, numberOfActions ));
        }
        LogManager.logDetail(CONTEXT, new Object[]{"Undoing ",new Integer(numberOfActions)," action(s)"}); //$NON-NLS-1$ //$NON-NLS-2$

        Set result = null;
        synchronized( this.actionHistory ) {
            List actions = this.actionHistory.pop(numberOfActions);
            List undoActions = new ArrayList();
            Iterator iter = actions.iterator();
            while ( iter.hasNext() ) {
                ActionDefinition action = (ActionDefinition) iter.next();
                undoActions.add( action.getUndoActionDefinition() );
            }

            try {
                result = this.executeTransaction(undoActions, principalName);
                I18nLogManager.logInfo(CONTEXT, LogMessageKeys.CONFIG_0004,  new Object[]{new Integer(numberOfActions)});
            } catch ( ConfigurationException e ) {
                // put the actions back on the history ...
                this.actionHistory.addActionsForTransaction(actions);
                throw e;
            } catch ( ModificationException e ) {
                ConfigurationException me = new ConfigurationException(e,ErrorMessageKeys.CONFIG_0069, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0069));
                // put the actions back on the history ...
                this.actionHistory.addActionsForTransaction(actions);
                throw me;
            }
        }
        return result;
   }

    /**
     * Get the history of actions executed in transactions by this editor.
     * The actions at the front of the list will be those most recently executed.
     * @return the ordered list of actions in the history.
     * @throws ConfigurationException if an error occurred within or during
     * communication with the Configuration Service.
     */
    public List getHistory() throws ConfigurationException {
        return this.actionHistory.getHistory();
    }


    /**
     * Clear the history of all actions without undoing any of them.
     * @throws ConfigurationException if an error occurred within or during
     * communication with the Configuration Service.
     */
    public void clearHistory() throws ConfigurationException {
        this.actionHistory.clearHistory();
    }


    /**
     * Get the number of actions that are currently in the history.
     * @return the number of actions in the history.
     * @throws ConfigurationException if an error occurred within or during
     * communication with the Configuration Service.
     */
    public int getHistorySize() throws ConfigurationException {
        return this.actionHistory.getHistorySize();
    }


    /**
     * Set the limit on the number of actions in the history.  Note that the
     * history may at times be greater than this limit, because when actions
     * are removed from the history, all actions for a transactions are
     * removed at the same time.  If doing so would make the history size
     * smaller than the limit, no actions are removed.
     * @throws ConfigurationException if an error occurred within or during
     * communication with the Configuration Service.
     */
    public int getHistoryLimit() throws ConfigurationException {
        return this.actionHistory.getHistoryLimit();
    }


    /**
     * Set the limit on the number of actions in the history.  Note that the
     * history may at times be greater than this limit, because when actions
     * are removed from the history, all actions for a transactions are
     * removed at the same time.  If doing so would make the history size
     * smaller than the limit, no actions are removed.
     * @throws ConfigurationException if an error occurred within or during
     * communication with the Configuration Service.
     */
    public void setHistoryLimit(int maximumHistoryCount) throws ConfigurationException {
        this.actionHistory.setHistoryLimit(maximumHistoryCount);
    }

    /**
     * Return the time the server was started. If the state of the server is not "Started"
     * then a null is returned.
     *
     * @return Date Time server was started.
     * @throws ConfigurationException if an error occurred within or during
     * communication with the Configuration Service.
     */
    public Date getServerStartupTime() throws ConfigurationException {
        ConfigurationTransaction transaction = null;
        Date timestamp = null;
        try {
            transaction = getReadTransaction();
            timestamp = transaction.getServerStartupTime();
        } catch ( ManagedConnectionException e ) {
            throw new ConfigurationException(e, ErrorMessageKeys.CONFIG_0070, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0070));
        } finally {
           if ( transaction != null ) {
                try {
                    transaction.close();
                } catch ( Exception txne ) {
                    I18nLogManager.logError(CONTEXT, ErrorMessageKeys.CONFIG_0041, txne);
                }
                transaction = null;
            }
        }
        return timestamp;
    }

    protected void addProperty(Properties source, String sourceName, Properties props, String propName) {
        String value = source.getProperty(sourceName);
        if (value != null) {
            props.setProperty(propName, value);
        }
    }



    // ----------------------------------------------------------------------------------------
    //                 I N T E R N A L     M E T H O D S
    // ----------------------------------------------------------------------------------------


    protected String printActions( List actions ) {
        StringBuffer sb = new StringBuffer();
        Iterator iter = actions.iterator();
        if ( iter.hasNext() ) {
            sb.append( iter.next().toString() );
        }
        while ( iter.hasNext() ) {
            sb.append("; "); //$NON-NLS-1$
            sb.append( iter.next().toString() );
        }
        return sb.toString();
    }

    protected ConfigurationTransaction getReadTransaction() throws ManagedConnectionException {
        return (ConfigurationTransaction) this.transMgr.getReadTransaction();
    }

    protected ConfigurationTransaction getWriteTransaction() throws ManagedConnectionException {
        return (ConfigurationTransaction) this.transMgr.getWriteTransaction();
    }

    
    
    
//    /**
//     * Return the list of allowable values for the specified type.
//     * @param type the allowable type
//     * @return the map of values (keys) and descriptions (values) for the specified type.
//     * @throws ConfigurationException if an error occurred within or during communication with this connection.
//     */
//    private List getAllowableValues( Integer type, boolean includeDeprecated ) throws ConfigurationException {
//        if ( this.serviceIsClosed ) {
//            throw new ConfigurationException("This ConfigurationService instance is closed and may not accept requests");
//        }
//
//        Collection values = null;
//
//        ConfigurationTransaction transaction = null;
//        try {
//            transaction = getReadTransaction();
////                values = transaction.getAllowableValues(type);
//        } catch ( Exception e ) {
//            ConfigurationException e2 = new ConfigurationException(e,"Unable to find the allowable values");
//            LogManager.logError(CONTEXT, e2, "Error reading allowable values");
//            throw e2;
//        } finally {
//           if ( transaction != null ) {
//                try {
//                    transaction.close();
//                } catch ( Exception txne ) {
//                    I18nLogManager.logError(CONTEXT, ErrorMessageKeys.CONFIG_0041, txne);
//                }
//                transaction = null;
//            }
//        }
//
//        List result = new LinkedList(values);
//        if ( !includeDeprecated ) {
//            Iterator iter = result.iterator();
//            while ( iter.hasNext() ) {
//                ComponentType cType = (ComponentType) iter.next();
//                if ( cType.isDeprecated() ) {
//                    iter.remove();
//                }
//            }
//        }
//
//        return result;
//    }


//    protected boolean isClosed() {
//        return this.serviceIsClosed;
//    }

//    protected SessionToken getSessionToken() {
//        return this.token;
//    }

    //
    // Configuration Admin Methods
    //

    /**
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#addHost(java.lang.String,
     *      java.util.Properties)
     * @since 4.3
     */
    public Host addHost(String hostName,
                        String principalName,
                        Properties properties) throws ConfigurationException {
        com.metamatrix.common.config.api.Host host = null;

        ConfigurationObjectEditor editor = null;
        try {
            editor = createEditor();

            ConfigurationModelContainer config = this.getConfigurationModel(Configuration.NEXT_STARTUP);

            Properties defaultProps = config.getDefaultPropertyValues(Host.HOST_COMPONENT_TYPE_ID);
            Properties allProps = PropertiesUtils.clone(defaultProps, false);
            allProps.putAll(properties);

            
            host = editor.createHost(hostName);
            host = (com.metamatrix.common.config.api.Host)editor
                                                                .modifyProperties(host, allProps, ConfigurationObjectEditor.SET);

            executeTransaction(editor.getDestination().popActions(), principalName); 

        } catch (Exception theException) {
            // rollback
            if (editor != null) {
                editor.getDestination().popActions();
            }
            //final Object[] params = new Object[] {this.getClass().getName(), theException.getMessage()};
            final Object[] params = new Object[] {
                hostName
            };
            final String msg = PlatformPlugin.Util.getString("ConfigurationServiceImpl.Error_creating_New_Host", params); //$NON-NLS-1$

            throw new ConfigurationException(theException, msg); 
        }
        return host;
    }
    
    
    /**
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#addProcess(java.lang.String,
     *      java.lang.String, java.util.Properties)
     * @since 4.3
     */
    public VMComponentDefn addProcess(String processName,
                                      String hostName,
                                      String principalName,
                                      Properties properties) throws ConfigurationException {

        VMComponentDefn processDefn = null;

        ConfigurationObjectEditor editor = null;
        try {
            ConfigurationModelContainer config = this.getConfigurationModel(Configuration.NEXT_STARTUP);
            editor = createEditor();
            Host host = getHost(new HostID(hostName));
            if (host != null) {

            } else {
                final Object[] params = new Object[] {
                    hostName
                };
                final String msg = PlatformPlugin.Util.getString("ConfigurationServiceImpl.Host_not_found", params); //$NON-NLS-1$
                throw new ConfigurationException(msg);

            }

            // grab the default properties
            Properties defaultProps = config.getDefaultPropertyValues(VMComponentDefn.VM_COMPONENT_TYPE_ID);

            // create defn first
            processDefn = editor.createVMComponentDefn(config.getConfiguration(),
                                                       (HostID)host.getID(),
                                                       VMComponentDefn.VM_COMPONENT_TYPE_ID,
                                                       processName);
            
            Properties allProps = PropertiesUtils.clone(defaultProps, false);
            allProps.putAll(properties);
            if (processDefn != null) {
                processDefn = (VMComponentDefn)editor.modifyProperties(processDefn, allProps, ConfigurationObjectEditor.SET);

                // create deployed component next
                executeTransaction(editor.getDestination().popActions(), principalName); 
            }

        } catch (Exception theException) {
            // rollback
            if (editor != null) {
                editor.getDestination().popActions();
            }
            final Object[] params = new Object[] {
                processName, hostName
            };
            final String msg = PlatformPlugin.Util.getString("ConfigurationServiceImpl.Error_creating_Process", params); //$NON-NLS-1$
            throw new ConfigurationException(theException, msg);
        }
        return processDefn;
    }
    
    /**
     * Set System Property in Next Configuration
     * 
     * @param propertyName
     * @param propertyValue
     * @param principalName
     * @throws ConfigurationException
     * @throws InvalidSessionException
     * @throws AuthorizationException
     * @since 4.3
     */
    public void setSystemPropertyValue(String propertyName,
                                       String propertyValue,
                                       String principalName) throws ConfigurationException{
        ConfigurationObjectEditor editor = null;
        try {
            editor = createEditor();
            Configuration nextStartupConfig = getNextStartupConfiguration();
            editor.setProperty(nextStartupConfig, propertyName, propertyValue);
            executeTransaction(editor.getDestination().popActions(), principalName);

        } catch (Exception theException) {
            if (editor != null) {
                editor.getDestination().popActions();
            }
            final Object[] params = new Object[] {
                propertyName, theException.getMessage()
            };
            final String msg = PlatformPlugin.Util.getString("ConfigurationServiceImpl.Error_setting_Property", params); //$NON-NLS-1$
            throw new ConfigurationException(theException, msg);
        }
    }
    
    
    /**
     * Set System Property in Next Configuration
     * 
     * @param properties
     * @param principalName
     * @throws ConfigurationException
     * @throws InvalidSessionException
     * @throws AuthorizationException
     * @since 4.3
     */
    public void updateSystemPropertyValues(Properties properties,
                                        String principalName) throws ConfigurationException {
        ConfigurationObjectEditor editor = null;
        try {
            editor = createEditor();
            Configuration nextStartupConfig = getNextStartupConfiguration();
            
            for (Iterator iter = properties.keySet().iterator(); iter.hasNext(); ) {
                String key = (String) iter.next();
                String value = properties.getProperty(key);
                editor.setProperty(nextStartupConfig, key, value);
            }
            
            executeTransaction(editor.getDestination().popActions(), principalName);

        } catch (Exception theException) {
            if (editor != null) {
                editor.getDestination().popActions();
            }
            final Object[] params = new Object[] {
                theException.getMessage(), properties
            };
            final String msg = PlatformPlugin.Util.getString("ConfigurationServiceImpl.Error_updating_Properties", params); //$NON-NLS-1$
            throw new ConfigurationException(theException, msg);
        }
    }
    
    
    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#createConnectorBinding(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.util.Properties)
     * @since 4.3
     */
    public ConnectorBinding createConnectorBinding(String connectorBindingName,
                                                   String connectorType,
                                                   String pscName,
                                                   String principalName,
                                                   Properties properties) throws ConfigurationException {

        ConnectorBinding binding = null;
        ConfigurationObjectEditor editor = null;
        Configuration config = null;

        try {
            ComponentType ctConnector = getComponentType(connectorType, false);
            if (ctConnector == null) {
                final Object[] params = new Object[] {
                    connectorType
                };
                final String msg = PlatformPlugin.Util.getString("ConfigurationServiceImpl.Connector_Type_not_found", params); //$NON-NLS-1$

                throw new MetaMatrixComponentException(msg);
            }
            editor = createEditor();
            config = getNextStartupConfiguration();

            /* ServiceComponentDefn scd = */
            editor.createServiceComponentDefn((ConfigurationID)config.getID(),
                                              (ComponentTypeID)ctConnector.getID(),
                                              connectorBindingName);

            binding = createConnectorBinding(ctConnector, editor, connectorBindingName);
            binding = (ConnectorBinding)editor.modifyProperties(binding, properties, ConfigurationObjectEditor.SET);

            if (pscName != null && !pscName.equals("")) { //$NON-NLS-1$
                ProductServiceConfig psc = this.getPSCByName(config, pscName);
                ServiceComponentDefnID bindingID = (ServiceComponentDefnID)binding.getID();
                editor.addServiceComponentDefn(psc, bindingID);

                editor.deployServiceDefn(config, binding, (ProductServiceConfigID)psc.getID());
            }

            executeTransaction(editor.getDestination().popActions(), principalName);

        } catch (Exception theException) {
            // rollback
            if (editor != null) {
                editor.getDestination().popActions();
            }
            final Object[] params = new Object[] {
                this.getClass().getName(), theException.getMessage()
            };

            throw new ConfigurationException(theException,
                                             PlatformPlugin.Util.getString("ConfigurationServiceImpl.Error_creating_Connector_Binding", params)); //$NON-NLS-1$

        }
        return binding;
    }

    private ComponentType getComponentType(String connectorName,
                                           boolean deprecated) throws InvalidSessionException,
                                                              AuthorizationException,
                                                              ConfigurationException,
                                                              MetaMatrixComponentException {
        Collection arylConnectors = getAllComponentTypes(deprecated);
        Iterator itConnectors = arylConnectors.iterator();
        while (itConnectors.hasNext()) {
            ComponentType ctConnector = (ComponentType)itConnectors.next();
            if (ctConnector.getName().equals(connectorName)) {
                return ctConnector;
            }
        }
        return null;
    }

    private ProductServiceConfig getPSCByName(Configuration config,
                                              String pscName) throws InvalidArgumentException {
        ProductServiceConfig result = null;
        if (config != null) {
            ProductServiceConfigID pscID = new ProductServiceConfigID(((ConfigurationID)config.getID()), pscName);
            result = config.getPSC(pscID);
        }
        return result;
    }

    private ConnectorBinding createConnectorBinding(ComponentType ctConnector,
                                                    ConfigurationObjectEditor coe,
                                                    String sConnBindName) throws Exception {
        ConnectorBinding connectorBinding = coe.createConnectorComponent(Configuration.NEXT_STARTUP_ID, (ComponentTypeID)ctConnector.getID(),
                                                                         sConnBindName,
                                                                         null);
        return connectorBinding;
    }
    
    
    public Object modify(ComponentObject theObject,
                         Properties theProperties,
                         String principalName) throws ModificationException, ConfigurationLockException, ConfigurationException{
        ConfigurationObjectEditor editor = null;
        try {
            editor = createEditor();
            Object obj = editor.modifyProperties(theObject, theProperties, ConfigurationObjectEditor.SET);
            executeTransaction(editor.getDestination().popActions(), principalName);
            return obj;
        } catch (Exception theException) {
            // rollback
            if (editor != null) {
                editor.getDestination().popActions();
            }
            throw new ConfigurationException(theException);
        }
    }
    
    public ComponentType importConnectorType(InputStream inputStream,
                                             String name,
                                             String principalName) throws ConfigurationException {
        ComponentType newType = null;
        ConfigurationObjectEditor editor = createEditor();

        try {
            XMLConfigurationImportExportUtility util = new XMLConfigurationImportExportUtility();
            newType = util.importComponentType(inputStream, editor, name);

            executeTransaction(editor.getDestination().popActions(), principalName);

        } catch (Exception theException) {
            // rollback
            if (editor != null) {
                editor.getDestination().popActions();
            }
            final Object[] params = new Object[] {
                name, theException.getMessage()
            };
            final String msg = PlatformPlugin.Util.getString("ConfigurationServiceImpl.Error_importing_connector_type", params); //$NON-NLS-1$
            throw new ConfigurationException(theException, msg); 

        }
        return newType;

    }
    
    

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#importConnectorBinding(java.io.InputStream, java.lang.String, java.lang.String, java.lang.String)
     * @since 4.3
     */
    public ConnectorBinding importConnectorBinding(InputStream inputStream,
                                                   String name,
                                                   String pscName,
                                                   String principalName) throws ConfigurationException{
        ConnectorBinding newBinding = null;
        ConfigurationObjectEditor editor = createEditor();

        try {
            XMLConfigurationImportExportUtility util = new XMLConfigurationImportExportUtility();
            newBinding = util.importConnectorBinding(inputStream, editor, name);

            
            //deploy to the specified PSC
            Configuration config = getNextStartupConfiguration();
            if (pscName != null && !pscName.equals("")) { //$NON-NLS-1$
                ProductServiceConfig psc = this.getPSCByName(config, pscName);
                ServiceComponentDefnID bindingID = (ServiceComponentDefnID) newBinding.getID();
                editor.addServiceComponentDefn(psc, bindingID);

                editor.deployServiceDefn(config, newBinding, (ProductServiceConfigID)psc.getID());
            }            
            
            executeTransaction(editor.getDestination().popActions(), principalName);

        } catch (Exception theException) {
            // rollback
            if (editor != null) {
                editor.getDestination().popActions();
            }
            final Object[] params = new Object[] {
                name, theException.getMessage()
            };
            final String msg = PlatformPlugin.Util.getString("ConfigurationServiceImpl.Error_importing_connector_binding", params); //$NON-NLS-1$
            throw new ConfigurationException(theException, msg); 

        }
        return newBinding;
               
        
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#delete(com.metamatrix.common.config.api.ComponentObject, boolean)
     * @since 4.3
     */
    public void delete(ComponentObject theObject,
                       boolean theDeleteDependenciesFlag,
                       String principalName) throws ConfigurationException,
                                                         ModificationException {
        ConfigurationObjectEditor editor = null;

        try {
            editor = createEditor();
            Configuration config = getNextStartupConfiguration();

            editor.delete(theObject, config, theDeleteDependenciesFlag);
            // the editor won't have any actions if the ComponentObject
            // being deleted does not have any DeployedComponents under it,
            // a NPE is thrown if executeTransaction() is called
            if (editor.getDestination().getActionCount() != 0) {
                executeTransaction(editor.getDestination().popActions(),principalName);
            }
        } catch (ConfigurationException theException) {
            // rollback
            if (editor != null) {
                editor.getDestination().popActions();
            }
            throw theException;
        } catch (ServiceException err) {
//          rollback
            if (editor != null) {
                editor.getDestination().popActions();
            }
            throw err;
        } catch (ModificationException mex) {
//          rollback
            if (editor != null) {
                editor.getDestination().popActions();
            }
            throw mex;
        }
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#delete(com.metamatrix.common.config.api.ComponentType, java.lang.String)
     * @since 4.3
     */
    public void delete(ComponentType componentType,
                       String principalName) throws ConfigurationException,
                                            ModificationException {
        
        ConfigurationObjectEditor editor = null;
        try {
            editor = createEditor();
            editor.delete(componentType);
            executeTransaction(editor.getDestination().popActions(),principalName);
        } catch (ConfigurationException theException) {
                // rollback
                if (editor != null) {
                    editor.getDestination().popActions();
                }
                throw theException;
            }
    }
    
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
     * @throws ModificationException
     * @since 4.3
     */
    
    public Collection deployPSC(Host theHost,
                                VMComponentDefn theProcess,
                                String pscName,
                                String principalName) throws ConfigurationException,
                                                     ModificationException {

        Collection deployComponentList = null;

        ConfigurationObjectEditor editor = null;
        try {
            editor = createEditor();
            Configuration config = getNextStartupConfiguration();
            ProductServiceConfig thePSC = getPSCByName(config, pscName);
            deployComponentList = editor.deployProductServiceConfig(config,
                                                                    thePSC,
                                                                    (HostID)theHost.getID(),
                                                                    (VMComponentDefnID)theProcess.getID());
            executeTransaction(editor.getDestination().popActions(), principalName);
        } catch (ConfigurationException theException) {
            // rollback
            if (editor != null) {
                editor.getDestination().popActions();
            }
            throw theException;
        }
        return deployComponentList;
    }
    
    
    
    /**
     * Check whether the encrypted properties for the specified ComponentDefns can be decrypted.
     * @param defns List<ComponentDefn>
     * @return List<Boolean> in the same order as the paramater <code>defns</code>.
     * For each, true if the properties could be decrypted for that defn.
     * @throws ConfigurationException
     * @since 4.3
     */
    public List checkPropertiesDecryptable(List defns) throws ConfigurationException{
        
        List results = new ArrayList(defns.size());
        
        //for each ComponentDefn
        for (Iterator iter = defns.iterator(); iter.hasNext();) {
            ComponentDefn defn = (ComponentDefn) iter.next();
            Collection componentTypeDefns = getComponentTypeDefinitions(defn.getComponentTypeID());
    
            boolean result = ComponentCryptoUtil.checkPropertiesDecryptable(defn, componentTypeDefns);
            results.add(new Boolean(result));
        }
        
        return results;
    }


    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#checkPropertiesDecryptable(java.util.Properties, java.lang.String)
     * @since 4.3
     */
    public boolean checkPropertiesDecryptable(Properties props,
                                              String componentTypeIdentifier) throws ConfigurationException {
        Collection componentTypes = getAllComponentTypes(false);

        ComponentType actualType = null; 
        for ( Iterator typeItr = componentTypes.iterator(); typeItr.hasNext(); ) {
            ComponentType aType = (ComponentType)typeItr.next();
            if (aType.getName().equals(componentTypeIdentifier)) {
                actualType = aType;
                break;
            }
        }
        
        if ( actualType == null ) {
            throw new ConfigurationException(PlatformPlugin.Util.getString("ConfigurationServiceImpl.ConnectorType_not_found",  //$NON-NLS-1$
                                       new Object[] {componentTypeIdentifier})); 
        }

        Collection maskedPropertyNames = actualType.getMaskedPropertyNames();

        return ComponentCryptoUtil.checkPropertiesDecryptable(props, maskedPropertyNames);
    }   
}

