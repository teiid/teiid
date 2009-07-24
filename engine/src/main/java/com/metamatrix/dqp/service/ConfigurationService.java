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

package com.metamatrix.dqp.service;

import java.io.File;
import java.net.URL;
import java.util.List;
import java.util.Properties;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.application.ApplicationService;
import com.metamatrix.common.application.ClassLoaderManager;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.ConnectorBindingType;
import com.metamatrix.common.config.api.ExtensionModule;
import com.metamatrix.common.vdb.api.VDBArchive;
import com.metamatrix.platform.security.api.service.SessionListener;



/** 
 * Configuration Service, this is responsible for all the configuration
 * for other services and also DQP configuration. 
 * @since 4.3
 */
public interface ConfigurationService extends ApplicationService, ClassLoaderManager {
    public static final String NEXT_VDB_VERSION = "NEXT_VDB_VERSION";         //$NON-NLS-1$
    public static final String USER_DEFINED_FUNCTION_MODEL = "FunctionDefinitions.xmi"; //$NON-NLS-1$
    /**
     * Get the VDB contents for the "System.VDB" 
     * @return URL - URL to System.vdb file; null if one not found
     */
    public URL getSystemVdb();
    
    /**
     * Get DQP properties
     * @return Properties - properties
     * @throws MetaMatrixComponentException
     */
    public Properties getSystemProperties();
    
    /**
     * Set System property (Contents of ServerConfig.xml file)
     * @param key
     * @param value
     * @throws MetaMatrixComponentException
     */
    public void setSystemProperty(String key, String value) throws MetaMatrixComponentException;

    /**
     * Set several System properties (Contents of ServerConfig.xml file). 
     * Any properties not specified will remain unchanged.
     * 
     * @param properties The properties to set.
     * @throws MetaMatrixComponentException
     */
    public void updateSystemProperties(Properties properties) throws MetaMatrixComponentException;

    
    
    /**
     * Get the system Configuration object loaded from the configuration object, this is currently
     * only needed to support the exportConfiguration admin call. 
     * @return
     * @throws MetaMatrixComponentException
     * @since 4.3
     */
    public ConfigurationModelContainer getSystemConfiguration() throws MetaMatrixComponentException;
    
        
    /**
     * Get the user defined functions file name  
     * @return URL - URL to the User defined file; null if one not defined
     */
    public URL getUDFFile();
    
    /**
     * Save the given vdb file into underlying persistent mechanism used by
     * this configuration service 
     * @param vdb - VDB to be saved
     * @param version - Version to be saved as; when used text "NEXT_VDB_VERSION" as version 
     * save as the next available version.
     * @return
     * @throws MetaMatrixComponentException
     */
    public void saveVDB(VDBArchive vdb, String version) throws MetaMatrixComponentException;

    /**
     * Add the VDB to the configuration 
     * @param vdb - vdb to be added
     * @param replaceBindings - flag which specifies action to be taken in case there are conflicts in
     * connector bindings.
     * @return - added VDB instance.
     * @throws MetaMatrixComponentException
     */
    public VDBArchive addVDB(VDBArchive vdb, boolean replaceBindings) throws MetaMatrixComponentException;
    
    /**
     * Delete the VDB from the underlying persistent mechanism used by  this
     * configuration service
     * @param vdbName - Name of the VDB
     * @param vdbVersion - VDB version
     * @throws MetaMatrixComponentException
     */
    public void deleteVDB(VDBArchive vdb) throws MetaMatrixComponentException;
    
    /**
     * Assign a connector binding to the Model 
     * @param vdbName - Name of the VDB
     * @param version - version of the VDB
     * @param modelName - Model Name
     * @param bindings - bindings to be assigned
     * @return modified VDB
     */
    public void assignConnectorBinding(String vdbName, String version, String modelName, ConnectorBinding[] bindings)  throws MetaMatrixComponentException;
    
    /**
     * Get the list of VDB files available with the Configuration Service, this includes
     * all status (active, inactive)
     * @param vdbName - Name of the VDB
     * @param vdbVersio - VDB version. 
     * @return VDBArchive
     * @throws MetaMatrixComponentException
     */
    public VDBArchive getVDB(String vdbName, String vdbVersion) throws MetaMatrixComponentException;
    
    /**
     * Get a list of available VDBS from the configuration 
     * @return list of {@link com.metamatrix.common.vdb.api.VDBArchive}
     * @throws MetaMatrixComponentException
     * @since 4.3
     */
    public List<VDBArchive> getVDBs() throws MetaMatrixComponentException;
        
    /** 
     * Add the connector binding to the persistent store
     * @param connectorBindingName - Connector Binding Name
     * @param binding - Connector binding to Add
     * @param replace - flag to replace in case a duplicate found.
     */
    public ConnectorBinding addConnectorBinding(String connectorBindingName, ConnectorBinding binding, boolean replace) 
        throws MetaMatrixComponentException;    
    
    /** 
     * Delete the connector binding from the persistent store
     * @param connectorBindingName - Connector Binding Name
     */
    public void deleteConnectorBinding(String connectorBindingName) 
        throws MetaMatrixComponentException;
    
    /** 
     * get the connector binding from the persistent store
     * @param connectorBindingName - Connector Binding Name
     * @return Connector binding by the name given.
     */
    public ConnectorBinding getConnectorBinding(String connectorBindingName) 
        throws MetaMatrixComponentException;

    /**
     * Get the default properties for the Connector type 
     * @param type
     * @return properties
     */
    public Properties getDefaultProperties(ConnectorBindingType type);
    
    /** 
     * Update the Connector Binding, the assumption here that we kept the name same 
     * @param binding - Connector Binding to be modified
     * @return modified connector; usually same referenced object.
     */
    public ConnectorBinding updateConnectorBinding(ConnectorBinding binding) 
        throws MetaMatrixComponentException;
    
    /**
     * Get the list of connector bindings available in the configuration.  
     * @return list of {@link com.metamatrix.common.config.api.ConnectorBinding}
     * @throws MetaMatrixComponentException
     * @since 4.3
     */
    public List<ConnectorBinding> getConnectorBindings() throws MetaMatrixComponentException;
        
    
    /**
     * Get Component type for the given id  
     * @param id
     * @return
     * @throws MetaMatrixComponentException
     */
    public ConnectorBindingType getConnectorType(String connectorType) 
        throws MetaMatrixComponentException;
    
    /**
     * Add the Given component type to the persistent store 
     * @param type
     * @throws MetaMatrixComponentException
     */
    public void saveConnectorType(ConnectorBindingType type) 
        throws MetaMatrixComponentException;
    
    /**
     * Delete the Given component type to the persistent store 
     * @param connectorType - Name of the connector Type
     * @throws MetaMatrixComponentException
     */
    public void deleteConnectorType(String connectorType) 
        throws MetaMatrixComponentException;    

    /**
     * Get list of all the connector types available in the System. 
     * @return list of {@link com.metamatrix.common.config.api.ComponentType}
     * @throws MetaMatrixComponentException
     */
    public List<ComponentType> getConnectorTypes() 
        throws MetaMatrixComponentException;
        
    /**
     * Retun the context class path to be used by all the extension modules
     * to use. if one not specified a default must be supplied. Usally this
     * is directory where all the extension jars are stored or a URL
     * @return String URL - url to extension path; null if extensions are not used 
     * @since 4.3
     */
    public URL[] getExtensionPath();

    /**
     * Get the list of extension modules available in the store
     * @return list of Extension Modules {@link com.metamatrix.common.config.api.ExtensionModule} 
     * @throws MetaMatrixComponentException
     * @since 4.3
     */
    public List<ExtensionModule> getExtensionModules() throws MetaMatrixComponentException;

    /**
     * Get the extension module by the given identifier 
     * @param extModuleName - Module name
     * @return ExtensionModule; null if not found
     * @throws MetaMatrixComponentException
     * @since 4.3
     */
    public ExtensionModule getExtensionModule(String extModuleName) 
        throws MetaMatrixComponentException;
    
    /**
     * Save the given extension module 
     * @throws MetaMatrixComponentException
     * @since 4.3
     */
    public void saveExtensionModule(ExtensionModule extModule) throws MetaMatrixComponentException;
    
    /**
     * Delete the extension module from the configuration with name supplied 
     * @param extModuleName - extension module name
     * @throws MetaMatrixComponentException
     * @since 4.3
     */
    public void deleteExtensionModule(String extModuleName) 
        throws MetaMatrixComponentException;
    
    /**
     * Client Connection Listener object for the service; This will get notifications
     * about the connections currently available to DQP. 
     * @throws MetaMatrixComponentException
     * @since 4.3.2
     */
    public SessionListener getSessionListener(); 
     
 
    /**
     * Register a listener for the VDB life cycle events, and get notified for
     * when vdb is loded and unloaded 
     * @param listener - lister object
     */
    public void register(VDBLifeCycleListener listener);
    
    public void unregister(VDBLifeCycleListener listener);
    
    /**
     * Register a listener for the connector binding life cycle events, and get notified for
     * when connector binding is loded and unloaded 
     * @param listener - lister object
     */
    public void register(ConnectorBindingLifeCycleListener listener);
    
    public void unregister(ConnectorBindingLifeCycleListener listener);
        
    /**
     * Gets the reference URL to the Configuration File for the DQP.
     * @return URL to configuration file; null otherwise
     * @since 4.4
     */
    public URL getConfigFile();
    
    
    /**
     * Use the extension classpath defined in the connector bindins and load jars in
     * different class loader or in the same class loader as calling code
     * @return true - to load different class loader, false otherwise
     */
    public boolean useExtensionClasspath();
    
    
    /**
     * Get path(s) for VDB(s) that are availble from the configuration to the DQP
     * engine. 
     * @return URLs to the resources.
     */
    public URL[] getVDBLocations();
    
    
    /**
     * Use disk for buffering for result set management during the processing 
     * @return true if yes to use buffering; false otherwise
     */
    public boolean useDiskBuffering();
    
    /**
     * Get the directory to use for the disk buffering  
     * @return must a return a location 
     */
    public File getDiskBufferDirectory();
    

    /**
     * Size of Memory in MB allocated to be used by the Resultset Management before
     * disk buffering kicks in.  
     * @return must a return a location 
     */
    public String getBufferMemorySize();
    
    /**
     * Return some identifier which uniquely identifies the DQP. Usually this
     * is name given to this process 
     * @return unique number for DQP in a given JVM
     */
    public String getProcessName();    
    

    /**
     * Gets the processor batch size 
     * @return
     * @since 4.3
     */
    String getProcessorBatchSize();
    /**
     * Gets the connector batch size 
     * @return
     * @since 4.3
     */
    String getConnectorBatchSize();    
    
    /**
     * Unload the User defined functions file.  
     * @throws MetaMatrixComponentException
     */
    void unloadUDF() throws MetaMatrixComponentException;
    
    /**
     * Load the UDF function model 
     * @throws MetaMatrixComponentException
     */
    void loadUDF() throws MetaMatrixComponentException;
    
    void clearClassLoaderCache() throws MetaMatrixComponentException;
    
    boolean isFullyConfiguredVDB(VDBArchive vdb) throws MetaMatrixComponentException;
    
    String getClusterName();
}
