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

package com.metamatrix.dqp.embedded.services;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.SessionServiceException;
import com.metamatrix.common.application.AbstractClassLoaderManager;
import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.ConnectorBindingType;
import com.metamatrix.common.config.api.ExtensionModule;
import com.metamatrix.common.config.model.BasicConnectorBinding;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.protocol.URLHelper;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.common.vdb.api.ModelInfo;
import com.metamatrix.common.vdb.api.VDBArchive;
import com.metamatrix.common.vdb.api.VDBDefn;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.core.vdb.VDBStatus;
import com.metamatrix.dqp.embedded.DQPEmbeddedPlugin;
import com.metamatrix.dqp.embedded.DQPEmbeddedProperties;
import com.metamatrix.dqp.embedded.configuration.ExtensionModuleReader;
import com.metamatrix.dqp.embedded.configuration.ExtensionModuleWriter;
import com.metamatrix.dqp.embedded.configuration.ServerConfigFileReader;
import com.metamatrix.dqp.embedded.configuration.ServerConfigFileWriter;
import com.metamatrix.dqp.embedded.configuration.VDBConfigurationReader;
import com.metamatrix.dqp.embedded.configuration.VDBConfigurationWriter;
import com.metamatrix.dqp.service.ConfigurationService;
import com.metamatrix.dqp.service.ConnectorBindingLifeCycleListener;
import com.metamatrix.dqp.service.DQPServiceNames;
import com.metamatrix.dqp.service.VDBLifeCycleListener;
import com.metamatrix.dqp.util.LogConstants;
import com.metamatrix.platform.security.api.MetaMatrixSessionInfo;
import com.metamatrix.platform.security.api.service.SessionListener;
import com.metamatrix.platform.security.api.service.SessionServiceInterface;
import com.metamatrix.platform.util.ProductInfoConstants;
import com.metamatrix.query.function.FunctionLibraryManager;
import com.metamatrix.query.function.UDFSource;
import com.metamatrix.vdb.runtime.BasicModelInfo;
import com.metamatrix.vdb.runtime.BasicVDBDefn;
import com.metamatrix.vdb.runtime.VDBKey;


/** 
 * A File based configuration service implementation
 * Implementation notes
 * - While loading the VDBs also keep track of Connector Bindings loaded separately
 * - While loading the VDBs also keep track of Connector types loaded separately
 * - The above list also should contain the list from server config file.
 * - Onlu used get, save, delete of any configuration on public api
 * @since 4.3
 */
public class EmbeddedConfigurationService extends EmbeddedBaseDQPService implements ConfigurationService {
    
    private static final String VDB_LIST_SEPARATOR = ";"; //$NON-NLS-1$
    private static final String VDB = ".vdb"; //$NON-NLS-1$
    private static final String DEF = ".def"; //$NON-NLS-1$
    
	public final static String PROPERTIES_URL = "dqp.bootstrap"; //$NON-NLS-1$
    
    private Properties userPreferences;
    private URL bootStrapURL;
    private Map<VDBKey, VDBArchive> loadedVDBs = new HashMap<VDBKey, VDBArchive>();
    Map<String, ConnectorBinding> loadedConnectorBindings = new HashMap<String, ConnectorBinding>();
    Map<String, ComponentType> loadedConnectorTypes = new HashMap<String, ComponentType>(); 

    // load time constructs
    private Map<VDBKey, URL> availableVDBFiles = new HashMap<VDBKey, URL>();
    ConfigurationModelContainer configurationModel;
    private ArrayList<VDBLifeCycleListener> vdbLifeCycleListeners = new ArrayList<VDBLifeCycleListener>();
    private ArrayList<ConnectorBindingLifeCycleListener> connectorBindingLifeCycleListeners = new ArrayList<ConnectorBindingLifeCycleListener>();
    private UDFSource udfSource;
    private Map<ConnectorBindingType, Properties> defaultConnectorTypePropertiesCache = new HashMap<ConnectorBindingType, Properties>();
    
    private AbstractClassLoaderManager classLoaderManager = new AbstractClassLoaderManager(Thread.currentThread().getContextClassLoader(), true, true) {
    	
    	@Override
    	public String getCommonExtensionClassPath() {
    		return getUserPreferences().getProperty(DQPEmbeddedProperties.COMMON_EXTENSION_CLASPATH, ""); //$NON-NLS-1$
    	}
    	
    	@Override
    	public URL parseURL(String url) throws MalformedURLException {
    		return ExtensionModuleReader.resolveExtensionModule(url, getExtensionPath());
    	}
    };
    
    public URL getBootStrapURL() {
		return bootStrapURL;
	}
    
    @Inject public void setBootStrapURL(@Named("BootstrapURL") URL bootStrapURL) {
		this.bootStrapURL = bootStrapURL;
	}
    
    boolean valid(String str) {
        if (str != null) {
            str = str.trim();
            return str.length() > 0;
        }
        return false;   
    }
    
    /** 
     * @see com.metamatrix.dqp.service.ConfigurationService#getSystemVdb()
     * @since 4.3
     */
    public URL getSystemVdb() {
        String systemVDB = getUserPreferences().getProperty(DQPEmbeddedProperties.DQP_METADATA_SYSTEMURL);
        if (valid(systemVDB)) {
            return getFullyQualifiedPath(systemVDB);
        }
        return Thread.currentThread().getContextClassLoader().getResource("System.vdb"); //$NON-NLS-1$
    }

    /** 
     * @see com.metamatrix.dqp.service.ConfigurationService#getSystemProperties()
     * @since 4.3
     */
    public Properties getSystemProperties() {
        return getUserPreferences();
    }

    /** 
     * @see com.metamatrix.dqp.service.ConfigurationService#setSystemProperty(java.lang.String, java.lang.String)
     * @since 4.3
     */
    public void setSystemProperty(String key, String value) throws MetaMatrixComponentException {
        getUserPreferences().setProperty(key, value);
        DQPEmbeddedPlugin.logInfo("EmbeddedConfigurationService.add_system_property", new Object[] {key, value}); //$NON-NLS-1$
        this.configurationModel = ServerConfigFileWriter.addProperty(getSystemConfiguration(), key, value);
        saveSystemConfiguration(configurationModel);
    }
    
    /** 
     * @see com.metamatrix.dqp.service.ConfigurationService#updateSystemProperties(java.util.Properties)
     * @since 4.3
     */
    public void updateSystemProperties(Properties properties) throws MetaMatrixComponentException {
        getUserPreferences().putAll(properties);
        DQPEmbeddedPlugin.logInfo("EmbeddedConfigurationService.update_system_properties", new Object[] {properties}); //$NON-NLS-1$
        this.configurationModel = ServerConfigFileWriter.addProperties(getSystemConfiguration(), properties);
        saveSystemConfiguration(configurationModel);
    }
    
    
    
    /** 
     * @see com.metamatrix.dqp.service.ConfigurationService#getSystemConfiguration()
     * @since 4.3
     */
    public ConfigurationModelContainer getSystemConfiguration() 
        throws MetaMatrixComponentException {
        if (this.configurationModel == null) {            
            try {
                URL configFile = getConfigFile();
                ServerConfigFileReader configReader = new ServerConfigFileReader(configFile);
                this.configurationModel = configReader.getConfiguration();
            } catch (IOException e) {
                throw new MetaMatrixComponentException(e);
            } 
        }
        return this.configurationModel;
    }
    
    /** 
     * @see com.metamatrix.dqp.service.ConfigurationService#saveSystemConfiguration(com.metamatrix.common.config.api.ConfigurationModelContainer)
     * @since 4.3
     */
    void saveSystemConfiguration(ConfigurationModelContainer model) 
        throws MetaMatrixComponentException {        
        DQPEmbeddedPlugin.logInfo("EmbeddedConfigurationService.savingConfiguration", null); //$NON-NLS-1$                
        URL configFile = getConfigFile();
        ServerConfigFileWriter.write(model, configFile);
    }
    
    /**  
     * @see com.metamatrix.dqp.service.ConfigurationService#getConfigFile()
     */
    public URL getConfigFile() {
        String configFile = getUserPreferences().getProperty(DQPEmbeddedProperties.DQP_CONFIGFILE, "configuration.xml"); //$NON-NLS-1$
        if (valid(configFile)) {
            return getFullyQualifiedPath(configFile);
        }
        return null;
    }
    
    /** 
     * @see com.metamatrix.dqp.service.ConfigurationService#getUDFFile()
     * @since 4.3
     */
    public URL getUDFFile() {
        try {
			String udfFile = getUserPreferences().getProperty(DQPEmbeddedProperties.USER_DEFINED_FUNCTIONS);
			if (valid(udfFile)) {
				return ExtensionModuleReader.resolveExtensionModule(udfFile, getExtensionPath());
			}
			return ExtensionModuleReader.resolveExtensionModule(ExtensionModuleReader.MM_JAR_PROTOCOL+":"+USER_DEFINED_FUNCTION_MODEL, getExtensionPath()); //$NON-NLS-1$
		} catch (IOException e) {
			return null;
		}
    }
    
    @Override
    public ClassLoader getCommonClassLoader(String urls) {
    	return this.classLoaderManager.getCommonClassLoader(urls);
    }
    
    @Override
    public ClassLoader getPostDelegationClassLoader(String urls) {
    	return this.classLoaderManager.getPostDelegationClassLoader(urls);
    }
    
    public AbstractClassLoaderManager getClassLoaderManager() {
		return classLoaderManager;
	}
    
    /** 
     * @see com.metamatrix.dqp.service.ConfigurationService#getVDBs()
     * @since 4.3
     */
    public List<VDBArchive> getVDBs() throws MetaMatrixComponentException {
        return new ArrayList<VDBArchive>(loadedVDBs.values());
    }

    /** 
     * @see com.metamatrix.dqp.service.ConfigurationService#getVDB(java.lang.String, java.lang.String)
     * @since 4.3
     */
    public VDBArchive getVDB(String vdbName, String vdbVersion) 
        throws MetaMatrixComponentException {
        return loadedVDBs.get(vdbId(vdbName, vdbVersion));
    }
        
    /**
     * Look at the current VDB directory, and compute, query and find the
     * right file to save or overwrite the VDB given. 
     * @param vdb
     * @return URL - never null;
     * @since 4.3
     */
    URL getNewVDBLocation(VDBArchive vdb) {
        // This is a new VDB, so where other vdbs are stored. Also since we storing this as
        // single VDB/DEF combination, update the archive file info.
        String vdbName = vdb.getName()+"_"+vdb.getVersion()+VDB; //$NON-NLS-1$
        URL fileToSave = getFullyQualifiedPath(getVDBSaveLocation(), vdbName);  
        
		BasicVDBDefn def = vdb.getConfigurationDef();
		def.setFileName(vdbName);     
        
        return fileToSave;
    }
    
    URL getVDBSaveLocation() {
        URL[] urls = getVDBLocations();
        for (int i = 0; i < urls.length; i++) {
            String vdblocation = urls[i].toString().toLowerCase();
            if (!vdblocation.endsWith(VDB) && !vdblocation.endsWith(DEF)) {
                return urls[i];
            }
        }
        return urls[0];
    }    
    
    /** 
     * @see com.metamatrix.dqp.service.ConfigurationService#saveVDB(com.metamatrix.metadata.runtime.admin.vdb.VDBDefn)
     * @since 4.3
     */
    public void saveVDB(VDBArchive srcVdb, String version) throws MetaMatrixComponentException{
        
        // if the vdb version being saved is DELETED then if nobody using it 
        if (srcVdb.getStatus() == VDBStatus.DELETED && canDeleteVDB(srcVdb.getName(), srcVdb.getVersion())) {
            deleteVDB(srcVdb);
            return;
        }
        
        // if requested to save as latest version then find the latest version.
        BasicVDBDefn def = srcVdb.getConfigurationDef();
        if (version.equals(NEXT_VDB_VERSION)) {
            // get the latest version and change the version and VDB file.
            String nextVersion=Integer.toString(getNextVdbVersion(def.getName()));
            def.setVersion(nextVersion);
            
            String archiveFileName = def.getFileName();
            int index = archiveFileName.indexOf('.');
            if (index != -1) {
                archiveFileName = archiveFileName.substring(0, index)+"_"+nextVersion+".vdb"; //$NON-NLS-1$ //$NON-NLS-2$
            }
            def.setFileName(archiveFileName);
        }
        
        try {
			srcVdb.updateConfigurationDef(srcVdb.getConfigurationDef());
		} catch (IOException e) {
			throw new MetaMatrixComponentException(e);
		}
        
        // make sure we match up the connector binding based on user preferences
        URL vdbFile = availableVDBFiles.get(vdbId(srcVdb));
        if (vdbFile == null) {
        	vdbFile = getNewVDBLocation(srcVdb);
        	VDBConfigurationWriter.write(srcVdb, vdbFile);
        	srcVdb = VDBConfigurationReader.loadVDB(vdbFile, getDeployDir());
			deployVDB(vdbFile, srcVdb);
        	notifyVDBLoad(def.getName(), def.getVersion());
        } 
        
        DQPEmbeddedPlugin.logInfo("EmbeddedConfigurationService.vdb_saved", new Object[] {def.getName(), def.getVersion(), vdbFile}); //$NON-NLS-1$
    }

    private VDBArchive loadVDB(VDBArchive vdb, boolean replaceBindings) throws MetaMatrixComponentException {
        // check if this is a valid VDB
        if (!isValidVDB(vdb)) {
        	throw new MetaMatrixComponentException(DQPEmbeddedPlugin.Util.getString("EmbeddedConfigurationService.invalid_vdb", vdb.getName())); //$NON-NLS-1$
        }
                                      
        // add connector types from the VDB to the configuration
        addConnectorTypesInVDB(vdb, replaceBindings);
        
        // now try to add the connector bindings in the VDB to the configuration service
        addConnectorBindingsInVDB(vdb, replaceBindings);
        
        // make sure we have all the bindings, otherwise this is incomplete VDB
        if (!isFullyConfiguredVDB(vdb)) {
            // mark as in-active
            vdb.setStatus(VDBStatus.INCOMPLETE);                                
        }
        else {
            vdb.setStatus(VDBStatus.ACTIVE);
            DQPEmbeddedPlugin.logInfo("VDBService.vdb_active", new Object[] {vdb.getName(), vdb.getVersion()}); //$NON-NLS-1$                                
        }    	
        
        return vdb;
    }
    
    /**  
     * @see com.metamatrix.dqp.service.ConfigurationService#addVDB(com.metamatrix.common.vdb.api.VDBDefn, boolean)
     */
    public VDBArchive addVDB(VDBArchive vdb, boolean replaceBindings) throws MetaMatrixComponentException{
        if (vdb != null) {             
            boolean exists = false;

            // check to see if we already have vdb with same name and version.
            VDBArchive existingVdb = getVDB(vdb.getName(), vdb.getVersion());
            if (existingVdb != null) {                
                exists = true;
                DQPEmbeddedPlugin.logWarning("VDBService.vdb_already_exists", new Object[] {existingVdb.getName(), existingVdb.getVersion()}); //$NON-NLS-1$ 
            } 
            
            // load the vdb an its connector bindings
            vdb = loadVDB(vdb, replaceBindings);
            
            // Now save the VDB for future use using the Configuration.
            // configuration may alter the connector bindings on VDB based
            // upon preferences set.
            saveVDB(vdb, exists ? ConfigurationService.NEXT_VDB_VERSION : vdb.getVersion());
                        
            DQPEmbeddedPlugin.logInfo("VDBService.vdb_deployed", new Object[] {vdb.getName(), vdb.getVersion()}); //$NON-NLS-1$
            
            return vdb;
        }
        throw new MetaMatrixComponentException(DQPEmbeddedPlugin.Util.getString("VDBService.failed_load")); //$NON-NLS-1$
    }

    /**
     * Add the connector bindings in the given vdb to the configuration service. 
     */
    void addConnectorBindingsInVDB(VDBArchive vdb, boolean replace) 
        throws MetaMatrixComponentException {
        
    	BasicVDBDefn def = vdb.getConfigurationDef();
    	
        Collection bindings = def.getConnectorBindings().values();   
        for (final Iterator i = bindings.iterator(); i.hasNext();) {
            
            final ConnectorBinding binding = (ConnectorBinding)i.next();
            
            // check if there are any bindings with same name/ or shared connector binding
            String deployedBindingName = binding.getDeployedName();
            if (deployedBindingName == null) {
            	deployedBindingName = binding.getFullName();
            }
            
            // if replace is true we can accept the partial match other wise not.
            ConnectorBinding existing = getConnectorBinding(deployedBindingName);
            
            // only in the case that we do not find the binding we want to add the new connector
            // as bindings are vdb scoped they will never find one, unless there is shared one, 
            // then we need to use that one
            if (existing == null || replace) {
                saveConnectorBinding(deployedBindingName, binding, false);
            }
            else {
                // if the not being replaced, need to use the current one, then
                // vdb need to be updated to use the current one, so that this
                // holds true when DQP restarted. Also, this will be only the case
                // when shared binding is used.                    
                def.addConnectorBinding(existing);
                saveVDB(vdb, vdb.getVersion());
            }            
        }        
    }    
    
    /**
     * Add connector types from the VDB to the configuration.  
     */
    void addConnectorTypesInVDB(VDBArchive vdb, boolean replace) 
        throws MetaMatrixComponentException{
        
    	BasicVDBDefn def = vdb.getConfigurationDef();
        Map types = def.getConnectorTypes();
        for (final Iterator i = types.keySet().iterator(); i.hasNext();) {
            final String typeName = (String)i.next();
            ConnectorBindingType localType = getConnectorType(typeName);
            if (localType == null || replace) {
                final ConnectorBindingType type = (ConnectorBindingType)types.get(typeName);
               	saveConnectorType(type, false);
            }                        
        } // for
    }
    
    /**
     * Find the latest version of the given vdb  
     * @param vdb
     * @param latestVersion
     * @since 4.3
     */
    int getNextVdbVersion(String vdbName) {
        int latestVersion = 0;
        for (VDBArchive vdb: loadedVDBs.values()) {
            if (vdb.getName().equals(vdbName)) { 
                latestVersion = Math.max(latestVersion, Integer.parseInt(vdb.getVersion()));
            }
        }
        return latestVersion+1;
    }
    
    /** 
     * @see com.metamatrix.dqp.service.ConfigurationService#deleteVDB(java.lang.String, java.lang.String)
     * @since 4.3
     */
    public void deleteVDB(VDBArchive vdb) 
        throws MetaMatrixComponentException {
                
    	try {
    		
    		URL vdbFile = availableVDBFiles.remove(vdbId(vdb));
    		
    		Assertion.isNotNull(vdbFile);
    		
	        // delete the def/vdb files
	        VDBConfigurationWriter.deleteVDB(vdb, vdbFile);
	
	        // Notify any listeners that vdb is deleted
	        notifyVDBUnLoad(vdb.getName(), vdb.getVersion());
	        
	        // remove from local references.
	        loadedVDBs.remove(vdbId(vdb));
	        
	        VDBDefn def = vdb.getConfigurationDef();
	                
	        deleteOrphanedConnectorBindings(def.getConnectorBindings().values());
	        
	        DQPEmbeddedPlugin.logInfo("EmbeddedConfigurationService.vdb_delete", new Object[] {vdb.getName(), vdb.getVersion()}); //$NON-NLS-1$
    	}finally {
    		vdb.close();
    	}
    }
    
    /**
	 * Delete any connector bindings that are in orphan state.
	 */
	 private void deleteOrphanedConnectorBindings(Collection connectorBindings) throws MetaMatrixComponentException {
		 for (Iterator i = connectorBindings.iterator(); i.hasNext();) {
			 ConnectorBinding binding = (ConnectorBinding)i.next();
			 // may be we do not want to delete the bindings in the config.xml??
			 List vdbs = getVdbsThatUseConnectorBinding(binding.getDeployedName());
			 if (vdbs == null || vdbs.isEmpty()) {
				 deleteConnectorBinding(binding.getDeployedName());
			 }
	 	}
	 }    
    
    /**  
     * @see com.metamatrix.dqp.service.ConfigurationService#assignConnectorBinding(java.lang.String, java.lang.String, java.lang.String, com.metamatrix.common.config.api.ConnectorBinding[])
     */
    public void assignConnectorBinding(String vdbName, String version, String modelName, ConnectorBinding[] bindings)  
        throws MetaMatrixComponentException {
        
        ConnectorBinding binding = bindings[0];        
        VDBArchive vdb = getVDB(vdbName, version);
        
        BasicVDBDefn def = vdb.getConfigurationDef();
        if (binding != null && def != null) {
            // Get the model from the VDB, note that we are referencing BasicModelInfo
            // this is implementation of the ModelInfo class.             
            BasicModelInfo model = (BasicModelInfo)def.getModel(modelName);
            if (model != null) {
                // Build the new routing list
                List newBindingNames = new ArrayList();
                for (int i=0; i<bindings.length; i++) {
                    
                    // now since this binding is OK we can put it in the VDB
                    ConnectorBinding tgtBinding = bindings[i];
                    
                    // always full name is deployed name and name is name of the connector binding
                    // inside the vdb
                    newBindingNames.add(tgtBinding.getFullName());
                    
                    // get connector type from the configuration service
                    ComponentType type = getConnectorType(tgtBinding.getComponentTypeID().getName());

                    // Also add into the new binding into the VDB, Since they are stored in hash
                    // only one copy is stored, or replaced.
                    def.addConnectorType(type);
                    def.addConnectorBinding(model.getName(), tgtBinding);
                }                           
                
                // Update the model with the new info
                model.setConnectorBindingNames(newBindingNames);
                
                // with above changes there may be verbose connector bindings in vdb from 
                // from previous assignment, we need remove them. Otherwise we will have
                // Unnecessary objects hanging around.
                List orphanBindings = new ArrayList();
                Map currentBindings = def.getConnectorBindings();
                for (Iterator i = currentBindings.values().iterator(); i.hasNext();) {
                    ConnectorBinding currentBinding = (ConnectorBinding)i.next();
                    if (!def.isBindingInUse(currentBinding)) {
                    	def.removeConnectorBinding(currentBinding.getFullName());
                        orphanBindings.add(currentBinding);
                    }
                }
                
                // Save the new vdb/model defination into the persistent store.
                saveVDB(vdb, vdb.getVersion());
                
                deleteOrphanedConnectorBindings(orphanBindings);
                
                DQPEmbeddedPlugin.logInfo("VDBService.connector_binding_changed", new Object[] {vdbName, version, modelName, newBindingNames}); //$NON-NLS-1$
            }
            else {
            	throw new MetaMatrixComponentException(DQPEmbeddedPlugin.Util.getString("VDBService.VDB_does_not_exist._3", new Object[] {vdbName, version, modelName})); //$NON-NLS-1$
            }
        } else {
        	throw new MetaMatrixComponentException(DQPEmbeddedPlugin.Util.getString("VDBService.VDB_does_not_exist._2", new Object[] {vdbName, version})); //$NON-NLS-1$
        }
    }
    
    /** 
     * @see com.metamatrix.dqp.service.ConfigurationService#getConnectorBindings()
     * @since 4.3
     */
    public List getConnectorBindings() throws MetaMatrixComponentException {
        return new ArrayList(loadedConnectorBindings.values());
    }
    
    
    /** 
     * @see com.metamatrix.dqp.service.ConfigurationService#getConnectorBinding(java.lang.String)
     * @since 4.3
     */
    public ConnectorBinding getConnectorBinding(String deployedBindingName) 
    	throws MetaMatrixComponentException {      
    	return loadedConnectorBindings.get(deployedBindingName);
    }
    
    /**
     * Get the default properties for the connector binding given.
     * @param binding
     * @return properties for the connector binding given
     */   
    public Properties getDefaultProperties(ConnectorBindingType type) {
    	if (type != null) {
	    	Properties props = null;
	    	synchronized (this.defaultConnectorTypePropertiesCache) {
	    		props = this.defaultConnectorTypePropertiesCache.get(type);
	    		if (props == null) {
	    	    	props = new Properties(getUserPreferences());
	    	        props = configurationModel.getDefaultPropertyValues(props, (ComponentTypeID)type.getID());
	    	        if (props.isEmpty()) {
	    	        	props = type.getDefaultPropertyValues(props);
	    	        }      
	    	        this.defaultConnectorTypePropertiesCache.put(type, props);
	    		}
			}
	        return props;
    	}
    	return new Properties(getUserPreferences());
    }
    
    /**
     * Return a list of VDBs which are using the given connector binding 
     * @param binding - ConnectorBinding
     * @return list of bindings use the binding or empty list
     */    
    List<VDBArchive> getVdbsThatUseConnectorBinding(String deployedBindingName) {
        List<VDBArchive> list = new ArrayList();
        
        for (VDBArchive vdb:loadedVDBs.values()) {
        	BasicVDBDefn def = vdb.getConfigurationDef();
            Collection bindings = def.getConnectorBindings().values();
            for (final Iterator j = bindings.iterator(); j.hasNext();) {
                final ConnectorBinding deployedBinding = (ConnectorBinding)j.next();
                if (deployedBinding.getDeployedName().equals(deployedBindingName)) {
                    list.add(vdb);
                }
            } // for
        }
        return list;        
    }
    
    /**  
     * @see com.metamatrix.dqp.service.ConfigurationService#addConnectorBinding(java.lang.String, com.metamatrix.common.config.api.ConnectorBinding, boolean)
     */
    public ConnectorBinding addConnectorBinding(String deployedBindingName, ConnectorBinding binding, boolean replace) 
        throws MetaMatrixComponentException {
        
        boolean add = true;

        // check to see if the binding already exists
        ConnectorBinding existingBinding = getConnectorBinding(deployedBindingName);
        if (existingBinding != null) {
            if (replace) {
            	DQPEmbeddedPlugin.logInfo("DataService.Connector_exists_replace", new Object[] {existingBinding.getDeployedName()}); //$NON-NLS-1$
                
                // if this binding is currently assigned, the next save will replace the
                // original so no need to delete; that will save us to make the vdb 
                // un-assign before and re-assign after.
            	if (!getVdbsThatUseConnectorBinding(existingBinding.getDeployedName()).isEmpty()) {
            		notifyConnectorBindingUnLoad(existingBinding.getDeployedName());
                }
                else {
                	deleteConnectorBinding(existingBinding.getDeployedName());
                }
            }
            else {
                add = false;
                DQPEmbeddedPlugin.logInfo("DataService.Connector_exists", new Object[] {existingBinding.getDeployedName()}); //$NON-NLS-1$
            }
        }

        // now that we figured we need to add this to the configuration
        if (add) {
            // Check if we have connector type for the binding
            String typeName = binding.getComponentTypeID().getName();
            ComponentType type = getConnectorType(typeName);           
            if (type != null) {
                // Ask the Configuration Manager to save the connector Binding
                binding = saveConnectorBinding(deployedBindingName, binding, true);
                DQPEmbeddedPlugin.logInfo("DataService.Connector_Added", new Object[] {binding.getDeployedName()}); //$NON-NLS-1$                
                return binding;
            }
            throw new MetaMatrixComponentException(DQPEmbeddedPlugin.Util.getString("DataService.Connector_type_does_not_exists", new Object[] {typeName})); //$NON-NLS-1$ 
        }
        return binding;
    }
        
    /**  
     * @see com.metamatrix.dqp.service.ConfigurationService#updateConnectorBinding(com.metamatrix.common.config.api.ConnectorBinding)
     */
    public ConnectorBinding updateConnectorBinding(ConnectorBinding binding) 
        throws MetaMatrixComponentException {
       return saveConnectorBinding(binding.getDeployedName(), binding, false); 
    }
    
    /**
     * Save the Connector Binding to the internal list.  
     */
    ConnectorBinding saveConnectorBinding(String deployedBindingName, ConnectorBinding binding, boolean updateConfiguration) 
        throws MetaMatrixComponentException {        
        
        if (binding != null) {         
        	
        	// add the binding to the internal list.
        	binding = deployConnectorBinding(deployedBindingName, binding);
        	
            // check if it is being used in any vdbs
            List<VDBArchive> usedVDBs = getVdbsThatUseConnectorBinding(deployedBindingName);
            boolean used = (usedVDBs != null && !usedVDBs.isEmpty());
            if (used) {
                for (VDBArchive vdb:usedVDBs) {

                	BasicVDBDefn def = vdb.getConfigurationDef();
                	def.addConnectorBinding(binding);
                	
                    // we may need to save the VDB's here..
                    saveVDB(vdb, vdb.getVersion());                    
                }
            }
            
            if (updateConfiguration || isGlobalConnectorBinding(binding)) {
	            this.configurationModel = ServerConfigFileWriter.addConnectorBinding(configurationModel, binding);
	            saveSystemConfiguration(this.configurationModel);
	            DQPEmbeddedPlugin.logInfo("EmbeddedConfigurationService.connector_save", new Object[] {deployedBindingName}); //$NON-NLS-1$
            }
        }        
        return binding;
    }
    
    private boolean isGlobalConnectorBinding(ConnectorBinding binding) {
    	return ServerConfigFileReader.containsBinding(this.configurationModel, binding.getDeployedName());
	}

	/** 
     * @see com.metamatrix.dqp.service.ConfigurationService#deleteConnectorBinding(java.lang.String)
     * @since 4.3
     */
    public void deleteConnectorBinding(String deployedConnectorBindingName) throws MetaMatrixComponentException {        
        ConnectorBinding binding = getConnectorBinding(deployedConnectorBindingName);
        
        if (binding != null) {
            // First make sure they are not being used before we delete
            List<VDBArchive> usedVDBs = getVdbsThatUseConnectorBinding(binding.getDeployedName());
            boolean used = (usedVDBs != null && !usedVDBs.isEmpty());
            if (used) {
            	VDBArchive vdb = usedVDBs.get(0);
                throw new MetaMatrixComponentException(DQPEmbeddedPlugin.Util.getString("EmbeddedConfigurationService.ConnectorBinding_in_use_failed_delete", new Object[] {deployedConnectorBindingName, vdb.getName(), vdb.getVersion()})); //$NON-NLS-1$
            }
            
            // notify all the listeners that connector binding is being deleted.
            notifyConnectorBindingUnLoad(binding.getDeployedName());
            
            // Now remove from local, server configuration. We not need to save VDB
            // as they probably already been saved, when they mapped to another binding.
            DQPEmbeddedPlugin.logInfo("EmbeddedConfigurationService.connector_delete", new Object[] {binding.getDeployedName()}); //$NON-NLS-1$            
            loadedConnectorBindings.remove(binding.getDeployedName());

            if (isGlobalConnectorBinding(binding)) {
	            // only save to the configuration xml only if the shared tag is set to true
	            this.configurationModel = ServerConfigFileWriter.deleteConnectorBinding(configurationModel, binding);
	            saveSystemConfiguration(this.configurationModel);
            }
        }
        else {
            throw new MetaMatrixComponentException(DQPEmbeddedPlugin.Util.getString("EmbeddedConfigurationService.connector_binding_delete_failed", deployedConnectorBindingName)); //$NON-NLS-1$
        }
    }
        
    /** 
     * @see com.metamatrix.dqp.service.ConfigurationService#getConnectorTypes()
     * @since 4.3
     */
    public List getConnectorTypes() throws MetaMatrixComponentException {
        return new ArrayList(loadedConnectorTypes.values());
    } 
    
    /** 
     * @see com.metamatrix.dqp.service.ConfigurationService#getExtensionPath()
     * @since 4.3
     */
    public URL[] getExtensionPath() {
        String path = getUserPreferences().getProperty(DQPEmbeddedProperties.DQP_EXTENSIONS, "./extensions/"); //$NON-NLS-1$
        if (valid(path)) {
        	ArrayList<URL> urlPaths = new ArrayList<URL>();
        	StringTokenizer st = new StringTokenizer(path, ";"); //$NON-NLS-1$
        	while(st.hasMoreElements()) {
        		urlPaths.add(getFullyQualifiedPath(st.nextToken()));
        	}
            return urlPaths.toArray(new URL[urlPaths.size()]);
        }
        return new URL[0];
    }
    
    /**  
     * @see com.metamatrix.dqp.service.ConfigurationService#useExtensionClasspath()
     */
    public boolean useExtensionClasspath() {
        String path = getUserPreferences().getProperty(DQPEmbeddedProperties.DQP_EXTENSIONS);
        return valid(path);
    }
    
    /** 
     * @see com.metamatrix.dqp.service.ConfigurationService#getExtensionModule(java.lang.String)
     * @since 4.3
     */
    public ExtensionModule getExtensionModule(String extModuleName) throws MetaMatrixComponentException {
    	return ExtensionModuleReader.loadExtensionModule(extModuleName, getExtensionPath());
    }
    
    /** 
     * @see com.metamatrix.dqp.service.ConfigurationService#getExtensionModules()
     * @since 4.3
     */
    public List<ExtensionModule> getExtensionModules() throws MetaMatrixComponentException {
    	return ExtensionModuleReader.loadExtensionModules(getExtensionPath());
    }
    
    /** 
     * @see com.metamatrix.dqp.service.ConfigurationService#saveExtensionModule(com.metamatrix.common.config.api.ExtensionModule)
     * @since 4.3
     */
    public void saveExtensionModule(ExtensionModule extModule) throws MetaMatrixComponentException {
    	this.classLoaderManager.clearCache();
        ExtensionModuleWriter.write(extModule, getExtensionPath());                 
    }

    /** 
     * @see com.metamatrix.dqp.service.ConfigurationService#deleteExtensionModule(java.lang.String)
     * @since 4.3
     */
    public void deleteExtensionModule(String extModuleName) throws MetaMatrixComponentException {
    	this.classLoaderManager.clearCache();
    	ExtensionModuleWriter.deleteModule(extModuleName, getExtensionPath());
    }    
    
    /** 
     * @see com.metamatrix.dqp.service.ConfigurationService#getConnectorType(com.metamatrix.common.config.api.ComponentTypeID)
     * @since 4.3
     */
    public ConnectorBindingType getConnectorType(String connectorType) throws MetaMatrixComponentException {
        return (ConnectorBindingType)loadedConnectorTypes.get(connectorType);
    }

    /** 
     * @see com.metamatrix.dqp.service.ConfigurationService#addConnectorType(com.metamatrix.common.config.api.ComponentType)
     * @since 4.3
     */
    public void saveConnectorType(ConnectorBindingType type) throws MetaMatrixComponentException {
    	saveConnectorType(type, true);
    }
    
    private void saveConnectorType(ConnectorBindingType type, boolean updateConfiguration) throws MetaMatrixComponentException {
    	type = (ConnectorBindingType)ServerConfigFileReader.resolvePropertyDefns(type, this.configurationModel);
        loadedConnectorTypes.put(type.getName(), type);

        if (updateConfiguration) {
	        // Also add binding type to the configuration and save.        
	        DQPEmbeddedPlugin.logInfo("EmbeddedConfigurationService.connector_type_save", new Object[] {type.getName()}); //$NON-NLS-1$
	        this.configurationModel = ServerConfigFileWriter.addConnectorType(configurationModel, type);
	        saveSystemConfiguration(this.configurationModel);
        }
    }    

    /** 
     * @see com.metamatrix.dqp.service.ConfigurationService#deleteConnectorType(java.lang.String)
     * @since 4.3
     */
    public void deleteConnectorType(String deployedConnectorType) throws MetaMatrixComponentException {
        ConnectorBindingType type = (ConnectorBindingType)loadedConnectorTypes.remove(deployedConnectorType);
        if (type != null) {
            if (isConnectorTypeInUse(type)) {
                throw new MetaMatrixComponentException(DQPEmbeddedPlugin.Util.getString("EmbeddedConfigurationService.connector_type_in_use", deployedConnectorType)); //$NON-NLS-1$
            }
            // Also delete binding type to the configuration and save.        
            DQPEmbeddedPlugin.logInfo("EmbeddedConfigurationService.connector_type_delete", new Object[] {deployedConnectorType}); //$NON-NLS-1$
            this.configurationModel = ServerConfigFileWriter.deleteConnectorType(configurationModel, type);
            saveSystemConfiguration(this.configurationModel);
        }
        else {
            throw new MetaMatrixComponentException(DQPEmbeddedPlugin.Util.getString("EmbeddedConfigurationService.connector_type_delete_failed", deployedConnectorType)); //$NON-NLS-1$
        }
    }   
     
    /**
     * Check to see if the connector type is currently in use. 
     * @param type - Connector Type
     * @return true if yes; false otherwise
     */
    boolean isConnectorTypeInUse(ConnectorBindingType type) {        
        for (Iterator i = loadedConnectorBindings.values().iterator(); i.hasNext();) {
            ConnectorBinding binding = (ConnectorBinding)i.next();
            if (binding.getComponentTypeID().equals(type.getID())) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Load the User defined functions file 
     * @throws MetaMatrixComponentException
     * @since 4.3
     */
    public void loadUDF() throws MetaMatrixComponentException {
        URL udfFile = getUDFFile();
        if(udfFile != null && exists(udfFile)) {
            try {
            	
            	// un-register the old UDF model, if there is one.
            	unloadUDF();

        		this.udfSource = new UDFSource(udfFile.openStream(), getCommonClassLoader(null));
				FunctionLibraryManager.registerSource(this.udfSource);
				LogManager.logInfo(LogConstants.CTX_DQP, DQPEmbeddedPlugin.Util.getString("EmbeddedConfigurationService.udf_load", udfFile)); //$NON-NLS-1$
			} catch (IOException e) {
				LogManager.logError(LogConstants.CTX_DQP, e, e.getMessage());
			}
        }
    }
    
    /**
     * Makes sure the URL is pointing to a valid resource
     * @param url
     * @return true if url is valid;false otherwise
     */
    boolean exists(URL url) {
    	try {
			InputStream in = url.openStream();
			in.read();
			return true;
		} catch (IOException e) {			
		}
		return false;
    }
    
    /**
     * Unload the UDF file at the end of the DQP, because the same JVM could be used
     * to load another DQP and we do want the static function library hanging on to
     * old function libraries   
     */
    public void unloadUDF() {
        if (this.udfSource != null) {
            FunctionLibraryManager.deregisterSource(this.udfSource);
            this.udfSource = null;
            DQPEmbeddedPlugin.logInfo("EmbeddedConfigurationService.udf_unload", new Object[] {}); //$NON-NLS-1$
        }
    }
    
    /** 
     * @see com.metamatrix.dqp.embedded.services.EmbeddedBaseDQPService#initializeService(java.util.Properties)
     * @since 4.3
     */
    public void initializeService(Properties properties) 
        throws ApplicationInitializationException {
        
        try {
            this.setUserPreferences(properties);
                        
            DQPEmbeddedPlugin.logInfo("EmbeddedConfigurationService.dqp_loading", new Object[] {getProcessName()}); //$NON-NLS-1$
            
            // load the configuration file.
            this.configurationModel = getSystemConfiguration();        
            ServerConfigFileReader configReader = loadServerConfigFile();
            
            // Add properties to all the user preferences.
            getUserPreferences().putAll(configReader.getSystemProperties());
            
            // Get the alternate connector bindings from the server configuration
            Map connectorBindings = configReader.getConnectorBindings();
            
            // Add all the connector types.
            Map connectorTypes = configReader.getConnectorTypes();
            
            // load the connector bindings
            loadConnectorBindings(connectorBindings, connectorTypes);
            
            // Load the User defined functions 
            loadUDF();
                        
            // Find all the VDB File in the configuration
            // Load them the available VDBs
            loadVDBs();
                        
        } catch (MetaMatrixComponentException e) {
            throw new ApplicationInitializationException(e);
        } 
    }

    /** 
     * @see com.metamatrix.dqp.embedded.services.EmbeddedBaseDQPService#startService(com.metamatrix.common.application.ApplicationEnvironment)
     * @since 4.3
     */
    public void startService(ApplicationEnvironment environment) 
        throws ApplicationLifecycleException {
    }

    /** 
     * @see com.metamatrix.dqp.embedded.services.EmbeddedBaseDQPService#stopService()
     * @since 4.3
     */
    public void stopService() throws ApplicationLifecycleException {       
    	for(VDBArchive vdb: loadedVDBs.values()) {
    		vdb.close();
    	}
        loadedVDBs.clear();
        loadedConnectorBindings.clear();                       
        loadedConnectorTypes.clear();
        availableVDBFiles.clear();
        unloadUDF();
    }
        
    /**
     * Get path(s) for VDB(s) that are available from the configuration to the DQP
     * engine. 
     * @return URLs to the resources.
     */
    public URL[] getVDBLocations() {
        ArrayList vdbs = new ArrayList();
        String vdbProperty = getUserPreferences().getProperty(DQPEmbeddedProperties.VDB_DEFINITION); 
		if (vdbProperty != null  && vdbProperty.length() != 0) {
            StringTokenizer st = new StringTokenizer(vdbProperty, VDB_LIST_SEPARATOR);
            while( st.hasMoreTokens() ) {
                String token = st.nextToken();
                String vdbLocation = token.toLowerCase();
                URL vdbURL = null;
                if (!vdbLocation.endsWith(VDB) && !vdbLocation.endsWith(DEF) && !vdbLocation.endsWith("/")) { //$NON-NLS-1$
                    token = token+"/"; //$NON-NLS-1$
                }
                vdbURL = getFullyQualifiedPath(token);
                vdbs.add(vdbURL);
            }            
        }
        return (URL[])vdbs.toArray(new URL[vdbs.size()]);
    }
 
    /**
     * Load the Connector Bindings from the VDBS and ServerConfig.xml
     * file. VDB based bindings are preferred, however if a "useConfigFileBindings" is
     * set then the bindings from the config file are used, or they are also used
     * if one binding is not specified in the VDB   
     */
    void loadConnectorBindings(Map configBindings, Map configTypes) 
        throws MetaMatrixComponentException{

        this.loadedConnectorTypes = configTypes;
        
        // assign deploy names to the bindings that are loaded from the configuration.
 	 	for (Iterator i = configBindings.keySet().iterator(); i.hasNext();) {
			String key = (String) i.next();
			BasicConnectorBinding binding = (BasicConnectorBinding) configBindings.get(key);
			if (binding.getDeployedName() == null) {
				binding.setDeployedName(binding.getFullName());
			}
			deployConnectorBinding(binding.getDeployedName(), binding);
		} 	 	
    }



	/** 
     * Add the connector binding with new deployment name
     * @param binding
     * @param deployedName
     */
    private ConnectorBinding deployConnectorBinding(final String deployedName, ConnectorBinding binding) 
        throws MetaMatrixComponentException{
        
        // if this binding is replacing an older binding with same name (could be fully 
        // qualified or partial) we need to replace the original.
        ConnectorBinding previousBinding = getConnectorBinding(deployedName);
        if (previousBinding != null) {
        	notifyConnectorBindingUnLoad(previousBinding.getDeployedName());
            this.loadedConnectorBindings.remove(previousBinding.getDeployedName());
        }
        
        BasicConnectorBinding deployedBinding = (BasicConnectorBinding)binding;        
        deployedBinding.setDeployedName(deployedName);
        loadedConnectorBindings.put(deployedName, deployedBinding);
        notifyConnectorBindingLoad(deployedName);
        DQPEmbeddedPlugin.logInfo("EmbeddedConfigurationService.connector_binding_deployed", new Object[] {deployedName}); //$NON-NLS-1$        
        return deployedBinding;
    }
                
    /**
     * Load all the VDBs specified in the given configuration property.
     * @param vdbFile - Set of VDB files  
     * @throws ApplicationInitializationException
     * @since 4.3
     */
    void loadVDBs() throws ApplicationInitializationException, MetaMatrixComponentException {
        // Get the files to load
        HashMap<URL, VDBArchive> vdbFiles;
		try {
			vdbFiles = VDBConfigurationReader.loadVDBS(getVDBLocations(), getDeployDir()); 
		} catch (MetaMatrixComponentException e) {
			throw new ApplicationInitializationException(e);
		}

        for (URL vdbURL:vdbFiles.keySet()){                               
            
            VDBArchive vdb = vdbFiles.get(vdbURL);
            
            if (vdb != null) {
            	// Check to make sure there are two identical VDBs with same version 
        		// being loaded into DQP
        		if (getVDB(vdb.getName(), vdb.getVersion()) != null) {
        		    throw new ApplicationInitializationException(DQPEmbeddedPlugin.Util.getString("EmbeddedConfigurationService.duplicate_vdb_found", new Object[] {vdbURL})); //$NON-NLS-1$
        		}
            	
            	vdb = loadVDB(vdb, false);
                deployVDB(vdbURL, vdb);
            }
        }
    }

	private void deployVDB(URL vdbURL, VDBArchive vdb) {		
		// add vdb to loaded VDBS
		loadedVDBs.put(vdbId(vdb), vdb);
		availableVDBFiles.put(vdbId(vdb), vdbURL);
		DQPEmbeddedPlugin.logInfo("EmbeddedConfigurationService.loaded_vdb", new Object[] {vdbURL}); //$NON-NLS-1$
	}

	protected File getDeployDir() {
        String deployDirectory = getUserPreferences().getProperty(DQPEmbeddedProperties.DQP_DEPLOYDIR);
        File deployDir = new File(deployDirectory);
        return deployDir;
	}
        
    /**
     * Load a config.xml server configuration file.  This is optional as we are really
     * only grabbing properties out of here.  As an alternative, you can just specify 
     * the properties in the main properties file - those properties should override
     * anything in this config file. 
     */
    ServerConfigFileReader loadServerConfigFile() throws ApplicationInitializationException {
        
        // get the URL to the configuration file
        URL configFile = getConfigFile();
        try {
            if(configFile != null) {
                ServerConfigFileReader configReader = new ServerConfigFileReader(configFile);
                return configReader;                    
            }
            DQPEmbeddedPlugin.logError("EmbeddedConfigurationService.Server_Config_notdefined", null); //$NON-NLS-1$
            throw new ApplicationInitializationException(DQPEmbeddedPlugin.Util.getString("EmbeddedConfigurationService.Server_Config_notdefined")); //$NON-NLS-1$                
        } catch (IOException e) {
            DQPEmbeddedPlugin.logError("EmbeddedConfigurationService.Server_Config_failedload", new Object[] {configFile}); //$NON-NLS-1$
            throw new ApplicationInitializationException(DQPEmbeddedPlugin.Util.getString("EmbeddedConfigurationService.Server_Config_failedload", new Object[] {configFile})); //$NON-NLS-1$
        } 
    }
        
    
    /**
     * Based on the DQP.properties file, get the absolute path to the given file location  
     * @param file - relative or abs path
     * @return - abs path always
     * @throws MetaMatrixComponentException
     * @since 4.3
     */
    URL getFullyQualifiedPath(String file){
        if (file != null) {
            try {
                // since DQP can use metamatrix specific URLs to load the DQP, and we can not
                // register the URLStreamHandler, we need to create the URL with correct handler
                // URLHelper will let us do that.
                return URLHelper.buildURL(bootStrapURL, file);
            } catch (MalformedURLException e) {
                throw new MetaMatrixRuntimeException(e);
            }
        }
        throw new MetaMatrixRuntimeException("bad configuration"); //$NON-NLS-1$
    }
    
    URL getFullyQualifiedPath(URL context, String file){
        if (file != null) {
            try {                
                // since DQP can use metamatrix specific URLs to load the DQP, and we can not
                // register the URLStreamHandler, we need to create the URL with correct handler
                // URLHelper will let us do that.
                return URLHelper.buildURL(context, file);
            } catch (MalformedURLException e) {
                throw new MetaMatrixRuntimeException(e);
            }
        }
        throw new MetaMatrixRuntimeException("bad configuration"); //$NON-NLS-1$
    }    
        
    /** 
     * @see com.metamatrix.dqp.service.ConfigurationService#getConnectionListener()
     * @since 4.3.2
     */
    public SessionListener getSessionListener() {
        return new SessionListener() {
            /**
             * A Client Connection to DQP has been added  
             */
            public void sessionCreated(MetaMatrixSessionInfo connection) {
            }
            
            /**
             * A Client Connection to DQP has been removed  
             */
            public void sessionClosed(MetaMatrixSessionInfo session) {
                
            	String vdbName = session.getProductInfo(ProductInfoConstants.VIRTUAL_DB);
            	String vdbVersion = session.getProductInfo(ProductInfoConstants.VDB_VERSION);
            	if (canDeleteVDB(vdbName, vdbVersion)) {
            		runVDBCleanUp(vdbName, vdbVersion);
            	}
            }
        };
    }
    
    /**
     * Can the given VDB be deleted 
     * @param vdb
     * @return true vdb can be deleted; false otherwise
     */
    private boolean canDeleteVDB(String vdbName, String vdbVersion) {
       	try {
			SessionServiceInterface ssi = (SessionServiceInterface)lookupService(DQPServiceNames.SESSION_SERVICE);
			Collection<MetaMatrixSessionInfo> active = ssi.getSessionsLoggedInToVDB(vdbName, vdbVersion);
			return active.isEmpty();
		} catch (SessionServiceException e) {
			LogManager.logError(LogConstants.CTX_DQP, e, e.getMessage());
		}
		return false;
    }

    /**
     * If the VDB has been marked deleted before now is the time to cleanup 
     * @param vdbName
     * @param vdbVersion
     * @since 4.3
     */
    void runVDBCleanUp(String vdbName, String vdbVersion) {
        try {
            VDBArchive vdb = getVDB(vdbName, vdbVersion);
            if (vdb != null && vdb.getStatus() == VDBStatus.DELETED) {
                deleteVDB(vdb);
            }
        } catch (MetaMatrixComponentException e) {
            DQPEmbeddedPlugin.logError(e, "EmbeddedConfigurationService.vdb_delete_failed", new Object[] {vdbName, vdbVersion}); //$NON-NLS-1$
        }
    }
          
    public boolean isFullyConfiguredVDB(VDBArchive vdb) throws MetaMatrixComponentException{
    	VDBDefn def = vdb.getConfigurationDef();
    	Collection models = def.getModels();
    	
        for (Iterator i = models.iterator(); i.hasNext();) {
            ModelInfo model = (ModelInfo)i.next();
            if (model.isPhysical()) {
                if (model.getConnectorBindingNames().isEmpty()) {
                    DQPEmbeddedPlugin.logWarning("VDBService.vdb_missing_bindings", new Object[] {vdb.getName(), vdb.getVersion()}); //$NON-NLS-1$
                    return false;
                }

                // make sure we have connector binding in the 
                // configuration service. 
                String bindingName = (String)model.getConnectorBindingNames().get(0); 
                ConnectorBinding binding = def.getConnectorBindingByName(bindingName);
                if (binding == null || getConnectorBinding(binding.getDeployedName()) == null) {
                    DQPEmbeddedPlugin.logWarning("VDBService.vdb_missing_bindings", new Object[] {vdb.getName(), vdb.getVersion()}); //$NON-NLS-1$
                    return false;
                }
            }
        }   
        return true;
    }

    
    /**
     * Register a listener for the VDB life cycle events, and get notified for
     * when vdb is loded and unloaded 
     * @param listener - lister object
     */
    public void register(ConnectorBindingLifeCycleListener listener) {
        this.connectorBindingLifeCycleListeners.add(listener);
    }    
    
    void notifyConnectorBindingLoad(String bindingName) {
        for (Iterator i = this.connectorBindingLifeCycleListeners.iterator(); i.hasNext();) {
            ConnectorBindingLifeCycleListener listener = (ConnectorBindingLifeCycleListener)i.next();
            listener.loaded(bindingName);
        }
    }

    void notifyConnectorBindingUnLoad(String bindingName) {
        for (Iterator i = this.connectorBindingLifeCycleListeners.iterator(); i.hasNext();) {
            ConnectorBindingLifeCycleListener listener = (ConnectorBindingLifeCycleListener)i.next();
            listener.unloaded(bindingName);
        }       
    }
    
    /**
     * Register a listener for the VDB life cycle events, and get notified for
     * when vdb is loded and unloaded 
     * @param listener - lister object
     */
    public void register(VDBLifeCycleListener listener) {
        vdbLifeCycleListeners.add(listener);
    }
    
    void notifyVDBLoad(String vdbName, String vdbVersion) {
        for (Iterator i = vdbLifeCycleListeners.iterator(); i.hasNext();) {
            VDBLifeCycleListener listener = (VDBLifeCycleListener)i.next();
            listener.loaded(vdbName, vdbVersion);
        }
    }

    void notifyVDBUnLoad(String vdbName, String vdbVersion) {
        for (Iterator i = vdbLifeCycleListeners.iterator(); i.hasNext();) {
            VDBLifeCycleListener listener = (VDBLifeCycleListener)i.next();
            listener.unloaded(vdbName, vdbVersion);
        }       
    }    
    
    /**
     * Use disk for buffering for result set management during the processing 
     * @return true if yes to use buffering; false otherwise
     */
    public boolean useDiskBuffering() {
        return Boolean.valueOf(getUserPreferences().getProperty(DQPEmbeddedProperties.BufferService.DQP_BUFFER_USEDISK, "true")).booleanValue(); //$NON-NLS-1$        
    }
    
    private File getWorkDir() {
        String workDirectory = getUserPreferences().getProperty(DQPEmbeddedProperties.DQP_WORKDIR);
        File workDir = new File(workDirectory);
        return workDir;
    }
    
    /**
     * Get the directory to use for the disk buffering  
     * @return must a return a location; and exist too.
     */
    public File getDiskBufferDirectory() {
        File bufferDir = new File(getWorkDir(), "buffer"); //$NON-NLS-1$
        
        // create the buffer directory if not already exists.
        if (!bufferDir.exists()) {
            bufferDir.mkdirs();
        }        
        return bufferDir;
    }
    
    /**
     * Size of Memory in MB allocated to be used by the Resultset Management before
     * disk buffering kicks in.  
     * @return must a return a location 
     */
    public String getBufferMemorySize() {
       return getUserPreferences().getProperty(DQPEmbeddedProperties.BufferService.DQP_BUFFER_MEMORY, "64"); //$NON-NLS-1$        
    }
    
    /**  
     * @see com.metamatrix.dqp.service.ConfigurationService#getProcessName()
     */
    public String getProcessName() {
        return getUserPreferences().getProperty(DQPEmbeddedProperties.PROCESSNAME);
    }
    
    
    public String getProcessorBatchSize() {
        return getUserPreferences().getProperty(DQPEmbeddedProperties.BufferService.DQP_PROCESSOR_BATCH_SIZE, "2000"); //$NON-NLS-1$
    }
    public String getConnectorBatchSize() {
        return getUserPreferences().getProperty(DQPEmbeddedProperties.BufferService.DQP_CONNECTOR_BATCH_SIZE, "2000"); //$NON-NLS-1$
    }

	@Override
	public void unregister(VDBLifeCycleListener listener) {
		this.vdbLifeCycleListeners.remove(listener);
	}

	@Override
	public void unregister(ConnectorBindingLifeCycleListener listener) {
		this.connectorBindingLifeCycleListeners.remove(listener);
	}    
	
	@Override
	public void clearClassLoaderCache() throws MetaMatrixComponentException {
		this.classLoaderManager.clearCache();
	}

	void setUserPreferences(Properties userPreferences) {
		this.userPreferences = userPreferences;
		//test hack
		URL url = (URL)userPreferences.get(PROPERTIES_URL);
		if (url != null) {
			this.bootStrapURL = url;
		}
	}

	Properties getUserPreferences() {
		return userPreferences;
	}

	@Override
	public String getClusterName() {
		return getUserPreferences().getProperty(DQPEmbeddedProperties.CLUSTERNAME, "embedded"); //$NON-NLS-1$
	}
}

