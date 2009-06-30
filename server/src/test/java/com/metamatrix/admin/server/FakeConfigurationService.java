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

package com.metamatrix.admin.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.teiid.adminapi.Resource;

import com.metamatrix.common.actions.ActionDefinition;
import com.metamatrix.common.actions.CreateObject;
import com.metamatrix.common.actions.ModificationException;
import com.metamatrix.common.application.ClassLoaderManager;
import com.metamatrix.common.comm.ClientServiceRegistry;
import com.metamatrix.common.config.CurrentConfiguration;
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
import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.config.api.DeployedComponentID;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.HostID;
import com.metamatrix.common.config.api.HostType;
import com.metamatrix.common.config.api.ServiceComponentDefnID;
import com.metamatrix.common.config.api.SharedResource;
import com.metamatrix.common.config.api.SharedResourceID;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.config.api.VMComponentDefnID;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.api.exceptions.InvalidConfigurationException;
import com.metamatrix.common.config.model.BasicComponentObject;
import com.metamatrix.common.config.model.BasicConfigurationObjectEditor;
import com.metamatrix.common.config.model.BasicConnectorBindingType;
import com.metamatrix.common.config.model.BasicHost;
import com.metamatrix.common.config.model.BasicSharedResource;
import com.metamatrix.common.config.model.ConfigurationModelContainerImpl;
import com.metamatrix.common.config.xml.XMLConfigurationImportExportUtility;
import com.metamatrix.common.queue.WorkerPoolStats;
import com.metamatrix.common.stats.ConnectionPoolStats;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.platform.PlatformPlugin;
import com.metamatrix.platform.config.ConfigUpdateMgr;
import com.metamatrix.platform.config.api.service.ConfigurationServiceInterface;
import com.metamatrix.platform.service.api.ServiceID;
import com.metamatrix.platform.service.controller.ServiceData;


/**
 * Use the ConfigUpdateMgr to set the config.xml to use
 * 
 *  example:  ConfigUpdateMgr.createSystemProperties("config_multihost.xml");
 *  
 * @author vanhalbert
 *
 */
public class FakeConfigurationService implements ConfigurationServiceInterface {

 //   private String CONFIG_FILE_PATH = null;
    private ConnectorBinding cb = null;
 //   private ConfigurationModelContainerImpl config;
    
    private ConfigUpdateMgr mgr = new ConfigUpdateMgr();

    public FakeConfigurationService() {
        super();
        
        CurrentConfiguration.reset();
//        CONFIG_FILE_PATH = UnitTestUtil.getTestDataPath() + "/config/" + "config.xml"; //$NON-NLS-1$ //$NON-NLS-2$
//        File configFile = new File(CONFIG_FILE_PATH);
 //       config = (ConfigurationModelContainerImpl)importConfigurationModel(configFile, Configuration.NEXT_STARTUP_ID);
        
        
    }
    
    






	/**
     * Import a configuration file to work with.
     *  
     * @param fileToImport
     * @param configID
     * @return
     * @since 5.0
     */
//    private ConfigurationModelContainer importConfigurationModel(File fileToImport, ConfigurationID configID) {
//        Collection configObjects = null;
//        ConfigurationObjectEditor editor = new BasicConfigurationObjectEditor(false);
//        ConfigurationModelContainerImpl configModel = null;
//        try {
//            XMLConfigurationImportExportUtility io = new XMLConfigurationImportExportUtility();
//            FileInputStream inputStream = new FileInputStream(fileToImport);
//            configObjects = io.importConfigurationObjects(inputStream, editor, configID.getFullName());
//            configModel = new ConfigurationModelContainerImpl();
//            configModel.setConfigurationObjects(configObjects);            
//        } catch(Exception ioe) {
//            configModel = null;
//        }
//        
//        return configModel;
//    }
    
//    private ProductServiceConfig getPSCByName(Configuration config,
//            String pscName) throws InvalidArgumentException {
//		ProductServiceConfig result = null;
//		if (config != null) {
//			ProductServiceConfigID pscID = new ProductServiceConfigID(((ConfigurationID)config.getID()), pscName);
//			result = config.getPSC(pscID);
//		}
//		return result;
//	}

    public Host addHost(String hostName,
                        String principalName,
                        Properties properties) throws ConfigurationException{
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
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#createEditor()
     */
    public ConfigurationObjectEditor createEditor() throws ConfigurationException{
        return mgr.getEditor();
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#getCurrentConfigurationID()
     */
    public ConfigurationID getCurrentConfigurationID() throws ConfigurationException{
        return null;
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#getNextStartupConfigurationID()
     */
    public ConfigurationID getNextStartupConfigurationID() throws ConfigurationException{
        return mgr.getConfigModel().getConfigurationID();
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#getStartupConfigurationID()
     */
    public ConfigurationID getStartupConfigurationID() throws ConfigurationException{
        return mgr.getConfigModel().getConfigurationID();
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#baselineCurrentConfiguration(java.lang.String)
     */
    public void baselineCurrentConfiguration(String principalName) throws ConfigurationException{
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#getCurrentConfiguration()
     */
    public Configuration getCurrentConfiguration() throws ConfigurationException {
        return mgr.getConfigModel().getConfiguration();
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#getNextStartupConfiguration()
     */
    public Configuration getNextStartupConfiguration() throws ConfigurationException{
        return mgr.getConfigModel().getConfiguration();
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#getStartupConfiguration()
     */
    public Configuration getStartupConfiguration() throws ConfigurationException{
        return mgr.getConfigModel().getConfiguration();
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#getConfiguration(java.lang.String)
     */
    public Configuration getConfiguration(String configName) throws InvalidConfigurationException,
                                                            ConfigurationException{
    	
    	// TODO:  need to change to use config.xml
        return mgr.getConfigModel().getConfiguration();
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#getConfigurationModel(java.lang.String)
     */
    public ConfigurationModelContainer getConfigurationModel(String configName) throws InvalidConfigurationException,
                                                                               ConfigurationException{
		return mgr.getConfigModel();
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#getConfigurationAndDependents(com.metamatrix.common.config.api.ConfigurationID)
     */
    public Collection getConfigurationAndDependents(ConfigurationID configID) throws ConfigurationException{
        return mgr.getConfigModel().getAllObjects();
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#getAllGlobalConfigObjects()
     */
    public Collection getAllGlobalConfigObjects() throws ConfigurationException {
        return mgr.getConfigModel().getAllObjects();
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#getComponentTypeDefinitions(java.util.Collection)
     */
    public Map getComponentTypeDefinitions(Collection componentIDs) throws ConfigurationException{
        return null;
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#getComponentTypeDefinitions(com.metamatrix.common.config.api.ComponentTypeID)
     */
    public Collection getComponentTypeDefinitions(ComponentTypeID componentTypeID) throws ConfigurationException{
        return mgr.getConfigModel().getAllComponentTypeDefinitions(componentTypeID);
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#getAllComponentTypeDefinitions(com.metamatrix.common.config.api.ComponentTypeID)
     */
    public Collection getAllComponentTypeDefinitions(ComponentTypeID componentTypeID) throws ConfigurationException{
        return mgr.getConfigModel().getAllComponentTypeDefinitions(componentTypeID);
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#getDependentComponentTypeDefinitions(java.util.Collection)
     */
    public Map getDependentComponentTypeDefinitions(Collection componentIDs) throws ConfigurationException{
        return null;
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#getDependentComponentTypeDefinitions(com.metamatrix.common.config.api.ComponentTypeID)
     */
    public Collection getDependentComponentTypeDefinitions(ComponentTypeID componentTypeID) throws ConfigurationException{
        return null;
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#getMonitoredComponentTypes(boolean)
     */
    public Collection getMonitoredComponentTypes(boolean includeDeprecated) throws ConfigurationException{
        return null;
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#getComponentType(com.metamatrix.common.config.api.ComponentTypeID)
     */
    public ComponentType getComponentType(ComponentTypeID id) throws ConfigurationException {
        return mgr.getConfigModel().getComponentType(id.getFullName());
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#getAllComponentTypes(boolean)
     */
    public Collection getAllComponentTypes(boolean includeDeprecated) throws ConfigurationException{
    	Collection types = new ArrayList();
    	types.addAll(mgr.getConfigModel().getComponentTypes().values());
    	return types;
    	
//        List results = new ArrayList();
//        
//        ComponentTypeID typeID1 = new ComponentTypeID("connectorType1"); //$NON-NLS-1$
//        BasicConnectorBindingType type1 = new BasicConnectorBindingType(typeID1, typeID1, typeID1, true, false, true);
//        type1.setComponentTypeCode(ComponentType.CONNECTOR_COMPONENT_TYPE_CODE);
//        results.add(type1);
//        
//        
//        ComponentTypeID typeID2 = new ComponentTypeID("connectorType2"); //$NON-NLS-1$
//        BasicConnectorBindingType type2 = new BasicConnectorBindingType(typeID2, typeID2, typeID2, true, false, true);
//        type2.setComponentTypeCode(ComponentType.CONNECTOR_COMPONENT_TYPE_CODE);
//        
//        results.add(type2);  
//        
//        return results;
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#getHost(com.metamatrix.common.config.api.HostID)
     */
    public Host getHost(HostID hostID) throws ConfigurationException {
        return mgr.getConfigModel().getHost(hostID.getFullName());
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#getHosts()
     */
    public Collection getHosts() throws ConfigurationException {
    	return mgr.getConfigModel().getHosts();
    	
//        List hosts = new ArrayList();
//        
//        HostID hostID1 = new HostID("1.1.1.1"); //$NON-NLS-1$
//        Host host1 = new BasicHost(Configuration.NEXT_STARTUP_ID, hostID1, new ComponentTypeID(HostType.COMPONENT_TYPE_NAME));
//        hosts.add(host1);
//        
//        HostID hostID2 = new HostID("2.2.2.2"); //$NON-NLS-1$
//        Host host2 = new BasicHost(Configuration.NEXT_STARTUP_ID, hostID2, new ComponentTypeID(HostType.COMPONENT_TYPE_NAME));
//        hosts.add(host2);
//        
//        
//        return hosts;
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#getComponentDefn(com.metamatrix.common.config.api.ConfigurationID, com.metamatrix.common.config.api.ComponentDefnID)
     */
    public ComponentDefn getComponentDefn(ConfigurationID configurationID,
                                          ComponentDefnID componentDefnID) throws ConfigurationException {
        return this.getConfigurationModel(null).getConfiguration().getComponentDefn(componentDefnID);
     }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#getComponentDefns(java.util.Collection, com.metamatrix.common.config.api.ConfigurationID)
     */
    public Collection getComponentDefns(Collection componentDefnIDs,
                                        ConfigurationID configurationID) throws ConfigurationException {
        return null;
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#getResourcePools(com.metamatrix.common.config.api.ConfigurationID)
     */
    public Collection getResourcePools(ConfigurationID configurationID) throws ConfigurationException {
        return null;
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#getResourcePoolTypes(com.metamatrix.common.config.api.ConfigurationID)
     */
    public Collection getResourcePoolTypes(ConfigurationID configurationID) throws ConfigurationException {
        return null;
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#getPoolableResourcePoolTypes(com.metamatrix.common.config.api.ConfigurationID)
     */
    public Collection getPoolableResourcePoolTypes(ConfigurationID configurationID) throws ConfigurationException {
        return null;
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#getResourcePools(com.metamatrix.common.config.api.ConfigurationID, com.metamatrix.common.config.api.ComponentTypeID)
     */
    public Collection getResourcePools(ConfigurationID configurationID,
                                       ComponentTypeID componentTypeID) throws ConfigurationException {
        return null;
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#getResources()
     */
    public Collection getResources() throws ConfigurationException {
    	return mgr.getConfigModel().getResources();
//        List results = new ArrayList();
//        
//        SharedResourceID resourceID1 = new SharedResourceID("resource1"); //$NON-NLS-1$
//        SharedResource resource1 = new BasicSharedResource(resourceID1, SharedResource.MISC_COMPONENT_TYPE_ID);
//        
//        BasicComponentObject target = (BasicComponentObject) resource1;
//
//        target.addProperty("prop1", "value1");
//        target.addProperty(Resource.RESOURCE_POOL, "pool");
//        
//        results.add(resource1);
//        
//        
//        SharedResourceID resourceID2 = new SharedResourceID("resource2"); //$NON-NLS-1$
//        SharedResource resource2 = new BasicSharedResource(resourceID2, SharedResource.MISC_COMPONENT_TYPE_ID);
//        results.add(resource2);
//
//        
//        return results;
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#getResources(com.metamatrix.common.config.api.ComponentTypeID)
     */
    public Collection getResources(ComponentTypeID componentTypeID) throws ConfigurationException {
        return null;
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#saveResources(java.util.Collection, java.lang.String)
     */
    public void saveResources(Collection resourceDescriptors,
                              String principalName) throws ConfigurationException {
    	
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#getProductReleaseInfos()
     */
    public Collection getProductReleaseInfos() throws ConfigurationException {
        return null;
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#executeTransaction(com.metamatrix.common.actions.ActionDefinition, java.lang.String)
     */
    public Set executeTransaction(ActionDefinition action,
                                  String principalName) throws ConfigurationException {
    	
       if (action != null) {
        	
        	Set resultset = null;
        	mgr.initTransactions(new Properties());
        	
        	List actions = new ArrayList(1);
        	actions.add(action);
			resultset = mgr.commit(actions);
			
			
			return resultset;
//            for (Iterator it=actions.iterator(); it.hasNext();) {
//                Object o = it.next();
//                if (o instanceof CreateObject) {
//                  CreateObject co = (CreateObject) o;
//                  Object[] objs = co.getArguments();
//                  config.addObject(objs[0]);  
//                }
//            }
        }

        return null;
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#executeTransaction(java.util.List, java.lang.String)
     */
    public Set executeTransaction(List actions,
                                  String principalName) throws ConfigurationException {
        if (actions != null) {
        	
        	Set resultset = null;
        	mgr.initTransactions(new Properties());
        	
			resultset = mgr.commit(actions);
			
			
			return resultset;
//            for (Iterator it=actions.iterator(); it.hasNext();) {
//                Object o = it.next();
//                if (o instanceof CreateObject) {
//                  CreateObject co = (CreateObject) o;
//                  Object[] objs = co.getArguments();
//                  config.addObject(objs[0]);  
//                }
//            }
        }
        return null;
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#executeInsertTransaction(com.metamatrix.common.config.api.ConfigurationID, java.util.List, java.lang.String)
     */
    public Set executeInsertTransaction(ConfigurationID assignConfigurationID,
                                        List actions,
                                        String principalName) throws ModificationException,
                                                             ConfigurationException {
    	Set resultset = null;
    	mgr.initTransactions(new Properties());
    	resultset = mgr.commit(actions);	
		
		return resultset;

    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#undoActionsAsTransaction(int, java.lang.String)
     */
    public Set undoActionsAsTransaction(int numberOfActions,
                                        String principalName) throws ConfigurationException {
        return null;
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#getHistory()
     */
    public List getHistory() throws ConfigurationException {
        return null;
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#clearHistory()
     */
    public void clearHistory() throws ConfigurationException {
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#getHistorySize()
     */
    public int getHistorySize() throws ConfigurationException {
        return 0;
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#getHistoryLimit()
     */
    public int getHistoryLimit() throws ConfigurationException {
        return 0;
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#setHistoryLimit(int)
     */
    public void setHistoryLimit(int maximumHistoryCount) throws ConfigurationException {
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#getServerStartupTime()
     */
    public Date getServerStartupTime() throws ConfigurationException {
        return new Date(1234);
    }


    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#addProcess(java.lang.String, java.lang.String, java.lang.String, java.util.Properties)
     */
    public VMComponentDefn addProcess(String processName,
                                      String hostName,
                                      String principalName,
                                      Properties properties) throws ConfigurationException {
        return null;
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#setSystemPropertyValue(java.lang.String, java.lang.String, java.lang.String)
     */
    public void setSystemPropertyValue(String propertyName,
                                       String propertyValue,
                                       String principalName) throws ConfigurationException {
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#updateSystemPropertyValues(java.util.Properties, java.lang.String)
     */
    public void updateSystemPropertyValues(Properties properties,
                                           String principalName) throws ConfigurationException {
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#createConnectorBinding(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.util.Properties)
     */
    public ConnectorBinding createConnectorBinding(String connectorBindingName,
                                                   String connectorType,
                                                   String vmName,
                                                   String principalName,
                                                   Properties properties) throws ConfigurationException {
    	
    	BasicConfigurationObjectEditor editor = new BasicConfigurationObjectEditor(false);
    	ComponentTypeID id = new ComponentTypeID(connectorType);
    	
    	this.cb = editor.createConnectorComponent(Configuration.NEXT_STARTUP_ID, id, connectorBindingName, null);
        return cb;
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#modify(com.metamatrix.common.config.api.ComponentObject, java.util.Properties, java.lang.String)
     */
    public Object modify(ComponentObject theObject,
                         Properties theProperties,
                         String principalName) throws ConfigurationException{
    	
    	ConfigurationObjectEditor editor = this.createEditor();
    	
    	theObject = editor.modifyProperties(theObject, theProperties, ConfigurationObjectEditor.SET);
    	
    	this.executeTransaction(editor.getDestination().popActions(), "FakeConfigurationService");
    	
        return theObject;
    }


    public ConnectorBinding importConnectorBinding(InputStream inputStream,
            String name,
            String pscName,
            String principalName) throws ConfigurationException {
		ConnectorBinding newBinding = null;
		ConfigurationObjectEditor editor = createEditor();
		
		try {
			XMLConfigurationImportExportUtility util = new XMLConfigurationImportExportUtility();
			newBinding = util.importConnectorBinding(inputStream, editor, name);
			
			
			//deploy to the specified PSC
			Configuration config = getNextStartupConfiguration();
			if (pscName != null && !pscName.equals("")) { //$NON-NLS-1$
//			ProductServiceConfig psc = this.getPSCByName(config, pscName);
			ServiceComponentDefnID bindingID = (ServiceComponentDefnID) newBinding.getID();
//			editor.addServiceComponentDefn(psc, bindingID);
			
			VMComponentDefn vm = (VMComponentDefn) config.getVMComponentDefns().iterator().next();
			editor.deployServiceDefn(config, newBinding, (VMComponentDefnID) vm.getID());
			}            
		
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

	public ComponentType importConnectorType(InputStream inputStream,
	      String name,
	      String principalName) throws ConfigurationException {
		ComponentType newType = null;
		ConfigurationObjectEditor editor = createEditor();
		
		try {
			XMLConfigurationImportExportUtility util = new XMLConfigurationImportExportUtility();
			newType = util.importComponentType(inputStream, editor, name);
			
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
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#delete(com.metamatrix.common.config.api.ComponentObject, boolean, java.lang.String)
     */
    public void delete(ComponentObject theObject,
                       boolean theDeleteDependenciesFlag,
                       String principalName) throws ConfigurationException {
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#delete(com.metamatrix.common.config.api.ComponentType, java.lang.String)
     */
    public void delete(ComponentType componentType,
                       String principalName) throws ConfigurationException{
    }

    
    

    @Override
	public DeployedComponent deployService(VMComponentDefnID theProcessID,
			String serviceName, String principalName)
			throws ConfigurationException{
		// TODO Auto-generated method stub
    	
    	ServiceComponentDefnID svcID = (ServiceComponentDefnID) this.getConfigurationModel(null).getConfiguration().getServiceComponentDefn(serviceName).getID();
    	VMComponentDefn vm =  this.getConfigurationModel(null).getConfiguration().getVMComponentDefn(theProcessID);
		return mgr.getConfigModel().getConfiguration().getDeployedServiceForVM(svcID, vm);
	}

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#checkPropertiesDecryptable(java.util.List)
     */
    public List checkPropertiesDecryptable(List defns) throws ConfigurationException {
        return null;
    }

    /** 
     * @see com.metamatrix.platform.config.api.service.ConfigurationServiceInterface#checkPropertiesDecryptable(java.util.Properties, java.lang.String)
     */
    public boolean checkPropertiesDecryptable(Properties props,
                                              String componentTypeIdentifier) throws ConfigurationException {
        return false;
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#die()
     */
    public void die()  {
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#dieNow()
     */
    public void dieNow()  {
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#checkState()
     */
    public void checkState() {
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#getProperties()
     */
    public Properties getProperties()  {
        return null;
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#getStartTime()
     */
    public Date getStartTime() {
        return null;
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#getHostname()
     */
    public String getHostname()  {
        return null;
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#getVMID()
     */
    public String getProcessName()  {
        return null;
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#isAlive()
     */
    public boolean isAlive(){
        return false;
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#getServiceType()
     */
    public String getServiceType(){
        return null;
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#getCurrentState()
     */
    public int getCurrentState(){
        return 0;
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#getStateChangeTime()
     */
    public Date getStateChangeTime() {
        return null;
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#getID()
     */
    public ServiceID getID() {
        return null;
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#getQueueStatistics()
     */
    public Collection getQueueStatistics()  {
        return null;
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#getQueueStatistics(java.lang.String)
     */
    public WorkerPoolStats getQueueStatistics(String name) {
        return null;
    }

	public void init(ServiceID id, DeployedComponentID deployedComponentID,
			Properties props, ClientServiceRegistry listenerRegistry, ClassLoaderManager clManager) {
	}

	public void setInitException(Throwable t) {
	}

	public void updateState(int state) {
	}

	@Override
	public Throwable getInitException() {
		return null;
	}
	
	@Override
	public ServiceData getServiceData() {
		return null;
	}








	@Override
	public Collection<ConnectionPoolStats> getConnectionPoolStats() {
		return null;
	}
	
	

}
