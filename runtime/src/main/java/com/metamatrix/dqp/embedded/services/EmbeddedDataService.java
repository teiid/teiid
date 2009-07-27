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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorPropertyNames;
import org.teiid.connector.metadata.runtime.ConnectorMetadata;
import org.teiid.dqp.internal.datamgr.impl.ConnectorManager;
import org.teiid.dqp.internal.process.DQPWorkContext;

import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.common.comm.api.ResultsReceiver;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ComponentTypeDefn;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.ConnectorBindingType;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.common.util.crypto.CryptoException;
import com.metamatrix.common.util.crypto.CryptoUtil;
import com.metamatrix.common.vdb.api.VDBArchive;
import com.metamatrix.common.vdb.api.VDBDefn;
import com.metamatrix.core.vdb.VDBStatus;
import com.metamatrix.dqp.embedded.DQPEmbeddedPlugin;
import com.metamatrix.dqp.internal.datamgr.ConnectorID;
import com.metamatrix.dqp.message.AtomicRequestID;
import com.metamatrix.dqp.message.AtomicRequestMessage;
import com.metamatrix.dqp.message.AtomicResultsMessage;
import com.metamatrix.dqp.message.RequestMessage;
import com.metamatrix.dqp.service.ConnectorBindingLifeCycleListener;
import com.metamatrix.dqp.service.ConnectorStatus;
import com.metamatrix.dqp.service.DQPServiceNames;
import com.metamatrix.dqp.service.DataService;
import com.metamatrix.dqp.service.VDBService;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities;


/** 
 * A DataService implementation for the DQP.
 * @since 4.3
 */
public class EmbeddedDataService extends EmbeddedBaseDQPService implements DataService {
    // Map of connector binding name to ConnectorID
    private Map connectorIDs = new HashMap();

    // Map of ConnectorID to ConnectorData
    private Map connectorMgrs = new HashMap();
    
    // A counter to keep track of connector ids
    private AtomicInteger counter = new AtomicInteger();
    
    // Connector List
    private Map loadedConnectorBindingsMap = new HashMap();
    
    private ApplicationEnvironment env;
    
    private ConnectorBindingLifeCycleListener listener = new ConnectorBindingLifeCycleListener() {

        public void loaded(String bindingName) {
            try {
				startConnectorBinding(bindingName);
			} catch (Exception e) {
				DQPEmbeddedPlugin.logError(e, "DataService.FailedStart", new Object[] {bindingName}); //$NON-NLS-1$
			} 
        }

        public void unloaded(String bindingName) {
            try {
                stopConnectorBinding(bindingName);
            } catch (Exception e) {
                DQPEmbeddedPlugin.logError(e, "DataService.FailedStop", new Object[] {bindingName}); //$NON-NLS-1$
            }
        }                
    };
    
    /**
     * Select a connector to use for the given connector binding.
     * @param deployedConnectorBindingName Connector binding identifier
     * @return ConnectorID identifying a connector instance
     */
    public ConnectorID selectConnector(String deployedConnectorBindingName) 
        throws MetaMatrixComponentException {        
        ConnectorID id = (ConnectorID) connectorIDs.get(deployedConnectorBindingName);
        if (id == null) {
            ConnectorBinding binding = getConnectorBinding(deployedConnectorBindingName);
            if (binding != null) {
                id = (ConnectorID) connectorIDs.get(binding.getDeployedName());
            }
            if (id == null) {
                throw new ComponentNotFoundException(DQPEmbeddedPlugin.Util.getString("DataService.Connector_State_invalid", new Object[] { deployedConnectorBindingName })); //$NON-NLS-1$
            }
        }
        return id;
    }
    
    @Override
    public ConnectorMetadata getConnectorMetadata(String vdbName,
    		String vdbVersion, String modelName, Properties importProperties) throws MetaMatrixComponentException {
    	VDBService vdbService = (VDBService)this.lookupService(DQPServiceNames.VDB_SERVICE);
    	List<String> bindingNames = vdbService.getConnectorBindingNames(vdbName, vdbVersion, modelName);
    	if (bindingNames.isEmpty()) {
    		throw new MetaMatrixComponentException(DQPEmbeddedPlugin.Util.getString("DataService.no_connectors_defined")); //$NON-NLS-1$
    	}
    	String deployedConnectorBindingName = bindingNames.get(0);
    	ConnectorID connector = selectConnector(deployedConnectorBindingName);
    	ConnectorManager mgr = getConnectorManager(connector);
    	if (mgr == null) {
    		throw new ComponentNotFoundException(DQPEmbeddedPlugin.Util.getString("DataService.Unable_to_find_connector_manager_for_{0}_1", new Object[] { connector })); //$NON-NLS-1$
    	}
    	
    	try {
			return mgr.getMetadata(modelName, importProperties);
		} catch (ConnectorException e) {
			throw new MetaMatrixComponentException(e);
		}
    }
    
     /**
     * Execute the given request on a <code>Connector</code>. The results are passed in to the 
     * listener object. 
      * @see com.metamatrix.dqp.service.DataService#executeRequest(com.metamatrix.dqp.message.AtomicRequestMessage, com.metamatrix.dqp.internal.datamgr.ConnectorID, com.metamatrix.common.comm.api.MessageListener)
      * @since 4.3
      */
	public void executeRequest(AtomicRequestMessage request,
			ConnectorID connector,
			ResultsReceiver<AtomicResultsMessage> resultListener)
			throws MetaMatrixComponentException {
        ConnectorManager mgr = getConnectorManager(connector);
        if(mgr != null) {
			mgr.executeRequest(resultListener, request);
        }
        else {
            throw new ComponentNotFoundException(DQPEmbeddedPlugin.Util.getString("DataService.Unable_to_find_connector_manager_for_{0}_1", new Object[] { connector })); //$NON-NLS-1$
        }
    }
	
	public void cancelRequest(AtomicRequestID request, ConnectorID connectorId)
			throws MetaMatrixComponentException {
        ConnectorManager mgr = getConnectorManager(connectorId);
        if (mgr == null ) {                        
            throw new ComponentNotFoundException(DQPEmbeddedPlugin.Util.getString("DataService.Unable_to_find_connector_manager_for_{0}_1", new Object[] { connectorId })); //$NON-NLS-1$            
        }
        mgr.cancelRequest(request);
	}

	public void closeRequest(AtomicRequestID request, ConnectorID connectorId)
			throws MetaMatrixComponentException {
        ConnectorManager mgr = getConnectorManager(connectorId);
        if (mgr == null ) {                        
            throw new ComponentNotFoundException(DQPEmbeddedPlugin.Util.getString("DataService.Unable_to_find_connector_manager_for_{0}_1", new Object[] { connectorId })); //$NON-NLS-1$            
        }
        mgr.closeRequest(request);
	}
	
	public void requestBatch(AtomicRequestID request, ConnectorID connectorId)
		throws MetaMatrixComponentException {
        ConnectorManager mgr = getConnectorManager(connectorId);
        if (mgr == null ) {                        
            throw new ComponentNotFoundException(DQPEmbeddedPlugin.Util.getString("DataService.Unable_to_find_connector_manager_for_{0}_1", new Object[] { connectorId })); //$NON-NLS-1$            
        }
        mgr.requstMore(request);
	}

	public SourceCapabilities getCapabilities(RequestMessage request,
			DQPWorkContext dqpWorkContext, ConnectorID connector)
			throws MetaMatrixComponentException {
        ConnectorManager mgr = getConnectorManager(connector);
        if (mgr != null ) {                        
            try {
				return mgr.getCapabilities(dqpWorkContext.getRequestID(request.getExecutionId()), request.getExecutionPayload(), dqpWorkContext);
			} catch (ConnectorException e) {
				throw new MetaMatrixComponentException(e);
			}
        }
        throw new ComponentNotFoundException(DQPEmbeddedPlugin.Util.getString("DataService.Unable_to_find_connector_manager_for_{0}_1", new Object[] { connector })); //$NON-NLS-1$            
	}

    /** 
     * @see com.metamatrix.dqp.service.DataService#getConnectorBindingStatistics(java.lang.String)
     * @since 4.3
     */
    public Collection getConnectorBindingStatistics(String connectorBindingName) throws MetaMatrixComponentException {
    	ConnectorBinding binding = getConnectorBinding(connectorBindingName);
        if (binding != null) {
            ConnectorManager mgr = getConnectorManager(binding);
            if (mgr != null ) {            
                return mgr.getQueueStatistics();
            }
        }
        throw new ComponentNotFoundException(DQPEmbeddedPlugin.Util.getString("DataService.Unable_to_find_connector_manager_for_{0}_1", new Object[] { connectorBindingName })); //$NON-NLS-1$
    }  
    
    /** 
     * @see com.metamatrix.dqp.service.DataService#getConnectionPoolStatistics(java.lang.String)
     * @since 6.1
     */
    public Collection getConnectionPoolStatistics(String connectorBindingName) throws MetaMatrixComponentException {
    	ConnectorBinding binding = getConnectorBinding(connectorBindingName);
        if (binding != null) {
            ConnectorManager mgr = getConnectorManager(binding);
            if (mgr != null ) {  
            	return mgr.getConnectionPoolStats();
            }
        }
        throw new ComponentNotFoundException(DQPEmbeddedPlugin.Util.getString("DataService.Unable_to_find_connector_manager_for_{0}_1", new Object[] { connectorBindingName })); //$NON-NLS-1$
    }     
    
    /** 
     * @see com.metamatrix.dqp.service.DataService#clearConnectorBindingCache(java.lang.String)
     * @since 4.3
     */
    public void clearConnectorBindingCache(String connectorBindingName) throws MetaMatrixComponentException {
    	ConnectorBinding binding = getConnectorBinding(connectorBindingName);
    	if (binding != null) {
            ConnectorManager mgr = getConnectorManager(binding);
            if (mgr != null ) {            
                mgr.clearCache();
                return;
            }
        }
        throw new ComponentNotFoundException(DQPEmbeddedPlugin.Util.getString("DataService.Unable_to_find_connector_manager_for_{0}_1", new Object[] { connectorBindingName })); //$NON-NLS-1$       
    }    

    /** 
     * @see com.metamatrix.dqp.service.DataService#startConnectorBinding(java.lang.String)
     * @since 4.3
     */
    public void startConnectorBinding(String deployedConnectorBindingName) 
        throws ApplicationLifecycleException, MetaMatrixComponentException {
        
        ConnectorBinding binding = getConnectorBinding(deployedConnectorBindingName);
        if (binding != null) {
            ConnectorManager mgr = getConnectorManager(binding);
            if (mgr != null && mgr.getStatus() == ConnectorStatus.NOT_INITIALIZED) {
                // Start the manager
                mgr.start(env);
                
                // Add the references to the mgr as loaded.
                ConnectorID connID = mgr.getConnectorID(); 
                this.connectorIDs.put(binding.getDeployedName(), connID);
                this.connectorMgrs.put(connID, mgr);                
                this.loadedConnectorBindingsMap.put(binding.getDeployedName(), binding);
                
                DQPEmbeddedPlugin.logInfo("DataService.Connector_Started", new Object[] {binding.getDeployedName()}); //$NON-NLS-1$
            }
        }
        else {
            throw new ApplicationLifecycleException(DQPEmbeddedPlugin.Util.getString("DataService.Unable_to_find_connector", deployedConnectorBindingName)); //$NON-NLS-1$
        }        
    }

    /** 
     * @see com.metamatrix.dqp.service.DataService#stopConnectorBinding(java.lang.String)
     * @since 4.3
     */
    public void stopConnectorBinding(String deployedConnectorBindingName) 
        throws ApplicationLifecycleException, MetaMatrixComponentException {

        ConnectorBinding binding = getConnectorBinding(deployedConnectorBindingName);
        if (binding != null) {
            ConnectorManager mgr = getConnectorManager(binding, false);
            if (mgr != null ) {
                // Run the stop command no matter what state they are in, since the Alive status is not
                // always reliable, it is only based on the Connector implementation. This is fool proof. 
                mgr.stop();
                
                // remove from the local configuration. We want to create a new connector binding each time
                // we start, so that we can initialize with correct properties, in case they chnaged.
                removeConnectorBinding(binding.getDeployedName());
                
                DQPEmbeddedPlugin.logInfo("DataService.Connector_Stopped", new Object[] {binding.getDeployedName()}); //$NON-NLS-1$
            }
        }
        else {
            throw new ApplicationLifecycleException(DQPEmbeddedPlugin.Util.getString("DataService.Unable_to_find_connector", deployedConnectorBindingName)); //$NON-NLS-1$
        }
    }

    /** 
     * @see com.metamatrix.dqp.service.DataService#getConnectorBindings()
     * @since 4.3
     */
    public List getConnectorBindings() throws MetaMatrixComponentException {
        List list =  getConfigurationService().getConnectorBindings();
//        ConnectorBinding indexConnector = (ConnectorBinding)loadedConnectorBindingsMap.get(SYSTEM_PHYSICAL_MODEL_NAME);
//        list.remove(indexConnector);
        return list;
    }
    
    /** 
     * @see com.metamatrix.dqp.service.DataService#getConnectorBindingState(java.lang.String)
     * @since 4.3
     */
    public ConnectorStatus getConnectorBindingState(String deployedConnectorBindingName) 
        throws MetaMatrixComponentException {
        ConnectorBinding binding = getConnectorBinding(deployedConnectorBindingName);
        if (binding != null) {
            ConnectorManager mgr = getConnectorManager(binding);
            if (mgr != null) {
                return mgr.getStatus();
            }
            return ConnectorStatus.CLOSED;
        }
        throw new MetaMatrixComponentException(DQPEmbeddedPlugin.Util.getString("DataService.Unable_to_find_connector", deployedConnectorBindingName)); //$NON-NLS-1$
    } 
    
    /**
     * Initialize the service with the specified properties.
     * @param props Initialialization properties
     * @throws com.metamatrix.common.application.exception.ApplicationInitializationException If an error occurs during initialization
     */
    public void initializeService(Properties props) throws ApplicationInitializationException {
    }

    /**
     * Start the service with the specified environment.  The environment can
     * be used to find other services or resources.
     * @param environment Environment
     * @throws com.metamatrix.common.application.exception.ApplicationLifecycleException If an error occurs while starting
     */
    public void startService(ApplicationEnvironment environment) 
        throws ApplicationLifecycleException {
    	this.env = environment;
        getConfigurationService().register(this.listener);

        try {                
            // Start System Model connector Binding
            startConnectorBinding(SYSTEM_PHYSICAL_MODEL_NAME);
                        
            // Loop through the available ACTIVE VDBS and only start those
            // connector bindings that are ACTIVE VDBS.
            List otherBindings = new ArrayList();
            List<VDBArchive> vdbs = getConfigurationService().getVDBs();
            for (VDBArchive vdb:vdbs) {
            	VDBDefn def = vdb.getConfigurationDef();
                if (vdb.getStatus() == VDBStatus.ACTIVE || vdb.getStatus() == VDBStatus.ACTIVE_DEFAULT) {                                        
                    for (final Iterator it = def.getConnectorBindings().values().iterator(); it.hasNext();) {
                    	final ConnectorBinding binding = (ConnectorBinding)it.next();
                 	 	otherBindings.add(binding);                   
                	} // for                    
                }                
            }
                        
            // Now start any other vdb related connectors.
            for(Iterator i = otherBindings.iterator(); i.hasNext();) {
                ConnectorBinding binding = null;
                try {
                	binding = (ConnectorBinding)i.next();     
                	startConnectorBinding(binding.getDeployedName());
                } catch (ApplicationLifecycleException e) {
                    // we need continue loading connectors even if one fails  
                    DQPEmbeddedPlugin.logError(e, "DataService.Connector_failed_start", new Object[] {binding.getDeployedName()}); //$NON-NLS-1$                    
                } catch(MetaMatrixComponentException e) {
                    DQPEmbeddedPlugin.logError(e, "DataService.Connector_failed_start", new Object[] {binding.getDeployedName()}); //$NON-NLS-1$
                }
            }            
            DQPEmbeddedPlugin.logInfo("DataService.Started", null); //$NON-NLS-1$            
        } catch (MetaMatrixComponentException e) {
            DQPEmbeddedPlugin.logError(e, "DataService.Failed_To_Start", null); //$NON-NLS-1$
            throw new ApplicationLifecycleException(e);
        }         
    }

    /**
     * Stop the service.
     * @throws ApplicationLifecycleException If an error occurs while starting
     */
    public void stopService() throws ApplicationLifecycleException {
    	getConfigurationService().unregister(this.listener);
        // Avoid concurrent modification as stop binding also modifies the 
        // map.
        String[] connectorBindings = (String[])loadedConnectorBindingsMap.keySet().toArray(new String[loadedConnectorBindingsMap.keySet().size()]);        
        for(int i = 0; i < connectorBindings.length; i++) {
            try {
                stopConnectorBinding(connectorBindings[i]);
            } catch (MetaMatrixComponentException e) {
                throw new ApplicationLifecycleException(e);
            }
        }
               
        // remove every thing to be gc'ed
        connectorMgrs.clear();   
        connectorIDs.clear();
        loadedConnectorBindingsMap.clear();
        
        DQPEmbeddedPlugin.logInfo("DataService.Stopped", null); //$NON-NLS-1$        
    }
    
    /**
     * When somebody asks for a connector manager and manager is not loaded then
     * load the connector binding from the configuration service and add a connector
     * manager. This is needed to startConnectorBinding
     */    
    ConnectorManager getConnectorManager(ConnectorBinding binding, boolean create) 
        throws MetaMatrixComponentException{
        
        ConnectorID connectionId = (ConnectorID)connectorIDs.get(binding.getDeployedName());
        if (connectionId == null && create) {
            return createConnectorManger(binding);
        }
        return (ConnectorManager)connectorMgrs.get(connectionId);
    }

    ConnectorManager getConnectorManager(ConnectorBinding binding) 
        throws MetaMatrixComponentException{
        return getConnectorManager(binding, true);
    }
    
    /**
     * When somebody asks for the connector manager by their ID, that means
     * Manager has been already created, we do not need to load the connector
     * binding. 
     */
    ConnectorManager getConnectorManager(ConnectorID connID) {        
        if (connID != null) {
            return (ConnectorManager)connectorMgrs.get(connID);
        }
        return null;
    }    

    public ConnectorBinding getConnectorBinding(String deployedConnectorBindingName) 
        throws MetaMatrixComponentException{
        
        ConnectorBinding binding = (ConnectorBinding)loadedConnectorBindingsMap.get(deployedConnectorBindingName);
        if (binding == null) {
            if (SYSTEM_PHYSICAL_MODEL_NAME.equals(deployedConnectorBindingName)) {
                binding = getSystemModelBinding();
            }
            else {
                // if connector binding not found load from the configuration service.
                binding = getConfigurationService().getConnectorBinding(deployedConnectorBindingName);
            }
        }
        return binding;
    }    
    
      
    ConnectorManager createConnectorManger(ConnectorBinding binding) 
        throws MetaMatrixComponentException{
                
        // Decrypt properties first
        Properties connectorProperties = getDecryptedProperties(binding);
        
        String connectorId = String.valueOf(counter.getAndIncrement());
        connectorProperties.setProperty(ConnectorPropertyNames.CONNECTOR_ID, connectorId);
        connectorProperties.setProperty(ConnectorPropertyNames.CONNECTOR_BINDING_NAME, binding.getFullName());
        
        try {
            ConnectorManager mgr = new ConnectorManager();       
            mgr.setClassLoaderManager(this.getConfigurationService());
            mgr.initialize(connectorProperties);            
            return mgr;
        } catch(Exception e) {
            DQPEmbeddedPlugin.logError(e, "DataService.Failed_Initialize_CM", new Object[] {binding.getDeployedName()}); //$NON-NLS-1$            
            throw new MetaMatrixComponentException(e);
        }
    }
    
    private void removeConnectorBinding(String connectorBindingName) 
        throws MetaMatrixComponentException {
        // do house cleanup of the objects.
        ConnectorID id = selectConnector(connectorBindingName);
        connectorMgrs.remove(id);   
        connectorIDs.remove(connectorBindingName);
        loadedConnectorBindingsMap.remove(connectorBindingName);                    
    }

    Properties getDecryptedProperties(ConnectorBinding binding) 
        throws MetaMatrixComponentException{
                
        Properties decryptedProperties = null;        
        
        // Get all the default properties for the connector type, so that
        // if the connector binding does not have all the properties then these
        // will take over, otherwise the connector binding ones overwrite
        ComponentTypeID id = binding.getComponentTypeID();
        ConnectorBindingType type = getConfigurationService().getConnectorType(id.getName());

        // Index connector has no formal definition in the configuration file. 
        decryptedProperties = new Properties(getConfigurationService().getDefaultProperties(type));

        Properties bindingProperties = binding.getProperties();
        
        // now overlay the custom properties from the default properties.
        decryptedProperties.putAll(bindingProperties);
        
        Iterator it = bindingProperties.keySet().iterator();
        while(it.hasNext()) {
            String name = (String)it.next();
            if (isMaskedProperty(name, type)) {
                try {
                    String value = decryptProperty(bindingProperties.getProperty(name));
                    decryptedProperties.setProperty(name, value);
                } catch (CryptoException e) {
                    DQPEmbeddedPlugin.logError(e, "DataService.decryption_failed", new Object[] {binding.getDeployedName(), name}); //$NON-NLS-1$
                }
            }
        }
        return PropertiesUtils.resolveNestedProperties(decryptedProperties);
    }
    
    /**
     * Check to see if the property read is a masked/encoded property 
     * @param propName
     * @param type
     * @return
     * @since 4.3
     */
    protected boolean isMaskedProperty(String  propName, ComponentType type) {
        if (type != null) {
            ComponentTypeDefn typeDef = type.getComponentTypeDefinition(propName);
            if (typeDef != null && typeDef.getPropertyDefinition().isMasked()) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Decrypt the given property using the Crypto libraries. 
     * @param value
     * @return decrypted property.
     * @since 4.3
     */
    protected String decryptProperty(String value) 
        throws CryptoException{
        if (value != null && value.length() > 0 && CryptoUtil.isValueEncrypted(value)) {
            return CryptoUtil.stringDecrypt(value);
        }
        return value;
    }    
    
    /**
     * Create a Connector Binding for the System Model. 
     * @return
     * @since 4.3
     */
    ConnectorBinding getSystemModelBinding() {
        return new DefaultIndexConnectorBinding();          
    }    
        
}
