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

/*
 * Date: Sep 25, 2003
 * Time: 4:36:24 PM
 */
package com.metamatrix.server.connector.service;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorPropertyNames;
import org.teiid.dqp.internal.datamgr.impl.ConnectorManager;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.dqp.internal.process.DQPWorkContext;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.application.ApplicationService;
import com.metamatrix.common.application.ClassLoaderManager;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.common.comm.ClientServiceRegistry;
import com.metamatrix.common.comm.api.ResultsReceiver;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ComponentTypeDefn;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.DeployedComponentID;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.object.PropertyDefinition;
import com.metamatrix.common.queue.WorkerPoolStats;
import com.metamatrix.common.stats.ConnectionPoolStats;
import com.metamatrix.common.util.LogCommonConstants;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.common.util.crypto.CryptoException;
import com.metamatrix.common.util.crypto.CryptoUtil;
import com.metamatrix.dqp.client.ClientSideDQP;
import com.metamatrix.dqp.internal.datamgr.ConnectorID;
import com.metamatrix.dqp.message.AtomicRequestID;
import com.metamatrix.dqp.message.AtomicRequestMessage;
import com.metamatrix.dqp.message.AtomicResultsMessage;
import com.metamatrix.dqp.message.RequestID;
import com.metamatrix.dqp.service.ConnectorStatus;
import com.metamatrix.dqp.service.DQPServiceNames;
import com.metamatrix.platform.service.api.CacheAdmin;
import com.metamatrix.platform.service.api.ServiceID;
import com.metamatrix.platform.service.api.ServiceState;
import com.metamatrix.platform.service.api.exception.ServiceStateException;
import com.metamatrix.platform.service.controller.AbstractService;
import com.metamatrix.platform.service.controller.ServicePropertyNames;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities;
import com.metamatrix.server.ServerPlugin;

/**
 * ConnectorService.
 */
public class ConnectorService extends AbstractService implements ConnectorServiceInterface, CacheAdmin {
    private static final String RESULT_SET_CACHE_NAME = "ConnectorResultSetCache"; //$NON-NLS-1$
    
    private ConnectorManager connectorMgr;
    private String connectorMgrName;
    private boolean monitoringEnabled = true;
    
    private ClientServiceRegistry registry;
    private ClassLoaderManager clManager;
    
    /**
     * Initialize ConnectorService
     */
    public void init(ServiceID id, DeployedComponentID deployedComponentID, Properties props, ClientServiceRegistry listenerRegistry, ClassLoaderManager clManager) {
    	//this assumes that the dqp has already been initialized and i
    	this.registry = listenerRegistry;
    	this.clManager = clManager;
        
        super.init(id, deployedComponentID, props, listenerRegistry, clManager);
        //read value of monitoringEnabled
        String monitoringEnabledString = getProperties().getProperty(ServicePropertyNames.SERVICE_MONITORING_ENABLED);
        if (monitoringEnabledString != null) {
            monitoringEnabled = Boolean.valueOf(monitoringEnabledString).booleanValue();
        }   
        logOK("ConnectorService.Data_source_monitoring_enabled", new Boolean(monitoringEnabled)); //$NON-NLS-1$
    }

    //=========================================================================
    // Methods from ConnectorServiceInterface
    //=========================================================================

    public ConnectorID getConnectorID() throws ServiceStateException {
        return this.connectorMgr.getConnectorID();
    }
    
	public void cancelRequest(AtomicRequestID request)
			throws MetaMatrixComponentException {
    	this.connectorMgr.cancelRequest(request);
	}

	public void closeRequest(AtomicRequestID request)
			throws MetaMatrixComponentException {
    	this.connectorMgr.closeRequest(request);
	}

	public void executeRequest(AtomicRequestMessage request,
			ResultsReceiver<AtomicResultsMessage> resultListener)
			throws MetaMatrixComponentException {
		this.connectorMgr.executeRequest(resultListener, request);
	}

	public void requestBatch(AtomicRequestID request)
			throws MetaMatrixComponentException {
    	this.connectorMgr.requstMore(request);
	}
    
    /**
     * Build and intialize the Connector Manager class. 
     * @param deMaskedProps
     * @param loader
     * @throws ApplicationLifecycleException
     * @throws ApplicationInitializationException
     */
    private ConnectorManager createConnectorManager(Properties deMaskedProps) throws ApplicationLifecycleException {        
        ConnectorManager connectorManager = new ConnectorManager();
        connectorManager.setClassLoaderManager(clManager);
        // Create a stringified connector ID from the serviceID
        ServiceID id = this.getID();
        String connID = id.getHostName()+"|"+ id.getProcessName() + "|" + id.getID();   //$NON-NLS-1$ //$NON-NLS-2$
        deMaskedProps.put(ConnectorPropertyNames.CONNECTOR_ID, connID);
        deMaskedProps.put(ConnectorPropertyNames.CONNECTOR_BINDING_NAME, getInstanceName());
        deMaskedProps.put(ConnectorPropertyNames.CONNECTOR_VM_NAME, CurrentConfiguration.getInstance().getProcessName());
        connectorManager.initialize(deMaskedProps);
        return connectorManager;
    }
    
    /**
     * Perform initialization and commence processing. This method is called only once.
     */
    protected void initService(Properties props) throws ApplicationLifecycleException, ApplicationInitializationException {
        // Decrypt masked properties
        Properties deMaskedProps = decryptMaskedProperties(props);

        // Build a Connector manager using the custom class loader and initialize
        // the service.
        this.connectorMgr = createConnectorManager(deMaskedProps);

        ApplicationEnvironment env = new ApplicationEnvironment();
        env.bindService(DQPServiceNames.REGISTRY_SERVICE, new ClientServiceRegistryService(this.registry));
        //this assumes that the QueryService is local and has been started
        for (int i = 0; i < DQPServiceNames.ALL_SERVICES.length; i++) {
        	final String serviceName = DQPServiceNames.ALL_SERVICES[i];
        	env.bindService(serviceName, (ApplicationService)Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[] {DQPServiceNames.ALL_SERVICE_CLASSES[i]}, new InvocationHandler() {
        		@Override
        		public Object invoke(Object proxy, Method method, Object[] args)
        				throws Throwable {
        	        DQPCore dqp = (DQPCore)registry.getClientService(ClientSideDQP.class);
        	        if (dqp == null) {
        	        	throw new IllegalStateException("A local QueryService is not available"); //$NON-NLS-1$
        	        }
        			ApplicationService instance = dqp.getEnvironment().findService(serviceName);
        			if (instance == null) {
        	        	throw new IllegalStateException(serviceName + " is not available"); //$NON-NLS-1$
        	        }
        			try {
        				return method.invoke(instance, args);
        			} catch (InvocationTargetException e) {
        				throw e.getCause();
        			}
        		}
        	}));
        }

        try {
            // Start the connector manager
            this.connectorMgr.start(env);
            this.connectorMgrName = connectorMgr.getName();
        } catch (ApplicationLifecycleException e) {
            killService();
            throw e;
        }
    }   
    
    /**
     * Close the service to new work if applicable. After this method is called
     * the service should no longer accept new work to perform but should continue
     * to process any outstanding work. This method is called by die().
     */
    protected void closeService() throws ApplicationLifecycleException {
        waitForServiceToClear();
        if ( connectorMgr != null ) {
            killService();
        }
    }

    protected void waitForServiceToClear() throws ApplicationLifecycleException {
    }

    /**
     * Terminate all processing and reclaim resources. This method is called by dieNow()
     * and is only called once.
     */
    protected void killService() {
        if ( connectorMgr != null ) {
            try {
                Object[] params = new Object[]{connectorMgrName};
                LogManager.logInfo(LogCommonConstants.CTX_CONFIG, ServerPlugin.Util.getString("ConnectorService.Killing_connectorMgr", params)); //$NON-NLS-1$
                connectorMgr.stop();
            } catch (ApplicationLifecycleException e) {
                Object[] params = new Object[]{connectorMgrName, e.getMessage()};
                LogManager.logError(LogCommonConstants.CTX_CONFIG, e, ServerPlugin.Util.getString("ConnectorService.Unable_to_shutdown_connectorMgr", params)); //$NON-NLS-1$
            } finally {
                connectorMgr = null;
            }
        }
    }
    
    
    /**
     * Check the underlying connectorManager's state, then call the super.checkState().
     */
    public void checkState() throws ServiceStateException {

        if (monitoringEnabled && connectorMgr != null) {
            ConnectorStatus status = connectorMgr.getStatus();
            int state = getCurrentState();
            if (state == ServiceState.STATE_OPEN && status == ConnectorStatus.DATA_SOURCE_UNAVAILABLE) {
                updateState(ServiceState.STATE_DATA_SOURCE_UNAVAILABLE);
                
                logOK("ConnectorService.Change_state_to_data_source_unavailable", connectorMgrName); //$NON-NLS-1$
                
                //TODO: store the exception in the registry
            }
            
            if (state == ServiceState.STATE_DATA_SOURCE_UNAVAILABLE && status == ConnectorStatus.OPEN) {
                this.updateState(ServiceState.STATE_OPEN);
                
                logOK("ConnectorService.Change_state_to_open", connectorMgrName); //$NON-NLS-1$                
            }
        }
        
        if (getCurrentState() != ServiceState.STATE_DATA_SOURCE_UNAVAILABLE) { 
            super.checkState();
        }
    }
    
	public SourceCapabilities getCapabilities(RequestID requestId,
			Serializable executionPayload,
			DQPWorkContext message)
			throws ConnectorException {
    	return this.connectorMgr.getCapabilities(requestId, executionPayload, message);
    }
    
    /**
     * Returns a list of QueueStats objects that represent the queues in
     * this service.
     * If there are no queues, null is returned.
     */
    public Collection getQueueStatistics() {
        if ( this.connectorMgr != null ) {
        	Collection result = this.connectorMgr.getQueueStatistics();
        	if ( result != null ) {
            	return result;
            }
        }
        
        return new ArrayList();
    }

    /**
     * Returns a QueueStats object that represent the queue in
     * this service.
     * If there is no queue with the given name, null is returned.
     */
    public WorkerPoolStats getQueueStatistics(String name) {
        Collection results = null;
    	if ( this.connectorMgr != null ) {
            results = this.connectorMgr.getQueueStatistics(name);
        }

        WorkerPoolStats poolStats = new WorkerPoolStats();
        if ( results != null ) {
            Iterator resultsItr = results.iterator();
            // There is only one result (if any) in this results collection
            if ( resultsItr.hasNext() ) {
                Object aPoolStat = resultsItr.next();
                if ( aPoolStat != null && aPoolStat instanceof WorkerPoolStats ) {
                    poolStats = (WorkerPoolStats) aPoolStat;
                }
            }
        }
        return poolStats;
    }

    /**
     * Decrypt any and all properties that are marked as masked.  The decryption will undo
     * any encryption that has been performed by the MetaMatrix system during storage of
     * the properties.  This should only occur on properties that are marked as "masked"
     * in the connector's property definitions.
     *
     * @param maskedProps The properties to decrypt.  If a property is "masked" then it will be
     * decrypted.
     * @return Decrypted properties that are cloned from <code>maskedProps</code> with the masked
     * property values overwritten with the decrypted value.
     * @throws ApplicationInitializationException if property value could not be decrypted
     */
    private Properties decryptMaskedProperties(Properties maskedProps) throws ApplicationInitializationException {
        Properties result = PropertiesUtils.clone(maskedProps, false);
        
        //Case 5797 hack.  Remove ServiceName property so that the Driver property is the only instance of this
        //property that is in the properties object
        result.remove(ServicePropertyNames.SERVICE_NAME);
        
        String connectorBindingName = maskedProps.getProperty(ServicePropertyNames.SERVICE_NAME);

        if ( connectorBindingName == null ) {
            String msg = ServerPlugin.Util.getString("ConnectorService.Unable_to_get_connector_binding_name_from_connector_properties"); //$NON-NLS-1$
            throw  new ApplicationInitializationException(msg);
        }
// DEBUG:
//System.out.println(" *** ConnectorService.decryptMaskedProperties - ConnectorBindingName: " + connectorBindingName);
        Configuration currentConfig = null;
        try {
            currentConfig = CurrentConfiguration.getInstance().getConfiguration();
        } catch (ConfigurationException e) {
            Object[] params = new Object[] {connectorBindingName};
            String msg = ServerPlugin.Util.getString("ConnectorService.Unable_to_get_Configuration_for_connector_binding_{0}", params); //$NON-NLS-1$
            throw new ApplicationInitializationException(e, msg);
        }
        ConfigurationModelContainer configModel = null;
        try {
            configModel = CurrentConfiguration.getInstance().getConfigurationModel();
        } catch (ConfigurationException e) {
            Object[] params = new Object[]{connectorBindingName};
            String msg = ServerPlugin.Util.getString("ConnectorService.Unable_to_get_ConfigurationModelContainer_for_connector_binding_{0}", params); //$NON-NLS-1$
            throw new ApplicationInitializationException(e, msg);
        }

        ConnectorBinding configConnBinding = currentConfig.getConnectorBinding(connectorBindingName);
        if ( configConnBinding != null ) {
            ComponentType componentType = configModel.getComponentType(configConnBinding.getComponentTypeID().getName());
            if ( componentType == null ) {
                Object[] params = new Object[]{configConnBinding.getComponentTypeID().getName()};
                String msg = ServerPlugin.Util.getString("ConnectorService.Unable_to_get_connector_ComponentType_for_ComponentTyepID_name_{0}", params); //$NON-NLS-1$
                throw  new ApplicationInitializationException(msg);
            }

            Collection compTypeDefns = configModel.getAllComponentTypeDefinitions((ComponentTypeID)componentType.getID());
            String propValue;
            String propName;
            for ( Iterator compTypeDefnItr = compTypeDefns.iterator(); compTypeDefnItr.hasNext(); ) {
                ComponentTypeDefn typeDefn = (ComponentTypeDefn) compTypeDefnItr.next();
                PropertyDefinition propDefn =  typeDefn.getPropertyDefinition();
                propName = propDefn.getName();
                propValue = maskedProps.getProperty(propName);
                if ( propValue != null ) {
                    if ( propDefn.isMasked() ) {
                        try {
                            propValue = CryptoUtil.stringDecrypt(propValue);
                        } catch (CryptoException e) {
                            throw new ApplicationInitializationException(e,ServerPlugin.Util.getString("ConnectorService.Failed_decrypting_masked_prop", propName)); //$NON-NLS-1$
                        }
                    }
                    result.setProperty(propName, propValue);
                }
            }
        }

        return result;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.platform.service.api.CacheAdmin#getCaches()
     */
    public Map getCaches() throws MetaMatrixComponentException {
        Map names = new HashMap();
        names.put(RESULT_SET_CACHE_NAME, CacheAdmin.CONNECTOR_RESULT_SET_CACHE);
        return names;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.platform.service.api.CacheAdmin#clearCache(java.lang.String, java.util.Properties)
     */
    public void clearCache(String name, Properties props) throws MetaMatrixComponentException {
        if(name.equals(RESULT_SET_CACHE_NAME) && this.connectorMgr != null ) {
        	this.connectorMgr.clearCache();
        }
    }
    
    private static void logOK(String messageProperty, Object value) {
        LogManager.logInfo(LogCommonConstants.CTX_CONFIG, ServerPlugin.Util.getString(messageProperty, value)); 
    }
    
	public Collection<ConnectionPoolStats> getConnectionPoolStats() {
//		return Collections.EMPTY_LIST;
		
		return this.connectorMgr.getConnectionPoolStats();
	}
    
}