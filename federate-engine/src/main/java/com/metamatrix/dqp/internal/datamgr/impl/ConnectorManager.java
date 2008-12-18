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

/*
 * Date: Aug 25, 2003
 * Time: 3:53:37 PM
 */
package com.metamatrix.dqp.internal.datamgr.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.application.basic.BasicApplication;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.common.comm.api.ResultsReceiver;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.queue.WorkerPool;
import com.metamatrix.common.queue.WorkerPoolFactory;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.data.api.Connection;
import com.metamatrix.data.api.Connector;
import com.metamatrix.data.api.ConnectorCapabilities;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.data.api.GlobalCapabilitiesProvider;
import com.metamatrix.data.api.SecurityContext;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.monitor.AliveStatus;
import com.metamatrix.data.monitor.ConnectionStatus;
import com.metamatrix.data.monitor.MonitoredConnector;
import com.metamatrix.data.xa.api.XAConnector;
import com.metamatrix.dqp.DQPPlugin;
import com.metamatrix.dqp.ResourceFinder;
import com.metamatrix.dqp.internal.cache.ResultSetCache;
import com.metamatrix.dqp.internal.cache.connector.CacheConnector;
import com.metamatrix.dqp.internal.datamgr.CapabilitiesConverter;
import com.metamatrix.dqp.internal.datamgr.ConnectorID;
import com.metamatrix.dqp.internal.datamgr.ConnectorPropertyNames;
import com.metamatrix.dqp.internal.process.DQPWorkContext;
import com.metamatrix.dqp.message.AtomicRequestID;
import com.metamatrix.dqp.message.AtomicRequestMessage;
import com.metamatrix.dqp.message.AtomicResultsMessage;
import com.metamatrix.dqp.message.RequestID;
import com.metamatrix.dqp.service.DQPServiceNames;
import com.metamatrix.dqp.service.MetadataService;
import com.metamatrix.dqp.service.TrackingService;
import com.metamatrix.dqp.service.TransactionService;
import com.metamatrix.dqp.spi.TrackerLogConstants;
import com.metamatrix.dqp.transaction.TransactionServer;
import com.metamatrix.dqp.util.LogConstants;
import com.metamatrix.query.optimizer.capabilities.BasicSourceCapabilities;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities.Scope;
import com.metamatrix.query.sql.lang.Command;

/**
 * The <code>ConnectorManager</code> manages a {@link com.metamatrix.data.api.Connector Connector}
 * and its associated workers' state.
 */
public class ConnectorManager extends BasicApplication {

    public static final int DEFAULT_MAX_PROCESSOR_THREADS = 15;
    public static final int DEFAULT_PROCESSOR_TREAD_TTL = 120000;

    //state constructed in start
    private Connector connector;
    private MonitoredConnector monitoredConnector;
    private ConnectorID connectorID;
    private WorkerPool connectorWorkerPool;
    private ResultSetCache rsCache;
	private String connectorName;
    private int maxResultRows;
    private boolean exceptionOnMaxRows = true;
    private boolean pooledResource;

    //services acquired in start
    private MetadataService metadataService;
    private TrackingService tracker;
    private TransactionService transactionService;
    
    private volatile Boolean started;

    // known requests
    private ConcurrentHashMap<AtomicRequestID, ConnectorWorkItem> requestStates = new ConcurrentHashMap<AtomicRequestID, ConnectorWorkItem>();

    // Lazily created and used for asynch query execution
    private Timer timer;
    
    public SourceCapabilities getCapabilities(RequestID requestID, Serializable executionPayload, DQPWorkContext message) throws ConnectorException {
        Connection conn = null;
        // Defect 17536 - Set the thread-context classloader to the non-delegating classloader when calling
        // methods on the connector.
        Thread currentThread = Thread.currentThread();
        ClassLoader threadContextLoader = currentThread.getContextClassLoader();
        try {
        	ConnectorCapabilities caps = null;
            currentThread.setContextClassLoader(connector.getClass().getClassLoader());
            if (connector instanceof GlobalCapabilitiesProvider) {
            	caps = ((GlobalCapabilitiesProvider)connector).getCapabilities();
            }
            boolean global = true;
            if (caps == null) {
            	SecurityContext context = new ExecutionContextImpl(
                        message.getVdbName(),
                        message.getVdbVersion(),
                        message.getUserName(),
                        message.getTrustedPayload(),
                        executionPayload,
                        "capabilities-request", //$NON-NLS-1$
                        connectorID.getID(), 
                        requestID.toString(), 
                        "capabilities-request", "0", false); //$NON-NLS-1$ //$NON-NLS-2$ 

            	conn = connector.getConnection(context);
            	caps = conn.getCapabilities();
            	global = caps.getCapabilitiesScope() == ConnectorCapabilities.SCOPE.GLOBAL;
            }
            BasicSourceCapabilities resultCaps = CapabilitiesConverter.convertCapabilities(caps, getName());
            resultCaps.setScope(global?Scope.SCOPE_GLOBAL:Scope.SCOPE_PER_USER);
            return resultCaps;
        } finally {
        	if ( conn != null ) {
                conn.release();
            }
            currentThread.setContextClassLoader(threadContextLoader);
        }
    }
    
    public void clearCache() {
        if (rsCache != null) {
        	rsCache.clear();
        }
    }
        
    public void executeRequest(ResultsReceiver<AtomicResultsMessage> receiver, AtomicRequestMessage message) {
        // Set the connector ID to be used; if not already set. 
    	AtomicRequestID atomicRequestId = message.getAtomicRequestID();
    	LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {atomicRequestId, "Create State"}); //$NON-NLS-1$
    	
    	ConnectorWorkItem item = null;
    	if (pooledResource) {
    		item = new SynchConnectorWorkItem(message, this, receiver);
    	} else {
    		item = new AsynchConnectorWorkItem(message, this, receiver);
    	}

        Assertion.isNull(requestStates.put(atomicRequestId, item), "State already existed"); //$NON-NLS-1$
        message.markProcessingStart();
        enqueueRequest(item);
    }
    
    private void enqueueRequest(ConnectorWorkItem state) {
        this.connectorWorkerPool.execute(state);
    }
    
    void reenqueueRequest(AsynchConnectorWorkItem state) {
    	enqueueRequest(state);
    }
    
    ConnectorWorkItem getState(AtomicRequestID requestId) {
        return requestStates.get(requestId);
    }
    
    public void requstMore(AtomicRequestID requestId) {
    	ConnectorWorkItem workItem = getState(requestId);
    	if (workItem == null) {
    		return; //already closed
    	}
        ClassLoader contextloader = Thread.currentThread().getContextClassLoader();
        try {
        	Thread.currentThread().setContextClassLoader(this.connector.getClass().getClassLoader());
    	    workItem.requestMore();
        } finally {
        	Thread.currentThread().setContextClassLoader(contextloader);
        }
    }
    
    public void cancelRequest(AtomicRequestID requestId) {
    	ConnectorWorkItem workItem = getState(requestId);
    	if (workItem == null) {
    		return; //already closed
    	}
        ClassLoader contextloader = Thread.currentThread().getContextClassLoader();
        try {
        	Thread.currentThread().setContextClassLoader(this.connector.getClass().getClassLoader());
    	    workItem.requestCancel();
        } finally {
        	Thread.currentThread().setContextClassLoader(contextloader);
        }
    }
    
    public void closeRequest(AtomicRequestID requestId) {
    	ConnectorWorkItem workItem = getState(requestId);
    	if (workItem == null) {
    		return; //already closed
    	}
        ClassLoader contextloader = Thread.currentThread().getContextClassLoader();
        try {
        	Thread.currentThread().setContextClassLoader(this.connector.getClass().getClassLoader());
    	    workItem.requestClose();
        } finally {
        	Thread.currentThread().setContextClassLoader(contextloader);
        }
    }
        
    /**
     * Schedule a task to be executed after the specified delay (in milliseconds) 
     * @param task The task to execute
     * @param delay The delay to wait (in ms) before executing the task
     * @since 4.3.3
     */
    public void scheduleTask(final AsynchConnectorWorkItem state, long delay) {
        synchronized(this) {
            if(this.timer == null) {
                this.timer = new Timer("AsynchRequestThread", true); //$NON-NLS-1$
            }
        }
        
        this.timer.schedule(new TimerTask() {
			@Override
			public void run() {
				state.requestMore();
			}}, delay);
    }
    
    /**
     * Remove the state associated with
     * the given <code>RequestID</code>.
     */
    void removeState(AtomicRequestID id) {
    	LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {id, "Remove State"}); //$NON-NLS-1$
        requestStates.remove(id);    
    }

    int size() {
        return requestStates.size();
    }
    
    /**
     * @see com.metamatrix.dqp.internal.datamgr.ConnectorManager#isAlive()
     */
    public ConnectionStatus getStatus() {
        if (monitoredConnector != null) {
            return this.monitoredConnector.getStatus();
        }
        return new ConnectionStatus(AliveStatus.UNKNOWN);
    }
    
    /**
     * initialize this <code>ConnectorManager</code>.
     */
    public synchronized void start() throws ApplicationLifecycleException {
    	if (this.started != null) {
    		throw new ApplicationLifecycleException("ConnectorManager.cannot_restart"); //$NON-NLS-1$
    	}
        Properties appProps = getEnvironment().getApplicationProperties();

        connectorName = appProps.getProperty(ConnectorPropertyNames.CONNECTOR_BINDING_NAME, "Unknown_Binding_Name"); //$NON-NLS-1$

        String connIDStr = appProps.getProperty(ConnectorPropertyNames.CONNECTOR_ID);
        connectorID = new ConnectorID(connIDStr);
        
        //connector Name - logical name<Unique Id>
        connectorName = connectorName + '<' + connIDStr + '>';

        LogManager.logInfo(LogConstants.CTX_CONNECTOR, DQPPlugin.Util.getString("ConnectorManagerImpl.Initializing_connector", connectorName)); //$NON-NLS-1$

        this.setTransactionService((TransactionService) getEnvironment().findService(DQPServiceNames.TRANSACTION_SERVICE));

        // Set the class name for the connector class
        String connectorClassString = appProps.getProperty(ConnectorPropertyNames.CONNECTOR_CLASS);
        if ( connectorClassString == null || connectorClassString.trim().length() == 0 ) {             
            throw new ApplicationLifecycleException(DQPPlugin.Util.getString("Missing_required_property", new Object[]{ConnectorPropertyNames.CONNECTOR_CLASS, connectorName})); //$NON-NLS-1$
        }

        // Create the Connector env
        Properties clonedProps = PropertiesUtils.clone(appProps);
        ConnectorEnvironment connectorEnv = new ConnectorEnvironmentImpl(clonedProps, new DefaultConnectorLogger(connectorID), this.getEnvironment());

        // Initialize and start the connector
        initStartConnector(connectorName, connectorEnv);

        // Get the metadata service
        this.metadataService = (MetadataService) getEnvironment().findService(DQPServiceNames.METADATA_SERVICE);
        if ( this.metadataService == null ) {
            throw new ApplicationLifecycleException(DQPPlugin.Util.getString("Failed_to_find_service", new Object[]{DQPServiceNames.METADATA_SERVICE, connectorName})); //$NON-NLS-1$
        }

        this.tracker = (TrackingService) getEnvironment().findService(DQPServiceNames.TRACKING_SERVICE);

        int maxThreads = PropertiesUtils.getIntProperty(appProps, ConnectorPropertyNames.MAX_THREADS, DEFAULT_MAX_PROCESSOR_THREADS);
        int threadTTL = PropertiesUtils.getIntProperty(appProps, ConnectorPropertyNames.THREAD_TTL, DEFAULT_PROCESSOR_TREAD_TTL);

        connectorWorkerPool = WorkerPoolFactory.newWorkerPool(connectorName, maxThreads, threadTTL);
        
        this.maxResultRows = PropertiesUtils.getIntProperty(appProps, ConnectorPropertyNames.MAX_RESULT_ROWS, 0);
        this.exceptionOnMaxRows = PropertiesUtils.getBooleanProperty(appProps, ConnectorPropertyNames.EXCEPTION_ON_MAX_ROWS, false);
        this.pooledResource = PropertiesUtils.getBooleanProperty(appProps, ConnectorPropertyNames.POOLED_RESOURCE, true);

        this.started = true;
    }
    
    /**
     * Initialize and start the connector.
     * @param env
     * @throws ApplicationLifecycleException
     */
    private void initStartConnector(String connectorName, ConnectorEnvironment env) throws ApplicationLifecycleException {
        String connectorClassName = env.getProperties().getProperty(ConnectorPropertyNames.CONNECTOR_CLASS);
        // Create connector instance...
        ClassLoader loader = (ClassLoader)this.getEnvironment().getApplicationProperties().get(ConnectorPropertyNames.CONNECTOR_CLASS_LOADER);
        if(loader == null){
            loader = getClass().getClassLoader();
        }
        Class clazz = null;
        try {
            clazz = loader.loadClass(connectorClassName);
        } catch (ClassNotFoundException e) {
            throw new ApplicationLifecycleException(e, DQPPlugin.Util.getString("failed_find_Connector_class", connectorClassName)); //$NON-NLS-1$
        }

        try {
            this.connector = (Connector) clazz.newInstance();
            if(this.connector instanceof XAConnector){
                if (this.getTransactionService() == null) {                    
                    throw new ApplicationLifecycleException(DQPPlugin.Util.getString("no_txn_manager", connectorName)); //$NON-NLS-1$
                }
                
                // add this connector as the recovery source
                TransactionServer ts = this.getTransactionService().getTransactionServer(); 
                ts.registerRecoverySource(connectorName, (XAConnector)this.connector);
            }
            
            if(this.connector instanceof MonitoredConnector) {
                this.monitoredConnector = (MonitoredConnector)connector;
            } 
            wrapCacheConnector(env);
        } catch (InstantiationException e) {
            throw new ApplicationLifecycleException(e, DQPPlugin.Util.getString("failed_instantiate_Connector_class", new Object[]{connectorClassName})); //$NON-NLS-1$
        } catch (IllegalAccessException e) {
            throw new ApplicationLifecycleException(e, DQPPlugin.Util.getString("failed_access_Connector_class", new Object[]{connectorClassName})); //$NON-NLS-1$
        }
        
        // Defect 17536 - Set the thread-context classloader to the non-delegating classloader when calling methods on the connector.
        Thread currentThread = Thread.currentThread();
        ClassLoader threadContextLoader = currentThread.getContextClassLoader();
        try {
            currentThread.setContextClassLoader(loader);
            // Initialize connector instance...
            try {
                this.connector.initialize(env);
            } catch (ConnectorException e) {
                throw new ApplicationLifecycleException(e, DQPPlugin.Util.getString("failed_to_initialize", new Object[]{connectorClassName})); //$NON-NLS-1$
            }
    
            //start connector
            try {
                this.connector.start();
            } catch (ConnectorException e) {
                throw new ApplicationLifecycleException(e, DQPPlugin.Util.getString("failed_start_Connector", new Object[] {this.getConnectorID(), e.getMessage()})); //$NON-NLS-1$
            }    
        } finally {
            currentThread.setContextClassLoader(threadContextLoader);
        }
    }

	private void wrapCacheConnector(ConnectorEnvironment connectorEnv) {
        //check result set cache
        Properties props = connectorEnv.getProperties();
        Properties rsCacheProps = null;
        if(Boolean.valueOf(props.getProperty(ConnectorPropertyNames.USE_RESULTSET_CACHE, "false")).booleanValue()){ //$NON-NLS-1$
        	rsCacheProps = new Properties();
        	rsCacheProps.setProperty(ResultSetCache.RS_CACHE_MAX_SIZE, props.getProperty(ConnectorPropertyNames.MAX_RESULTSET_CACHE_SIZE, "0")); //$NON-NLS-1$
        	rsCacheProps.setProperty(ResultSetCache.RS_CACHE_MAX_AGE, props.getProperty(ConnectorPropertyNames.MAX_RESULTSET_CACHE_AGE, "0")); //$NON-NLS-1$
        	rsCacheProps.setProperty(ResultSetCache.RS_CACHE_SCOPE, props.getProperty(ConnectorPropertyNames.RESULTSET_CACHE_SCOPE, ResultSetCache.RS_CACHE_SCOPE_VDB)); 
        	try {
        		rsCache = new ResultSetCache(rsCacheProps, ResourceFinder.getCacheFactory());
        		connector = new CacheConnector(connector, rsCache);
			} catch (MetaMatrixComponentException e) {
				// this does not really affect the 
				//function of connector, log warning for now
                LogManager.logWarning(LogConstants.CTX_CONNECTOR, e, DQPPlugin.Util.getString("DQPCORE.6")); //$NON-NLS-1$
			}
        }
	}

    /**
     * Queries the Connector Manager, if it already has been started. 
     * @return
     * @since 4.3
     */
    public boolean started() {
    	if (this.started != null) {
    		return this.started;
    	}
        return false;
    }
    
    /**
     * Stop this connector.
     */
    public void stop() throws ApplicationLifecycleException {        
        super.stop();
        synchronized (this) {
        	if (this.started == null || this.started == false) {
        		return;
        	}
            this.started = false;
		}
        if (this.connectorWorkerPool != null) {
        	this.connectorWorkerPool.shutdownNow();
        }
        
        //ensure that all requests receive a response
        for (ConnectorWorkItem workItem : this.requestStates.values()) {
        	try {
        		workItem.resultsReceiver.exceptionOccurred(new ConnectorException(DQPPlugin.Util.getString("Connector_Shutting_down", new Object[] {workItem.id, connectorID}))); //$NON-NLS-1$
        	} catch (Exception e) {
        		//ignore
        	}
		}
        
        if ( this.connector != null ) {

            if(this.connector instanceof XAConnector){
                if (this.getTransactionService() != null) {
                    TransactionServer ts = this.getTransactionService().getTransactionServer();
                    ts.removeRecoverySource(connectorName);
                }
            }
            
            Thread currentThread = Thread.currentThread();
            ClassLoader threadContextLoader = currentThread.getContextClassLoader();
            try {
                currentThread.setContextClassLoader(connector.getClass().getClassLoader());
                this.connector.stop();
            } finally {
                currentThread.setContextClassLoader(threadContextLoader);
            }
            
        }
        
        if(this.timer != null) {
            this.timer.cancel();
            this.timer = null;
        }
        
        if (this.rsCache != null) {
        	this.rsCache.shutDown();
        	this.rsCache = null;
        }
    }

    /**
     * Returns a list of QueueStats objects that represent the queues in
     * this service.
     * If there are no queues, an empty Collection is returned.
     */
    public Collection getQueueStatistics() {
        if ( this.connectorWorkerPool == null ) {
            return Collections.EMPTY_LIST;
        }

        Collection statList = new ArrayList(1);
        statList.add(connectorWorkerPool.getStats());
        return statList;
    }

    /**
     * Returns a QueueStats object that represent the queue in
     * this service.
     * If there is no queue with the given name, an empty Collection is returned.
     */
    public Collection getQueueStatistics(String name) {
        if ( connectorID == null ||
             !name.equalsIgnoreCase(connectorID.getID()) ||
             connectorWorkerPool == null ) {
            return Collections.EMPTY_LIST;
        }
        Collection stats = new ArrayList(2);
        stats.add(connectorWorkerPool.getStats());
        return stats;
    }

    /**
     * Add begin point to transaction monitoring table.
     * @param qr Request that contains the MetaMatrix command information in the transaction.
     */
    void logSRCCommand(AtomicRequestMessage qr, ExecutionContext context, short cmdStatus, int finalRowCnt) {
        if(tracker == null || !tracker.willRecordSrcCmd()){
            return;
        }
        Command sqlCmd = null;
        if(cmdStatus == TrackerLogConstants.CMD_STATUS.NEW){
            sqlCmd = qr.getCommand();
        }
        String userName = qr.getWorkContext().getUserName();
        String transactionID = null;
        if ( qr.isTransactional() ) {
            transactionID = qr.getTransactionContext().getTxnID();
        }
        
        String modelName = qr.getModelName();
        AtomicRequestID id = qr.getAtomicRequestID();
                
        tracker.log(qr.getRequestID().toString(), id.getNodeID(), transactionID,
                cmdStatus, modelName == null ? "null" : modelName, connectorName, //$NON-NLS-1$
                cmdStatus == TrackerLogConstants.CMD_STATUS.NEW ? TrackerLogConstants.CMD_POINT.BEGIN : TrackerLogConstants.CMD_POINT.END,
                qr.getWorkContext().getConnectionID(), userName == null ? "unknown" : userName, sqlCmd, finalRowCnt, context); //$NON-NLS-1$
    }
    
    /**
     * Get the <code>Connector</code> object managed by this
     * manager.
     * @return the <code>Connector</code>.
     */
    Connector getConnector() {
        return this.connector;
    }
    
    void setConnector(Connector connector) {
    	this.connector = connector;
    }

	int getMaxResultRows() {
		return maxResultRows;
	}

	void setMetadataService(MetadataService metadataService) {
		this.metadataService = metadataService;
	}

	MetadataService getMetadataService() {
		return metadataService;
	}

	boolean isExceptionOnMaxRows() {
		return exceptionOnMaxRows;
	}

	void setTransactionService(TransactionService transactionService) {
		this.transactionService = transactionService;
	}

	TransactionService getTransactionService() {
		return transactionService;
	}
	
    /**
     * The identifier that code will use to identify this object.
     * @return The <code>ConnectorID</code>.
     */
    public ConnectorID getConnectorID() {
        return connectorID;
    }
    
    /**
     * Get the human-readable name that this connector is known by.
     * <p>Will be <code>null</code> if connector is not started.</p>
     * @return The connector's name.
     */
    public String getName() {
        return this.connectorName;
    }

	void setConnectorWorkerPool(WorkerPool connectorWorkerPool) {
		this.connectorWorkerPool = connectorWorkerPool;
	}

}
