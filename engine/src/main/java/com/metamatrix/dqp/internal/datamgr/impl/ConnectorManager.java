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
 * Date: Aug 25, 2003
 * Time: 3:53:37 PM
 */
package com.metamatrix.dqp.internal.datamgr.impl;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import javax.transaction.xa.XAResource;

import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.application.ApplicationService;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.common.comm.api.ResultsReceiver;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.queue.WorkerPool;
import com.metamatrix.common.queue.WorkerPoolFactory;
import com.metamatrix.common.queue.WorkerPoolStats;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.connector.api.Connection;
import com.metamatrix.connector.api.Connector;
import com.metamatrix.connector.api.ConnectorCapabilities;
import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.api.ConnectorException;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.api.ConnectorAnnotations.ConnectionPooling;
import com.metamatrix.connector.api.ConnectorAnnotations.SynchronousWorkers;
import com.metamatrix.connector.internal.ConnectorPropertyNames;
import com.metamatrix.connector.xa.api.XAConnection;
import com.metamatrix.connector.xa.api.XAConnector;
import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.core.util.ReflectionHelper;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.dqp.DQPPlugin;
import com.metamatrix.dqp.internal.cache.ResultSetCache;
import com.metamatrix.dqp.internal.datamgr.CapabilitiesConverter;
import com.metamatrix.dqp.internal.datamgr.ConnectorID;
import com.metamatrix.dqp.internal.pooling.connector.PooledConnector;
import com.metamatrix.dqp.internal.process.DQPWorkContext;
import com.metamatrix.dqp.internal.transaction.TransactionProvider;
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
 * The <code>ConnectorManager</code> manages a {@link com.metamatrix.connector.api.Connector Connector}
 * and its associated workers' state.
 */
public class ConnectorManager implements ApplicationService {

    public static final int DEFAULT_MAX_PROCESSOR_THREADS = 15;
    public static final int DEFAULT_PROCESSOR_TREAD_TTL = 120000;

    //state constructed in start
    private ConnectorWrapper connector;
    private ConnectorID connectorID;
    private WorkerPool connectorWorkerPool;
    private ResultSetCache rsCache;
	private String connectorName;
    private int maxResultRows;
    private boolean exceptionOnMaxRows = true;
    private boolean synchWorkers;
    private boolean isXa;

    //services acquired in start
    private MetadataService metadataService;
    private TrackingService tracker;
    private TransactionService transactionService;
    
    private volatile Boolean started;

    // known requests
    private ConcurrentHashMap<AtomicRequestID, ConnectorWorkItem> requestStates = new ConcurrentHashMap<AtomicRequestID, ConnectorWorkItem>();

    // Lazily created and used for asynch query execution
    private Timer timer;
    
    private Properties props;
	private ClassLoader classloader;
    
    public void initialize(Properties props) {
    	this.props = props;
    }
    
    public ClassLoader getClassloader() {
		return classloader;
	}
    
    public SourceCapabilities getCapabilities(RequestID requestID, Serializable executionPayload, DQPWorkContext message) throws ConnectorException {
        Connection conn = null;
        // Defect 17536 - Set the thread-context classloader to the non-delegating classloader when calling
        // methods on the connector.
        Thread currentThread = Thread.currentThread();
        ClassLoader threadContextLoader = currentThread.getContextClassLoader();
        try {
        	ConnectorCapabilities caps = connector.getCapabilities();
            currentThread.setContextClassLoader(classloader);
            boolean global = true;
            if (caps == null) {
            	ExecutionContext context = new ExecutionContextImpl(
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
            	global = false;
            }
            caps = (ConnectorCapabilities) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[] {ConnectorCapabilities.class}, new CapabilitesOverloader(caps, this.props));
            BasicSourceCapabilities resultCaps = CapabilitiesConverter.convertCapabilities(caps, getName(), isXa);
            resultCaps.setScope(global?Scope.SCOPE_GLOBAL:Scope.SCOPE_PER_USER);
            return resultCaps;
        } finally {
        	if ( conn != null ) {
                conn.close();
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
    	if (synchWorkers) {
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
	    workItem.requestMore();
    }
    
    public void cancelRequest(AtomicRequestID requestId) {
    	ConnectorWorkItem workItem = getState(requestId);
    	if (workItem == null) {
    		return; //already closed
    	}
        ClassLoader contextloader = Thread.currentThread().getContextClassLoader();
        try {
        	Thread.currentThread().setContextClassLoader(classloader);
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
	    workItem.requestClose();
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
    public Boolean getStatus() {
        ClassLoader contextloader = Thread.currentThread().getContextClassLoader();
        try {
        	Thread.currentThread().setContextClassLoader(classloader);
            return this.connector.getStatus();
        } finally {
        	Thread.currentThread().setContextClassLoader(contextloader);
        }
    }
    
    /**
     * initialize this <code>ConnectorManager</code>.
     */
    public synchronized void start(ApplicationEnvironment env) throws ApplicationLifecycleException {
    	if (this.started != null) {
    		throw new ApplicationLifecycleException("ConnectorManager.cannot_restart"); //$NON-NLS-1$
    	}
        connectorName = props.getProperty(ConnectorPropertyNames.CONNECTOR_BINDING_NAME, "Unknown_Binding_Name"); //$NON-NLS-1$

        String connIDStr = props.getProperty(ConnectorPropertyNames.CONNECTOR_ID);
        connectorID = new ConnectorID(connIDStr);
        
        //connector Name - logical name<Unique Id>
        connectorName = connectorName + '<' + connIDStr + '>';

        LogManager.logInfo(LogConstants.CTX_CONNECTOR, DQPPlugin.Util.getString("ConnectorManagerImpl.Initializing_connector", connectorName)); //$NON-NLS-1$

        this.setTransactionService((TransactionService) env.findService(DQPServiceNames.TRANSACTION_SERVICE));

        // Set the class name for the connector class
        String connectorClassString = props.getProperty(ConnectorPropertyNames.CONNECTOR_CLASS);
        if ( connectorClassString == null || connectorClassString.trim().length() == 0 ) {             
            throw new ApplicationLifecycleException(DQPPlugin.Util.getString("Missing_required_property", new Object[]{ConnectorPropertyNames.CONNECTOR_CLASS, connectorName})); //$NON-NLS-1$
        }

        int maxThreads = PropertiesUtils.getIntProperty(props, ConnectorPropertyNames.MAX_THREADS, DEFAULT_MAX_PROCESSOR_THREADS);
        int threadTTL = PropertiesUtils.getIntProperty(props, ConnectorPropertyNames.THREAD_TTL, DEFAULT_PROCESSOR_TREAD_TTL);

        connectorWorkerPool = WorkerPoolFactory.newWorkerPool(connectorName, maxThreads, threadTTL);

        // Create the Connector env
        Properties clonedProps = PropertiesUtils.clone(props);
        ConnectorEnvironment connectorEnv = new ConnectorEnvironmentImpl(clonedProps, new DefaultConnectorLogger(connectorID), env, connectorWorkerPool);

        // Get the metadata service
        this.metadataService = (MetadataService) env.findService(DQPServiceNames.METADATA_SERVICE);
        if ( this.metadataService == null ) {
            throw new ApplicationLifecycleException(DQPPlugin.Util.getString("Failed_to_find_service", new Object[]{DQPServiceNames.METADATA_SERVICE, connectorName})); //$NON-NLS-1$
        }

        this.tracker = (TrackingService) env.findService(DQPServiceNames.TRACKING_SERVICE);

        this.maxResultRows = PropertiesUtils.getIntProperty(props, ConnectorPropertyNames.MAX_RESULT_ROWS, 0);
        this.exceptionOnMaxRows = PropertiesUtils.getBooleanProperty(props, ConnectorPropertyNames.EXCEPTION_ON_MAX_ROWS, false);
    	this.synchWorkers = PropertiesUtils.getBooleanProperty(props, ConnectorPropertyNames.SYNCH_WORKERS, true);

        // Initialize and start the connector
        initStartConnector(connectorName, connectorEnv);

        this.started = true;
    }
    
    /**
     * Initialize and start the connector.
     * @param env
     * @throws ApplicationLifecycleException
     */
    private void initStartConnector(String connectorName, ConnectorEnvironment env) throws ApplicationLifecycleException {
        String connectorClassName = env.getProperties().getProperty(ConnectorPropertyNames.CONNECTOR_CLASS);
        if(classloader == null){
            classloader = getClass().getClassLoader();
        } else {
        	env.getProperties().setProperty(ConnectorPropertyNames.USING_CUSTOM_CLASSLOADER, Boolean.TRUE.toString());
        }
        Thread currentThread = Thread.currentThread();
        ClassLoader threadContextLoader = currentThread.getContextClassLoader();
        try {
        	currentThread.setContextClassLoader(classloader);
        	Connector c;
			try {
				c = (Connector)ReflectionHelper.create(connectorClassName, null, classloader);
			} catch (MetaMatrixCoreException e) {
	            throw new ApplicationLifecycleException(e, DQPPlugin.Util.getString("failed_find_Connector_class", connectorClassName)); //$NON-NLS-1$
			}
            if(c instanceof XAConnector){
            	this.isXa = true;
                if (this.getTransactionService() == null) {                    
                    throw new ApplicationLifecycleException(DQPPlugin.Util.getString("no_txn_manager", connectorName)); //$NON-NLS-1$
                }
            }
            if (this.synchWorkers) {
                SynchronousWorkers synchWorkerAnnotation = (SynchronousWorkers) c.getClass().getAnnotation(SynchronousWorkers.class);
            	if (synchWorkerAnnotation != null) {
            		this.synchWorkers = synchWorkerAnnotation.enabled();
            	}
            }
        	c = wrapPooledConnector(c, env);
            if (c instanceof ConnectorWrapper) {
            	this.connector = (ConnectorWrapper)c;
            } else {
            	this.connector = new ConnectorWrapper(c);
            }
            this.connector.start(env);
            if (this.isXa) {
                if (this.connector.supportsSingleIdentity()) {
                	// add this connector as the recovery source
	                TransactionServer ts = this.getTransactionService().getTransactionServer(); 
	                ts.registerRecoverySource(connectorName, new TransactionProvider.XAConnectionSource() {
	                	XAConnection conn = null;
	                	
	                	@Override
	                	public XAResource getXAResource() throws SQLException {
	                		if (conn == null) {
	                			try {
									conn = ((XAConnector)connector).getXAConnection(null, null);
								} catch (ConnectorException e) {
									throw new SQLException(e);
								}
	                		}
	                		try {
								return conn.getXAResource();
							} catch (ConnectorException e) {
								throw new SQLException(e);
							}
	                	}
	                	
	                	@Override
	                	public void close() {
	                		if (conn != null) {
	                			conn.close();
	                		}
	                	}
	                });
                } else {
                	LogManager.logWarning(LogConstants.CTX_CONNECTOR, DQPPlugin.Util.getString("ConnectorManager.cannot_add_to_recovery", this.getName())); //$NON-NLS-1$	                
                }
            }
        } catch (ConnectorException e) {
            throw new ApplicationLifecycleException(e, DQPPlugin.Util.getString("failed_start_Connector", new Object[] {this.getConnectorID(), e.getMessage()})); //$NON-NLS-1$
        } finally {
        	currentThread.setContextClassLoader(threadContextLoader);
        }
    }
    
    private Connector wrapPooledConnector(Connector c, ConnectorEnvironment connectorEnv) {
    	//the pooling annotation overrides the connector binding
        ConnectionPooling connectionPooling = (ConnectionPooling) c.getClass().getAnnotation(ConnectionPooling.class);
    	boolean connectionPoolPropertyEnabled = PropertiesUtils.getBooleanProperty(connectorEnv.getProperties(), ConnectorPropertyNames.CONNECTION_POOL_ENABLED, true);
    	boolean propertySet = connectorEnv.getProperties().contains(ConnectorPropertyNames.CONNECTION_POOL_ENABLED);
    	boolean poolingEnabled = false;
        if (propertySet) {
        	poolingEnabled = connectionPoolPropertyEnabled && (connectionPooling == null || connectionPooling.enabled());
        } else {
        	poolingEnabled = connectionPooling != null && connectionPooling.enabled();
        }
        if (poolingEnabled) {
           	LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Automatic connection pooling was enabled for connector " + getName()); //$NON-NLS-1$
        	if (!this.synchWorkers) {
            	LogManager.logWarning(LogConstants.CTX_CONNECTOR, DQPPlugin.Util.getString("ConnectorManager.asynch_worker_warning", ConnectorPropertyNames.SYNCH_WORKERS)); //$NON-NLS-1$	                
        	}
        	return new PooledConnector(c);
        }         
        return c;
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

            if(this.isXa){
                if (this.getTransactionService() != null) {
                    TransactionServer ts = this.getTransactionService().getTransactionServer();
                    ts.removeRecoverySource(connectorName);
                }
            }
            
            Thread currentThread = Thread.currentThread();
            ClassLoader threadContextLoader = currentThread.getContextClassLoader();
            try {
                currentThread.setContextClassLoader(classloader);
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
    public Collection<WorkerPoolStats> getQueueStatistics() {
        if ( this.connectorWorkerPool == null ) {
            return Collections.emptyList();
        }
        return Arrays.asList(connectorWorkerPool.getStats());
    }

    /**
     * Returns a QueueStats object that represent the queue in
     * this service.
     * If there is no queue with the given name, an empty Collection is returned.
     */
    public Collection<WorkerPoolStats> getQueueStatistics(String name) {
        if ( connectorID == null ||
             !name.equalsIgnoreCase(connectorID.getID()) ||
             connectorWorkerPool == null ) {
            return Collections.emptyList();
        }
        return Arrays.asList(connectorWorkerPool.getStats());
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
    ConnectorWrapper getConnector() {
        return this.connector;
    }
    
    void setConnector(ConnectorWrapper connector) {
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
	
	public boolean isXa() {
		return isXa;
	}
	
	public void setClassloader(ClassLoader classloader) {
		this.classloader = classloader;
	}
	
	/**
	 * Overloads the connector capabilities with one defined in the connector binding properties
	 */
    static final class CapabilitesOverloader implements InvocationHandler {
    	ConnectorCapabilities caps; 
    	Properties properties;
    	
    	CapabilitesOverloader(ConnectorCapabilities caps, Properties properties){
    		this.caps = caps;
    		this.properties = properties;
    	}
    	
		@Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			String value = this.properties.getProperty(method.getName());
			if (value == null || value.trim().length() == 0 || (args != null && args.length != 0)) {
				return method.invoke(this.caps, args);
			}
			return StringUtil.valueOf(value, method.getReturnType());
		}
	}
}
