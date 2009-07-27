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
package org.teiid.dqp.internal.datamgr.impl;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import javax.transaction.xa.XAResource;

import org.teiid.connector.api.Connection;
import org.teiid.connector.api.Connector;
import org.teiid.connector.api.ConnectorCapabilities;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorPropertyNames;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.ConnectorAnnotations.ConnectionPooling;
import org.teiid.connector.api.ConnectorAnnotations.SynchronousWorkers;
import org.teiid.connector.metadata.runtime.ConnectorMetadata;
import org.teiid.connector.metadata.runtime.MetadataFactory;
import org.teiid.connector.xa.api.XAConnection;
import org.teiid.connector.xa.api.XAConnector;
import org.teiid.dqp.internal.cache.DQPContextCache;
import org.teiid.dqp.internal.cache.ResultSetCache;
import org.teiid.dqp.internal.datamgr.CapabilitiesConverter;
import org.teiid.dqp.internal.pooling.connector.PooledConnector;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.dqp.internal.transaction.TransactionProvider;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.application.ApplicationService;
import com.metamatrix.common.application.ClassLoaderManager;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.common.comm.api.ResultsReceiver;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.queue.WorkerPool;
import com.metamatrix.common.queue.WorkerPoolFactory;
import com.metamatrix.common.queue.WorkerPoolStats;
import com.metamatrix.common.stats.ConnectionPoolStats;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.core.util.ReflectionHelper;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.dqp.DQPPlugin;
import com.metamatrix.dqp.ResourceFinder;
import com.metamatrix.dqp.internal.datamgr.ConnectorID;
import com.metamatrix.dqp.message.AtomicRequestID;
import com.metamatrix.dqp.message.AtomicRequestMessage;
import com.metamatrix.dqp.message.AtomicResultsMessage;
import com.metamatrix.dqp.message.RequestID;
import com.metamatrix.dqp.service.BufferService;
import com.metamatrix.dqp.service.CommandLogMessage;
import com.metamatrix.dqp.service.ConnectorStatus;
import com.metamatrix.dqp.service.DQPServiceNames;
import com.metamatrix.dqp.service.MetadataService;
import com.metamatrix.dqp.service.TransactionService;
import com.metamatrix.dqp.util.LogConstants;
import com.metamatrix.query.optimizer.capabilities.BasicSourceCapabilities;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities.Scope;
import com.metamatrix.query.sql.lang.Command;

/**
 * The <code>ConnectorManager</code> manages a {@link org.teiid.connector.basic.BasicConnector Connector}
 * and its associated workers' state.
 */
public class ConnectorManager implements ApplicationService {

	public static final int DEFAULT_MAX_THREADS = 20;
    private static final String DEFAULT_MAX_RESULTSET_CACHE_SIZE = "20"; //$NON-NLS-1$
    private static final String DEFAULT_MAX_RESULTSET_CACHE_AGE = "3600000"; //$NON-NLS-1$

    //state constructed in start
    private ConnectorWrapper connector;
    private ConnectorID connectorID;
    private WorkerPool connectorWorkerPool;
    private ResultSetCache rsCache;
    private ConnectorWorkItemFactory workItemFactory;
	private String connectorName;
    private int maxResultRows;
    private boolean exceptionOnMaxRows = true;
    private boolean synchWorkers;
    private boolean isXa;
    private boolean isImmutable;
    
    private volatile ConnectorStatus state = ConnectorStatus.NOT_INITIALIZED;

    //services acquired in start
    private MetadataService metadataService;
    private TransactionService transactionService;
    private BufferService bufferService;
    
    private ClassLoaderManager clManager;

    // known requests
    private ConcurrentHashMap<AtomicRequestID, ConnectorWorkItem> requestStates = new ConcurrentHashMap<AtomicRequestID, ConnectorWorkItem>();

    private Properties props;
	private ClassLoader classloader;
    
    public void initialize(Properties props) {
    	this.props = props;
    	this.isImmutable = PropertiesUtils.getBooleanProperty(props, ConnectorPropertyNames.IS_IMMUTABLE, false);
    }
    
    public boolean isImmutable() {
        return isImmutable;
    }

    public ClassLoader getClassloader() {
		return classloader;
	}
    
    public ConnectorMetadata getMetadata(String modelName, Properties importProperties) throws ConnectorException {
    	MetadataFactory factory;
		try {
			factory = new MetadataFactory(modelName, this.metadataService.getBuiltinDatatypes(), importProperties);
		} catch (MetaMatrixComponentException e) {
			throw new ConnectorException(e);
		}
		Thread currentThread = Thread.currentThread();
		ClassLoader threadContextLoader = currentThread.getContextClassLoader();
		try {
			currentThread.setContextClassLoader(classloader);
			this.connector.getConnectorMetadata(factory);
		} finally {
			currentThread.setContextClassLoader(threadContextLoader);
		}
		return factory;
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
            	ExecutionContextImpl context = new ExecutionContextImpl(
                        message.getVdbName(),
                        message.getVdbVersion(),
                        message.getUserName(),
                        message.getTrustedPayload(),
                        executionPayload,
                        "capabilities-request", //$NON-NLS-1$
                        connectorID.getID(), 
                        requestID.toString(), 
                        "capabilities-request", "0"); //$NON-NLS-1$ //$NON-NLS-2$ 
            	
            	context.setContextCache(getContextCache());

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

    	ConnectorWorkItem item = workItemFactory.createWorkItem(message, receiver);
    	
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
        this.connectorWorkerPool.schedule(new Runnable() {
        	@Override
        	public void run() {
        		state.requestMore();
        	}
        }, delay, TimeUnit.MILLISECONDS);        
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
    
    public ConnectorStatus getStatus() {
    	ConnectorWrapper connectorWrapper = this.connector;
    	ConnectorStatus result = this.state;
    	if (result != ConnectorStatus.OPEN) {
    		return result;
    	}
        ClassLoader contextloader = Thread.currentThread().getContextClassLoader();
        try {
        	Thread.currentThread().setContextClassLoader(classloader);
            return connectorWrapper.getStatus();
        } finally {
        	Thread.currentThread().setContextClassLoader(contextloader);
        }
    }
    
    /**
     * initialize this <code>ConnectorManager</code>.
     */
    public synchronized void start(ApplicationEnvironment env) throws ApplicationLifecycleException {
    	if (this.state != ConnectorStatus.NOT_INITIALIZED) {
    		return;
    	}
    	this.state = ConnectorStatus.INIT_FAILED;
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

        int maxThreads = PropertiesUtils.getIntProperty(props, ConnectorPropertyNames.MAX_CONNECTIONS, DEFAULT_MAX_THREADS);

        connectorWorkerPool = WorkerPoolFactory.newWorkerPool(connectorName, maxThreads);

        // Create the Connector env
        Properties clonedProps = new Properties(this.props);
        
        ConnectorEnvironment connectorEnv = new ConnectorEnvironmentImpl(clonedProps, new DefaultConnectorLogger(connectorID), env, connectorWorkerPool);

        // Get the metadata service
        this.metadataService = (MetadataService) env.findService(DQPServiceNames.METADATA_SERVICE);
        if ( this.metadataService == null ) {
            throw new ApplicationLifecycleException(DQPPlugin.Util.getString("Failed_to_find_service", new Object[]{DQPServiceNames.METADATA_SERVICE, connectorName})); //$NON-NLS-1$
        }

        this.maxResultRows = PropertiesUtils.getIntProperty(props, ConnectorPropertyNames.MAX_RESULT_ROWS, 0);
        this.exceptionOnMaxRows = PropertiesUtils.getBooleanProperty(props, ConnectorPropertyNames.EXCEPTION_ON_MAX_ROWS, false);
    	this.synchWorkers = PropertiesUtils.getBooleanProperty(props, ConnectorPropertyNames.SYNCH_WORKERS, true);

     	this.bufferService = (BufferService) env.findService(DQPServiceNames.BUFFER_SERVICE);
        // Initialize and start the connector
        initStartConnector(connectorEnv);
        //check result set cache
        if(PropertiesUtils.getBooleanProperty(props, ConnectorPropertyNames.USE_RESULTSET_CACHE, false)) {
            Properties rsCacheProps = new Properties();
        	rsCacheProps.setProperty(ResultSetCache.RS_CACHE_MAX_SIZE, props.getProperty(ConnectorPropertyNames.MAX_RESULTSET_CACHE_SIZE, DEFAULT_MAX_RESULTSET_CACHE_SIZE)); 
        	rsCacheProps.setProperty(ResultSetCache.RS_CACHE_MAX_AGE, props.getProperty(ConnectorPropertyNames.MAX_RESULTSET_CACHE_AGE, DEFAULT_MAX_RESULTSET_CACHE_AGE)); 
        	rsCacheProps.setProperty(ResultSetCache.RS_CACHE_SCOPE, props.getProperty(ConnectorPropertyNames.RESULTSET_CACHE_SCOPE, ResultSetCache.RS_CACHE_SCOPE_VDB)); 
    		this.rsCache = createResultSetCache(rsCacheProps);
        }
		this.workItemFactory = new ConnectorWorkItemFactory(this, this.rsCache, synchWorkers);
    	this.state = ConnectorStatus.OPEN;
    }
    
	private String buildClasspath(Properties connectorProperties) {
		StringBuilder sb = new StringBuilder();
		appendlasspath(connectorProperties.getProperty(ConnectorPropertyNames.CONNECTOR_CLASSPATH), sb); // this is user defined, could be very specific to the binding
        appendlasspath(connectorProperties.getProperty(ConnectorPropertyNames.CONNECTOR_TYPE_CLASSPATH), sb); // this is system defined; type classpath
        return sb.toString();
	}
	
	private void appendlasspath(String path, StringBuilder builder) {
        if (path != null && path.length() > 0) {
        	builder.append(path);
        	if (!path.endsWith(";")) { //$NON-NLS-1$
        		builder.append(";"); //$NON-NLS-1$
        	}
        }
	} 
	
    /**
     * Initialize and start the connector.
     * @param env
     * @throws ApplicationLifecycleException
     */
    private void initStartConnector(ConnectorEnvironment env) throws ApplicationLifecycleException {
    	
        String connectorClassName = env.getProperties().getProperty(ConnectorPropertyNames.CONNECTOR_CLASS);
        
        String classPath = buildClasspath(env.getProperties());

        Thread currentThread = Thread.currentThread();
        ClassLoader threadContextLoader = currentThread.getContextClassLoader();
        
        if (classPath == null || classPath.trim().length() == 0) {
        	classloader = threadContextLoader;
        } else {
        	env.getProperties().setProperty(ConnectorPropertyNames.USING_CUSTOM_CLASSLOADER, Boolean.TRUE.toString());
            
            boolean postDelegation = PropertiesUtils.getBooleanProperty(env.getProperties(), ConnectorPropertyNames.USE_POST_DELEGATION, false);
            
            LogManager.logInfo(LogConstants.CTX_CONNECTOR, DQPPlugin.Util.getString("ConnectorManager.useClassloader", connectorName, postDelegation, classPath)); //$NON-NLS-1$
            
            if (postDelegation) {
            	this.classloader = this.clManager.getPostDelegationClassLoader(classPath);
            } else {
            	this.classloader = this.clManager.getCommonClassLoader(classPath);
            }
        }
        
        try {
        	currentThread.setContextClassLoader(classloader);
        	Connector c;
			try {
				Object o = ReflectionHelper.create(connectorClassName, null, classloader);
				if (o instanceof Connector) {
					c = (Connector)o;
					this.isXa = PropertiesUtils.getBooleanProperty(env.getProperties(), ConnectorPropertyNames.IS_XA, false);
				} else {
					try {
						Class legacyConnector = classloader.loadClass("com.metamatrix.data.api.Connector"); //$NON-NLS-1$
						if (!legacyConnector.isAssignableFrom(o.getClass())) {
							throw new ApplicationLifecycleException(DQPPlugin.Util.getString("failed_legacy", connectorClassName)); //$NON-NLS-1$
						}
						c = (Connector)ReflectionHelper.create("com.metamatrix.dqp.internal.datamgr.ConnectorWrapper", new Object[] {o}, new Class[] {legacyConnector}, classloader); //$NON-NLS-1$
						this.isXa = classloader.loadClass("com.metamatrix.data.xa.api.XAConnector").isAssignableFrom(o.getClass()); //$NON-NLS-1$
					} catch (ClassNotFoundException e) {
						throw new ApplicationLifecycleException(e, DQPPlugin.Util.getString("failed_legacy", connectorClassName)); //$NON-NLS-1$
					} 
				}
			} catch (MetaMatrixCoreException e) {
	            throw new ApplicationLifecycleException(e, DQPPlugin.Util.getString("failed_find_Connector_class", connectorClassName)); //$NON-NLS-1$
			}
			if (this.isXa) {
	            if(!(c instanceof XAConnector)){
	            	throw new ApplicationLifecycleException(DQPPlugin.Util.getString("non_xa_connector", connectorName)); //$NON-NLS-1$
	            }
                if (this.getTransactionService() == null) {                    
                    throw new ApplicationLifecycleException(DQPPlugin.Util.getString("no_txn_manager", connectorName)); //$NON-NLS-1$
                }
			}
            if (this.synchWorkers) {
                SynchronousWorkers synchWorkerAnnotation = c.getClass().getAnnotation(SynchronousWorkers.class);
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
	                TransactionService ts = this.getTransactionService(); 
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
        ConnectionPooling connectionPooling = c.getClass().getAnnotation(ConnectionPooling.class);
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
    
    protected ResultSetCache createResultSetCache(Properties rsCacheProps) {
		return new ResultSetCache(rsCacheProps, ResourceFinder.getCacheFactory());
	}

    /**
     * Stop this connector.
     */
    public void stop() throws ApplicationLifecycleException {        
        synchronized (this) {
        	if (this.state == ConnectorStatus.CLOSED) {
        		return;
        	}
            this.state= ConnectorStatus.CLOSED;
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
                    TransactionService ts = this.getTransactionService();
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
        String sqlStr = null;
        if(cmdStatus == CommandLogMessage.CMD_STATUS_NEW){
        	Command cmd = qr.getCommand();
            sqlStr = cmd != null ? cmd.toString() : null;
        }
        String userName = qr.getWorkContext().getUserName();
        String transactionID = null;
        if ( qr.isTransactional() ) {
            transactionID = qr.getTransactionContext().getTxnID();
        }
        
        String modelName = qr.getModelName();
        AtomicRequestID id = qr.getAtomicRequestID();
        
        short cmdPoint = cmdStatus == CommandLogMessage.CMD_STATUS_NEW ? CommandLogMessage.CMD_POINT_BEGIN : CommandLogMessage.CMD_POINT_END;
        String principal = userName == null ? "unknown" : userName; //$NON-NLS-1$
        
        CommandLogMessage message = null;
        if (cmdPoint == CommandLogMessage.CMD_POINT_BEGIN) {
            message = new CommandLogMessage(System.currentTimeMillis(), qr.getRequestID().toString(), id.getNodeID(), transactionID, modelName, connectorName, qr.getWorkContext().getConnectionID(), principal, sqlStr, context);
        } 
        else {
            boolean isCancelled = false;
            boolean errorOccurred = false;

            if (cmdStatus == CommandLogMessage.CMD_STATUS_CANCEL) {
                isCancelled = true;
            } else if (cmdStatus == CommandLogMessage.CMD_STATUS_ERROR) {
                errorOccurred = true;
            }
            message = new CommandLogMessage(System.currentTimeMillis(), qr.getRequestID().toString(), id.getNodeID(), transactionID, modelName, connectorName, qr.getWorkContext().getConnectionID(), principal, finalRowCnt, isCancelled, errorOccurred, context);
        }         
        LogManager.log(MessageLevel.INFO, LogConstants.CTX_COMMANDLOGGING, message);
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
    
    DQPContextCache getContextCache() {
     	if (bufferService != null) {
    		return bufferService.getContextCache();
    	}

    	return null;
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
	
	public void setClassLoaderManager(ClassLoaderManager clManager) {
		this.clManager = clManager;
	}
	
	public void setWorkItemFactory(ConnectorWorkItemFactory workItemFactory) {
		this.workItemFactory = workItemFactory;
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

	public Collection<ConnectionPoolStats> getConnectionPoolStats() {
	     if (connector instanceof  PooledConnector) {
    	 	PooledConnector pc = (PooledConnector) connector;
    	 	
    	 	return pc.getConnectionPoolStats();
	     }
	     return Collections.emptyList();
	}
}
