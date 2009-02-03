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

package com.metamatrix.dqp.internal.pooling.connector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.data.DataPlugin;
import com.metamatrix.data.api.Connection;
import com.metamatrix.data.api.SecurityContext;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.monitor.AliveStatus;
import com.metamatrix.data.monitor.ConnectionStatus;
import com.metamatrix.data.pool.ConnectorIdentity;
import com.metamatrix.data.pool.SingleIdentity;
import com.metamatrix.data.pool.PoolAwareConnection;
import com.metamatrix.data.xa.api.TransactionContext;
import com.metamatrix.data.xa.api.XAConnector;
import com.metamatrix.dqp.internal.datamgr.impl.ConnectorWrapper;
import com.metamatrix.dqp.util.LogConstants;

public class ConnectionPool {
	
    /**
     * This property is used to specify the length of time between JDBC Source test connections
     * How often (in seconds) to test that the data source is available by establishing a new connection.  Default to 600 seconds.
     */
    public static final String SOURCE_CONNECTION_TEST_INTERVAL = "SourceConnectionTestInterval"; //$NON-NLS-1$

    /**
     * Maximum connections for this pool. Default to 0, which means there is no limit.
     */
    public static final String MAX_CONNECTIONS = "com.metamatrix.data.pool.max_connections"; //$NON-NLS-1$

    /**
     * Maximum connection for each ConnectorIdentity. Default to 0, which means there is no limit.
     */
    public static final String MAX_CONNECTIONS_FOR_EACH_ID = "com.metamatrix.data.pool.max_connections_for_each_id"; //$NON-NLS-1$

    /**
     * Idle time of the connection before it should be closed in seconds. Default to 60 seconds.
     */
    public static final String LIVE_AND_UNUSED_TIME = "com.metamatrix.data.pool.live_and_unused_time"; //$NON-NLS-1$

    /**
     * Time to wait if the connection is not available in milliseconds.  Default to 2 seconds.
     */
    public static final String WAIT_FOR_SOURCE_TIME = "com.metamatrix.data.pool.wait_for_source_time"; //$NON-NLS-1$

    /**
     * Interval for running the cleaning thread in seconds. Default to 60 seconds.
     */
    public static final String CLEANING_INTERVAL = "com.metamatrix.data.pool.cleaning_interval"; //$NON-NLS-1$

    /**
     * Whether to enable pool shrinking.  Default to true.
     */
    public static final String ENABLE_SHRINKING = "com.metamatrix.data.pool.enable_shrinking"; //$NON-NLS-1$
    
    private static final String CTX_CONNECTOR = LogConstants.CTX_CONNECTOR;

    static final int DEFAULT_MAX_CONNECTION = 20;
    static final int DEFAULT_MAX_CONNECTIONS_FOR_EACH_ID = 20;
    static final int DEFAULT_LIVE_AND_UNUSED_TIME = 60;
    static final int DEFAULT_WAIT_FOR_SOURCE_TIME = 120000;
    static final int DEFAULT_CLEANING_INTERVAL = 60;
    static final boolean DEFAULT_ENABLE_SHRINKING = true; 
    static final int DEFAULT_SOURCE_CONNECTION_TEST_INTERVAL = 600;  //10 minutes 

    private static class ConnectionsForId {
        LinkedList<ConnectionWrapper> used = new LinkedList<ConnectionWrapper>();
        LinkedList<ConnectionWrapper> unused = new LinkedList<ConnectionWrapper>();
        Semaphore idSemaphore;
    }
    
    private int maxConnections = DEFAULT_MAX_CONNECTION;
    private int maxConnectionsForEachID = DEFAULT_MAX_CONNECTIONS_FOR_EACH_ID;
    private int liveAndUnusedTime = DEFAULT_LIVE_AND_UNUSED_TIME;
    private int waitForSourceTime = DEFAULT_WAIT_FOR_SOURCE_TIME;
    private int cleaningInterval = DEFAULT_CLEANING_INTERVAL;
    private boolean enableShrinking = DEFAULT_ENABLE_SHRINKING;
    
    
    /**How often (in ms) to test that the data source is available by establishing a new connection.*/
    private int testConnectInterval;
    private ConnectorWrapper connectionFactory;

    private Map<ConnectorIdentity, ConnectionsForId> idConnections = new HashMap<ConnectorIdentity, ConnectionsForId>();
    private Map<ConnectionWrapper, ConnectorIdentity> reverseIdConnections = new IdentityHashMap<ConnectionWrapper, ConnectorIdentity>();

    private Timer cleaningThread;
    
    private Object lock = new Object();
    
    private Semaphore poolSemaphore;

    private volatile int totalConnectionCount;

    private volatile boolean shuttingDownPool;

    protected volatile boolean lastConnectionAttemptFailed = false;

    /**Exception received during last failed connection attempt*/
    private volatile Exception lastConnectionAttemptException = null;

    /**Time of last failed connection attempt*/
    private volatile Date lastConnectionAttemptDate = null;
    
    /**
     * Construct the connection pool with a connection factory
     *
     * @param connectionFactory The factory for creating connections and ConnectorIdentity objects.
     */
    public ConnectionPool(ConnectorWrapper connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    /**
     * Initialize the connection pool. Default value will be used if the property
     * is not set.
     *
     * @param poolProperties Properties of the pool as defined in this class. May be empty, but may not be null.
     * @throws ConnectionPoolException if the connection pool fails to initialize.
     */
    public void initialize(Properties poolProperties) throws ConnectionPoolException {
        ArgCheck.isNotNull(poolProperties);

        maxConnections = PropertiesUtils.getIntProperty(poolProperties, MAX_CONNECTIONS, DEFAULT_MAX_CONNECTION);
        if (maxConnections < 1) {
            throw new ConnectionPoolException(DataPlugin.Util.getString("ConnectionPool.The_conn_value", maxConnections)); //$NON-NLS-1$
        }
        
        poolSemaphore = new Semaphore(maxConnections, true);

        maxConnectionsForEachID = PropertiesUtils.getIntProperty(poolProperties, MAX_CONNECTIONS_FOR_EACH_ID, DEFAULT_MAX_CONNECTIONS_FOR_EACH_ID);
        if (maxConnectionsForEachID < 1) {
            throw new ConnectionPoolException(DataPlugin.Util.getString("ConnectionPool.The_conn_value", maxConnectionsForEachID)); //$NON-NLS-1$
        }                

        liveAndUnusedTime = PropertiesUtils.getIntProperty(poolProperties, LIVE_AND_UNUSED_TIME, DEFAULT_LIVE_AND_UNUSED_TIME) * 1000;
        waitForSourceTime = PropertiesUtils.getIntProperty(poolProperties, WAIT_FOR_SOURCE_TIME, DEFAULT_WAIT_FOR_SOURCE_TIME);
        cleaningInterval = PropertiesUtils.getIntProperty(poolProperties, CLEANING_INTERVAL, DEFAULT_CLEANING_INTERVAL) * 1000;
        enableShrinking = PropertiesUtils.getBooleanProperty(poolProperties, ENABLE_SHRINKING, DEFAULT_ENABLE_SHRINKING);
        testConnectInterval = PropertiesUtils.getIntProperty(poolProperties, SOURCE_CONNECTION_TEST_INTERVAL, DEFAULT_SOURCE_CONNECTION_TEST_INTERVAL);
        
        if (enableShrinking && !this.shuttingDownPool) {
        	this.cleaningThread = new Timer("ConnectionPoolCleaningThread", true); //$NON-NLS-1$
        	cleaningThread.schedule(new TimerTask() {
				@Override
				public void run() {
					ConnectionPool.this.cleanUp(false);
				}
        	}, cleaningInterval, cleaningInterval);
        }

        LogManager.logInfo(CTX_CONNECTOR, DataPlugin.Util.getString("ConnectionPool.Connection_pool_created_1")); //$NON-NLS-1$
    }
    
    private long timeRemaining(long startTime) {
        return startTime + this.waitForSourceTime - System.currentTimeMillis();
    }
    
    public ConnectionWrapper obtain(SecurityContext securityContext) throws ConnectionPoolException {
    	return this.obtain(securityContext, null, false);
    }

    /**
     * Return a connection from the connection pool.
     *
     * @return Connection from the pool.
     * @throws ConnectionPoolException if there is any error occurred.
     */
    public ConnectionWrapper obtain(SecurityContext securityContext, TransactionContext transactionContext, boolean xa) throws ConnectionPoolException {
    	if ( shuttingDownPool ) {
            throw new ConnectionPoolException(DataPlugin.Util.getString("ConnectionPool.No_connection_pool_available._8")); //$NON-NLS-1$
        }

        long startTime = System.currentTimeMillis();

        ConnectorIdentity id = null;
        if (securityContext != null) {
        	id = securityContext.getConnectorIdentity();
        }
        if (id == null) {
        	id = new SingleIdentity(securityContext);
        }
                
        LogManager.logTrace(CTX_CONNECTOR, new Object[] {"Obtaining connection for id", id}); //$NON-NLS-1$ 

        ConnectionsForId connLists = null;

        synchronized (this.lock) {
            connLists = (ConnectionsForId) this.idConnections.get(id);
            if ( connLists == null ) {
                connLists = new ConnectionsForId();
                if (this.maxConnectionsForEachID < this.maxConnections) {
                    connLists.idSemaphore = new Semaphore(this.maxConnectionsForEachID, true);
                }
                this.idConnections.put(id, connLists);
            }
        }

        boolean poolLockHeld = false;
        boolean idLockHeld = false;
        boolean success = false;

        try {
            if (!poolLockHeld && !this.poolSemaphore.tryAcquire(this.timeRemaining(startTime), TimeUnit.MILLISECONDS)) {
                throw new ConnectionPoolException(DataPlugin.Util.getString("ConnectionPool.ExceededWait", id, new Integer(this.waitForSourceTime))); //$NON-NLS-1$
            }
            poolLockHeld = true;
            
            if (connLists.idSemaphore != null && !idLockHeld && !connLists.idSemaphore.tryAcquire(this.timeRemaining(startTime), TimeUnit.MILLISECONDS)) {
                throw new ConnectionPoolException(DataPlugin.Util.getString("ConnectionPool.ExceededConnections", id, new Integer(this.maxConnectionsForEachID) )); //$NON-NLS-1$
            }
            idLockHeld = true;
            
            //try to reuse my existing at least once
            do {
                synchronized (connLists) {
                    if (connLists.unused.isEmpty()) {
                    	break;
                    }
                        
                    ConnectionWrapper conn = connLists.unused.removeFirst();
                    if ( conn.isAlive() ) { 
                        LogManager.logTrace(CTX_CONNECTOR,  new Object[] {"Existing connection leased for", id}); //$NON-NLS-1$
                        connLists.used.addLast(conn);
                        success = true;
                        return conn;
                    }
                    closeSourceConnection(conn, id);
                }
            } while (timeRemaining(startTime) > 0);

            //at this point we've made a best effort to ensure we've used existing connections.
            //allow a new connection to be created outside of locking.
            ConnectionWrapper connection = createConnection(securityContext, transactionContext, xa);
            
            int idSize = 0;
            
            synchronized (connLists) {
                connLists.used.addLast(connection);
                idSize = connLists.used.size() + connLists.unused.size();
            }
            
            updateStateWithNewConnection(id, connection, idSize);

            success = true;
            return connection;
        } catch (InterruptedException err) {
            throw new ConnectionPoolException(err);
        } finally {
            if (!success) {
                if (idLockHeld && connLists.idSemaphore != null) {
                    connLists.idSemaphore.release();
                }
                if (poolLockHeld) {
                    poolSemaphore.release();
                }
            }
        }
    }

    private void updateStateWithNewConnection(ConnectorIdentity id,
                                              ConnectionWrapper connection,
                                              int idSize) {
        Collection ids = null;
        
        synchronized (this.lock) {
            this.reverseIdConnections.put(connection, id);
            this.totalConnectionCount++;
            
            if (this.totalConnectionCount > this.maxConnections) {
                ids = new ArrayList(this.idConnections.values());
            }
            
            // Log warnings if we hit the max connection count or the max count per pool - if both
            // are hit, we will only log the max connection count for the pool as that is more likely
            // to make sense to users, esp. users using a standard SingleIdentity pool
            if(this.totalConnectionCount == this.maxConnections) {
               LogManager.logWarning(CTX_CONNECTOR, DataPlugin.Util.getString("ConnectionPool.Max_conn_reached")); //$NON-NLS-1$
            } else if(idSize == this.maxConnectionsForEachID) {
               LogManager.logWarning(CTX_CONNECTOR, DataPlugin.Util.getString("ConnectionPool.Max_conn_per_id_reached")); //$NON-NLS-1$
            }
        }
  
        //release any unused connection
        //TODO: this search is biased and slow
        if (ids != null) { 
            for (Iterator i = ids.iterator(); i.hasNext() && this.totalConnectionCount > this.maxConnections;) {
                ConnectionsForId connsForId = (ConnectionsForId)i.next();
                synchronized (connsForId) {
                    if (connsForId.unused.isEmpty()) {
                        continue;
                    }
                    ConnectionWrapper conn = (ConnectionWrapper) connsForId.unused.removeFirst();
                    closeSourceConnection(conn, id);
                    break;
                }
            }
        }
    }

    private ConnectionWrapper createConnection(SecurityContext id, TransactionContext transactionContext, boolean xa) throws ConnectionPoolException {
    	ConnectionWrapper sourceConnection = null;
        try {
        	Connection connection = null;
        	if (xa) {
        		connection = ((XAConnector)this.connectionFactory.getActualConnector()).getXAConnection(id, transactionContext);
        	} else {
        		connection = this.connectionFactory.getActualConnector().getConnection(id);
        	}
        	sourceConnection = new ConnectionWrapper(connection, this, testConnectInterval);
        	LogManager.logTrace(CTX_CONNECTOR, new Object[] {"Connection pool created a connection for", id}); //$NON-NLS-1$
        } catch (ConnectorException e) {
        	lastConnectionAttemptFailed = true;
            lastConnectionAttemptException = e;
            lastConnectionAttemptDate = new Date();
            throw new ConnectionPoolException(e);
        }
        lastConnectionAttemptFailed = false;
        return sourceConnection;
    }

    void release(ConnectionWrapper connection, boolean forceClose) {
        ConnectionsForId connLists = null;
        ConnectorIdentity id = null;
        synchronized (this.lock) {
            id = (ConnectorIdentity) this.reverseIdConnections.get(connection);
            connLists = (ConnectionsForId) this.idConnections.get(id);
        }
            
        if (connLists == null) {
        	return;
        }
        
        synchronized (connLists) {
            //release it only if there is one.
            //If the same connection is to be released twice, just ignore
            if ( connLists.used.remove(connection)) {
                LogManager.logTrace(CTX_CONNECTOR, new Object[] {"ConnectionPool(release) connection released:", id}); //$NON-NLS-1$
                if ( forceClose || shuttingDownPool ) {
                    closeSourceConnection(connection, id);
                } else {
                    /*TODO: this is problematic - scenario:
                     *thread 1 lease
                     *thread 1 release
                     *thread 2 lease same connection
                     *thread 1 redundant release
    				 *thread 3 leases same connection at the same time as 2
                     */
                	connLists.unused.addLast(connection);
                }
                if (connLists.idSemaphore != null) {
                    connLists.idSemaphore.release();
                }
                poolSemaphore.release();
            }
        }
    }
    
    /**
     * Check the status of connections in this pool.
     * The pool is operational if it has at least one live
     * connection available to it.
     *
     * @return AliveStatus.ALIVE if there are any connections in use, or any live unused connections.
     * <p>AliveStatus.DEAD if there are no live connections, and the connection pool cannot create a new connection.
     * <p>AliveStatus.UNKNOWN if there are no live connections, and we don't have the ability to test getting a new connection. 
     */
    public ConnectionStatus getStatus() {
    	AliveStatus poolStatus;

        Collection values = null;
        synchronized (this.lock) {
            values = new LinkedList(this.idConnections.values());
        }
        
        poolStatus = checkStatusOfUsedConnections(values);

        if (poolStatus.equals(AliveStatus.UNKNOWN)) {
            poolStatus = checkStatusOfUnusedConnections(values);
        }

        if (poolStatus.equals(AliveStatus.UNKNOWN)) {
            poolStatus = testGetConnection();
        }

        if (poolStatus.equals(AliveStatus.UNKNOWN) && lastConnectionAttemptFailed) {
            poolStatus = AliveStatus.DEAD;
        }
        
        //never set the status of "UserIdentity" connectors to DEAD.
        if (poolStatus.equals(AliveStatus.DEAD) && (!connectionFactory.supportsSingleIdentity())) {
            poolStatus = AliveStatus.UNKNOWN;
        }
        
        return new ConnectionStatus(poolStatus, getTotalConnectionCount(), lastConnectionAttemptException, 
            lastConnectionAttemptDate);
    }

    private AliveStatus checkStatusOfUsedConnections(Collection connectionInfos) {
        // Check size of all used pools.  If any > 0, pool is alive.
        // Note that this only proves pool is alive for one ConnectorIdentity.
        
        for (Iterator i = connectionInfos.iterator(); i.hasNext(); ) {
            ConnectionsForId connLists = (ConnectionsForId) i.next();
            
            synchronized (connLists) {
                // check size of this used conn list for one identity
                if ( connLists.used.size() > 0 ) {
                    return AliveStatus.ALIVE;                
                }
            }
        }
        return AliveStatus.UNKNOWN;
    }

    private AliveStatus checkStatusOfUnusedConnections(Collection connectionInfos) {
        // If we're here, we haven't found a live connection yet.
        // Must query unused connections.
        for (Iterator i = connectionInfos.iterator(); i.hasNext(); ) {
            ConnectionsForId connLists = (ConnectionsForId)i.next();

            synchronized (connLists) {
            // check size of this used conn list for one identity
                Iterator unusedConnItr = connLists.unused.iterator();
                while (unusedConnItr.hasNext()) {
                    if (((ConnectionWrapper)unusedConnItr.next()).isAlive()) {
                        return AliveStatus.ALIVE;
                    } 
                    //TODO: remove connection
                }
            }
        }
        return AliveStatus.UNKNOWN;
    }
    
    /**
     * Test datasource availability by getting a connection.
     * @return
     * @since 4.3
     */
    private AliveStatus testGetConnection() {
        if (connectionFactory.supportsSingleIdentity()) { 
            try {
                ConnectionWrapper connection = obtain(null);
                boolean alive = connection.isAlive();
                release(connection, !alive);
                return (alive ? AliveStatus.ALIVE : AliveStatus.DEAD);
            } catch (ConnectionPoolException e) {
                return AliveStatus.DEAD;
            }   
        }
        
        return AliveStatus.UNKNOWN;
    }
    
    /**
     * Shut down the pool.
     */
    public void shutDown() {
        //log that we're shutting down the pool
        if (LogManager.isMessageToBeRecorded(CTX_CONNECTOR, MessageLevel.TRACE)) {
            LogManager.logTrace(CTX_CONNECTOR, DataPlugin.Util.getString("ConnectionPool.Shut_down")); //$NON-NLS-1$
        }
        
        shuttingDownPool = true;
        
        //close cleaning thread
        if (this.cleaningThread != null) {
	        this.cleaningThread.cancel();
    	}        
        
        this.cleanUp(true);
    }

    protected void cleanUp(boolean forceClose) {        
        Map values = null;
        synchronized (this.lock) {
            values = new HashMap(this.idConnections);
        }
        
        for (Iterator i = values.entrySet().iterator(); i.hasNext();) {
        	Map.Entry entry = (Map.Entry)i.next();
            ConnectionsForId connLists = (ConnectionsForId)entry.getValue();

            synchronized (connLists) {
                for ( Iterator unusedIter = connLists.unused.iterator(); unusedIter.hasNext(); ) {
                    ConnectionWrapper unusedConnection = (ConnectionWrapper) unusedIter.next();
                    if (forceClose || (enableShrinking && System.currentTimeMillis() - unusedConnection.getTimeReturnedToPool() >= this.liveAndUnusedTime)
                            || !unusedConnection.isAlive() ) {
                        unusedIter.remove();
                        
                        closeSourceConnection(unusedConnection, (ConnectorIdentity)entry.getKey());
                    }
                }
            }
        }
    }
    
    /**
     * @param connection
     * @return true if we succeeded in closing the connection
     * @throws Exception
     * @since 4.3
     */
    private void closeSourceConnection(ConnectionWrapper connection, ConnectorIdentity id) {
    	synchronized (this.lock) {
            this.totalConnectionCount--;
            this.reverseIdConnections.remove(connection);
        }
    	try {
            connection.close();
        
            //log that we removed a connection
            if (LogManager.isMessageToBeRecorded(CTX_CONNECTOR, MessageLevel.TRACE)) {
                LogManager.logTrace(CTX_CONNECTOR, DataPlugin.Util.getString("ConnectionPool.Removed_conn", id)); //$NON-NLS-1$
            }
            
        } catch (Exception e) {
            LogManager.logError(CTX_CONNECTOR, DataPlugin.Util.getString("ConnectionPool.Failed_close_a_connection__2", id)); //$NON-NLS-1$
        }
    }
        
    //for testing purpose
    final List<ConnectionWrapper> getUsedConnections(PoolAwareConnection connection) {
    	ConnectorIdentity id = null;
    	ConnectionsForId connLists = null;
    	synchronized (this.lock) {
            id = (ConnectorIdentity) this.reverseIdConnections.get(connection);
            connLists = (ConnectionsForId) this.idConnections.get(id);
		}
        if ( connLists != null ) {
        	synchronized (connLists) {
            	return new ArrayList<ConnectionWrapper>(connLists.used);
			}
        }
        return Collections.emptyList();
    }

    //for testing purpose
    final List<ConnectionWrapper> getUnusedConnections(PoolAwareConnection connection) {
    	ConnectorIdentity id = null;
    	ConnectionsForId connLists = null;
    	synchronized (this.lock) {
            id = (ConnectorIdentity) this.reverseIdConnections.get(connection);
            connLists = (ConnectionsForId) this.idConnections.get(id);
		}
        if ( connLists != null ) {
        	synchronized (connLists) {
            	return new ArrayList<ConnectionWrapper>(connLists.unused);
			}
        }
        return Collections.emptyList();
    }

    //for testing purpose
    int getTotalConnectionCount() {
        return this.totalConnectionCount;
    }

}