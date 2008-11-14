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
 */
package com.metamatrix.data.pool;

import java.util.Properties;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.data.DataPlugin;
import com.metamatrix.data.api.SecurityContext;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.monitor.AliveStatus;
import com.metamatrix.data.monitor.ConnectionStatus;

/**
 * The connection pool implementation.
 */
public class DisabledConnectionPool extends ConnectionPool {

    /**
     * Duplicated from com.metamatrix.dqp.util.LogConstants.
     * TODO: refactor so this does not need to be duplicated 
     */
    private static final String CTX_CONNECTOR = "CONNECTOR"; //$NON-NLS-1$
    
    /**
     * Construct the connection pool with a connection factory
     *
     * @param connectionFactory The factory for creating connections and ConnectorIdentity objects.
     */
    public DisabledConnectionPool(SourceConnectionFactory connectionFactory) {
        super(connectionFactory);
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

        LogManager.logInfo(CTX_CONNECTOR, DataPlugin.Util.getString("ConnectionPool.Connection_pool_created_1")); //$NON-NLS-1$
    }

    /**
     * Return a connection from the connection pool.
     *
     * @return Connection from the pool.
     * @throws ConnectionPoolException if there is any error occurred.
     */
    public SourceConnection obtain(SecurityContext securityContext) throws ConnectionPoolException {
        ConnectorIdentity id = null;
        // Create identity object
        try {
            id = getConnectionFactory().createIdentity(securityContext);
            //create new connection
            SourceConnection connection = getConnectionFactory().createConnection(id);
            
            //log that we created a new connection
            if (LogManager.isMessageToBeRecorded(CTX_CONNECTOR, MessageLevel.TRACE)) {
                LogManager.logTrace(CTX_CONNECTOR, DataPlugin.Util.getString("ConnectionPool.New_conn", id)); //$NON-NLS-1$
            }
            
            return connection;
        } catch (ConnectorException e) {
            throw new ConnectionPoolException(e);
        }
    }

    /**
     * Release a connection back into the pool
     *
     * @param connection The connection
     */
    public void release(SourceConnection connection) {
        closeSourceConnection(connection);                    
    }

    /**
     * Indicate that while in use, this connection suffered an error condition.
     *
     * @param connection The connection
     */
    public void error(SourceConnection connection) {
        closeSourceConnection(connection);                    
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
        return new ConnectionStatus(testConnectionStatus());
    }
    
    private AliveStatus testConnectionStatus() {
        SourceConnection connection = null;
        try {
            connection = obtain(null);
            boolean alive = connection.isAlive();
            return (alive ? AliveStatus.ALIVE : AliveStatus.DEAD);
        } catch (ConnectionPoolException e) {
            return AliveStatus.DEAD;
        } finally {
            release(connection);
        }

    }
        
    
    /**
     * Shut down the pool.
     */
    public void shutDown() {
    }

    /**
     * @param connection
     * @return true if we succeeded in closing the connection
     * @throws Exception
     * @since 4.3
     */
    private boolean closeSourceConnection(SourceConnection connection) {
        try {
            connection.closeSource();
        
            //log that we closed a connection
            if (LogManager.isMessageToBeRecorded(CTX_CONNECTOR, MessageLevel.TRACE)) {
                LogManager.logTrace(CTX_CONNECTOR, DataPlugin.Util.getString("ConnectionPool.Closed_conn")); //$NON-NLS-1$
            }

            return true;
        } catch (Exception e) {
            LogManager.logError(CTX_CONNECTOR, DataPlugin.Util.getString("ConnectionPool.Failed_close_conn")); //$NON-NLS-1$
            return false;
        }
    }
        
}
