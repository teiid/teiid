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
package org.teiid.rhq.comm;

import java.util.Properties;




/**
 * The primary interface for the ConnectionPool used by the connection manager
 * responsible for handing out connections to the discovery and resource JON objects.
 *
 * After the instantiation of the ConnectionPool, the @link #initialize(String, Properties) method
 * will be called so the pool can be initialized.  
 * @since 1.0
 *  
 */
public interface ConnectionPool extends ConnectionPoolConstants {
    

    /**
     * Set the connection factory responsible for creating a connection
     * when the pool needs a new one.   @see ConnectionPoolConstants for the
     * avalable settings to configure the pool.
      * @param env are the environment variables needed to iniatilize the connection pool.
     * @since 1.0
     */
    void initialize(Properties env, ClassLoader cl) throws ConnectionException ;
    
    /**
     * Return a unique key that identifies this connection pool
     * 
     * @return
     */
    String getKey();
    
    
    /**
     * Returns a {@link Connection} from the pool.
     * @return Connction
     */
    Connection getConnection() throws ConnectionException;
    
    
    /**
     * Return the connection to the pool.   It's up to the 
     * implementation of the pool if the connection is reused or not
     * @param connection returned to the pool
     */
    void close(Connection connection)  throws ConnectionException;
    
    
    /** 
     * Called to shutdown the pool. 
     * 
     */
    void shutdown()  throws ConnectionException;
    
    
    /**
     * Return the <code>ClassLoader</code> used to instantiate the {@link ConnectionFactory} and
     * will be used on all calls in the {@link Connection} 
     * @return
     */
    ClassLoader getClassLoader();
    
    /**
     * Return the number of connection that are currently in use.
     * @return int is the number of connections in use
     *
     * @since
     */
    int getConnectionsInUseCount();
    
    /**
     * Return the number of connections that are currently available in the pool.
     * @return int is the number of connections currently available in the pool.
     *
     * @since 6.2
     */
    int getAvailableConnectionCount();
    
}
