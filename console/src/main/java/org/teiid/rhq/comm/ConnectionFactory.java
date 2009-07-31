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
 * The primary interface to be implemented by the pool user.  The ConnectionFactory
 * is called by the {@link ConnectionPool} to create and close the connection. 
 * 
 * 
 * This
 */
public interface ConnectionFactory {
	
    public static final String CONNECTION_FACTORY_DEFAULT="org.teiid.rhq.comm.impl.TeiidConnectionFactory";  //$NON-NLS-1$



    /**
     * Set the environment that this factory is being run in - typically used 
     * to obtain properties when creating connections and to log messages as 
     * necessary.
     * @param env The environment properties needed to create a connection by the connector manager
     *     {@link ConnectionConstants} 
     * @param connectionPool is passed so the factory can set the pool on the connection.
     */
    void initialize(Properties env, ConnectionPool connectionPool) throws ConnectionException;
    
    /**
     * Return the url used to connect to the server.
     * @return String url
     */
    String getURL();

    /**
     * Create the connection. This connection is to an existing MetaMatrix server.
     * @return The Connection form of source-specific connection
     * @throws ConnectorException If an error occurs while creating the connection
     */
    Connection createConnection() throws ConnectionException;
    
    
    /**
     * Called by the {@link ConnectionPool} when a connection is being cleaned up from the pool.
     * This will allow the creator of the connection to do any last cleanup steps necessary in order
     * to release all resources.   
     * 
     * In cases where the {@link Connection}, when the {@link Connection#close()} method only returns
     * the connection to the pool and does not actually close the connection, this close will be 
     * responsible for actually closing the connection to the source.
     * @param connection

     */
    void closeConnection(Connection connection) ;

}
