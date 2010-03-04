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

package org.teiid.connector.api;

import javax.security.auth.Subject;


/**
 * <p>The primary entry point for a Connector.  This interface should be implemented
 * by the connector writer.</p>
 * 
 * <p>The JCA Container will instantiate the implementation of this class. Once the class has been 
 * instantiated, the {@link #initialize(ConnectorEnvironment)} method will be called
 * with all necessary connector properties. </p>  
 */
public interface Connector {

	/**
	 * Initialize the connector with supplied configuration
	 * @param config
	 */
	void initialize(ConnectorEnvironment config) throws ConnectorException;
	
    /**
     * Obtain a connection with the connector.  The connection typically is associated
     * with a particular security context.  The connection is assumed to be pooled by container 
     * if pooling is necessary - the connection will be closed when execution has completed against it.
     *   
     * If you need to authenticate/authorize and need to get access to {{@link Subject}, then use
     * {@link ConnectionContext}
     *   
     * @return A Connection, created by the Connector
     * @throws ConnectorException If an error occurred obtaining a connection
     */
    Connection getConnection() throws ConnectorException;
    
    /**
     * Get the capabilities of this connector.  The capabilities affect what kinds of 
     * queries (and other commands) will be sent to the connector.
     * @return ConnectorCapabilities, may return null if the Connector provides User scoped capabilities {@link Connection#getCapabilities()}
     */
    ConnectorCapabilities getCapabilities() throws ConnectorException;
    
    /**
     * Get the ConnectorEnvironment that this connector is initialized with.
     * @return
     * @throws ConnectorException
     */
    ConnectorEnvironment getConnectorEnvironment();
}
