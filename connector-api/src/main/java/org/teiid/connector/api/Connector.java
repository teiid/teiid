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


/**
 * <p>The primary entry point for a Connector.  This interface should be implemented
 * by the connector writer.</p>
 * 
 * <p>The Connector Manager will instantiate the implementation
 * of this class by reflection in an isolated classloader.  Once the class has been 
 * instantiated, the {@link #initialize(ConnectorEnvironment)} method will be called
 * with all necessary connector properties.  The {@link #start()} and {@link #stop()} 
 * methods are lifecycle methods called when starting or stopping the connector.</p>  
 */
public interface Connector {

    /**
     * Start the connector with the connector environment.  The environment
     * provides access to external resources the connector implementation may
     * need to use.  
     * @param environment The connector environment, provided by the Connector Manager
     * @throws ConnectorException
     */
    void start(ConnectorEnvironment environment) throws ConnectorException;

    /**
     * Stop the connector.  No commands will be executed on the connector when it is
     * stopped.
     */
    void stop();

    /**
     * Obtain a connection with the connector.  The connection typically is associated
     * with a particular security context.  The connection is assumed to be pooled in 
     * the underlying source if pooling is necessary - the connection will be closed 
     * when execution has completed against it.  
     * @param context The context of the current user that will be using this connection, 
     * may be null if this connection is for an administrative operation. 
     * @return A Connection, created by the Connector
     * @throws ConnectorException If an error occurred obtaining a connection
     */
    Connection getConnection( ExecutionContext context ) throws ConnectorException;
    
    /**
     * Get the capabilities of this connector.  The capabilities affect what kinds of 
     * queries (and other commands) will be sent to the connector.
     * @return ConnectorCapabilities, may return null if the Connector provides User scoped capabilities {@link Connection#getCapabilities()}
     */
    ConnectorCapabilities getCapabilities();
    
	/**
	 * Create an identity object based on a security context.
	 * 
	 * If single identity is not supported then an exception should be thrown when a
	 * null context is supplied.
	 * 
	 * Implementors of this class may use a different implementation of the 
	 * {@link ConnectorIdentity} interface to similarly affect pooling.
	 *  
	 * @param context The context provided by the Connector Manager
	 * @return The associated connector identity
	 * @throws ConnectorException If a null context is not accepted or an error occurs while creating the identity.
	 */
	ConnectorIdentity createIdentity(ExecutionContext context)
			throws ConnectorException;

}
