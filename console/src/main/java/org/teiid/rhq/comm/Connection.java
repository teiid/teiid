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

import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import org.teiid.rhq.admin.utils.SingletonConnectionManager;



public interface Connection {
    
    /**
     * Called to find out if the connection represents a valid connection to the
     * JBEDSP system.   This call should be used only after obtaining a connection
     * from the {@link SingletonConnectionManager#getConnection(String)}.
     * The connection can become invalid if the JBEDSP system goes down.
     */
    boolean isValid();
    
    /**
     * Called by the {@link ConnectionPool} to check if the connection is still open. 
     * @return true is the connection is alive.

     */
    boolean isAlive();
	
    /**
     * Call to indicate the connection is no longer needed and that
     * resources can be released. 
     */
    public void close() ;
     
	
	/**
	 * This method is called by a component of the JBEDSP RHQ plugin. The
	 * component identifier, type and metric operation are used to execute the correlating logic.
	 * The valueMap is a map of values (0-n) that are required by
	 * the logic to determine the metric.
	 * @param componentType @see ConnectionConstants.ComponentType
	 * @param identifier 
	 * @param metric
	 * @param valueMap
	 * @return Object
     * @throws ConnectionException
	 */
	public Object getMetric(final String componentType, String identifier, final String metric, final Map valueMap) throws ConnectionException;

	/**
	 * This method is called by a component of the JBEDSP RHQ plugin. The
	 * component type and operation are used to execute the correlating logic.
	 * The valueMap is a collection of values (0-n) that are required by
	 * the operation.
	 * {@link ConnectionConstants.ComponentType} for <code>componentType</code> values
	 * @param result
	 * @param componentType
	 * @param operationName
	 * @param valueMap
     * @throws ConnectionException
	 */
	public void executeOperation(final ExecutedResult result, final Map valueMap) throws ConnectionException;
	
	/**
	 * This method is called by a component to determine if that particular component is con. The
	 * component type and identifier are used to execute the correlating logic.
	 * 
     * {@link ConnectionConstants.ComponentType} for <code>componentType</code> values
     * 
	 * The return value is true if UP else false if DOWN
	 * 
	 * @param componentType
	 * @param identifier
	 * @return Boolean 
     * @throws ConnectionException
	 */
	public Boolean isAvailable(final String componentType, final String identifier) throws ConnectionException;
	
   
    /**
     *  Return the properties for component of a specified resource type {@link ConnectionConstants.ComponentType}  
     * @param componentType {@link ConnectionConstants.ComponentType}
     * @param identifier
     * @return
     * @throws ConnectionException
     * @since 4.3
     */
    
    public Properties getProperties(String componenType, String identifier) throws ConnectionException; 
    
	/**
	 * Returns a property for a given identifier
	 * 
	 * @param identifier
	 * @param property
     * @throws ConnectionException
	 */
	public String getProperty(String identifier, String property) throws ConnectionException;
	
	/**
	 * Returns the unique key that maps this connection to the system that is being connected to.
	 * This key used during the enterprise monitoring to keep track of which connection belongs to
	 * what's being monitored.
	 * 
	 * @return key JBEDSP represented by this connection
     * @throws Exception
	 */
	public String getKey() throws Exception;
    
    /**
     * Returns a <code>Collection</code> of {@link Component Component}s for the given identifier for the
     * given name of the {@link ConnectionConstants.ComponentType}
     * @param componentType
     * @param identifier
     * @return
     * @throws ConnectionException
     */
    public Collection<Component> discoverComponents(String componentType, String identifier) throws ConnectionException;
   
}
