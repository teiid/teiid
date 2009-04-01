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

package com.metamatrix.platform.config.persistence.api;

import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;

public interface PersistentConnection {
	
	/**
     * call close when the connection is no longer needed.  This resource
     * will be cleaned up.
	 * @throws ConfigurationException
	 */
    void close();
    
    /**
     * call to determine is the connection is still available.
     * @return boolean true if the connection is available
     * @return
     */
    boolean isClosed();
	
	/**
     * Returns an ConfigurationModelContainer based on how the implementation read configuation information
     * @param configID indicates which configuration to read
     * @return ConfigurationModel
     * @throws ConfigurationException if an error occurs
     */
    ConfigurationModelContainer read(ConfigurationID configID) throws ConfigurationException;
    
    /**
     * Writes the model to its persistent store based on the implementation of
     * the persistent connection.
     * @param model to be writen to output
     * @param principal is the user executing the write
     * @throws ConfigurationException if an error occurs
     */
    void write(ConfigurationModelContainer model, String principal) throws ConfigurationException;
    
    /**
     * Writes the collection of models to its persistent store based on the implementation of
     * the persistent connection.
     * @param models to be writen to output
     * @param principal is the user executing the write
     * @throws ConfigurationException if an error occurs
     */
    
    void delete(ConfigurationID configID, String principal) throws ConfigurationException;
    
    void rollback() throws ConfigurationException;
    
    void commit() throws ConfigurationException;
    
}
