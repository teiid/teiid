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

import com.metamatrix.common.config.StartupStateException;
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
	 * Call to set the startup state to @see {StartupStateController.STARTING Starting}.
	 * The server must be in the STOPPED state in order for this to work.  Otherwise,
	 * a StartpStateException will be thrown.
	 * @throws StartupStateException is thrown if the server state is not currently
	 * set to STOPPED.
	 */	
    void setServerStarting() throws StartupStateException, ConfigurationException;

	/**
	 * Call to forcibly set the startup state to @see {StartupStateController.STARTING Starting},
	 * regardless of the current state of the server.
	 * @throws StartupStateException is thrown if the server state cannot be set.
	 */	
    void setServerStarting( boolean force) throws StartupStateException, ConfigurationException;
	
	/**
	 * Call to set the startup state to @see {StartupStateController.STARTED Started}.
	 * The server must be in the STARTING state in order for this to work.  Otherwise,
	 * a StartpStateException will be thrown.
	 * @throws StartupStateException is thrown if the server state cannot be set.
	 */	
    void setServerStarted( ) throws StartupStateException, ConfigurationException;
	
	
	/**
	 * Call to set the startup state to @see {StartupStateController.STOPPED Stopped}.
	 * This is normally called when the system is shutdown.
	 * @throws StartupStateException is thrown if the server state cannot be set.
	 */	
    void setServerStopped() throws StartupStateException, ConfigurationException;
	
		
	
	/**
	 * Call to get the current state
	 * @return int state @see {StartupStateController Controller}
	 * @throws ConfigurationException if an error occurs
	 */
	int getServerState() throws ConfigurationException;


	/**
	 * Call to get the startup time of the server.  If the current state of the server
	 * is not @see {StartupStateController.STARTED STARTED}, then the return value
	 * will be null.
	 * @return time the server stated, may be null if not in a started state
	 * @throws ConfigurationException if an error occurs
	 */
	java.util.Date getStartupTime() throws ConfigurationException;


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
    
    void beginTransaction() throws ConfigurationException;
    
    void rollback() throws ConfigurationException;
    
    void commit() throws ConfigurationException;
    
}
