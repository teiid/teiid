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
 * Date: Jun 24, 2003
 * Time: 1:13:49 PM
 */
package com.metamatrix.dqp.embedded;

import java.net.URL;
import java.util.Date;
import java.util.Properties;

import com.metamatrix.common.application.DQPConfigSource;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.dqp.internal.process.DQPCore;
import com.metamatrix.dqp.service.ConfigurationService;
import com.metamatrix.dqp.service.DQPServiceNames;

/**
 * Manages the life cycle of the embedded DQP. Like handling the creation and deletion
 * of the DQP. Knows about configuration elements and connection management to the
 * Embedded DQP. All direct operations to the DQP must go though the manager object
 * (other than what goes on Connection object requests), there should not be any 
 * referecnes to DQP instance directly manuplulating the API. If such case arises 
 * implement as methods on DQP interface.
 */
public class DQPEmbeddedManager {

    private DQPConfigSource configSource;
    private DQPCore dqpInstance;
    private long dqpStarttime = -1; 
    DQPListener dqpListener = null;
    
    /**
     * Construct DQPEmbeddedManager. 
     */
    public DQPEmbeddedManager(URL dqpURL, Properties connectionProperties, DQPListener listener) throws ApplicationInitializationException {
        this.configSource = new EmbeddedConfigSource(dqpURL, connectionProperties);
        this.dqpListener = listener;
    }

    /**
     * Main access method to DQP.  Will either find the DQP or
     * start and initialize as necessary.
     * @return The DQP to use.
     * @throws ApplicationInitializationException when an error occurs
     * during DQP initialization.
     */
    public synchronized DQPCore createDQP() throws ApplicationInitializationException {        
        if ( dqpInstance == null ) {
            dqpInstance = new DQPCore();
            dqpInstance.start(configSource);
            dqpStarttime = System.currentTimeMillis();
            DQPEmbeddedPlugin.logInfo("DQPEmbeddedManager.start_dqp", new Object[] {new Date(System.currentTimeMillis()).toString()}); //$NON-NLS-1$
            
            // Notify the listener that DQP started
            if (dqpListener != null) {
                dqpListener.onStart();
            }
        }
        return dqpInstance;
    }

    /**
     * Get the DQP instance holded by this manager 
     * @return DQP instance if has been created;null otherwise
     */
    public DQPCore getDQP() {
        return dqpInstance;
    }
    
    /**
     * Shutdown the DQP and its connections. 
     * @throws ApplicationLifecycleException
     */
    public synchronized void shutdown() throws ApplicationLifecycleException {
        if (dqpInstance != null) {
                                    
            // stop the DQP
            dqpInstance.stop();
            dqpInstance = null;
            configSource = null;
            dqpStarttime = -1;
            
            // Notify the listener that DQP being stopped
            if (dqpListener != null) {
                dqpListener.onShutdown();
                dqpListener = null;
            }            
                                    
            DQPEmbeddedPlugin.logInfo("DQPEmbeddedManager.shutdown_dqp", new Object[] {new Date(System.currentTimeMillis()).toString()});             //$NON-NLS-1$
            
            // shutdown the logger
            EmbeddedConfigUtil.shutdownLogger();            
        }
    }
    
    /**
     * Returns the long defining the time in milliseconds when it started.  
     * @return -1 if the DQP not started.
     * @since 4.3
     */
    public long getDQPStartTime() {
        return dqpStarttime;
    }
    
    /**
     * Check if the DQP has been started. 
     * @return
     * @since 4.3
     */
    public boolean isDQPAlive() {
        return dqpInstance != null;
    }

    /**
     * Get the properties of the DQP. 
     * @return null if the DQP not started. properties started with otherwise.
     * @since 4.3
     */
    public Properties getDQPProperties() {
        if (isDQPAlive()) {
            return ((ConfigurationService)dqpInstance.getEnvironment().findService(DQPServiceNames.CONFIGURATION_SERVICE)).getSystemProperties();
        }
        return null;
    }       
}
