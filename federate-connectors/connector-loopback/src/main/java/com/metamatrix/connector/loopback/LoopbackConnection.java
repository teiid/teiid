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

package com.metamatrix.connector.loopback;

import com.metamatrix.data.api.*;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;

/**
 * Serves as a connection for the Loopback connector.  Since there is no actual
 * data source, this "connection" doesn't really have any state.  
 */
public class LoopbackConnection implements Connection {

    private ConnectorCapabilities capabilities;

    private ConnectorEnvironment env;

    /**
     * 
     */
    public LoopbackConnection(ConnectorEnvironment env) {
        this.env = env;
        
        // Set up capabilities
        String capabilityClass = env.getProperties().getProperty(LoopbackProperties.CAPABILITIES_CLASS);
        loadCapabilities(capabilityClass, env.getLogger());
    }
    
    /**
     * Load the capabilities class based on the properties OR default to the LoopbackCapabilities.
     * @param capabilityClass Class name for implementation of ConnectorCapabilities
     */
    void loadCapabilities(String capabilityClass, ConnectorLogger logger) {
        if(capabilityClass != null && capabilityClass.length() > 0) {
            try {
            	Class clazz = Thread.currentThread().getContextClassLoader().loadClass(capabilityClass);
                capabilities = (ConnectorCapabilities) clazz.newInstance();
                logger.logInfo("Loaded " + capabilityClass + " for LoopbackConnector"); //$NON-NLS-1$ //$NON-NLS-2$
            } catch(ClassNotFoundException cnfe) {
                logger.logError("Capabilities class not found: " + capabilityClass, cnfe); //$NON-NLS-1$
            } catch(IllegalAccessException iae) {
                logger.logError("Unable to create capabilities class: " + capabilityClass, iae); //$NON-NLS-1$
            } catch(InstantiationException ie) {
                logger.logError("Unable to create capabilities class: " + capabilityClass, ie); //$NON-NLS-1$
            } catch(ClassCastException cce) {
                logger.logError("Capabilities class does not extend ConnectorCapabilities: " + capabilityClass, cce); //$NON-NLS-1$
            }
        } 
        
        if(this.capabilities == null) {
            capabilities = new LoopbackCapabilities();            
        }        
    }

    /* 
     * @see com.metamatrix.data.Connection#getCapabilities()
     */
    public ConnectorCapabilities getCapabilities() {
        return this.capabilities;
    }

    /* 
     * @see com.metamatrix.data.Connection#createSynchExecution(com.metamatrix.data.language.ICommand, com.metamatrix.data.metadata.runtime.RuntimeMetadata)
     */
    public Execution createExecution(int executionMode, ExecutionContext executionContext, RuntimeMetadata metadata) {
        return new LoopbackExecution(env, metadata);
    }

    /* 
     * @see com.metamatrix.data.Connection#getMetadata()
     */
    public ConnectorMetadata getMetadata() {
        // Don't support
        return null;
    }

    /* 
     * @see com.metamatrix.data.Connection#close()
     */
    public void release() {
        // nothing to do
    }

}
