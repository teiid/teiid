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

package com.metamatrix.connector.loopback;

import org.teiid.connector.api.*;
import org.teiid.connector.basic.BasicConnector;

/**
 * Starting point for the Loopback connector.
 */
public class LoopbackConnector extends BasicConnector {

    private ConnectorEnvironment env;
    
    private ConnectorCapabilities capabilities = new LoopbackCapabilities();

    /**
     * 
     */
    public LoopbackConnector() {
        super();
    }

    @Override
    public void start(ConnectorEnvironment environment) throws ConnectorException {
        this.env = environment;
        
        String capabilityClass = env.getProperties().getProperty(LoopbackProperties.CAPABILITIES_CLASS);
        
        if(capabilityClass != null && capabilityClass.length() > 0) {
            try {
            	Class clazz = Thread.currentThread().getContextClassLoader().loadClass(capabilityClass);
                capabilities = (ConnectorCapabilities) clazz.newInstance();
                env.getLogger().logInfo("Loaded " + capabilityClass + " for LoopbackConnector"); //$NON-NLS-1$ //$NON-NLS-2$
            } catch(ClassNotFoundException cnfe) {
            	env.getLogger().logError("Capabilities class not found: " + capabilityClass, cnfe); //$NON-NLS-1$
            } catch(IllegalAccessException iae) {
            	env.getLogger().logError("Unable to create capabilities class: " + capabilityClass, iae); //$NON-NLS-1$
            } catch(InstantiationException ie) {
            	env.getLogger().logError("Unable to create capabilities class: " + capabilityClass, ie); //$NON-NLS-1$
            } catch(ClassCastException cce) {
            	env.getLogger().logError("Capabilities class does not extend ConnectorCapabilities: " + capabilityClass, cce); //$NON-NLS-1$
            }
        } 
    }

    /* 
     * @see com.metamatrix.data.Connector#stop()
     */
    public void stop() {
        // nothing to do
    }

    /* 
     * @see com.metamatrix.data.Connector#getConnection(com.metamatrix.data.SecurityContext)
     */
    public Connection getConnection(ExecutionContext context) throws ConnectorException {
        return new LoopbackConnection(env);
    }

	public ConnectorCapabilities getCapabilities() {
		return capabilities;
	}

}
