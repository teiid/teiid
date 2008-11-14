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

package com.metamatrix.connector.xmlsource;

import com.metamatrix.data.api.Connection;
import com.metamatrix.data.api.ConnectorCapabilities;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ConnectorMetadata;
import com.metamatrix.data.api.Execution;
import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;

/**
 * A Base XML Connection to an XML Source
 */
public abstract class XMLSourceConnection implements Connection {

    protected ConnectorEnvironment env;
    protected boolean connected = false;
    
    /**
     * Constructor. 
     * @param env
     */
    public XMLSourceConnection(ConnectorEnvironment env) throws ConnectorException {
        this.env = env;
    }

    /**
     * Create Execution object which can execute the request. 
     * @see com.metamatrix.data.api.Connection#createExecution(int, com.metamatrix.data.api.ExecutionContext, com.metamatrix.data.metadata.runtime.RuntimeMetadata)
     */
    public Execution createExecution(int executionMode, ExecutionContext executionContext, RuntimeMetadata metadata) 
        throws ConnectorException {
        
        if (XMLSourceCapabilities.INSTANCE.supportsExecutionMode(executionMode)) {
            XMLSourcePlugin.logDetail(env.getLogger(), "creating_execution"); //$NON-NLS-1$
            return createExecution(executionContext, metadata);
        }
        throw new ConnectorException(XMLSourcePlugin.Util.getString("execution_mode_not_supported", new Object[] {new Integer(executionMode)})); //$NON-NLS-1$
    }

    /**  
     * @see com.metamatrix.data.api.Connection#getMetadata()
     */
    public ConnectorMetadata getMetadata() {
        return new XMLSourceConnectorMetadata();
    }


    /**
     * @see com.metamatrix.data.Connection#getCapabilities()
     */
    public ConnectorCapabilities getCapabilities() {
        return XMLSourceCapabilities.INSTANCE;
    }
    
    /** 
     * @see com.metamatrix.data.api.Connection#release()
     */
    public void release() {            
        XMLSourcePlugin.logInfo(this.env.getLogger(), "Connection_closed"); //$NON-NLS-1$
    }
    
    /**
     * Connection specific implementation. 
     * @param executionContext
     * @param metadata
     * @return
     */
    protected abstract Execution createExecution (ExecutionContext executionContext, RuntimeMetadata metadata) throws ConnectorException;
        
    /**
     * Check if the connection is active. 
     * @return true if active; false otherwise
     */
    public abstract boolean isConnected();       
}
