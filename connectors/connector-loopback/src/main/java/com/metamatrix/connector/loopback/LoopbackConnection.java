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

import com.metamatrix.connector.api.*;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.language.ICommand;
import com.metamatrix.connector.metadata.runtime.RuntimeMetadata;

/**
 * Serves as a connection for the Loopback connector.  Since there is no actual
 * data source, this "connection" doesn't really have any state.  
 */
public class LoopbackConnection implements Connection {

    private ConnectorEnvironment env;

    /**
     * 
     */
    public LoopbackConnection(ConnectorEnvironment env) {
        this.env = env;
    }
    
    /* 
     * @see com.metamatrix.data.Connection#getCapabilities()
     */
    public ConnectorCapabilities getCapabilities() {
        return null;
    }

    
    @Override
    public Execution createExecution(ICommand command,
    		ExecutionContext executionContext, RuntimeMetadata metadata)
    		throws ConnectorException {
        return new LoopbackExecution(command, env, metadata);
    }
    
    @Override
    public void close() {
    	
    }

}
