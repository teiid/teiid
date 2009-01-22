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

package com.metamatrix.connector.metadata.adapter;

import com.metamatrix.connector.metadata.internal.IObjectSource;
import com.metamatrix.data.api.Connection;
import com.metamatrix.data.api.ConnectorCapabilities;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ConnectorMetadata;
import com.metamatrix.data.api.Execution;
import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.data.api.SecurityContext;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;

/**
 * Adapter to make object processing code comply with the standard connector API.
 */
public class ObjectConnection implements Connection {

    private ConnectorEnvironment environment;
    private SecurityContext securityContext;
    private ObjectConnector connector;

    public ObjectConnection(final ConnectorEnvironment environment, final SecurityContext context, ObjectConnector connector){
        this.securityContext = context;
        this.environment = environment;
        this.connector = connector;
    }

    /* 
     * @see com.metamatrix.data.Connection#createSynchExecution(com.metamatrix.data.language.ICommand, com.metamatrix.data.metadata.runtime.RuntimeMetadata)
     */
    public Execution createExecution(final int executionMode, final ExecutionContext executionContext, final RuntimeMetadata metadata) {
        switch(executionMode) {
            case ConnectorCapabilities.EXECUTION_MODE.SYNCH_QUERY:
                return new ObjectSynchExecution(metadata, this);
            case ConnectorCapabilities.EXECUTION_MODE.PROCEDURE:
                return new ObjectProcedureExecution(metadata, this, environment);
            default:
                return null;
        }
    }
    
    protected IObjectSource getMetadataObjectSource() throws ConnectorException {
        return connector.getMetadataObjectSource(securityContext);
    }
    
    protected Object getSysAdminobjectSource() throws ConnectorException {
        return connector.getSysAdminObjectSource(securityContext);
    }

    

    /* 
     * @see com.metamatrix.data.Connection#getMetadata()
     */
    public ConnectorMetadata getMetadata() {
        return null;
    }

    /* 
     * @see com.metamatrix.data.Connection#close()
     */
    public void release() {
        environment = null;
        securityContext = null;
        connector = null;

    }

    /* 
     * @see com.metamatrix.data.Connection#getCapabilities()
     */
    public ConnectorCapabilities getCapabilities() {
        return ObjectConnectorCapabilities.getInstance();
    }
}
