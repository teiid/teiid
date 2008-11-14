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

package com.metamatrix.connector.exec;
import java.util.List;

import com.metamatrix.data.api.Connection;
import com.metamatrix.data.api.ConnectorCapabilities;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ConnectorLogger;
import com.metamatrix.data.api.ConnectorMetadata;
import com.metamatrix.data.api.Execution;
import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;

/**
 * Implementation of Connection interface for soap connection.
 */
public class ExecConnection implements Connection {
    
    ConnectorEnvironment env;
    
    // Connector logger
    ConnectorLogger logger;
    private List exclusionList;
    
    // Capabilities
    private ConnectorCapabilities capabilities;
    
    /**
     * Constructor.
     * @param env
     */    
    ExecConnection(ConnectorEnvironment env, List exclusionThese) throws ConnectorException {
        this.env = env;
        this.logger = env.getLogger();
        this.capabilities = new ExecCapabilities();
        this.exclusionList = exclusionThese;
        // initialize the metadata properties that will be used to access the text file
        init();
    }
    
    /**
     * Create text execution.
     * @param command ICommand containing the query 
     */
    public Execution createExecution(int executionMode, ExecutionContext executionContext, RuntimeMetadata metadata)
    {
//        return new ExecExecution(env, metadata, logger, exclusionList);
        return new ExecAntExecution(env, metadata, logger, exclusionList);

    }

    /**
     * Get the metadata of the source the connector is connected to.
     * @return ConnectorMetadata
     */
    public ConnectorMetadata getMetadata() { 
        return null;  
    }

    public void release(){
        logger.logDetail("Exec Connection is successfully closed."); //$NON-NLS-1$
    }

    private void init() throws ConnectorException {
    }

    /* 
     * @see com.metamatrix.data.Connection#getCapabilities()
     */
    public ConnectorCapabilities getCapabilities() {
        return this.capabilities;
    }     
}
