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

package com.metamatrix.connector.metadata.adapter;

import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.ProcedureExecution;
import org.teiid.connector.api.ResultSetExecution;
import org.teiid.connector.basic.BasicConnection;
import org.teiid.connector.language.IProcedure;
import org.teiid.connector.language.IQuery;
import org.teiid.connector.language.IQueryCommand;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

import com.metamatrix.connector.metadata.MetadataProcedureExecution;
import com.metamatrix.connector.metadata.internal.IObjectSource;

/**
 * Adapter to make object processing code comply with the standard connector API.
 */
public class ObjectConnection extends BasicConnection {

    private ExecutionContext executionContext;
    private ObjectConnector connector;

    public ObjectConnection(final ConnectorEnvironment environment, final ExecutionContext context, ObjectConnector connector){
        this.executionContext = context;
        this.connector = connector;
    }

    @Override
    public ResultSetExecution createResultSetExecution(IQueryCommand command,
    		ExecutionContext executionContext, RuntimeMetadata metadata)
    		throws ConnectorException {
    	return new ObjectSynchExecution((IQuery)command, metadata, this);
    }
    
    @Override
    public ProcedureExecution createProcedureExecution(IProcedure command,
    		ExecutionContext executionContext, RuntimeMetadata metadata)
    		throws ConnectorException {
    	return new MetadataProcedureExecution(command, metadata, getMetadataObjectSource());
    }
    
    protected IObjectSource getMetadataObjectSource() throws ConnectorException {
        return connector.getMetadataObjectSource(executionContext);
    }
    
    @Override
    public void close() {
    }
}
