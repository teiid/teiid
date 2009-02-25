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

import org.teiid.connector.api.Connection;
import org.teiid.connector.api.ConnectorCapabilities;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.basic.BasicConnector;

import com.metamatrix.connector.metadata.internal.IObjectSource;

/**
 * Adapter to expose the object processing logic via the standard connector API.
 */
public abstract class ObjectConnector extends BasicConnector {
    private ConnectorEnvironment environment;
    
    public ConnectorCapabilities getCapabilities() {
    	return ObjectConnectorCapabilities.getInstance();
    }
    
    @Override
    public void start(final ConnectorEnvironment environment) throws ConnectorException {
        this.environment = environment;
    }

    @Override
    public void stop() {

    }

    /* 
     * @see com.metamatrix.data.Connector#getConnection(com.metamatrix.data.SecurityContext)
     */
    public Connection getConnection(final ExecutionContext context) throws ConnectorException {
        return new ObjectConnection(environment, context, this);
    }
    
    protected ConnectorEnvironment getEnvironment() {
        return this.environment;
    }

    /**
     * When a Metadata query is being executed, this method will be called to obtain the object source.
     * It is delayed until it is known if its a SystemAdmin query or a Metadata query.     
     * @param environment
     * @param context
     * @return
     * @throws ConnectorException
     * @since 4.3
     */
    protected abstract IObjectSource getMetadataObjectSource(final ExecutionContext context) throws ConnectorException ;
    
}
