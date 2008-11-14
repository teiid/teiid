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
import com.metamatrix.connector.sysadmin.extension.ISysAdminSource;
import com.metamatrix.data.api.*;
import com.metamatrix.data.exception.ConnectorException;

/**
 * Adapter to expose the object processing logic via the standard connector API.
 */
public abstract class ObjectConnector implements Connector, GlobalCapabilitiesProvider {
    private ConnectorEnvironment environment;
    
    public ConnectorCapabilities getCapabilities() {
    	return ObjectConnectorCapabilities.getInstance();
    }
    
    /* 
     * @see com.metamatrix.data.Connector#initialize(com.metamatrix.data.ConnectorEnvironment)
     */
    public void initialize(final ConnectorEnvironment environment) throws ConnectorException {
        this.environment = environment;
    }

    /* 
     * @see com.metamatrix.data.Connector#stop()
     */
    public void stop() {

    }

    /* 
     * @see com.metamatrix.data.Connector#start()
     */
    public void start() {

    }
        

    /* 
     * @see com.metamatrix.data.Connector#getConnection(com.metamatrix.data.SecurityContext)
     */
    public Connection getConnection(final SecurityContext context) throws ConnectorException {
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
    protected abstract IObjectSource getMetadataObjectSource(final SecurityContext context) throws ConnectorException ;
    
    /**
     * When a SystemAdmin query is being executed, this method will be called to obtain the object source.
     * It is delayed until it is known if its a SystemAdmin query or a Metadata query.     
     * @param environment
     * @param context
     * @return
     * @throws ConnectorException
     * @since 4.3
     */
    
    protected abstract ISysAdminSource getSysAdminObjectSource(final SecurityContext context) throws ConnectorException;
    
}
