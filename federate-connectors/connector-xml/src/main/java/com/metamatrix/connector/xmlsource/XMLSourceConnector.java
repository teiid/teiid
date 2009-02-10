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

import com.metamatrix.connector.api.Connection;
import com.metamatrix.connector.api.Connector;
import com.metamatrix.connector.api.ConnectorCapabilities;
import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.api.ConnectorAnnotations.ConnectionPooling;
import com.metamatrix.connector.exception.ConnectorException;

/**
 * XML Source connector, will give provide a XML document as source to
 * Metamatrix engine.
 */
@ConnectionPooling
public class XMLSourceConnector implements Connector {

    private ConnectorEnvironment env;
    private XMLConnectionFacory connFactory;
    private boolean start = false;

    /**
     * Initialization with environment.
     */

    public void start(ConnectorEnvironment environment) throws ConnectorException {
        start = true;

        this.env = environment;
        this.connFactory = new XMLConnectionFacory(this.env);
        
        // logging
        XMLSourcePlugin.logInfo(this.env.getLogger(), "Connector_started"); //$NON-NLS-1$
    }

    /**
     * Stop the Connector 
     * @see com.metamatrix.connector.api.Connector#stop()
     */
    public void stop() {
        if (!start) {
            return;
        }
        start = false;
        XMLSourcePlugin.logInfo(this.env.getLogger(), "Connector_stoped"); //$NON-NLS-1$
    }

    /*
     * Get a Connection to the XML Source requested.
     * @see com.metamatrix.data.Connector#getConnection(com.metamatrix.data.SecurityContext)
     */
    @Override
    public Connection getConnection(ExecutionContext context) throws ConnectorException {
        return this.connFactory.createConnection(context);
    }

    @Override
	public ConnectorCapabilities getCapabilities() {
		return XMLSourceCapabilities.INSTANCE;
	}

}
