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
import com.metamatrix.data.api.Connector;
import com.metamatrix.data.api.ConnectorCapabilities;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.GlobalCapabilitiesProvider;
import com.metamatrix.data.api.SecurityContext;
import com.metamatrix.data.api.ConnectorAnnotations.ConnectionPooling;
import com.metamatrix.data.exception.ConnectorException;

/**
 * XML Source connector, will give provide a XML document as source to
 * Metamatrix engine.
 */
@ConnectionPooling
public class XMLSourceConnector implements Connector, GlobalCapabilitiesProvider {

    private ConnectorEnvironment env;
    private XMLConnectionFacory connFactory;
    private boolean start = false;

    /**
     * Initialization with environment.
     */
    public void initialize(ConnectorEnvironment environment) throws ConnectorException {

        this.env = environment;
        this.connFactory = new XMLConnectionFacory(this.env);
        
        // logging
        XMLSourcePlugin.logInfo(this.env.getLogger(), "Connector_intialized"); //$NON-NLS-1$
    }

    /**
     * Stop the Connector 
     * @see com.metamatrix.data.api.Connector#stop()
     */
    public void stop() {
        if (!start) {
            return;
        }
        start = false;
        XMLSourcePlugin.logInfo(this.env.getLogger(), "Connector_stoped"); //$NON-NLS-1$
    }

    /**
     * Start the Connector 
     * @see com.metamatrix.data.api.Connector#start()
     */
    public void start() {
        start = true;
        XMLSourcePlugin.logInfo(this.env.getLogger(), "Connector_started"); //$NON-NLS-1$
    }

    /*
     * Get a Connection to the XML Source requested.
     * @see com.metamatrix.data.Connector#getConnection(com.metamatrix.data.SecurityContext)
     */
    public Connection getConnection(SecurityContext context) throws ConnectorException {
        return this.connFactory.createConnection(context);
    }

	public ConnectorCapabilities getCapabilities() {
		return XMLSourceCapabilities.INSTANCE;
	}

}
