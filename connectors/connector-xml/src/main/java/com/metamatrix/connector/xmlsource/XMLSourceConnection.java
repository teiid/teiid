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

import com.metamatrix.connector.api.ConnectorCapabilities;
import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.basic.BasicConnection;
import com.metamatrix.connector.exception.ConnectorException;

/**
 * A Base XML Connection to an XML Source
 */
public abstract class XMLSourceConnection extends BasicConnection {

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
     * @see com.metamatrix.data.Connection#getCapabilities()
     */
    public ConnectorCapabilities getCapabilities() {
        return null;
    }
    
    /** 
     * @see com.metamatrix.connector.api.Connection#release()
     */
    public void close() {            
        XMLSourcePlugin.logInfo(this.env.getLogger(), "Connection_closed"); //$NON-NLS-1$
    }
    
    /**
     * Check if the connection is active. 
     * @return true if active; false otherwise
     */
    public abstract boolean isConnected();       
}
