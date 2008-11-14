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


package com.metamatrix.connector.xml.base;

import com.metamatrix.connector.xml.AbstractCachingConnector;
import com.metamatrix.data.api.Connection;
import com.metamatrix.data.api.ConnectorCapabilities;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ConnectorLogger;
import com.metamatrix.data.api.GlobalCapabilitiesProvider;
import com.metamatrix.data.api.SecurityContext;
import com.metamatrix.data.exception.ConnectorException;

public class XMLConnector extends AbstractCachingConnector implements GlobalCapabilitiesProvider {
	
	public XMLConnector() {
		super();          
	}

	public void initialize(ConnectorEnvironment env) throws ConnectorException {
        try {
    		super.initialize(env);	
            getLogger().logInfo("XML Connector Framework: connector has been initialized"); //$NON-NLS-1$
            getLogger().logTrace("XML Connector Framework: connector init properties: " + getEnvironment().getProperties()); //$NON-NLS-1$
        }
        catch (RuntimeException e) {
        	throw new ConnectorException(e);
        }
	}


	public void start() throws ConnectorException {
        try {
        	getLogger().logInfo("XML Connector Framework: connector has been started"); //$NON-NLS-1$
        }
        catch (RuntimeException e) {
            throw new ConnectorException(e);
        }
	}


	public void stop() {
		super.stop();
		ConnectorLogger logger = getLogger();
		if(logger != null) {
			getLogger().logInfo("XML Connector Framework: connector has been stopped");	 //$NON-NLS-1$
		}
	}


	public Connection getConnection(SecurityContext context) throws ConnectorException {
        try {
    		if (m_state == null) {
    			throw new ConnectorException(Messages.getString("XMLConnector.state.not.set")); //$NON-NLS-1$
    		}
    		return m_state.getConnection(this, context, getEnvironment());//new XMLConnectionImpl(this, context, getEnvironment());
        }
        catch (RuntimeException e) {
            throw new ConnectorException(e);
        }
	}

	public ConnectorCapabilities getCapabilities() {
		return XMLCapabilities.INSTANCE;
	}
}
