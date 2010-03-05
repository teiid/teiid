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


package org.teiid.connector.xml.base;

import org.teiid.connector.api.Connection;
import org.teiid.connector.api.ConnectorCapabilities;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;
import org.teiid.connector.basic.BasicConnector;
import org.teiid.connector.basic.BasicManagedConnectionFactory;
import org.teiid.connector.xml.StatefulConnector;
import org.teiid.connector.xml.XMLConnectorState;
import org.teiid.connector.xml.http.HTTPConnectorState;
import org.teiid.connector.xml.http.HTTPManagedConnectionFactory;

import com.metamatrix.connector.xml.base.Messages;
import com.metamatrix.connector.xml.base.XMLCapabilities;


public class XMLConnector extends BasicConnector implements StatefulConnector {
	
	protected HTTPManagedConnectionFactory config;
	private HTTPConnectorState state;
	
	public XMLConnector() {
		super();          
	}

	@Override
	public void initialize(ConnectorEnvironment env) throws ConnectorException {
		super.initialize(env);	
		this.config = (HTTPManagedConnectionFactory)env;
        this.config.getLogger().logInfo("XML Connector Framework: connector has been started"); //$NON-NLS-1$
        this.state = new HTTPConnectorState();
        this.state.setLogger(this.config.getLogger());
        this.state.setState(this.config);
	}


	public Connection getConnection() throws ConnectorException {
		if (this.state == null) {
			throw new ConnectorException(Messages.getString("XMLConnector.state.not.set")); //$NON-NLS-1$
		}
		return this.state.getConnection(this);
	}

    @Override
    public Class<? extends ConnectorCapabilities> getDefaultCapabilities() {
    	return XMLCapabilities.class;
    }

	@Override
	public ConnectorEnvironment getEnvironment() {
		return this.config;
	}

	@Override
	public ConnectorLogger getLogger() {
		return config.getLogger();
	}

	@Override
	public XMLConnectorState getState() {
		return this.state;
	}
}
