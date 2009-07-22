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
package com.metamatrix.connector.ldap;

import java.util.Properties;

import org.teiid.connector.api.Connection;
import org.teiid.connector.api.ConnectorCapabilities;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.basic.BasicConnector;


/** 
 * LDAPConnector.  This is responsible for initializing 
 * a connection factory, and obtaining connections to LDAP.
 */
public class LDAPConnector extends BasicConnector {
	private ConnectorEnvironment env;
	private ConnectorLogger logger;
	private Properties props;
	private LDAPConnectorCapabilities myCaps;

	@Override
	public ConnectorCapabilities getCapabilities() {
		return myCaps;
	}
	
    /*
     * @see com.metamatrix.data.Connector#getConnection(com.metamatrix.data.SecurityContext)
     */
	public Connection getConnection(ExecutionContext ctx) throws ConnectorException {
        final String msg = LDAPPlugin.Util.getString("LDAPSourceConnectionFactory.creatingConnection"); //$NON-NLS-1$
		logger.logDetail(msg); 
		return new LDAPConnection(ctx, this.props, this.logger);
	}
	
    /** 
	 * (non-Javadoc)
	 * @see org.teiid.connector.basic.BasicConnector#initialize(org.teiid.connector.api.ConnectorEnvironment)
	 */
	@Override
	public void start(ConnectorEnvironment env) throws ConnectorException {
		this.env = env;
		this.logger = this.env.getLogger();
		if(logger == null) {
            final String msg = LDAPPlugin.Util.getString("LDAPConnector.loggerNotFound"); //$NON-NLS-1$
            final ConnectorException e = new ConnectorException(msg);
			throw e; 
		}
		this.props = env.getProperties();
		
		// Create and configure capabilities class.
		myCaps = new LDAPConnectorCapabilities();
	}

	/** 
	 */
	@Override
	public void stop() {
	}
	
}
