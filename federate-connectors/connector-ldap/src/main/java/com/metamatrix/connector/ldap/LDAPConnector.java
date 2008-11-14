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
package com.metamatrix.connector.ldap;

import java.util.Properties;

import com.metamatrix.data.api.Connection;
import com.metamatrix.data.api.Connector;
import com.metamatrix.data.api.ConnectorCapabilities;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ConnectorLogger;
import com.metamatrix.data.api.SecurityContext;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.pool.ConnectionPool;

/** 
 * LDAPConnector.  This is responsible for initializing a connection pool, 
 * a connection factory, and obtaining connections to LDAP.
 */
public class LDAPConnector implements Connector {
	private ConnectorEnvironment env;
	private ConnectorLogger logger;
	private Properties props;
	private ConnectionPool pool;
	private LDAPSourceConnectionFactory ldapConnFactory;
	
    /*
     * @see com.metamatrix.data.Connector#getConnection(com.metamatrix.data.SecurityContext)
     */
	public Connection getConnection(SecurityContext ctx) throws ConnectorException {
		if(pool != null) {
			logger.logDetail("LDAPConnector is requesting a connection from the pool."); //$NON-NLS-1$
			LDAPConnection ldapConn = (LDAPConnection)pool.obtain(ctx);
			ldapConn.setConnectionPool(pool);
			return ldapConn;
		} 
        final String msg = LDAPPlugin.Util.getString("LDAPConnector.getConnectionFailed"); //$NON-NLS-1$
		throw new ConnectorException(msg); 
	}
	
    /** 
     * Initialize the connection pool and SourceConnectionFactory, providing appropriate properties
	 * to both.
	 * (non-Javadoc)
	 * @see com.metamatrix.data.api.Connector#initialize(com.metamatrix.data.api.ConnectorEnvironment)
	 */
	public void initialize(ConnectorEnvironment env) throws ConnectorException {
		this.env = env;
		this.logger = this.env.getLogger();
		if(logger == null) {
            final String msg = LDAPPlugin.Util.getString("LDAPConnector.loggerNotFound"); //$NON-NLS-1$
            final ConnectorException e = new ConnectorException(msg);
			throw e; 
		}
		this.props = env.getProperties();
		
		// Create and initialize LDAP connection factory
		ldapConnFactory = new LDAPSourceConnectionFactory(ConnectorCapabilities.EXECUTION_MODE.SYNCH_QUERY, this.props, this.logger);
		ldapConnFactory.initialize(env);
		
		// Create and initialize connection pool
		pool = new ConnectionPool(ldapConnFactory);
		pool.initialize(this.props);
	}
	


	/** 
	 * Do nothing. We do not attempt to connect to LDAP using a "test" connection,
	 * but this functionality could be added here, if needed. The test connection information
	 * must be added as a connector binding property.
	 * (non-Javadoc)
	 * @see com.metamatrix.data.api.Connector#start()
	 */
	public void start() throws ConnectorException {
	}

	/** Shutdown the connection pool, which closes all remaining connections.
	 */
	public void stop() {
		if(pool!=null) {
			pool.shutDown();
		}
	}
	
}
