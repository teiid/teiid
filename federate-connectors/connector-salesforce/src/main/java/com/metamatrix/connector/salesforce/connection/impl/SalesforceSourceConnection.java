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
package com.metamatrix.connector.salesforce.connection.impl;

import java.io.Serializable;
import java.net.URL;

import com.metamatrix.connector.salesforce.connection.SalesforceConnection;
import com.metamatrix.data.api.Connection;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.pool.ConnectionPool;
import com.metamatrix.data.pool.ConnectorIdentity;
import com.metamatrix.data.pool.CredentialMap;
import com.metamatrix.data.pool.SourceConnection;

public class SalesforceSourceConnection implements SourceConnection {

	private String username; 
	private String password;
	private URL url;
	private ConnectorEnvironment env;
	private ConnectionPool pool;
	
	private SalesforceConnection connection;
	
	public SalesforceSourceConnection(String username, String password, URL url, ConnectorEnvironment env, ConnectionPool pool) throws ConnectorException {
		env.getLogger().logTrace("Enter SalesforceSourceConnection(String username, String password,...)");
		this.username = username;
		this.password = password;
		this.url = url;
		this.env = env;
		this.pool = pool;
		this.connection = new SalesforceConnection(username, password, url, env, pool, this);
		env.getLogger().logTrace("Return SalesforceSourceConnection(String username, String password,...)");
	}

	public SalesforceSourceConnection(ConnectorIdentity ident, URL url, ConnectorEnvironment env, ConnectionPool pool) throws ConnectorException {
		env.getLogger().logTrace("Enter SalesforceSourceConnection(ConnectorIdentity ident,...)");
		Serializable trustedPayload = ident.getSecurityContext().getTrustedPayload();
		if(trustedPayload instanceof CredentialMap) {
	    	CredentialMap map = (CredentialMap) trustedPayload;
	    	username = map.getUser(ident.getSecurityContext().getConnectorIdentifier());
	    	password = map.getPassword(ident.getSecurityContext().getConnectorIdentifier());    		
	    } else { 
	    	throw new ConnectorException("Unknown trusted payload type"); 
	    }
		this.url = url;
		this.env = env;
		this.pool = pool;
		this.connection = new SalesforceConnection(username, password, url, env, pool, this);
		env.getLogger().logTrace("Return SalesforceSourceConnection(ConnectorIdentity ident,...)");
	}

	public void closeSource() throws ConnectorException {
		connection.close();
	}

	public boolean isAlive() {
		return connection.isAlive();
	}

	public boolean isFailed() {
		return !connection.isAlive();
	}

	public Connection getConnection() throws ConnectorException {
		Connection result;
		if(null != connection) {
			result = connection;
		} else {
			connection = new SalesforceConnection(username, password, url, env, pool, this);
			result = connection;
		}
		return result;
	}
}
