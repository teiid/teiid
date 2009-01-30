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

package com.metamatrix.connector.salesforce;

import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;

import com.metamatrix.connector.salesforce.connection.SalesforceConnection;
import com.metamatrix.data.api.Connection;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ConnectorLogger;
import com.metamatrix.data.api.SecurityContext;
import com.metamatrix.data.api.ConnectorAnnotations.ConnectionPooling;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.pool.ConnectorIdentity;
import com.metamatrix.data.pool.ConnectorIdentityFactory;
import com.metamatrix.data.pool.CredentialMap;
import com.metamatrix.data.pool.SingleIdentityFactory;
import com.metamatrix.data.pool.UserIdentityFactory;

@ConnectionPooling
public class Connector implements com.metamatrix.data.api.Connector, ConnectorIdentityFactory {

	private ConnectorLogger logger;

	private ConnectorEnvironment connectorEnv;
	private ConnectorIdentityFactory connectorIdentityFactory;
	private ConnectorState state;
	private boolean singleIdentity;
	private String username;
	private String password;
	private URL url;

	// ///////////////////////////////////////////////////////////
	// Connector implementation
	// ///////////////////////////////////////////////////////////
	public Connection getConnection(SecurityContext secContext)
			throws ConnectorException {
		logger.logTrace("Enter SalesforceSourceConnection.getConnection()");
		Connection connection = null;
		if (singleIdentity) {
			connection = new SalesforceConnection(username, password, url, connectorEnv);
		} else {
			Serializable trustedPayload = secContext.getTrustedPayload();
			if(trustedPayload instanceof CredentialMap) {
		    	CredentialMap map = (CredentialMap) trustedPayload;
		    	String username = map.getUser(secContext.getConnectorIdentifier());
		    	String password = map.getPassword(secContext.getConnectorIdentifier());    		
		    	connection = new SalesforceConnection(username, password, url, connectorEnv);
		    } else { 
		    	throw new ConnectorException("Unknown trusted payload type"); 
		    }
		}
		logger.logTrace("Return SalesforceSourceConnection.getConnection()");
		return connection;
	}

	public void initialize(ConnectorEnvironment env) throws ConnectorException {
		env.getLogger().logTrace("Enter SalesforceSourceConnection.initialize()");
		this.connectorEnv = env;
		this.logger = env.getLogger();
		this.state = new ConnectorState(env.getProperties(), getLogger());
		getLogger().logInfo(getLogPreamble().append("Initialized").toString()); //$NON-NLS-1$
		getLogger().logTrace(getLogPreamble()
				.append("Initialization Properties: " + env.getProperties()).toString()); //$NON-NLS-1$
		String urlString =  env.getProperties().getProperty("URL");
		if(null != urlString && 0 != urlString.length()) {
			try {
				url = new URL(urlString);
			} catch (MalformedURLException e) {
				throw new ConnectorException(e, e.getMessage());
			}
		}
		
		String username = env.getProperties().getProperty("username");
		String password =  env.getProperties().getProperty("password");
		
		//validate that both are empty or both have values
		if(null == username && null == password) {
			
		} else if ((null == username || username.equals("")) && (null != password || !password.equals("")) ||
				((null == password || password.equals("")) && (null != username || !username.equals("")))) {
					String msg = Messages.getString("SalesforceSourceConnectionFactory.Invalid.username.password.pair");
					env.getLogger().logError(msg);
					throw new ConnectorException(msg);
		} else if(null != username || !username.equals("")) {
			singleIdentity = true;
			this.password = password;
			this.username = username;
		}
		if (singleIdentity) {
			this.connectorIdentityFactory = new SingleIdentityFactory();
		} else {
			this.connectorIdentityFactory = new UserIdentityFactory();
		}
		logger.logTrace("Return SalesforceSourceConnection.initialize()");
	}

	public void start() throws ConnectorException {
		getLogger().logInfo(getLogPreamble().append("Started").toString()); //$NON-NLS-1$
	}

	public void stop() {
		try {
			getLogger().logInfo(getLogPreamble().append("Stopped").toString());
		} catch (ConnectorException e) {
			// nothing to do here
		}
	}

	/////////////////////////////////////////////////////////////
	//Utilities
	/////////////////////////////////////////////////////////////

	public ConnectorLogger getLogger() throws ConnectorException {
		if(null == logger) {
			throw new ConnectorException("Error:  Connector initialize not called");
		}
		return logger;
	}

	public StringBuffer getLogPreamble() {
		StringBuffer preamble = new StringBuffer();
		preamble.append("Salesforce Connector id = ");
		preamble.append(connectorEnv.getConnectorName());
		preamble.append(":");
		return preamble;
	}

	public ConnectorState getState() {
		return state;
	}

	@Override
	public ConnectorIdentity createIdentity(SecurityContext context)
			throws ConnectorException {
		return this.connectorIdentityFactory.createIdentity(context);
	}
}
