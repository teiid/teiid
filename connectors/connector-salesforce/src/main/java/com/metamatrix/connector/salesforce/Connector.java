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

package com.metamatrix.connector.salesforce;

import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;

import org.teiid.connector.api.Connection;
import org.teiid.connector.api.ConnectorCapabilities;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;
import org.teiid.connector.api.CredentialMap;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.ConnectorAnnotations.ConnectionPooling;

import com.metamatrix.connector.salesforce.connection.SalesforceConnection;

@ConnectionPooling
public class Connector extends org.teiid.connector.basic.BasicConnector {

	private ConnectorLogger logger;

	private ConnectorEnvironment connectorEnv;
	private ConnectorState state;
	private boolean singleIdentity;
	private String username;
	private String password;
	private URL url;
	private SalesforceCapabilities salesforceCapabilites;

	// ///////////////////////////////////////////////////////////
	// Connector implementation
	// ///////////////////////////////////////////////////////////
	public Connection getConnection(ExecutionContext secContext)
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

	@Override
	public void start(ConnectorEnvironment env) throws ConnectorException {
		this.logger = env.getLogger();
		this.connectorEnv = env;
		getLogger().logInfo(getLogPreamble().append("Started").toString()); //$NON-NLS-1$
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
			
		} else if ((null == username || username.equals("")) && (null != password && !password.equals("")) ||
				((null == password || password.equals("")) && (null != username && !username.equals("")))) {
					String msg = Messages.getString("SalesforceSourceConnectionFactory.Invalid.username.password.pair");
					env.getLogger().logError(msg);
					throw new ConnectorException(msg);
		} else if(null != username && !username.equals("")) {
			singleIdentity = true;
			this.password = password;
			this.username = username;
		} else {
			this.setAdminConnectionsAllowed(false);
			this.setUseCredentialMap(true);
		}
		
		String capabilitiesClass = env.getProperties().getProperty("ConnectorCapabilities", SalesforceCapabilities.class.getName());
    	try {
    		Class clazz = Thread.currentThread().getContextClassLoader().loadClass(capabilitiesClass);
    		salesforceCapabilites = (SalesforceCapabilities) clazz.newInstance();
		} catch (Exception e) {
			throw new ConnectorException(e, "Unable to load Capabilities Class");
		}
		logger.logTrace("Return SalesforceSourceConnection.initialize()");
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
	public ConnectorCapabilities getCapabilities() {
		return salesforceCapabilites;
	}
}
