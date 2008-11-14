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

import com.metamatrix.connector.salesforce.connection.impl.SalesforceSourceConnection;
import com.metamatrix.connector.salesforce.connection.impl.SalesforceSourceConnectionFactory;
import com.metamatrix.data.api.Connection;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ConnectorLogger;
import com.metamatrix.data.api.SecurityContext;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.pool.ConnectionPool;
import com.metamatrix.data.pool.SourceConnectionFactory;

public class Connector implements com.metamatrix.data.api.Connector {

	private ConnectorLogger logger;

	private ConnectorEnvironment connectorEnv;

	private ConnectorState state;
	
	private ConnectionPool connPool;
	
	private SourceConnectionFactory connFactory;

	// ///////////////////////////////////////////////////////////
	// Connector implementation
	// ///////////////////////////////////////////////////////////
	public Connection getConnection(SecurityContext secContext)
			throws ConnectorException {
		logger.logTrace("Enter SalesforceSourceConnection.getConnection()");
		SalesforceSourceConnection srcConnection = (SalesforceSourceConnection) connPool.obtain(secContext);
		Connection result = srcConnection.getConnection();
		logger.logTrace("Return SalesforceSourceConnection.getConnection()");
		return result;
	}

	public void initialize(ConnectorEnvironment env) throws ConnectorException {
		env.getLogger().logTrace("Enter SalesforceSourceConnection.initialize()");
		this.connectorEnv = env;
		this.logger = env.getLogger();
		this.state = new ConnectorState(env.getProperties(), getLogger());
		connFactory = new SalesforceSourceConnectionFactory();
		connFactory.initialize(env);
		connPool = new ConnectionPool(connFactory);
		connPool.initialize(env.getProperties());
		((SalesforceSourceConnectionFactory) connFactory).setPool(connPool);
		getLogger().logInfo(getLogPreamble().append("Initialized").toString()); //$NON-NLS-1$
		getLogger().logTrace(getLogPreamble()
				.append("Initialization Properties: " + env.getProperties()).toString()); //$NON-NLS-1$
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
}
