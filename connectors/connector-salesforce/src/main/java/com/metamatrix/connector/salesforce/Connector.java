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

import javax.security.auth.Subject;

import org.teiid.connector.api.Connection;
import org.teiid.connector.api.ConnectionContext;
import org.teiid.connector.api.ConnectorCapabilities;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;

import com.metamatrix.connector.salesforce.connection.SalesforceConnection;

public class Connector extends org.teiid.connector.basic.BasicConnector {

	private SalesForceManagedConnectionFactory connectorEnv;
	private boolean singleIdentity;

	// ///////////////////////////////////////////////////////////
	// Connector implementation
	// ///////////////////////////////////////////////////////////
	@Override
	public Connection getConnection() throws ConnectorException {
		getLogger().logTrace("Enter SalesforceSourceConnection.getConnection()");
		Connection connection = null;
		if (singleIdentity) {
			connection = new SalesforceConnection(connectorEnv);
		} else {
			// if the security domain is enabled, then subject is not null.
			Subject subject = ConnectionContext.getSubject();
			if(subject != null) {
		    	connection = new SalesforceConnection(subject, connectorEnv);
		    } else { 
		    	throw new ConnectorException("Unknown trusted payload type"); 
		    }
		}
		getLogger().logTrace("Return SalesforceSourceConnection.getConnection()");
		return connection;
	}

	@Override
	public void initialize(ConnectorEnvironment env) throws ConnectorException {
		super.initialize(env);
		
		this.connectorEnv = (SalesForceManagedConnectionFactory)env;
		
		getLogger().logInfo("Started"); //$NON-NLS-1$
		getLogger().logInfo("Initialized"); //$NON-NLS-1$
		getLogger().logTrace("Initialization Properties: " + this.connectorEnv.toString()); //$NON-NLS-1$
		
		//validate that both are empty or both have values
		if(this.connectorEnv.getUsername() != null) {
			singleIdentity = true;
		}

		getLogger().logTrace("Return SalesforceSourceConnection.initialize()");
	}


	/////////////////////////////////////////////////////////////
	//Utilities
	/////////////////////////////////////////////////////////////

	private ConnectorLogger getLogger(){
		return this.config.getLogger();
	}


	@Override
    public Class<? extends ConnectorCapabilities> getDefaultCapabilities() {
    	return SalesforceCapabilities.class;
    }	
}
