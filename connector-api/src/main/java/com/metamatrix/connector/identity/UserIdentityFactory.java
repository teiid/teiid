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

package com.metamatrix.connector.identity;

import com.metamatrix.connector.DataPlugin;
import com.metamatrix.connector.api.ConnectorException;
import com.metamatrix.connector.api.CredentialMap;
import com.metamatrix.connector.api.ExecutionContext;

/**
 * Segregates connections by user determined by the CredentialMap.  
 */
public class UserIdentityFactory implements ConnectorIdentityFactory {

	private boolean useCredentialMap;
	private boolean adminConnectionsAllowed = true;
	private String connectorName;
	
	@Override
	public ConnectorIdentity createIdentity(ExecutionContext context)
			throws ConnectorException {
		if (context == null) {
			if (adminConnectionsAllowed) {
				return new SingleIdentity();
			}
			throw new ConnectorException(DataPlugin.Util.getString("UserIdentityFactory.single_identity_not_supported")); //$NON-NLS-1$
		}
		Object payload = context.getTrustedPayload();
		if (!(payload instanceof CredentialMap)) {
			if (useCredentialMap) {
				throw new ConnectorException(DataPlugin.Util.getString("UserIdentityFactory.single_identity_not_supported")); //$NON-NLS-1$
			}
			return new SingleIdentity();
		}
		CredentialMap credMap = (CredentialMap)payload;
		String user = credMap.getUser(connectorName);
		String password = credMap.getPassword(connectorName);
		if (user == null || password == null) {
			throw new ConnectorException("Payload missing credentials for " + connectorName); //$NON-NLS-1$
		}
		return new UserIdentity(context.getUser(), user, password);
	}
	
	public void setConnectorName(String connectorName) {
		this.connectorName = connectorName;
	}
	
	public void setUseCredentialMap(boolean useCredentialMap) {
		this.useCredentialMap = useCredentialMap;
	}
	
	public void setAdminConnectionsAllowed(boolean adminConnectionsAllowed) {
		this.adminConnectionsAllowed = adminConnectionsAllowed;
	}
	
}
