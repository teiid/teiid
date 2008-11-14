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

import java.net.MalformedURLException;
import java.net.URL;

import com.metamatrix.connector.salesforce.Messages;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.SecurityContext;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.pool.ConnectionPool;
import com.metamatrix.data.pool.ConnectorIdentity;
import com.metamatrix.data.pool.SingleIdentity;
import com.metamatrix.data.pool.SourceConnection;
import com.metamatrix.data.pool.SourceConnectionFactory;
import com.metamatrix.data.pool.UserIdentity;

public class SalesforceSourceConnectionFactory implements
		SourceConnectionFactory {

	private ConnectorEnvironment connEnv;
	private ConnectionPool pool;
	private boolean singleIdentity;
	private String username;
	private String password;
	private URL url;
	private SourceConnection singleConnection;
	
	
	public void setPool(ConnectionPool pool) {
		this.pool = pool;
	}
	
	public SourceConnection createConnection(ConnectorIdentity ident)
			throws ConnectorException {
		SourceConnection result;
		if(singleIdentity) {
			if(null == singleConnection) {
				singleConnection = new SalesforceSourceConnection(username, password, url, connEnv, pool);
			}
			result = singleConnection;
		} else {
			result = new SalesforceSourceConnection(ident, url, connEnv, pool);
		}
		
		return result;
	}

	public ConnectorIdentity createIdentity(SecurityContext ctx)
			throws ConnectorException {
		
		ConnectorIdentity result;
		if(singleIdentity) {
			result = new SingleIdentity(ctx);
		} else {
			result = new UserIdentity(ctx);
		}
		return result;
	}

	public void initialize(ConnectorEnvironment env) throws ConnectorException {
		connEnv = env;
		
		String urlString =  connEnv.getProperties().getProperty("URL");
		if(null != urlString && 0 != urlString.length()) {
			try {
				url = new URL(urlString);
			} catch (MalformedURLException e) {
				throw new ConnectorException(e, e.getMessage());
			}
		}
		
		String username = connEnv.getProperties().getProperty("username");
		String password =  connEnv.getProperties().getProperty("password");
		
		//validate that both are empty or both have values
		if(null == username && null == password) {
			return;
		} else if ((null == username || username.equals("")) && (null != password || !password.equals("")) ||
				((null == password || password.equals("")) && (null != username || !username.equals("")))) {
					String msg = Messages.getString("SalesforceSourceConnectionFactory.Invalid.username.password.pair");
					connEnv.getLogger().logError(msg);
					throw new ConnectorException(msg);
		} else if(null != username || !username.equals("")) {
			singleIdentity = true;
			this.password = password;
			this.username = username;
		}
	}

	public boolean isSingleIdentity() {
		return singleIdentity;
	}

}
