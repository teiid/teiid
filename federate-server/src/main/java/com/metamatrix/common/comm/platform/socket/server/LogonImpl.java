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

package com.metamatrix.common.comm.platform.socket.server;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.LogonException;
import com.metamatrix.api.exception.security.MetaMatrixAuthenticationException;
import com.metamatrix.api.exception.security.SessionServiceException;
import com.metamatrix.common.api.MMURL;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.pool.CredentialMap;
import com.metamatrix.dqp.client.ResultsFuture;
import com.metamatrix.dqp.internal.process.DQPWorkContext;
import com.metamatrix.jdbc.api.ConnectionProperties;
import com.metamatrix.platform.security.api.Credentials;
import com.metamatrix.platform.security.api.ILogon;
import com.metamatrix.platform.security.api.LogonResult;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;
import com.metamatrix.platform.security.api.MetaMatrixSessionInfo;
import com.metamatrix.platform.security.api.service.SessionServiceInterface;
import com.metamatrix.platform.security.util.LogSecurityConstants;
import com.metamatrix.platform.service.api.exception.ServiceException;
import com.metamatrix.platform.service.api.exception.ServiceStateException;
import com.metamatrix.platform.util.ProductInfoConstants;
import com.metamatrix.server.ServerPlugin;

public class LogonImpl implements ILogon {
	
	private SessionServiceInterface service;
	private String clusterName;

	public LogonImpl(SessionServiceInterface service, String clusterName) {
		this.service = service;
		this.clusterName = clusterName;
	}

	public LogonResult logon(Properties connProps) throws LogonException,
			ComponentNotFoundException {
		
        String applicationName = connProps.getProperty(MMURL.CONNECTION.APP_NAME);
        // user may be null if using trustedToken to log on
        String user = connProps.getProperty(MMURL.CONNECTION.USER_NAME);
        // password may be null if using trustedToken to log on
        String password = connProps.getProperty(MMURL.CONNECTION.PASSWORD);
		String productName = connProps.getProperty(MMURL.CONNECTION.PRODUCT_NAME);
		Credentials credential = null;
        if (password != null) {
            credential = new Credentials(password.toCharArray());
        }
        
        if(connProps.containsKey(ConnectionProperties.PROP_CREDENTIALS)) {
            handleCredentials(connProps, user, password);
        }
        
		Object payload = connProps.get(MMURL.CONNECTION.CLIENT_TOKEN_PROP);
        
		try {
			MetaMatrixSessionInfo sessionInfo = service.createSession(user,
					credential, (Serializable) payload, applicationName,
					productName, connProps);
			// logon
			MetaMatrixSessionID sessionID = updateDQPContext(sessionInfo);
			LogManager.logDetail(LogSecurityConstants.CTX_SESSION, new Object[] {
					"Logon successful for \"", user, "\" - created SessionID \"", "" + sessionID, "\"" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			return new LogonResult(sessionID, sessionInfo.getUserName(), sessionInfo.getProductInfo(), service.getPingInterval(), clusterName);
		} catch (MetaMatrixAuthenticationException e) {
			throw new LogonException(e, e.getMessage());
		} catch (ServiceException e) {
			throw new ComponentNotFoundException(e, e.getMessage());
		} catch (SessionServiceException e) {
			throw new LogonException(e, e.getMessage());
		}
	}

	private void handleCredentials(Properties connProps, String user,
			String password) throws LogonException {
		// Check if both credentials AND session token are used - if so, this is an error
		if(connProps.containsKey(ConnectionProperties.PROP_CLIENT_SESSION_PAYLOAD)) {
		    throw new LogonException(ServerPlugin.Util.getString("LogonImpl.Invalid_use_of_credentials_and_token"));                //$NON-NLS-1$
		} 
		
		// Parse credentials and store CredentialMap as session token
		try { 
		    String credentials = connProps.getProperty(ConnectionProperties.PROP_CREDENTIALS);
		    CredentialMap credentialMap = null;
		    boolean defaultToLogon = false;
		    if(credentials.startsWith(ConnectionProperties.DEFAULT_TO_LOGON)) {
		        defaultToLogon = true;
		    }
		    int parenIndex = credentials.indexOf("("); //$NON-NLS-1$
		    if(parenIndex >= 0) {
		        credentialMap = CredentialMap.parseCredentials(credentials.substring(parenIndex));                    
		    } else {
		        credentialMap = new CredentialMap();
		    }
		    if(defaultToLogon) {
		        credentialMap.setDefaultCredentialMode(CredentialMap.MODE_USE_DEFAULTS_GLOBALLY);
		        Map<String, String> defaultCredentials = new HashMap<String, String>();
		        defaultCredentials.put(CredentialMap.USER_KEYWORD, user);
		        defaultCredentials.put(CredentialMap.PASSWORD_KEYWORD, password);
		        credentialMap.setDefaultCredentials(defaultCredentials);
		    } else {
		        credentialMap.setDefaultCredentialMode(CredentialMap.MODE_IGNORE_DEFAULTS);
		    }
		    connProps.put(ConnectionProperties.PROP_CLIENT_SESSION_PAYLOAD, credentialMap);
		} catch(ConnectorException e) {
		    throw new LogonException(e.getMessage());
		}
		
		// Remove credentials from info properties
		connProps.remove(ConnectionProperties.PROP_CREDENTIALS);
	}

	private MetaMatrixSessionID updateDQPContext(MetaMatrixSessionInfo sessionInfo) {
		MetaMatrixSessionID sessionID = sessionInfo.getSessionID();
		DQPWorkContext workContext = DQPWorkContext.getWorkContext();
		workContext.setSessionToken(sessionInfo.getSessionToken());
		workContext.setAppName(sessionInfo.getApplicationName());
		workContext.setSessionId(sessionInfo.getSessionID());
		workContext.setTrustedPayload(sessionInfo.getTrustedToken());
		workContext.setUserName(sessionInfo.getUserName());
		workContext.setVdbName(sessionInfo.getProductInfo(ProductInfoConstants.VIRTUAL_DB));
		workContext.setVdbVersion(sessionInfo.getProductInfo(ProductInfoConstants.VDB_VERSION));
		return sessionID;
	}
	
	public ResultsFuture<?> logoff() throws InvalidSessionException, MetaMatrixComponentException {
		try {
			this.service.closeSession(DQPWorkContext.getWorkContext().getSessionId());
		} catch (SessionServiceException e) {
			throw new MetaMatrixComponentException(e);
		} catch (ServiceException e) {
			throw new MetaMatrixComponentException(e);
		}
		return null;
	}

	public ResultsFuture<?> ping() throws InvalidSessionException,
			MetaMatrixComponentException {
		try {
			this.service.pingServer(DQPWorkContext.getWorkContext().getSessionId());
		} catch (ServiceStateException e) {
			throw new MetaMatrixComponentException(e);
		}
		return null;
	}

	@Override
	public void assertIdentity(MetaMatrixSessionID sessionId)
			throws InvalidSessionException, MetaMatrixComponentException {
		MetaMatrixSessionInfo sessionInfo;
		try {
			sessionInfo = this.service.validateSession(sessionId);
		} catch (SessionServiceException e) {
			throw new MetaMatrixComponentException(e);
		} catch (ServiceException e) {
			throw new MetaMatrixComponentException(e);
		}
		this.updateDQPContext(sessionInfo);
	}

}
