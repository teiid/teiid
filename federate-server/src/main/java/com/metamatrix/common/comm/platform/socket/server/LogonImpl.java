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
import java.util.Properties;

import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.InvalidSessionException;
import com.metamatrix.api.exception.security.LogonException;
import com.metamatrix.api.exception.security.MetaMatrixAuthenticationException;
import com.metamatrix.api.exception.security.SessionServiceException;
import com.metamatrix.common.api.MMURL_Properties;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.dqp.client.ResultsFuture;
import com.metamatrix.dqp.internal.process.DQPWorkContext;
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

public class LogonImpl implements ILogon {
	
	private SessionServiceInterface service;

	public LogonImpl(SessionServiceInterface service) {
		this.service = service;
	}

	public LogonResult logon(Properties connProps) throws LogonException,
			ComponentNotFoundException {
		
		Object payload = connProps.get(MMURL_Properties.JDBC.CLIENT_TOKEN_PROP);
        if (payload == null) {
        	payload = connProps.get(MMURL_Properties.JDBC.TRUSTED_PAYLOAD_PROP);
        }
        String applicationName = connProps.getProperty(MMURL_Properties.JDBC.APP_NAME);
        // user may be null if using trustedToken to log on
        String user = connProps.getProperty(MMURL_Properties.JDBC.USER_NAME);
        // password may be null if using trustedToken to log on
        String password = connProps.getProperty(MMURL_Properties.JDBC.PASSWORD);
		String productName = connProps.getProperty(MMURL_Properties.CONNECTION.PRODUCT_NAME);
		Credentials credential = null;
        if (password != null) {
            credential = new Credentials(password.toCharArray());
        }
		try {
			MetaMatrixSessionInfo sessionInfo = service.createSession(user,
					credential, (Serializable) payload, applicationName,
					productName, connProps);
			// logon
			MetaMatrixSessionID sessionID = updateDQPContext(sessionInfo);
			LogManager.logDetail(LogSecurityConstants.CTX_SESSION, new Object[] {
					"Logon successful for \"", user, "\" - created SessionID \"", "" + sessionID, "\"" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			return new LogonResult(sessionID, sessionInfo.getUserName(), sessionInfo.getProductInfo(), service.getPingInterval());
		} catch (MetaMatrixAuthenticationException e) {
			throw new LogonException(e, e.getMessage());
		} catch (ServiceException e) {
			throw new ComponentNotFoundException(e, e.getMessage());
		} catch (SessionServiceException e) {
			throw new LogonException(e, e.getMessage());
		}
	}

	private MetaMatrixSessionID updateDQPContext(MetaMatrixSessionInfo sessionInfo) {
		MetaMatrixSessionID sessionID = sessionInfo.getSessionID();
		DQPWorkContext workContext = DQPWorkContext.getWorkContext();
		workContext.setSessionToken(sessionInfo.getSessionToken());
		workContext.setAppName(sessionInfo.getApplicationName());
		workContext.setSessionId(sessionInfo.getSessionID());
		workContext.setTrustedPayload(sessionInfo.getSessionToken().getTrustedToken());
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
