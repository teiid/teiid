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

package org.teiid.transport;

import java.util.Properties;

import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.security.Credentials;

import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.LogonException;
import com.metamatrix.api.exception.security.SessionServiceException;
import com.metamatrix.common.api.MMURL;
import com.metamatrix.common.comm.api.ServerConnection;
import com.metamatrix.core.CoreConstants;
import com.metamatrix.dqp.client.ResultsFuture;
import com.metamatrix.dqp.service.SessionService;
import com.metamatrix.platform.security.api.ILogon;
import com.metamatrix.platform.security.api.LogonResult;
import com.metamatrix.platform.security.api.SessionToken;

public class LogonImpl implements ILogon {
	
	private SessionService service;
	private String clusterName;

	public LogonImpl(SessionService service, String clusterName) {
		this.service = service;
		this.clusterName = clusterName;
	}

	public LogonResult logon(Properties connProps) throws LogonException,
			ComponentNotFoundException {
		
        String applicationName = connProps.getProperty(MMURL.CONNECTION.APP_NAME);
        // user may be null if using trustedToken to log on
        String user = connProps.getProperty(MMURL.CONNECTION.USER_NAME, CoreConstants.DEFAULT_ANON_USERNAME);
        // password may be null if using trustedToken to log on
        String password = connProps.getProperty(MMURL.CONNECTION.PASSWORD);
		Credentials credential = null;
        if (password != null) {
            credential = new Credentials(password.toCharArray());
        }
        
        boolean adminConnection = Boolean.parseBoolean(connProps.getProperty(MMURL.CONNECTION.ADMIN));
		try {
			SessionMetadata sessionInfo = service.createSession(user,credential, applicationName, connProps, adminConnection);
	        
			long sessionID = updateDQPContext(sessionInfo, adminConnection);
			if (Boolean.parseBoolean(connProps.getProperty(ServerConnection.LOCAL_CONNECTION))) {
				service.setLocalSession(sessionID);
			}
			return new LogonResult(sessionInfo.getAttachment(SessionToken.class), sessionInfo.getVDBName(), sessionInfo.getVDBVersion(), clusterName);
		} catch (LoginException e) {
			throw new LogonException(e.getMessage());
		} catch (SessionServiceException e) {
			throw new LogonException(e, e.getMessage());
		}
	}

	private long updateDQPContext(SessionMetadata s, boolean adminConnection) {
		long sessionID = s.getSessionId();
		
		DQPWorkContext workContext = DQPWorkContext.getWorkContext();
		if (workContext == null) {
			workContext = new DQPWorkContext();
		}
		workContext.setSessionToken(s.getAttachment(SessionToken.class));
		workContext.setAppName(s.getApplicationName());
		
		LoginContext loginContext = s.getAttachment(LoginContext.class);
		if (loginContext != null) {
			workContext.setSubject(loginContext.getSubject());
			workContext.setSecurityDomain(s.getSecurityDomain());
			workContext.setSecurityContext(s.getAttachment("SecurityContext"));
		}
		
		VDBMetaData vdb = s.getAttachment(VDBMetaData.class);
		if (vdb != null) {
			workContext.setVdbName(vdb.getName());
			workContext.setVdbVersion(vdb.getVersion());		
			workContext.setVdb(vdb);
		}
		
		if (adminConnection) {
			workContext.markAsAdmin();
		}
		DQPWorkContext.setWorkContext(workContext);
		return sessionID;
	}
		
	public ResultsFuture<?> logoff() throws InvalidSessionException {
		this.service.closeSession(DQPWorkContext.getWorkContext().getSessionId());
		DQPWorkContext.getWorkContext().reset();
		return ResultsFuture.NULL_FUTURE;
	}

	public ResultsFuture<?> ping() throws InvalidSessionException,MetaMatrixComponentException {
		// ping is double used to alert the aliveness of the client, as well as check the server instance is 
		// alive by socket server instance, so that they can be cached.
		long id = DQPWorkContext.getWorkContext().getSessionId();
		if (id != -1) {
			this.service.pingServer(id);
		}
		return ResultsFuture.NULL_FUTURE;
	}

	@Override
	public void assertIdentity(SessionToken checkSession) throws InvalidSessionException, MetaMatrixComponentException {
		SessionMetadata sessionInfo = null;
		try {
			sessionInfo = this.service.validateSession(checkSession.getSessionID());
		} catch (SessionServiceException e) {
			throw new MetaMatrixComponentException(e);
		}
		
		if (sessionInfo == null) {
			throw new InvalidSessionException();
		}
		
		SessionToken st = sessionInfo.getAttachment(SessionToken.class);
		if (!st.equals(checkSession)) {
			throw new InvalidSessionException();
		}
		this.updateDQPContext(sessionInfo, false);
	}
}
