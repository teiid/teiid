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

import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.Properties;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.client.security.ILogon;
import org.teiid.client.security.InvalidSessionException;
import org.teiid.client.security.LogonException;
import org.teiid.client.security.LogonResult;
import org.teiid.client.security.SessionToken;
import org.teiid.client.util.ResultsFuture;
import org.teiid.core.CoreConstants;
import org.teiid.core.TeiidComponentException;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.dqp.service.SessionService;
import org.teiid.dqp.service.SessionServiceException;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.net.CommunicationException;
import org.teiid.net.TeiidURL;
import org.teiid.net.socket.AuthenticationType;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.security.Credentials;


public class LogonImpl implements ILogon {
	
	private SessionService service;
	private String clusterName;

	public LogonImpl(SessionService service, String clusterName) {
		this.service = service;
		this.clusterName = clusterName;
	}

	public LogonResult logon(Properties connProps) throws LogonException, TeiidComponentException, CommunicationException {
		if (this.service.getGssSecurityDomain() != null && connProps.get(ILogon.KRB5TOKEN) != null) {
			Subject user = this.service.getSubjectInContext(this.service.getGssSecurityDomain());
			if (user == null) {
				throw new LogonException(RuntimePlugin.Util.getString("krb5_user_not_found")); //$NON-NLS-1$
			}
			return logon(connProps, (byte[])connProps.get(ILogon.KRB5TOKEN));
		}
		
		if (!AuthenticationType.CLEARTEXT.equals(service.getAuthType())) {
			throw new LogonException(RuntimePlugin.Util.getString("wrong_logon_type_jaas")); //$NON-NLS-1$
		}
		return logon(connProps, null);
	}
	
	
	private LogonResult logon(Properties connProps, byte[] krb5ServiceTicket) throws LogonException {
		DQPWorkContext workContext = DQPWorkContext.getWorkContext();
		String oldSessionId = workContext.getSessionId();
        String applicationName = connProps.getProperty(TeiidURL.CONNECTION.APP_NAME);
        // user may be null if using trustedToken to log on
        String user = connProps.getProperty(TeiidURL.CONNECTION.USER_NAME, CoreConstants.DEFAULT_ANON_USERNAME);
        // password may be null if using trustedToken to log on
        String password = connProps.getProperty(TeiidURL.CONNECTION.PASSWORD);
		Credentials credential = null;
        if (password != null) {
            credential = new Credentials(password.toCharArray());
        }
        
        boolean adminConnection = Boolean.parseBoolean(connProps.getProperty(TeiidURL.CONNECTION.ADMIN));
		try {
			SessionMetadata sessionInfo = service.createSession(user,credential, applicationName, connProps, adminConnection, true);
	        updateDQPContext(sessionInfo);
	        if (DQPWorkContext.getWorkContext().getClientAddress() == null) {
				sessionInfo.setEmbedded(true);
	        }
	        if (oldSessionId != null) {
	        	try {
					this.service.closeSession(oldSessionId);
				} catch (InvalidSessionException e) {
				}
	        }
			LogonResult result = new LogonResult(sessionInfo.getSessionToken(), sessionInfo.getVDBName(), sessionInfo.getVDBVersion(), clusterName);
			if (krb5ServiceTicket != null) {
				result.addProperty(ILogon.KRB5TOKEN, krb5ServiceTicket);
			}
			return result;
		} catch (LoginException e) {
			throw new LogonException(e.getMessage());
		} catch (SessionServiceException e) {
			throw new LogonException(e, e.getMessage());
		}
	}
	  
	class GssAction implements PrivilegedAction<GSSResult> {
		byte[] serviceTicket;
		
		public GssAction(byte[] ticket) {
			this.serviceTicket = ticket;
		}
		
		@Override
		public GSSResult run() {
			GSSContext context = null;
			try {
				GSSManager manager = GSSManager.getInstance();
				context = manager.createContext((GSSCredential)null);				
				this.serviceTicket = context.acceptSecContext(this.serviceTicket, 0, this.serviceTicket.length);				
				return new GSSResult(context, serviceTicket);
			} catch (GSSException e) {
				LogManager.logError(LogConstants.CTX_SECURITY, e, "Kerberos context login failed"); //$NON-NLS-1$
			}
			return null;
		}	
	}
	
	class GSSResult {
		GSSContext context;
		byte[] serviceTicket;
		public GSSResult(GSSContext context, byte[] serviceTicket) {
			this.context = context;
			this.serviceTicket = serviceTicket;
		}
	}
	
	@Override
	public LogonResult neogitiateGssLogin(Properties connProps, byte[] serviceTicket, boolean createSession) throws LogonException {
		
		if (!AuthenticationType.GSS.equals(service.getAuthType())) {
			throw new LogonException(RuntimePlugin.Util.getString("wrong_logon_type_krb5")); //$NON-NLS-1$
		}		
		
        String user = connProps.getProperty(TeiidURL.CONNECTION.USER_NAME);
        String password = connProps.getProperty(TeiidURL.CONNECTION.PASSWORD);		
		
		try {
			String securityDomain = service.getGssSecurityDomain();
			if (securityDomain == null) {
				throw new LogonException(RuntimePlugin.Util.getString("no_security_domains")); //$NON-NLS-1$
			}
			// If this KRB5 and using keytab, user and password callback handler never gets called 
			LoginContext ctx = service.createLoginContext(securityDomain, user, password);
			ctx.login();
			Subject subject = ctx.getSubject();
			GSSResult result =  Subject.doAs(subject, new GssAction(serviceTicket));
			if (result == null) {
				throw new LogonException(RuntimePlugin.Util.getString("krb5_login_failed")); //$NON-NLS-1$
			}
			
			if (result.context.isEstablished()) {
				service.associateSubjectInContext(securityDomain, subject);
			}
			
			if (!result.context.isEstablished() || !createSession) {
				LogonResult logonResult = new LogonResult(new SessionToken(0, "temp"), "internal", 0, "internal"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
				logonResult.addProperty(ILogon.KRB5TOKEN, result.serviceTicket);
				logonResult.addProperty(ILogon.KRB5_ESTABLISHED, new Boolean(result.context.isEstablished()));
				return logonResult;
			}		
			
			LogManager.logDetail(LogConstants.CTX_SECURITY, "Kerberos context established"); //$NON-NLS-1$
			//connProps.setProperty(TeiidURL.CONNECTION.PASSTHROUGH_AUTHENTICATION, "true"); //$NON-NLS-1$
			return logon(connProps, result.serviceTicket);
		} catch (LoginException e) {
			throw new LogonException(e, RuntimePlugin.Util.getString("krb5_login_failed")); //$NON-NLS-1$
		} 
	}
	
	private String updateDQPContext(SessionMetadata s) {
		String sessionID = s.getSessionId();
		
		DQPWorkContext workContext = DQPWorkContext.getWorkContext();
		workContext.setSession(s);
		return sessionID;
	}
		
	public ResultsFuture<?> logoff() throws InvalidSessionException {
		this.service.closeSession(DQPWorkContext.getWorkContext().getSessionId());
		DQPWorkContext.getWorkContext().getSession().setSessionId(null);
		return ResultsFuture.NULL_FUTURE;
	}

	public ResultsFuture<?> ping() throws InvalidSessionException,TeiidComponentException {
		// ping is double used to alert the aliveness of the client, as well as check the server instance is 
		// alive by socket server instance, so that they can be cached.
		String id = DQPWorkContext.getWorkContext().getSessionId();
		if (id != null) {
			this.service.pingServer(id);
		}
		LogManager.logTrace(LogConstants.CTX_SECURITY, "Ping", id); //$NON-NLS-1$
		return ResultsFuture.NULL_FUTURE;
	}
	
	@Override
	public ResultsFuture<?> ping(Collection<String> sessions)
			throws TeiidComponentException, CommunicationException {
		for (String string : sessions) {
			try {
				this.service.pingServer(string);
			} catch (InvalidSessionException e) {
			}
		}
		return ResultsFuture.NULL_FUTURE;
	}

	@Override
	public void assertIdentity(SessionToken checkSession) throws InvalidSessionException, TeiidComponentException {
		SessionMetadata sessionInfo = null;
		try {
			sessionInfo = this.service.validateSession(checkSession.getSessionID());
		} catch (SessionServiceException e) {
			throw new TeiidComponentException(e);
		}
		
		if (sessionInfo == null) {
			throw new InvalidSessionException();
		}
		
		SessionToken st = sessionInfo.getSessionToken();
		if (!st.equals(checkSession)) {
			throw new InvalidSessionException();
		}
		this.updateDQPContext(sessionInfo);
	}
}
