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

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import org.ietf.jgss.GSSCredential;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.client.security.*;
import org.teiid.client.util.ResultsFuture;
import org.teiid.core.CoreConstants;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.util.Base64;
import org.teiid.core.util.LRUCache;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.dqp.internal.process.DQPWorkContext.Version;
import org.teiid.dqp.service.GSSResult;
import org.teiid.dqp.service.SessionService;
import org.teiid.dqp.service.SessionServiceException;
import org.teiid.jdbc.BaseDataSource;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.net.CommunicationException;
import org.teiid.net.TeiidURL;
import org.teiid.net.socket.AuthenticationType;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.security.Credentials;
import org.teiid.security.SecurityHelper;


public class LogonImpl implements ILogon {
	
	private SessionService service;
	private String clusterName;
	protected Map<String, Object> gssServiceTickets = Collections.synchronizedMap(new LRUCache<String, Object>()); 

	public LogonImpl(SessionService service, String clusterName) {
		this.service = service;
		this.clusterName = clusterName;
	}

	public LogonResult logon(Properties connProps) throws LogonException {
		String vdbName = connProps.getProperty(BaseDataSource.VDB_NAME);
		String vdbVersion = connProps.getProperty(BaseDataSource.VDB_VERSION);
		String user = connProps.getProperty(BaseDataSource.USER_NAME);
		
		AuthenticationType authType = this.service.getAuthenticationType(vdbName, vdbVersion, user);
		
		// the presence of the KRB5 token take as GSS based login.
		if (connProps.get(ILogon.KRB5TOKEN) != null) {
			if (authType == AuthenticationType.GSS) {
				Object previous = null;
				boolean assosiated = false;
				SecurityHelper securityHelper = service.getSecurityHelper();
				try {
					byte[] krb5Token = (byte[])connProps.get(ILogon.KRB5TOKEN);
					Object securityContext = this.gssServiceTickets.remove(Base64.encodeBytes(MD5(krb5Token)));
					if (securityContext == null) {
						 throw new LogonException(RuntimePlugin.Event.TEIID40054, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40054));
					}				
					previous = securityHelper.associateSecurityContext(securityContext);
					assosiated = true;
					return logon(connProps, krb5Token, AuthenticationType.GSS);
				} finally {
					if (assosiated) {
						securityHelper.associateSecurityContext(previous);
					}
				}
			} else {
				//shouldn't really get here, but we'll try user name password anyway
			}
		} else if (authType == AuthenticationType.GSS) {
			Version v = DQPWorkContext.getWorkContext().getClientVersion();
			//send a login result with a GSS challange
			if (v.compareTo(Version.EIGHT_7) >= 0) {
				LogonResult result = new LogonResult();
				result.addProperty(ILogon.AUTH_TYPE, authType); 
				return result;
			}
			//throw an exception
			throw new LogonException(RuntimePlugin.Event.TEIID40055, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40055, authType));
		}
		
		//default to username password
		
		if (!AuthenticationType.USERPASSWORD.equals(authType)) {
			 throw new LogonException(RuntimePlugin.Event.TEIID40055, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40055, authType));
		}		
		return logon(connProps, null, AuthenticationType.USERPASSWORD);
	}

	private LogonResult logon(Properties connProps, byte[] krb5ServiceTicket, AuthenticationType authType) throws LogonException {

		String vdbName = connProps.getProperty(BaseDataSource.VDB_NAME);
		String vdbVersion = connProps.getProperty(BaseDataSource.VDB_VERSION);
        String applicationName = connProps.getProperty(TeiidURL.CONNECTION.APP_NAME);
        String user = connProps.getProperty(TeiidURL.CONNECTION.USER_NAME, CoreConstants.DEFAULT_ANON_USERNAME);
        String password = connProps.getProperty(TeiidURL.CONNECTION.PASSWORD);
		Credentials credential = null;
        if (password != null) {
            credential = new Credentials(password.toCharArray());
        }
        
		try {
			SessionMetadata sessionInfo = service.createSession(vdbName, vdbVersion, authType, user,credential, applicationName, connProps);
			
			if (connProps.get(GSSCredential.class.getName()) != null) {
			    addCredentials(sessionInfo.getSubject(), (GSSCredential)connProps.get(GSSCredential.class.getName()));
			}
			
	        updateDQPContext(sessionInfo);
	        if (DQPWorkContext.getWorkContext().getClientAddress() == null) {
				sessionInfo.setEmbedded(true);
	        }
	        //if (oldSessionId != null) {
	        	//TODO: we should be smarter about disassociating the old sessions from the client.  we'll just rely on 
	        	//ping based clean up
	        //}
			LogonResult result = new LogonResult(sessionInfo.getSessionToken(), sessionInfo.getVDBName(), sessionInfo.getVDBVersion(), clusterName);
			if (krb5ServiceTicket != null) {
				result.addProperty(ILogon.KRB5TOKEN, krb5ServiceTicket);
			}
			return result;
		} catch (LoginException e) {
			 throw new LogonException(e);
		} catch (SessionServiceException e) {
			 throw new LogonException(e);
		}
	}
		
	@Override
	public LogonResult neogitiateGssLogin(Properties connProps, byte[] serviceTicket, boolean createSession) throws LogonException {
		String vdbName = connProps.getProperty(BaseDataSource.VDB_NAME);
		String vdbVersion = connProps.getProperty(BaseDataSource.VDB_VERSION);
		String user = connProps.getProperty(BaseDataSource.USER_NAME);
		
		AuthenticationType authType = this.service.getAuthenticationType(vdbName, vdbVersion, user);
		
		if (!AuthenticationType.GSS.equals(authType)) {
			 throw new LogonException(RuntimePlugin.Event.TEIID40055, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40055, "Kerberos")); //$NON-NLS-1$
		}
		
		try {
			// Using SPENGO security domain establish a token and subject.
			GSSResult result = service.neogitiateGssLogin(user, vdbName, vdbVersion, serviceTicket);
			if (result == null) {
				 throw new LogonException(RuntimePlugin.Event.TEIID40014, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40014));
			}
			
			if (result.isAuthenticated()) {
				LogManager.logDetail(LogConstants.CTX_SECURITY, "Kerberos context established"); //$NON-NLS-1$	
				connProps.setProperty(TeiidURL.CONNECTION.USER_NAME, result.getUserName());
		    	this.gssServiceTickets.put(Base64.encodeBytes(MD5(result.getServiceToken())), result.getSecurityContext());
			}
						
			// kerberoes (odbc) will always return here from below block
			if (!result.isAuthenticated() || !createSession) {
				LogonResult logonResult = new LogonResult(new SessionToken(0, "temp"), "internal", 0, "internal"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
				logonResult.addProperty(ILogon.KRB5TOKEN, result.getServiceToken());
				logonResult.addProperty(ILogon.KRB5_ESTABLISHED, new Boolean(result.isAuthenticated()));
				if (result.isAuthenticated()) {
				    logonResult.addProperty(GSSCredential.class.getName(), result.getDelegationCredentail());
				}
				return logonResult;
			}		
			
			// GSS API (jdbc) will make the session in one single call			
			connProps.put(ILogon.KRB5TOKEN, result.getServiceToken());
			if(result.getDelegationCredentail() != null){
				connProps.put(GSSCredential.class.getName(), result.getDelegationCredentail());
			}
			LogonResult logonResult =  logon(connProps);
			return logonResult;
		} catch (LoginException e) {
			 throw new LogonException(RuntimePlugin.Event.TEIID40014, e, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40014));
		}
	}
	
	protected static byte[] MD5(byte[] content) {
		try {
			java.security.MessageDigest md = java.security.MessageDigest.getInstance("MD5"); //$NON-NLS-1$
			return md.digest(content);
		} catch (java.security.NoSuchAlgorithmException e) {
			return content;
		}		
	}
	
	private String updateDQPContext(SessionMetadata s) {
		String sessionID = s.getSessionId();
		
		DQPWorkContext workContext = DQPWorkContext.getWorkContext();
		workContext.setSession(s);
		return sessionID;
	}
		
	public ResultsFuture<?> logoff() throws InvalidSessionException {
		DQPWorkContext workContext = DQPWorkContext.getWorkContext();
		this.service.closeSession(workContext.getSessionId());
		workContext.getSession().setSessionId(null);
		workContext.getSession().setSecurityContext(null);
		workContext.getSession().getSessionVariables().clear();
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
		if (checkSession == null) {
			//disassociate
			this.updateDQPContext(new SessionMetadata());
			return;
		}
		SessionMetadata sessionInfo = null;
		try {
			sessionInfo = this.service.validateSession(checkSession.getSessionID());
		} catch (SessionServiceException e) {
			 throw new TeiidComponentException(RuntimePlugin.Event.TEIID40062, e);
		}
		
		if (sessionInfo == null) {
			 throw new InvalidSessionException(RuntimePlugin.Event.TEIID40063);
		}
		
		SessionToken st = sessionInfo.getSessionToken();
		if (!st.equals(checkSession)) {
			 throw new InvalidSessionException(RuntimePlugin.Event.TEIID40064);
		}
		this.updateDQPContext(sessionInfo);
	}
	
	public SessionService getSessionService() {
		return service;
	}
	
	static void addCredentials(final Subject subject, final GSSCredential cred) {
	    if (System.getSecurityManager() == null) {
	        subject.getPrivateCredentials().add(cred);
	    }
	    
	    AccessController.doPrivileged(new PrivilegedAction<Void>() { 
	        public Void run() {
	            subject.getPrivateCredentials().add(cred);
	            return null;
	        }
	    });        
	}	
}
