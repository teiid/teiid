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

package org.teiid.services;


import java.io.IOException;
import java.security.Principal;
import java.security.acl.Group;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.teiid.adminapi.VDB;
import org.teiid.adminapi.VDB.ConnectionType;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.client.security.InvalidSessionException;
import org.teiid.client.security.LogonException;
import org.teiid.client.security.SessionToken;
import org.teiid.core.util.ArgCheck;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.deployers.VDBRepository;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.dqp.service.GSSResult;
import org.teiid.dqp.service.SessionService;
import org.teiid.dqp.service.SessionServiceException;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.net.ServerConnection;
import org.teiid.net.TeiidURL;
import org.teiid.net.socket.AuthenticationType;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.security.Credentials;
import org.teiid.security.SecurityHelper;


/**
 * This class serves as the primary implementation of the Session Service.
 */
public class SessionServiceImpl implements SessionService {
	public static final String GSS_PATTERN_PROPERTY = "gss-pattern"; //$NON-NLS-1$
	public static final String PASSWORD_PATTERN_PROPERTY = "password-pattern"; //$NON-NLS-1$
	public static final String SECURITY_DOMAIN_PROPERTY = "security-domain"; //$NON-NLS-1$
	public static final String AUTHENTICATION_TYPE_PROPERTY = "authentication-type"; //$NON-NLS-1$
	public static final String AT = "@"; //$NON-NLS-1$
	/*
	 * Configuration state
	 */
    private long sessionMaxLimit = DEFAULT_MAX_SESSIONS;
	private long sessionExpirationTimeLimit = DEFAULT_SESSION_EXPIRATION;
	private AuthenticationType defaultAuthenticationType = AuthenticationType.USERPASSWORD;
	
	private static boolean CHECK_PING = PropertiesUtils.getBooleanProperty(System.getProperties(), "org.teiid.checkPing", true);
	/*
	 * Injected state
	 */
	private VDBRepository vdbRepository;
    protected SecurityHelper securityHelper;

    private DQPCore dqp;

    private Map<String, SessionMetadata> sessionCache = new ConcurrentHashMap<String, SessionMetadata>();
    private Timer sessionMonitor = new Timer("SessionMonitor", true); //$NON-NLS-1$    
    private List<String> securityDomainNames;
    
    public void setSecurityDomain(String domainName) {
    	if (domainName == null) {
    		this.securityDomainNames = null;
    	} else {
    		//allow for legacy multiple domain names
    		this.securityDomainNames = Arrays.asList(domainName.split(",")); //$NON-NLS-1$
    	}
    }
    
    // -----------------------------------------------------------------------------------
    // S E R V I C E - R E L A T E D M E T H O D S
    // -----------------------------------------------------------------------------------

    private void monitorSessions() {
		long currentTime = System.currentTimeMillis();
		for (SessionMetadata info : sessionCache.values()) {
			try {
    			if (CHECK_PING && !info.isEmbedded() && currentTime - info.getLastPingTime() > ServerConnection.PING_INTERVAL * 5) {
    				LogManager.logInfo(LogConstants.CTX_SECURITY, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40007, info.getSessionId()));
    				closeSession(info.getSessionId());
    			} else if (sessionExpirationTimeLimit > 0 && currentTime - info.getCreatedTime() > sessionExpirationTimeLimit) {
    				LogManager.logInfo(LogConstants.CTX_SECURITY,RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40008, info.getSessionId()));
    				closeSession(info.getSessionId());
    			}
			} catch (Exception e) {
				LogManager.logDetail(LogConstants.CTX_SECURITY, e, "error running session monitor, unable to monitor:", info.getSessionId()); //$NON-NLS-1$
			}
		}
	}

	@Override
	public void closeSession(String sessionID) throws InvalidSessionException {
		LogManager.logDetail(LogConstants.CTX_SECURITY, new Object[] {"closeSession", sessionID}); //$NON-NLS-1$
		SessionMetadata info = getSessionInfo(sessionID, true);
		if (info.getVDBName() != null) {
            try {
    			dqp.terminateSession(info.getSessionId());
            } catch (Exception e) {
                LogManager.logWarning(LogConstants.CTX_SECURITY,e, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40018));
            }
		}
	}
	
	@Override
	public SessionMetadata createSession(String vdbName,
			String vdbVersion, AuthenticationType authType, String userName,
			Credentials credentials, String applicationName, Properties properties)
			throws LoginException, SessionServiceException {
		ArgCheck.isNotNull(applicationName);
        ArgCheck.isNotNull(properties);
        
        Object securityContext = null;
        Subject subject = null;
        
        // Validate VDB and version if logging on to server product...
        VDBMetaData vdb = null;
        if (vdbName != null) {
        	vdb = getActiveVDB(vdbName, vdbVersion);
        }

        if (sessionMaxLimit > 0 && getActiveSessionsCount() >= sessionMaxLimit) {
             throw new SessionServiceException(RuntimePlugin.Event.TEIID40043, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40043, new Long(sessionMaxLimit)));
        }
        
        String securityDomain = getSecurityDomain(userName, vdbName, vdbVersion, vdb);
        
		if (securityDomain != null) {
	        // Authenticate user...
	        // if not authenticated, this method throws exception
            LogManager.logDetail(LogConstants.CTX_SECURITY, new Object[] {"authenticateUser", userName, applicationName}); //$NON-NLS-1$

        	boolean onlyAllowPassthrough = Boolean.valueOf(properties.getProperty(TeiidURL.CONNECTION.PASSTHROUGH_AUTHENTICATION, "false")); //$NON-NLS-1$
        	TeiidLoginContext membership = null;
        	if (onlyAllowPassthrough || authType.equals(AuthenticationType.GSS)) {
                membership = passThroughLogin(userName, securityDomain);
        	} else {
	        	membership = authenticate(userName, credentials, applicationName, securityDomain);
        	}
	        userName = membership.getUserName();
	        securityDomain = membership.getSecurityDomain();
	        securityContext = membership.getSecurityContext();
	        subject = membership.getSubject();
		}
		else {
        	LogManager.logDetail(LogConstants.CTX_SECURITY, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40117)); 
		}
        
        long creationTime = System.currentTimeMillis();

        // Return a new session info object
        SessionMetadata newSession = new SessionMetadata();
        newSession.setSessionToken(new SessionToken(userName));
        newSession.setSessionId(newSession.getSessionToken().getSessionID());
        newSession.setUserName(userName);
        newSession.setCreatedTime(creationTime);
        newSession.setApplicationName(applicationName);
        newSession.setClientHostName(properties.getProperty(TeiidURL.CONNECTION.CLIENT_HOSTNAME));
        newSession.setIPAddress(properties.getProperty(TeiidURL.CONNECTION.CLIENT_IP_ADDRESS));
        newSession.setClientHardwareAddress(properties.getProperty(TeiidURL.CONNECTION.CLIENT_MAC));
        newSession.setSecurityDomain(securityDomain);
        if (vdb != null) {
	        newSession.setVDBName(vdb.getName());
	        newSession.setVDBVersion(vdb.getVersion());
        }
        
        // these are local no need for monitoring.
        newSession.setSubject(subject);
        newSession.setSecurityContext(securityContext);
        newSession.setVdb(vdb);
        LogManager.logDetail(LogConstants.CTX_SECURITY, new Object[] {"Logon successful, created", newSession }); //$NON-NLS-1$ 
        this.sessionCache.put(newSession.getSessionId(), newSession);
        return newSession;
	}

	public TeiidLoginContext passThroughLogin(String userName, String securityDomain) throws LoginException {
		Subject existing = this.securityHelper.getSubjectInContext(securityDomain);
		if (existing != null) {
			return new TeiidLoginContext(getUserName(existing, userName)+AT+securityDomain, existing, securityDomain, this.securityHelper.getSecurityContext());
		}
		throw new LoginException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40087));
	}
	
	private String getUserName(Subject subject, String userName) {
		Set<Principal> principals = subject.getPrincipals();
		for (Principal p:principals) {
			if (p instanceof Group) {
				continue;
			}
			return p.getName();
		}
		return getBaseUsername(userName);
	}
	
	/**
	 * 
	 * @param userName
	 * @param credentials
	 * @param applicationName
	 * @param domains
	 * @return
	 * @throws LoginException
	 */
	protected TeiidLoginContext authenticate(String userName, Credentials credentials, String applicationName, String securityDomain)
			throws LoginException {
		return passThroughLogin(userName, securityDomain);
	}

	protected VDBMetaData getActiveVDB(String vdbName, String vdbVersion) throws SessionServiceException {
		VDBMetaData vdb = null;
		
		// handle the situation when the version is part of the vdb name.
		
		int firstIndex = vdbName.indexOf('.');
		int lastIndex = vdbName.lastIndexOf('.');
		if (firstIndex != -1) {
			if (firstIndex != lastIndex || vdbVersion != null) {
				 throw new SessionServiceException(RuntimePlugin.Event.TEIID40044, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40044, vdbName, vdbVersion));
			}
			vdbVersion = vdbName.substring(firstIndex+1);
			vdbName = vdbName.substring(0, firstIndex);
		}
		
		try {
			if (vdbVersion == null) {
				vdbVersion = "latest"; //$NON-NLS-1$
				vdb = this.vdbRepository.getLiveVDB(vdbName);
			}
			else {
				vdb = this.vdbRepository.getLiveVDB(vdbName, Integer.parseInt(vdbVersion));
			}         
		} catch (NumberFormatException e) {
			 throw new SessionServiceException(RuntimePlugin.Event.TEIID40045, e, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40045, vdbVersion));
		}
		if (vdb == null) {
			 throw new SessionServiceException(RuntimePlugin.Event.TEIID40046, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40046, vdbName, vdbVersion));
		}
		if (vdb.getConnectionType() == ConnectionType.NONE) {
			 throw new SessionServiceException(RuntimePlugin.Event.TEIID40048, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40048, vdbName, vdbVersion));
		}
		return vdb;
	}

	@Override
	public LoginContext createLoginContext(final String securityDomain, final String user, final String password) throws LoginException{
		CallbackHandler handler = new CallbackHandler() {
			@Override
			public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
				for (int i = 0; i < callbacks.length; i++) {
					if (callbacks[i] instanceof NameCallback) {
						NameCallback nc = (NameCallback)callbacks[i];
						nc.setName(user);
					} else if (callbacks[i] instanceof PasswordCallback) {
						PasswordCallback pc = (PasswordCallback)callbacks[i];
						if (password != null) {
							pc.setPassword(password.toCharArray());
						}
					} else {
						throw new UnsupportedCallbackException(callbacks[i], "Unrecognized Callback"); //$NON-NLS-1$
					}
				}
			}
		}; 		
		
		return new LoginContext(securityDomain, handler);
	}
	
	@Override
	public Collection<SessionMetadata> getActiveSessions() {
		return new ArrayList<SessionMetadata>(this.sessionCache.values());
	}
	
	@Override
	public SessionMetadata getActiveSession(String sessionID) {
		return this.sessionCache.get(sessionID);
	}	

	@Override
	public int getActiveSessionsCount() throws SessionServiceException{
		return this.sessionCache.size();
	}

	@Override
	public Collection<SessionMetadata> getSessionsLoggedInToVDB(String VDBName, int vdbVersion)
			throws SessionServiceException {
		if (VDBName == null || vdbVersion <= 0) {
			return Collections.emptyList();
		}
		ArrayList<SessionMetadata> results = new ArrayList<SessionMetadata>();
		for (SessionMetadata info : this.sessionCache.values()) {
			if (VDBName.equalsIgnoreCase(info.getVDBName()) && vdbVersion == info.getVDBVersion()) {
				results.add(info);
			}
		}
		return results;
	}

	@Override
	public void pingServer(String sessionID) throws InvalidSessionException {
		SessionMetadata info = getSessionInfo(sessionID, false);
		info.setLastPingTime(System.currentTimeMillis());
		this.sessionCache.put(sessionID, info);
		LogManager.logDetail(LogConstants.CTX_SECURITY, "Keep-alive ping received for session:", sessionID); //$NON-NLS-1$
	}

	@Override
	public boolean terminateSession(String terminatedSessionID, String adminSessionID) {
		Object[] params = {adminSessionID, terminatedSessionID};
		LogManager.logInfo(LogConstants.CTX_SECURITY, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40009, params));
		try {
			closeSession(terminatedSessionID);
			return true;
		} catch (InvalidSessionException e) {
			LogManager.logDetail(LogConstants.CTX_SECURITY,e,e.getMessage());
			return false;
		}
	}

	@Override
	public SessionMetadata validateSession(String sessionID) throws InvalidSessionException, SessionServiceException {
		SessionMetadata info = getSessionInfo(sessionID, false);
		return info;
	}

	private SessionMetadata getSessionInfo(String sessionID, boolean remove)
			throws InvalidSessionException {
		if (sessionID == null) {
			 throw new InvalidSessionException(RuntimePlugin.Event.TEIID40041, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40041));
		}
		SessionMetadata info = remove?this.sessionCache.remove(sessionID):this.sessionCache.get(sessionID);
		if (info == null) {
			 throw new InvalidSessionException(RuntimePlugin.Event.TEIID40042, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40042, sessionID));
		}
		return info;
	}
	
	public long getSessionMaxLimit() {
		return this.sessionMaxLimit;
	}
	
	public void setSessionMaxLimit(long limit) {
		this.sessionMaxLimit = limit;
	}
	
	public long getSessionExpirationTimeLimit() {
		return this.sessionExpirationTimeLimit;
	}
	
	public void setSessionExpirationTimeLimit(long limit) {
		this.sessionExpirationTimeLimit = limit;
	}
		
	public void setAuthenticationType(AuthenticationType flag) {
		this.defaultAuthenticationType = flag;		
	}	
	
	public void start() {
		LogManager.logDetail(LogConstants.CTX_SECURITY, new Object[] {"Default security domain configured=", this.securityDomainNames}); //$NON-NLS-1$
        this.sessionMonitor.schedule(new TimerTask() {
        	@Override
        	public void run() {
        		monitorSessions();
        	}
        }, ServerConnection.PING_INTERVAL * 3, ServerConnection.PING_INTERVAL * 2);
	}

	public void stop(){
		this.sessionMonitor.cancel();
		this.sessionCache.clear();
	}

	public void setVDBRepository(VDBRepository repo) {
		this.vdbRepository = repo;
	}
	
	public void setSecurityHelper(SecurityHelper securityHelper) {
		this.securityHelper = securityHelper;
	}
	
	public void setDqp(DQPCore dqp) {
		this.dqp = dqp;
	}
	
	@Override
	public SecurityHelper getSecurityHelper() {
		return securityHelper;
	}

    protected static String getBaseUsername(String username) {
        if (username == null) {
            return username;
        }
        
        int index = getQualifierIndex(username);

        String result = username;
        
        if (index != -1) {
            result = username.substring(0, index);
        }
        
        //strip the escape character from the remaining ats
        return result.replaceAll("\\\\"+AT, AT); //$NON-NLS-1$
    }
    
    static String escapeName(String name) {
        if (name == null) {
            return name;
        }
        
        return name.replaceAll(AT, "\\\\"+AT); //$NON-NLS-1$
    }
    
    static String getDomainName(String username) {
        if (username == null) {
            return username;
        }
        
        int index = getQualifierIndex(username);
        
        if (index != -1) {
            return username.substring(index + 1);
        }
        
        return null;
    }
    
    static int getQualifierIndex(String username) {
        int index = username.length();
        while ((index = username.lastIndexOf(AT, --index)) != -1) {
            if (index > 0 && username.charAt(index - 1) != '\\') {
                return index;
            }
        }
        
        return -1;
    }
    
	@Override
	public AuthenticationType getAuthenticationType(String vdbName, String version, String userName) throws LogonException {
		if (vdbName != null) {
			VDB vdb;
			try {
				vdb = getActiveVDB(vdbName, version);
			} catch (SessionServiceException e) {
				throw new LogonException(e);
			}
			
			String gssPattern = vdb.getPropertyValue(GSS_PATTERN_PROPERTY);
			
			//TODO: cache the patterns
			
			if (gssPattern != null && Pattern.matches(gssPattern, userName)) {
				return AuthenticationType.GSS;
			}
			
			String passwordPattern = vdb.getPropertyValue(PASSWORD_PATTERN_PROPERTY);
			
			if (passwordPattern != null && Pattern.matches(passwordPattern, userName)) {
				return AuthenticationType.USERPASSWORD;
			}
			
			String typeProperty = vdb.getPropertyValue(AUTHENTICATION_TYPE_PROPERTY);
			if (typeProperty != null) {
				return AuthenticationType.valueOf(typeProperty);
			}
		}
		return this.defaultAuthenticationType;
	}

	public String getSecurityDomain(String userName, String vdbName, String version, VDB vdb) throws LoginException {
		String securityDomain = getDomainName(userName);
		if (vdbName != null) {
	    	try {    		
	    		if (vdb == null) {
	    			vdb = getActiveVDB(vdbName, version);
	    		}
				String typeProperty = vdb.getPropertyValue(SECURITY_DOMAIN_PROPERTY);				
				if (typeProperty != null) {
					if (securityDomain != null && !typeProperty.equals(securityDomain)) {
						//conflicting
		    			throw new LoginException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40116));
					}
					return typeProperty;
				}
			} catch (SessionServiceException e) {
				// ignore and return default, this only occur if the name and version are wrong 
			}			
		} 
		if (securityDomain != null) {
			if (this.securityDomainNames != null && this.securityDomainNames.contains(securityDomain)) {
				return securityDomain;
			}
			throw new LoginException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40116));
		}
		
		if (this.securityDomainNames != null && !this.securityDomainNames.isEmpty()) {
			return this.securityDomainNames.get(0);
		}
		
		//no default
		return null;
	}
	
	@Override
	public GSSResult neogitiateGssLogin(String user, String vdbName,
			String vdbVersion, byte[] serviceTicket) throws LoginException, LogonException {
		String securityDomain = getSecurityDomain(user, vdbName, vdbVersion, null);
		if (securityDomain == null ) {
			 throw new LogonException(RuntimePlugin.Event.TEIID40059, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40059));
		}
		return neogitiateGssLogin(securityDomain, serviceTicket);
	}
	
	public AuthenticationType getDefaultAuthenticationType() {
		return defaultAuthenticationType;
	}

	protected GSSResult neogitiateGssLogin(String securityDomain,
			byte[] serviceTicket) throws LoginException {
		// must be overridden in platform specific security domain
		return null;
	}
}
