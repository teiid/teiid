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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.jboss.managed.api.annotation.ManagementComponent;
import org.jboss.managed.api.annotation.ManagementObject;
import org.jboss.managed.api.annotation.ManagementProperties;
import org.jboss.managed.api.annotation.ManagementProperty;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.deployers.VDBRepository;
import org.teiid.deployers.VirtualDatabaseException;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.security.Credentials;
import org.teiid.security.SecurityHelper;

import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.api.exception.security.SessionServiceException;
import com.metamatrix.common.api.MMURL;
import com.metamatrix.common.comm.api.ServerConnection;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.LogConstants;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.dqp.embedded.DQPEmbeddedPlugin;
import com.metamatrix.dqp.service.SessionService;
import com.metamatrix.platform.security.api.SessionToken;

/**
 * This class serves as the primary implementation of the Session Service.
 */
@ManagementObject(componentType=@ManagementComponent(type="teiid",subtype="dqp"), properties=ManagementProperties.EXPLICIT)
public class SessionServiceImpl implements SessionService {
	public static final String SECURITY_DOMAINS = "securitydomains"; //$NON-NLS-1$
	
	/*
	 * Configuration state
	 */
    private long sessionMaxLimit = DEFAULT_MAX_SESSIONS;
	private long sessionExpirationTimeLimit = DEFAULT_SESSION_EXPIRATION;
	
	/*
	 * Injected state
	 */
	private VDBRepository vdbRepository;
    private SecurityHelper securityHelper;

    private DQPCore dqp;

    private Map<Long, SessionMetadata> sessionCache = new ConcurrentHashMap<Long, SessionMetadata>();
    private Timer sessionMonitor = new Timer("SessionMonitor", true); //$NON-NLS-1$
    private AtomicLong idSequence = new AtomicLong();
    private LinkedList<String> securityDomains = new LinkedList<String>();
    private LinkedList<String> adminSecurityDomains = new LinkedList<String>();
    
    
    // -----------------------------------------------------------------------------------
    // S E R V I C E - R E L A T E D M E T H O D S
    // -----------------------------------------------------------------------------------

    private void monitorSessions() {
		long currentTime = System.currentTimeMillis();
		for (SessionMetadata info : sessionCache.values()) {
			try {
    			if (currentTime - info.getLastPingTime() > ServerConnection.PING_INTERVAL * 5) {
    				LogManager.logInfo(LogConstants.CTX_SESSION, DQPEmbeddedPlugin.Util.getString( "SessionServiceImpl.keepaliveFailed", info.getSessionId())); //$NON-NLS-1$
    				closeSession(info.getSessionId());
    			} else if (sessionExpirationTimeLimit > 0 && currentTime - info.getCreatedTime() > sessionExpirationTimeLimit) {
    				LogManager.logInfo(LogConstants.CTX_SESSION, DQPEmbeddedPlugin.Util.getString( "SessionServiceImpl.expireSession", info.getSessionId())); //$NON-NLS-1$
    				closeSession(info.getSessionId());
    			}
			} catch (Exception e) {
				LogManager.logDetail(LogConstants.CTX_SESSION, e, "error running session monitor, unable to monitor: " + info.getSessionId()); //$NON-NLS-1$
			}
		}
	}

	@Override
	public void closeSession(long sessionID) throws InvalidSessionException {
		LogManager.logDetail(LogConstants.CTX_SESSION, new Object[] {"closeSession", sessionID}); //$NON-NLS-1$
		SessionMetadata info = this.sessionCache.remove(sessionID);
		if (info == null) {
			throw new InvalidSessionException(DQPEmbeddedPlugin.Util.getString("SessionServiceImpl.invalid_session", sessionID)); //$NON-NLS-1$
		}
		if (info.getVDBName() != null) {
            try {
    			dqp.terminateSession(info.getSessionId());
            } catch (Exception e) {
                LogManager.logWarning(LogConstants.CTX_SESSION,e,"Exception terminitating session"); //$NON-NLS-1$
            }
		}

        // try to log out of the context.
        try {
        	LoginContext context = info.getAttachment(LoginContext.class);
        	if (context != null) {
        		context.logout();
        	}
		} catch (LoginException e) {
			 LogManager.logWarning(LogConstants.CTX_SESSION,e,"Exception terminitating session"); //$NON-NLS-1$
		}
	}
	
	@Override
	public SessionMetadata createSession(String userName, Credentials credentials, String applicationName, Properties properties, boolean adminConnection) 
		throws LoginException, SessionServiceException {
		ArgCheck.isNotNull(applicationName);
        ArgCheck.isNotNull(properties);
        
        Properties productInfo = new Properties();

        LoginContext loginContext = null;
        String securityDomain = "none"; //$NON-NLS-1$
        Object securityContext = null;
        List<String> domains = this.securityDomains;
        if (adminConnection) {
        	domains = this.adminSecurityDomains;
        }
        
        if (!domains.isEmpty()) {
	        // Authenticate user...
	        // if not authenticated, this method throws exception
	        TeiidLoginContext membership = authenticate(userName, credentials, applicationName, domains);
	        loginContext = membership.getLoginContext();
	        userName = membership.getUserName();
	        securityDomain = membership.getSecurityDomain();
	        securityContext = membership.getSecurityContext(securityHelper);
        }

        // Validate VDB and version if logging on to server product...
        VDBMetaData vdb = null;
        String vdbName = properties.getProperty(MMURL.JDBC.VDB_NAME);
        if (vdbName != null) {
        	String vdbVersion = properties.getProperty(MMURL.JDBC.VDB_VERSION);
            try {
                if (vdbVersion == null) {
                	vdb = this.vdbRepository.getActiveVDB(vdbName);
                }
                else {
                	vdb = this.vdbRepository.getVDB(vdbName, Integer.parseInt(vdbVersion));
                }            
                
                // Reset product info with validated constants
                productInfo.put(MMURL.JDBC.VDB_NAME, vdb.getName());
                productInfo.put(MMURL.JDBC.VDB_VERSION, vdb.getVersion());                
            } catch (VirtualDatabaseException e) {
            	throw new SessionServiceException(DQPEmbeddedPlugin.Util.getString("VDBService.VDB_does_not_exist._2", vdbName, vdbVersion==null?"latest":vdbVersion)); //$NON-NLS-1$ //$NON-NLS-2$ 
			}            
        }

        if (sessionMaxLimit > 0 && getActiveSessionsCount() >= sessionMaxLimit) {
            throw new SessionServiceException(DQPEmbeddedPlugin.Util.getString("SessionServiceImpl.reached_max_sessions", new Object[] {new Long(sessionMaxLimit)})); //$NON-NLS-1$
        }
        
        long creationTime = System.currentTimeMillis();

        // Return a new session info object
        long id = idSequence.getAndIncrement();
        SessionMetadata newSession = new SessionMetadata();
        newSession.setSessionId(id);
        newSession.setUserName(userName);
        newSession.setCreatedTime(creationTime);
        newSession.setApplicationName(applicationName);
        newSession.setClientHostName(properties.getProperty(MMURL.CONNECTION.CLIENT_HOSTNAME));
        newSession.setIPAddress(properties.getProperty(MMURL.CONNECTION.CLIENT_IP_ADDRESS));
        newSession.setSecurityDomain(securityDomain);
        if (vdb != null) {
	        newSession.setVDBName(vdb.getName());
	        newSession.setVDBVersion(vdb.getVersion());
        }
        
        // these are local no need for monitoring.
        newSession.addAttchment(LoginContext.class, loginContext);
        newSession.addAttchment("SecurityContext", securityContext);
        newSession.addAttchment(VDBMetaData.class, vdb);
        newSession.addAttchment(SessionToken.class, new SessionToken(id, userName));
        LogManager.logDetail(LogConstants.CTX_SESSION, new Object[] {"Logon successful for \"", userName, "\" - created SessionID \"", "" + id, "\"" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        this.sessionCache.put(newSession.getSessionId(), newSession);
        return newSession;
	}

	protected TeiidLoginContext authenticate(String userName, Credentials credentials, String applicationName, List<String> domains)
			throws LoginException {
		TeiidLoginContext membership = new TeiidLoginContext();
        membership.authenticateUser(userName, credentials, applicationName, domains);                        
		return membership;
	}
	
	@Override
	public Collection<SessionMetadata> getActiveSessions() throws SessionServiceException {
		return new ArrayList<SessionMetadata>(this.sessionCache.values());
	}
	
	@Override
	public SessionMetadata getActiveSession(long sessionID) {
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
	public void pingServer(long sessionID) throws InvalidSessionException {
		SessionMetadata info = getSessionInfo(sessionID);
		info.setLastPingTime(System.currentTimeMillis());
		this.sessionCache.put(sessionID, info);
	}

	@Override
	public boolean terminateSession(long terminatedSessionID, long adminSessionID) {
		Object[] params = {adminSessionID, terminatedSessionID};
		LogManager.logInfo(LogConstants.CTX_SESSION, DQPEmbeddedPlugin.Util.getString( "SessionServiceImpl.terminateSession", params)); //$NON-NLS-1$
		try {
			closeSession(terminatedSessionID);
			return true;
		} catch (InvalidSessionException e) {
			LogManager.logWarning(LogConstants.CTX_SESSION,e,DQPEmbeddedPlugin.Util.getString("SessionServiceImpl.invalid_session", new Object[] {e.getMessage()})); //$NON-NLS-1$
			return false;
		}
	}

	@Override
	public SessionMetadata validateSession(long sessionID) throws InvalidSessionException, SessionServiceException {
		SessionMetadata info = getSessionInfo(sessionID);
		return info;
	}

	private SessionMetadata getSessionInfo(long sessionID)
			throws InvalidSessionException {
		SessionMetadata info = this.sessionCache.get(sessionID);
		if (info == null) {
			throw new InvalidSessionException(DQPEmbeddedPlugin.Util.getString("SessionServiceImpl.invalid_session", sessionID)); //$NON-NLS-1$
		}
		return info;
	}
	
	@ManagementProperty (description="Maximum number of sessions allowed by the system (default 5000)")
	public long getSessionMaxLimit() {
		return this.sessionMaxLimit;
	}
	
	public void setSessionMaxLimit(long limit) {
		this.sessionMaxLimit = limit;
	}
	
	@ManagementProperty(description="Max allowed time before the session is terminated by the system, 0 indicates unlimited (default 0)")
	public long getSessionExpirationTimeLimit() {
		return this.sessionExpirationTimeLimit;
	}
	
	public void setSessionExpirationTimeLimit(long limit) {
		this.sessionExpirationTimeLimit = limit;
	}	
	
	public void setSecurityDomains(String domainNameOrder) {
        if (domainNameOrder != null && domainNameOrder.trim().length()>0) {
        	LogManager.logDetail(LogConstants.CTX_MEMBERSHIP, "Security Enabled: true"); //$NON-NLS-1$

	        String[] domainNames = domainNameOrder.split(","); //$NON-NLS-1$
	        for (String domainName : domainNames) {
	            this.securityDomains.addLast(domainName);
	        }
        }		
	}
	
	public void setAdminSecurityDomain(String domain) {
		this.adminSecurityDomains.add(domain);
		LogManager.logDetail(LogConstants.CTX_MEMBERSHIP, "Admin Security Enabled: true"); //$NON-NLS-1$
	}

	public void start() {
        this.sessionMonitor.schedule(new TimerTask() {
        	@Override
        	public void run() {
        		monitorSessions();
        	}
        }, 0, ServerConnection.PING_INTERVAL * 5);
	}

	public void stop(){
		this.sessionMonitor.cancel();
		this.sessionCache.clear();
	}

	@Override
	public void setLocalSession(long sessionID) {
		SessionMetadata info = this.sessionCache.get(sessionID);
		if (info != null) {
			info.setLastPingTime(Long.MAX_VALUE);
		}
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
}
