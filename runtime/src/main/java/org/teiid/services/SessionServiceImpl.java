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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import org.jboss.as.security.plugins.SecurityDomainContext;
import org.teiid.adminapi.VDB;
import org.teiid.adminapi.VDB.ConnectionType;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.client.security.InvalidSessionException;
import org.teiid.client.security.SessionToken;
import org.teiid.core.util.ArgCheck;
import org.teiid.deployers.VDBRepository;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.dqp.service.SessionService;
import org.teiid.dqp.service.SessionServiceException;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.net.ServerConnection;
import org.teiid.net.TeiidURL;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.security.Credentials;
import org.teiid.security.SecurityHelper;


/**
 * This class serves as the primary implementation of the Session Service.
 */
public class SessionServiceImpl implements SessionService {
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

    private Map<String, SessionMetadata> sessionCache = new ConcurrentHashMap<String, SessionMetadata>();
    private Timer sessionMonitor = new Timer("SessionMonitor", true); //$NON-NLS-1$
    private Map<String, SecurityDomainContext> securityDomainMap;
    private List<String> securityDomainNames;
        
    public void setSecurityDomains(List<String> domainNames, Map<String, SecurityDomainContext> domains) {
    	this.securityDomainNames = domainNames;
    	this.securityDomainMap = domains;
    }
    
    // -----------------------------------------------------------------------------------
    // S E R V I C E - R E L A T E D M E T H O D S
    // -----------------------------------------------------------------------------------

    private void monitorSessions() {
		long currentTime = System.currentTimeMillis();
		for (SessionMetadata info : sessionCache.values()) {
			try {
    			if (!info.isEmbedded() && currentTime - info.getLastPingTime() > ServerConnection.PING_INTERVAL * 5) {
    				LogManager.logInfo(LogConstants.CTX_SECURITY, RuntimePlugin.Util.getString( "SessionServiceImpl.keepaliveFailed", info.getSessionId())); //$NON-NLS-1$
    				closeSession(info.getSessionId());
    			} else if (sessionExpirationTimeLimit > 0 && currentTime - info.getCreatedTime() > sessionExpirationTimeLimit) {
    				LogManager.logInfo(LogConstants.CTX_SECURITY, RuntimePlugin.Util.getString( "SessionServiceImpl.expireSession", info.getSessionId())); //$NON-NLS-1$
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
		if (sessionID == null) {
			throw new InvalidSessionException(RuntimePlugin.Util.getString("SessionServiceImpl.invalid_session", sessionID)); //$NON-NLS-1$
		}
		SessionMetadata info = this.sessionCache.remove(sessionID);
		if (info == null) {
			throw new InvalidSessionException(RuntimePlugin.Util.getString("SessionServiceImpl.invalid_session", sessionID)); //$NON-NLS-1$
		}
		if (info.getVDBName() != null) {
            try {
    			dqp.terminateSession(info.getSessionId());
            } catch (Exception e) {
                LogManager.logWarning(LogConstants.CTX_SECURITY,e,"Exception terminitating session"); //$NON-NLS-1$
            }
		}
	}
	
	@Override
	public SessionMetadata createSession(String userName, Credentials credentials, String applicationName, Properties properties, boolean authenticate) 
		throws LoginException, SessionServiceException {
		ArgCheck.isNotNull(applicationName);
        ArgCheck.isNotNull(properties);
        
        String securityDomain = "none"; //$NON-NLS-1$
        Object securityContext = null;
        Subject subject = null;
        List<String> domains = this.securityDomainNames;
        
        // Validate VDB and version if logging on to server product...
        VDBMetaData vdb = null;
        String vdbName = properties.getProperty(TeiidURL.JDBC.VDB_NAME);
        if (vdbName != null) {
        	String vdbVersion = properties.getProperty(TeiidURL.JDBC.VDB_VERSION);
        	vdb = getActiveVDB(vdbName, vdbVersion);
        }

        if (sessionMaxLimit > 0 && getActiveSessionsCount() >= sessionMaxLimit) {
            throw new SessionServiceException(RuntimePlugin.Util.getString("SessionServiceImpl.reached_max_sessions", new Object[] {new Long(sessionMaxLimit)})); //$NON-NLS-1$
        }
        
        if (!domains.isEmpty() && authenticate) {
	        // Authenticate user...
	        // if not authenticated, this method throws exception
        	boolean onlyAllowPassthrough = Boolean.valueOf(properties.getProperty(TeiidURL.CONNECTION.PASSTHROUGH_AUTHENTICATION, "false")); //$NON-NLS-1$
	        TeiidLoginContext membership = authenticate(userName, credentials, applicationName, domains, this.securityDomainMap, this.securityHelper, onlyAllowPassthrough);
	        userName = membership.getUserName();
	        securityDomain = membership.getSecurityDomain();
	        securityContext = membership.getSecurityContext();
	        subject = membership.getSubject();
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
        newSession.setSecurityDomain(securityDomain);
        if (vdb != null) {
	        newSession.setVDBName(vdb.getName());
	        newSession.setVDBVersion(vdb.getVersion());
        }
        
        // these are local no need for monitoring.
        newSession.setSubject(subject);
        newSession.setSecurityContext(securityContext);
        newSession.setVdb(vdb);
        LogManager.logDetail(LogConstants.CTX_SECURITY, new Object[] {"Logon successful for \"", userName, "\" - created SessionID \"", newSession.getSessionToken().getSessionID(), "\"" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        this.sessionCache.put(newSession.getSessionId(), newSession);
        return newSession;
	}

	VDBMetaData getActiveVDB(String vdbName, String vdbVersion) throws SessionServiceException {
		VDBMetaData vdb = null;
		
		// handle the situation when the version is part of the vdb name.
		
		int firstIndex = vdbName.indexOf('.');
		int lastIndex = vdbName.lastIndexOf('.');
		if (firstIndex != -1) {
			if (firstIndex != lastIndex || vdbVersion != null) {
				throw new SessionServiceException(RuntimePlugin.Util.getString("ambigious_name", vdbName, vdbVersion)); //$NON-NLS-1$
			}
			vdbVersion = vdbName.substring(firstIndex+1);
			vdbName = vdbName.substring(0, firstIndex);
		}
		
		try {
			if (vdbVersion == null) {
				vdbVersion = "latest"; //$NON-NLS-1$
				vdb = this.vdbRepository.getVDB(vdbName);
			}
			else {
				vdb = this.vdbRepository.getVDB(vdbName, Integer.parseInt(vdbVersion));
			}         
		} catch (NumberFormatException e) {
			throw new SessionServiceException(e, RuntimePlugin.Util.getString("VDBService.VDB_does_not_exist._3", vdbVersion)); //$NON-NLS-1$
		}
		
		if (vdb == null) {
			throw new SessionServiceException(RuntimePlugin.Util.getString("VDBService.VDB_does_not_exist._1", vdbName, vdbVersion)); //$NON-NLS-1$
		}
		
		if (vdb.getStatus() != VDB.Status.ACTIVE) {
			throw new SessionServiceException(RuntimePlugin.Util.getString("VDBService.VDB_does_not_exist._2", vdbName, vdbVersion)); //$NON-NLS-1$
		}
		if (vdb.getConnectionType() == ConnectionType.NONE) {
			throw new SessionServiceException(RuntimePlugin.Util.getString("VDBService.VDB_does_not_exist._4", vdbName, vdbVersion)); //$NON-NLS-1$
		}
		return vdb;
	}

	protected TeiidLoginContext authenticate(String userName, Credentials credentials, String applicationName, List<String> domains, Map<String, SecurityDomainContext> securityDomainMap, SecurityHelper helper, boolean onlyallowPassthrough)
			throws LoginException {
		TeiidLoginContext membership = new TeiidLoginContext(helper);
        membership.authenticateUser(userName, credentials, applicationName, domains, securityDomainMap, onlyallowPassthrough);                        
		return membership;
	}
	
	@Override
	public Collection<SessionMetadata> getActiveSessions() throws SessionServiceException {
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
		SessionMetadata info = getSessionInfo(sessionID);
		info.setLastPingTime(System.currentTimeMillis());
		this.sessionCache.put(sessionID, info);
		LogManager.logDetail(LogConstants.CTX_SECURITY, "Keep-alive ping received for session:", sessionID); //$NON-NLS-1$
	}

	@Override
	public boolean terminateSession(String terminatedSessionID, String adminSessionID) {
		Object[] params = {adminSessionID, terminatedSessionID};
		LogManager.logInfo(LogConstants.CTX_SECURITY, RuntimePlugin.Util.getString( "SessionServiceImpl.terminateSession", params)); //$NON-NLS-1$
		try {
			closeSession(terminatedSessionID);
			return true;
		} catch (InvalidSessionException e) {
			LogManager.logWarning(LogConstants.CTX_SECURITY,e,RuntimePlugin.Util.getString("SessionServiceImpl.invalid_session", new Object[] {e.getMessage()})); //$NON-NLS-1$
			return false;
		}
	}

	@Override
	public SessionMetadata validateSession(String sessionID) throws InvalidSessionException, SessionServiceException {
		SessionMetadata info = getSessionInfo(sessionID);
		return info;
	}

	private SessionMetadata getSessionInfo(String sessionID)
			throws InvalidSessionException {
		if (sessionID == null) {
			throw new InvalidSessionException(RuntimePlugin.Util.getString("SessionServiceImpl.invalid_session", sessionID)); //$NON-NLS-1$
		}
		SessionMetadata info = this.sessionCache.get(sessionID);
		if (info == null) {
			throw new InvalidSessionException(RuntimePlugin.Util.getString("SessionServiceImpl.invalid_session", sessionID)); //$NON-NLS-1$
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
