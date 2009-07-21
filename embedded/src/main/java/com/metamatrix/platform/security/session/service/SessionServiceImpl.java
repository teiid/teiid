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

package com.metamatrix.platform.security.session.service;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.teiid.dqp.internal.process.DQPCore;

import com.google.inject.Inject;
import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.admin.api.exception.security.MetaMatrixSecurityException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.api.exception.security.MetaMatrixAuthenticationException;
import com.metamatrix.api.exception.security.SessionServiceException;
import com.metamatrix.common.api.MMURL;
import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.common.comm.api.ServerConnection;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.LogConstants;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.dqp.embedded.DQPEmbeddedPlugin;
import com.metamatrix.dqp.service.VDBService;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseDoesNotExistException;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseException;
import com.metamatrix.platform.security.api.Credentials;
import com.metamatrix.platform.security.api.MetaMatrixPrincipal;
import com.metamatrix.platform.security.api.MetaMatrixPrincipalName;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;
import com.metamatrix.platform.security.api.MetaMatrixSessionInfo;
import com.metamatrix.platform.security.api.service.AuthenticationToken;
import com.metamatrix.platform.security.api.service.MembershipServiceInterface;
import com.metamatrix.platform.security.api.service.SessionListener;
import com.metamatrix.platform.security.api.service.SessionServiceInterface;
import com.metamatrix.platform.util.ProductInfoConstants;

/**
 * This class serves as the primary implementation of the Session Service.
 */
public class SessionServiceImpl implements SessionServiceInterface {

	/*
	 * Configuration state
	 */
    private long sessionMaxLimit;
	private long sessionTimeLimit;
	
	/*
	 * Injected state
	 */
    private MembershipServiceInterface membershipService;
	private DQPCore dqpCore;
	private VDBService vdbService;

    private Map<MetaMatrixSessionID, MetaMatrixSessionInfo> sessionCache = new ConcurrentHashMap<MetaMatrixSessionID, MetaMatrixSessionInfo>();
    private Timer sessionMonitor;
    private AtomicLong idSequence = new AtomicLong();
    private SessionListener sessionListener;
        
    // -----------------------------------------------------------------------------------
    // S E R V I C E - R E L A T E D M E T H O D S
    // -----------------------------------------------------------------------------------

    private void monitorSessions() {
		long currentTime = System.currentTimeMillis();
		for (MetaMatrixSessionInfo info : sessionCache.values()) {
			try {
    			if (currentTime - info.getLastPingTime() > ServerConnection.PING_INTERVAL * 5) {
    				LogManager.logInfo(LogConstants.CTX_SESSION, DQPEmbeddedPlugin.Util.getString( "SessionServiceImpl.keepaliveFailed", info.getSessionID())); //$NON-NLS-1$
    				closeSession(info.getSessionID());
    			} else if (sessionTimeLimit > 0 && currentTime - info.getTimeCreated() > sessionTimeLimit) {
    				LogManager.logInfo(LogConstants.CTX_SESSION, DQPEmbeddedPlugin.Util.getString( "SessionServiceImpl.expireSession", info.getSessionID())); //$NON-NLS-1$
    				closeSession(info.getSessionID());
    			}
			} catch (Exception e) {
				LogManager.logDetail(LogConstants.CTX_SESSION, e, "error running session monitor, unable to monitor: " + info.getSessionID()); //$NON-NLS-1$
			}
		}
	}

	@Override
	public void closeSession(MetaMatrixSessionID sessionID)
			throws InvalidSessionException, SessionServiceException{
		LogManager.logDetail(LogConstants.CTX_SESSION, new Object[] {"closeSession", sessionID}); //$NON-NLS-1$
		MetaMatrixSessionInfo info = this.sessionCache.remove(sessionID);
		if (info == null) {
			throw new InvalidSessionException(DQPEmbeddedPlugin.Util.getString("SessionServiceImpl.invalid_session", sessionID)); //$NON-NLS-1$
		}
		if (info.getProductInfo(ProductInfoConstants.VIRTUAL_DB) != null) {
            try {
    			dqpCore.terminateConnection(info.getSessionToken().getSessionIDValue());
            } catch (Exception e) {
                LogManager.logWarning(LogConstants.CTX_SESSION,e,"Exception terminitating session"); //$NON-NLS-1$
            }
		}
        if (this.sessionListener != null) {
        	this.sessionListener.sessionClosed(info);
        }		
	}
	
	@Override
	public MetaMatrixSessionInfo createSession(String userName,
			Credentials credentials, Serializable trustedToken,
			String applicationName, Properties properties)
			throws MetaMatrixAuthenticationException, SessionServiceException {
		ArgCheck.isNotNull(applicationName);
        ArgCheck.isNotNull(properties);
        
        Properties productInfo = new Properties();
        
        //
        // Authenticate user...
        // if not authenticated, this method throws exception
        //
        AuthenticationToken authenticatedToken = this.authenticateUser(userName, credentials, trustedToken, applicationName);

        String authenticatedUserName = authenticatedToken.getUserName();
        
        //
        // Validate VDB and version if logging on to server product...
        //
        String vdbName = properties.getProperty(ProductInfoConstants.VIRTUAL_DB);
        if (vdbName != null) {
            String vdbVersion = properties.getProperty(ProductInfoConstants.VDB_VERSION);
            try {
            	vdbVersion = vdbService.getActiveVDBVersion(vdbName, vdbVersion);
            } catch (VirtualDatabaseDoesNotExistException e) {
                throw new SessionServiceException(e);
            } catch (VirtualDatabaseException e) {
                if (vdbVersion == null) {
                    throw new SessionServiceException(e,DQPEmbeddedPlugin.Util.getString("SessionServiceImpl.Unexpected_error_finding_latest_version_of_Virtual_Database", new Object[] {vdbName})); //$NON-NLS-1$
                }
                throw new SessionServiceException(e,DQPEmbeddedPlugin.Util.getString("SessionServiceImpl.Unexpected_error_finding_latest_version_of_Virtual_Database_{0}_of_version_{1}", new Object[] {vdbName, vdbVersion})); //$NON-NLS-1$
            } catch (MetaMatrixComponentException e) {
				throw new SessionServiceException(e);
			}
            // Reset product info with validated constants
            productInfo.put(ProductInfoConstants.VIRTUAL_DB, vdbName);
            productInfo.put(ProductInfoConstants.VDB_VERSION, vdbVersion);
        }

        if (sessionMaxLimit > 0 && getActiveSessionsCount() >= sessionMaxLimit) {
            throw new SessionServiceException(DQPEmbeddedPlugin.Util.getString("SessionServiceImpl.reached_max_sessions", new Object[] {new Long(sessionMaxLimit)})); //$NON-NLS-1$
        }
        
        long creationTime = System.currentTimeMillis();

        // Get a new ID for this new session record
        MetaMatrixSessionID id = new MetaMatrixSessionID(idSequence.getAndIncrement());

        // Return a new session info object
        MetaMatrixSessionInfo newSession = new MetaMatrixSessionInfo(id,
        										authenticatedUserName,
        										creationTime,
        										applicationName,
        										productInfo,
                                                properties.getProperty(MMURL.CONNECTION.CLIENT_IP_ADDRESS),
                                                properties.getProperty(MMURL.CONNECTION.CLIENT_HOSTNAME));
        newSession.setTrustedToken(trustedToken);
        this.sessionCache.put(newSession.getSessionID(), newSession);
        if (this.sessionListener != null) {
        	this.sessionListener.sessionCreated(newSession);
        }
        return newSession;
	}
	
    private AuthenticationToken authenticateUser(String userName,
			Credentials credentials, Serializable trustedToken,
			String applicationName) throws SessionServiceException,
			MetaMatrixAuthenticationException {
		AuthenticationToken authenticatedToken = null;
		// Authenticate the principal ...
		try {
			authenticatedToken = this.membershipService.authenticateUser(userName,
							credentials, trustedToken, applicationName);
		}catch (MetaMatrixSecurityException e) {
			String msg = DQPEmbeddedPlugin.Util.getString("SessionServiceImpl.Membership_service_could_not_authenticate_user", new Object[] { userName }); //$NON-NLS-1$
			SessionServiceException se = new SessionServiceException(e, msg);
			throw se;
		}

		// Throw exception if not authenticated
		// Log the failure as a warning as it is not a system level failure, but
		// rather a processing
		// level issue.
		if (!authenticatedToken.isAuthenticated()) {
			Object[] params = new Object[] { userName };
			String msg = DQPEmbeddedPlugin.Util.getString("SessionServiceImpl.The_username_0_and/or_password_are_incorrect", params); //$NON-NLS-1$
			throw new MetaMatrixAuthenticationException(msg);
		}

		return authenticatedToken;
	}
	
	@Override
	public Collection<MetaMatrixSessionInfo> getActiveSessions() throws SessionServiceException {
		return new ArrayList<MetaMatrixSessionInfo>(this.sessionCache.values());
	}

	@Override
	public int getActiveSessionsCount() throws SessionServiceException{
		return this.sessionCache.size();
	}

	@Override
	public MetaMatrixPrincipal getPrincipal(MetaMatrixSessionID sessionID)
			throws InvalidSessionException, SessionServiceException {
        
        MetaMatrixSessionInfo sessionInfo = this.getSessionInfo(sessionID);

        try {
            return membershipService.getPrincipal(new MetaMatrixPrincipalName(sessionInfo.getUserName(), MetaMatrixPrincipal.TYPE_USER));
        } catch (MetaMatrixSecurityException e) {
            throw new SessionServiceException(e, DQPEmbeddedPlugin.Util.getString("SessionServiceImpl.failed_to_getprincipal",sessionInfo.getUserName())); //$NON-NLS-1$
        }
	}

	@Override
	public Collection<MetaMatrixSessionInfo> getSessionsLoggedInToVDB(String VDBName, String VDBVersion)
			throws SessionServiceException {
		if (VDBName == null || VDBVersion == null) {
			return Collections.emptyList();
		}
		ArrayList<MetaMatrixSessionInfo> results = new ArrayList<MetaMatrixSessionInfo>();
		for (MetaMatrixSessionInfo info : this.sessionCache.values()) {
			if (VDBName.equals(info.getProductInfo(ProductInfoConstants.VIRTUAL_DB)) && VDBVersion.equals(info.getProductInfo(ProductInfoConstants.VIRTUAL_DB))) {
				results.add(info);
			}
		}
		return results;
	}

	@Override
	public void pingServer(MetaMatrixSessionID sessionID)
			throws InvalidSessionException {
		MetaMatrixSessionInfo info = getSessionInfo(sessionID);
		info.setLastPingTime(System.currentTimeMillis());
		this.sessionCache.put(sessionID, info);
	}

	@Override
	public boolean terminateSession(MetaMatrixSessionID terminatedSessionID,MetaMatrixSessionID adminSessionID) throws AuthorizationException, SessionServiceException {
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
	public MetaMatrixSessionInfo validateSession(MetaMatrixSessionID sessionID)
			throws InvalidSessionException, SessionServiceException {
		MetaMatrixSessionInfo info = getSessionInfo(sessionID);
		return info;
	}

	private MetaMatrixSessionInfo getSessionInfo(MetaMatrixSessionID sessionID)
			throws InvalidSessionException {
		MetaMatrixSessionInfo info = this.sessionCache.get(sessionID);
		if (info == null) {
			throw new InvalidSessionException(DQPEmbeddedPlugin.Util.getString("SessionServiceImpl.invalid_session", sessionID)); //$NON-NLS-1$
		}
		return info;
	}
	
	@Inject
	public void setMembershipService(MembershipServiceInterface membershipService) {
		this.membershipService = membershipService;
	}
	
    public long getSessionMaxLimit() {
		return sessionMaxLimit;
	}

	public void setSessionMaxLimit(long sessionMaxLimit) {
		this.sessionMaxLimit = sessionMaxLimit;
	}

	public long getSessionTimeLimit() {
		return sessionTimeLimit;
	}

	public void setSessionTimeLimit(long sessionTimeLimit) {
		this.sessionTimeLimit = sessionTimeLimit;
	}
	
	@Inject
	public void setDqpCore(DQPCore dqpCore) {
		this.dqpCore = dqpCore;
	}

	@Override
	public void initialize(Properties props) throws ApplicationInitializationException {
		this.sessionMaxLimit = Long.parseLong(props.getProperty(MAX_SESSIONS, DEFAULT_MAX_SESSIONS));
		this.sessionTimeLimit = Long.parseLong(props.getProperty(SESSION_TIMEOUT, DEFAULT_SESSION_TIMEOUT));
	}

	@Override
	public void start(ApplicationEnvironment environment)
			throws ApplicationLifecycleException {
		this.sessionMonitor = new Timer("SessionMonitor", true); //$NON-NLS-1$
        this.sessionMonitor.schedule(new TimerTask() {
        	@Override
        	public void run() {
        		monitorSessions();
        	}
        }, 0, ServerConnection.PING_INTERVAL * 5);
	}

	@Override
	public void stop() throws ApplicationLifecycleException {
		this.sessionMonitor.cancel();
	}

	@Inject
	public void setVdbService(VDBService vdbService) {
		this.vdbService = vdbService;
	}

	public VDBService getVdbService() {
		return vdbService;
	}

	@Override
	public void register(SessionListener listener) {
		this.sessionListener = listener;
	}

}
