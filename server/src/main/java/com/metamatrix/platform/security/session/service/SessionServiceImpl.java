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
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.admin.api.exception.security.MetaMatrixSecurityException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.api.exception.security.MetaMatrixAuthenticationException;
import com.metamatrix.api.exception.security.SessionServiceException;
import com.metamatrix.cache.Cache;
import com.metamatrix.cache.CacheConfiguration;
import com.metamatrix.cache.CacheFactory;
import com.metamatrix.cache.CacheConfiguration.Policy;
import com.metamatrix.common.api.MMURL;
import com.metamatrix.common.comm.api.ServerConnection;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.id.dbid.DBIDController;
import com.metamatrix.common.id.dbid.DBIDGenerator;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.LogConstants;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.dqp.ResourceFinder;
import com.metamatrix.metadata.runtime.RuntimeMetadataCatalog;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseDoesNotExistException;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseException;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.platform.PlatformPlugin;
import com.metamatrix.platform.security.api.Credentials;
import com.metamatrix.platform.security.api.MetaMatrixPrincipal;
import com.metamatrix.platform.security.api.MetaMatrixPrincipalName;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;
import com.metamatrix.platform.security.api.MetaMatrixSessionInfo;
import com.metamatrix.platform.security.api.service.MembershipServiceInterface;
import com.metamatrix.platform.security.api.service.SessionServiceInterface;
import com.metamatrix.platform.security.membership.service.AuthenticationToken;
import com.metamatrix.platform.service.api.exception.ServiceException;
import com.metamatrix.platform.service.api.exception.ServiceStateException;
import com.metamatrix.platform.service.controller.AbstractService;
import com.metamatrix.platform.util.ErrorMessageKeys;
import com.metamatrix.platform.util.LogMessageKeys;
import com.metamatrix.platform.util.PlatformProxyHelper;
import com.metamatrix.platform.util.ProductInfoConstants;
import com.metamatrix.server.util.DataServerSessionTerminationHandler;

/**
 * This class serves as the primary implementation of the Session Service.
 */
public class SessionServiceImpl extends AbstractService implements
                                                       SessionServiceInterface {
	
    /**
     * Comma delimited string containing a list of SessionTerminationHandlers to be called when a session
     * is terminated.
     */
    private static final String MAX_ACTIVE_SESSIONS = "metamatrix.session.max.connections"; //$NON-NLS-1$
    private static final String SESSION_TIME_LIMIT = "metamatrix.session.time.limit"; //$NON-NLS-1$

    private static final String SESSION_ID = "SESSION_ID"; //$NON-NLS-1$
    
    private MembershipServiceInterface membershipService;
    private Cache<MetaMatrixSessionID, MetaMatrixSessionInfo> sessionCache;
    private long sessionMaxLimit;
    private long sessionTimeLimit;
    
    private DataServerSessionTerminationHandler queryTerminationHandler = new DataServerSessionTerminationHandler();
    
    private Timer sessionMonitor;
    private DBIDController idGenerator = DBIDGenerator.getInstance();

    // -----------------------------------------------------------------------------------
    // S E R V I C E - R E L A T E D M E T H O D S
    // -----------------------------------------------------------------------------------

    /**
     * Perform initialization and commence processing. This method is called only once.
     */
    protected void initService(Properties env) throws Exception {
        this.membershipService = PlatformProxyHelper.getMembershipServiceProxy(PlatformProxyHelper.ROUND_ROBIN_LOCAL);

        CacheFactory cf = ResourceFinder.getCacheFactory();
        this.sessionCache = cf.get(Cache.Type.SESSION, new CacheConfiguration(Policy.LRU, 24*60*60, 5000));

        Configuration config = CurrentConfiguration.getInstance().getConfiguration();
        Properties nextProperties = config.getProperties();
        this.sessionMaxLimit = PropertiesUtils.getIntProperty(nextProperties, MAX_ACTIVE_SESSIONS, 0);
        this.sessionTimeLimit = PropertiesUtils.getIntProperty(nextProperties, SESSION_TIME_LIMIT, 0) * 60000;
        this.sessionMonitor = new Timer("SessionMonitor", true); //$NON-NLS-1$
        this.sessionMonitor.schedule(new TimerTask() {
        	@Override
        	public void run() {
        		monitorSessions();
        	}
        }, 0, ServerConnection.PING_INTERVAL * 5);
    }

    private void monitorSessions() {
		long currentTime = System.currentTimeMillis();
		for (MetaMatrixSessionInfo info : sessionCache.values()) {
			try {
    			if (currentTime - info.getLastPingTime() > ServerConnection.PING_INTERVAL * 5) {
    				LogManager.logInfo(LogConstants.CTX_SESSION, PlatformPlugin.Util.getString( "SessionServiceImpl.keepaliveFailed", info.getSessionID())); //$NON-NLS-1$
    				closeSession(info.getSessionID());
    			} else if (sessionTimeLimit > 0 && currentTime - info.getTimeCreated() > sessionTimeLimit) {
    				LogManager.logInfo(LogConstants.CTX_SESSION, PlatformPlugin.Util.getString( "SessionServiceImpl.expireSession", info.getSessionID())); //$NON-NLS-1$
    				closeSession(info.getSessionID());
    			}
			} catch (Exception e) {
				LogManager.logDetail(LogConstants.CTX_SESSION, e, "error running session monitor, unable to monitor: " + info.getSessionID()); //$NON-NLS-1$
			}
		}
	}

    /**
     * Close the service to new work if applicable. After this method is called the service should no longer accept new work to
     * perform but should continue to process any outstanding work. This method is called by die().
     */
    protected void closeService() {
    	this.sessionMonitor.cancel();
    }

    /**
     * Wait until the service has completed all outstanding work. This method is called by die() just before calling dieNow().
     */
    protected void waitForServiceToClear() throws Exception {
        // It is not necessary to do anything here.
    }

    /**
     * Terminate all processing and reclaim resources. This method is called by dieNow() and is only called once.
     */
    protected void killService() {
    	closeService();
    }

	@Override
	public void closeSession(MetaMatrixSessionID sessionID)
			throws InvalidSessionException, SessionServiceException{
		LogManager.logDetail(LogConstants.CTX_SESSION, new Object[] {"closeSession", sessionID}); //$NON-NLS-1$
		MetaMatrixSessionInfo info = this.sessionCache.remove(sessionID);
		if (info == null) {
			throw new InvalidSessionException(ErrorMessageKeys.SEC_SESSION_0027, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_SESSION_0027, sessionID));
		}
		if (info.getProductInfo(ProductInfoConstants.VIRTUAL_DB) != null) {
            try {
                queryTerminationHandler.cleanup(info.getSessionToken());
            } catch (Exception e) {
                LogManager.logWarning(LogConstants.CTX_SESSION,e,PlatformPlugin.Util.getString(LogMessageKeys.SEC_SESSION_0028, DataServerSessionTerminationHandler.class.getName()));
            }
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
            VirtualDatabaseID vdbID = null;
            try {
                vdbID = RuntimeMetadataCatalog.getInstance().getActiveVirtualDatabaseID(vdbName, vdbVersion);
            } catch (VirtualDatabaseDoesNotExistException e) {
                throw new SessionServiceException(e);
            } catch (VirtualDatabaseException e) {
                if (vdbVersion == null) {
                    throw new SessionServiceException(e,PlatformPlugin.Util.getString("SessionServiceImpl.Unexpected_error_finding_latest_version_of_Virtual_Database", new Object[] {vdbName})); //$NON-NLS-1$
                }
                throw new SessionServiceException(e,PlatformPlugin.Util.getString("SessionServiceImpl.Unexpected_error_finding_latest_version_of_Virtual_Database_{0}_of_version_{1}", new Object[] {vdbName, vdbVersion})); //$NON-NLS-1$
            }
            // Reset product info with validated constants
            productInfo.put(ProductInfoConstants.VIRTUAL_DB, vdbID.getName());
            productInfo.put(ProductInfoConstants.VDB_VERSION, vdbID.getVersion());
        }

        if (sessionMaxLimit > 0 && getActiveSessionsCount() >= sessionMaxLimit) {
            throw new SessionServiceException(LogMessageKeys.SEC_SESSION_0067,PlatformPlugin.Util.getString(LogMessageKeys.SEC_SESSION_0067, new Object[] {new Long(sessionMaxLimit)}));
        }
        
        long creationTime = System.currentTimeMillis();

        
        // Get a new ID for this new session record
        MetaMatrixSessionID id = new MetaMatrixSessionID(getUniqueSessionID());

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
        return newSession;
	}
	
	public void setIdGenerator(DBIDController idGenerator) {
		this.idGenerator = idGenerator;
	}
	
    private long getUniqueSessionID() throws SessionServiceException {
        try {
            return idGenerator.getID(SESSION_ID);
        } catch (Exception e) {
            throw new SessionServiceException(ErrorMessageKeys.SEC_SESSION_0107, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_SESSION_0107));
        }
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
		} catch (ServiceException e) {
			String msg = PlatformPlugin.Util
					.getString("SessionServiceImpl.Unable_to_communicate_with_the_membership_service"); //$NON-NLS-1$
			SessionServiceException se = new SessionServiceException(e, msg);
			throw se;
		}catch (MetaMatrixSecurityException e) {
			String msg = PlatformPlugin.Util
					.getString(
							"SessionServiceImpl.Membership_service_could_not_authenticate_user", new Object[] { userName }); //$NON-NLS-1$
			SessionServiceException se = new SessionServiceException(e, msg);
			throw se;
		}

		// Throw exception if not authenticated
		// Log the failure as a warning as it is not a system level failure, but
		// rather a processing
		// level issue.
		if (!authenticatedToken.isAuthenticated()) {
			Object[] params = new Object[] { userName };
			String msg = PlatformPlugin.Util.getString("SessionServiceImpl.The_username_0_and/or_password_are_incorrect", params); //$NON-NLS-1$
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
        } catch (ServiceException e) {
            throw new SessionServiceException(e, ErrorMessageKeys.SEC_SESSION_0004,PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_SESSION_0004));
        } catch (MetaMatrixSecurityException e) {
            throw new SessionServiceException(e, LogMessageKeys.SEC_SESSION_0043,PlatformPlugin.Util.getString(LogMessageKeys.SEC_SESSION_0043,sessionInfo.getUserName()));
        }
	}

	@Override
	public Collection<MetaMatrixSessionInfo> getSessionsLoggedInToVDB(String VDBName, String VDBVersion)
			throws SessionServiceException, ServiceStateException {
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
			throws ServiceStateException, InvalidSessionException {
		MetaMatrixSessionInfo info = getSessionInfo(sessionID);
		info.setLastPingTime(System.currentTimeMillis());
		this.sessionCache.put(sessionID, info);
	}

	@Override
	public boolean terminateSession(MetaMatrixSessionID terminatedSessionID,
			MetaMatrixSessionID adminSessionID) throws 
			AuthorizationException, SessionServiceException {
		Object[] params = {adminSessionID, terminatedSessionID};
		LogManager.logInfo(LogConstants.CTX_SESSION, PlatformPlugin.Util.getString( "SessionServiceImpl.terminateSession", params)); //$NON-NLS-1$
		try {
			closeSession(terminatedSessionID);
			return true;
		} catch (InvalidSessionException e) {
			LogManager.logWarning(LogConstants.CTX_SESSION,e,PlatformPlugin.Util.getString(LogMessageKeys.SEC_SESSION_0034, new Object[] {e.getMessage()}));
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
			throw new InvalidSessionException(ErrorMessageKeys.SEC_SESSION_0027, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_SESSION_0027, sessionID));
		}
		return info;
	}

	void setMembershipService(MembershipServiceInterface membershipService) {
		this.membershipService = membershipService;
	}

	void setSessionCache(
			Cache<MetaMatrixSessionID, MetaMatrixSessionInfo> sessionCache) {
		this.sessionCache = sessionCache;
	}

}
