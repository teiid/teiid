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

package com.metamatrix.platform.security.session.service;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.api.exception.security.InvalidSessionException;
import com.metamatrix.api.exception.security.MetaMatrixAuthenticationException;
import com.metamatrix.api.exception.security.MetaMatrixSecurityException;
import com.metamatrix.api.exception.security.SessionServiceException;
import com.metamatrix.cache.Cache;
import com.metamatrix.cache.CacheConfiguration;
import com.metamatrix.cache.CacheFactory;
import com.metamatrix.cache.CacheConfiguration.Policy;
import com.metamatrix.common.api.MMURL_Properties;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.id.dbid.DBIDGenerator;
import com.metamatrix.common.id.dbid.DBIDGeneratorException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.MetaMatrixProductNames;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.dqp.ResourceFinder;
import com.metamatrix.metadata.runtime.RuntimeMetadataCatalog;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.metadata.runtime.exception.VirtualDatabaseDoesNotExistException;
import com.metamatrix.metadata.runtime.exception.VirtualDatabaseException;
import com.metamatrix.platform.PlatformPlugin;
import com.metamatrix.platform.security.api.Credentials;
import com.metamatrix.platform.security.api.MetaMatrixPrincipal;
import com.metamatrix.platform.security.api.MetaMatrixPrincipalName;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;
import com.metamatrix.platform.security.api.MetaMatrixSessionInfo;
import com.metamatrix.platform.security.api.MetaMatrixSessionState;
import com.metamatrix.platform.security.api.SessionToken;
import com.metamatrix.platform.security.api.service.MembershipServiceInterface;
import com.metamatrix.platform.security.api.service.SessionServiceInterface;
import com.metamatrix.platform.security.api.service.SessionServicePropertyNames;
import com.metamatrix.platform.security.api.service.SessionTerminationHandler;
import com.metamatrix.platform.security.membership.service.AuthenticationToken;
import com.metamatrix.platform.security.util.LogSecurityConstants;
import com.metamatrix.platform.service.api.exception.ServiceException;
import com.metamatrix.platform.service.api.exception.ServiceStateException;
import com.metamatrix.platform.service.controller.AbstractService;
import com.metamatrix.platform.util.ErrorMessageKeys;
import com.metamatrix.platform.util.LogMessageKeys;
import com.metamatrix.platform.util.PlatformProxyHelper;
import com.metamatrix.platform.util.ProductInfoConstants;

/**
 * This class serves as the primary implementation of the Session Service.
 */
public class SessionServiceImpl extends AbstractService implements
                                                       SessionServiceInterface {

    private static final String MAX_ACTIVE_Sessions = "metamatrix.session.max.connections"; //$NON-NLS-1$
    private static final String SESSION_TIME_LIMIT = "metamatrix.session.time.limit"; //$NON-NLS-1$

    public static final String SESSION_MONITOR_ACTIVITY_INTERVAL = "metamatrix.session.sessionMonitor.ActivityInterval"; //$NON-NLS-1$

    private long clientPingInterval = SessionServicePropertyNames.CLIENT_MONITOR_DEFAULT_PING_INTERVAL_VAL;

    private MembershipServiceInterface membershipService;
    private Cache<MetaMatrixSessionID, MetaMatrixSessionInfo> sessionCache;
    private long sessionMaxLimit;
    private long sessionTimeLimit;
    private String clusterName;
    
    // Product name -> ServiceTerminationHandler
    private Map<String, SessionTerminationHandler> terminationHandlerMap = new HashMap<String, SessionTerminationHandler>();
    
    private Timer sessionMonitor;

    // -----------------------------------------------------------------------------------
    // S E R V I C E - R E L A T E D M E T H O D S
    // -----------------------------------------------------------------------------------

    /**
     * Perform initialization and commence processing. This method is called only once.
     */
    protected void initService(Properties env) throws Exception {
        this.membershipService = PlatformProxyHelper.getMembershipServiceProxy(PlatformProxyHelper.ROUND_ROBIN_LOCAL);
        
        // Instantiate SessionTerminationHandlers
        String handlerString = env.getProperty(SessionServicePropertyNames.SESSION_TERMINATION_HANDLERS);
        if (handlerString != null && handlerString.trim().length() > 0) {
            List handlers = StringUtil.split(handlerString, ","); //$NON-NLS-1$
            initTerminationHandlers(handlers);
        }

        CacheFactory cf = ResourceFinder.getCacheFactory();
        this.sessionCache = cf.get(Cache.Type.SESSION, new CacheConfiguration(Policy.LRU, 24*60*60, 5000));

        Configuration config = CurrentConfiguration.getConfiguration();
        Properties nextProperties = config.getProperties();
        this.sessionMaxLimit = PropertiesUtils.getIntProperty(nextProperties, MAX_ACTIVE_Sessions, 0);
        this.sessionTimeLimit = PropertiesUtils.getIntProperty(nextProperties, SESSION_TIME_LIMIT, 0) * SessionServicePropertyNames.MILLIS_PER_MIN;
        this.clusterName = CurrentConfiguration.getSystemName();
        this.sessionMonitor = new Timer("SessionMonitor", true); //$NON-NLS-1$
        this.sessionMonitor.schedule(new TimerTask() {
        	@Override
        	public void run() {
        		monitorSessions();
        	}
        }, this.sessionTimeLimit > 0 ? this.sessionTimeLimit : clientPingInterval * 4, this.clientPingInterval);
    }

    private void monitorSessions() {
		long currentTime = System.currentTimeMillis();
		for (MetaMatrixSessionInfo info : sessionCache.values()) {
			try {
    			if (currentTime - info.getLastPingTime() > clientPingInterval * 4) {
    				LogManager.logInfo(LogSecurityConstants.CTX_SESSION, PlatformPlugin.Util.getString( "SessionServiceImpl.keepaliveFailed", info.getSessionID())); //$NON-NLS-1$
    				closeSession(info.getSessionID());
    			} else if (sessionTimeLimit > 0 && currentTime - info.getTimeCreated() > sessionTimeLimit) {
    				LogManager.logInfo(LogSecurityConstants.CTX_SESSION, PlatformPlugin.Util.getString( "SessionServiceImpl.expireSession", info.getSessionID())); //$NON-NLS-1$
    				closeSession(info.getSessionID());
    			}
			} catch (Exception e) {
				LogManager.logDetail(LogSecurityConstants.CTX_SESSION, e, "error running session monitor, unable to monitor: " + info.getSessionID()); //$NON-NLS-1$
			}
		}
	}

    private void initTerminationHandlers(List handlers) throws ServiceException {

        Iterator iter = handlers.iterator();
        while (iter.hasNext()) {
            String handler = (String)iter.next();
            try {
                SessionTerminationHandler sth = (SessionTerminationHandler)Class.forName(handler).newInstance();
                terminationHandlerMap.put(sth.getProductName(), sth);
            } catch (ClassNotFoundException e) {
                LogManager.logWarning(LogSecurityConstants.CTX_SESSION,e,PlatformPlugin.Util.getString(LogMessageKeys.SEC_SESSION_0002, new Object[] {handler}));
            } catch (Exception e) {
                throw new ServiceException(e, ErrorMessageKeys.SEC_SESSION_0003,PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_SESSION_0003, handler));
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
			throws InvalidSessionException, SessionServiceException,
			ServiceException {
		LogManager.logDetail(LogSecurityConstants.CTX_SESSION, new Object[] {"closeSession", sessionID}); //$NON-NLS-1$
		MetaMatrixSessionInfo info = this.sessionCache.remove(sessionID);
		if (info == null) {
			throw new InvalidSessionException(ErrorMessageKeys.SEC_SESSION_0027, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_SESSION_0027, sessionID));
		}
		SessionTerminationHandler handler = terminationHandlerMap.get(info.getProductName());
        if (handler != null) {
            try {
                handler.cleanup(info.getSessionToken());
            } catch (Exception e) {
                LogManager.logWarning(LogSecurityConstants.CTX_SESSION,e,PlatformPlugin.Util.getString(LogMessageKeys.SEC_SESSION_0028, new Object[] {handler.getProductName()}));
            }
        } else {
            LogManager.logDetail(LogSecurityConstants.CTX_SESSION,PlatformPlugin.Util.getString(LogMessageKeys.SEC_SESSION_0024, new Object[] {info.getProductName()}));
        }
	}
	
	@Override
	public MetaMatrixSessionInfo createSession(String userName,
			Credentials credentials, Serializable trustedToken,
			String applicationName, String productName, Properties properties)
			throws MetaMatrixAuthenticationException, SessionServiceException,
			ServiceException {
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
        if (productName != null && productName.equals(MetaMatrixProductNames.MetaMatrixServer.PRODUCT_NAME)) {
            String vdbName = (String)properties.get(ProductInfoConstants.VIRTUAL_DB);
            String vdbVersion = (String)properties.get(ProductInfoConstants.VDB_VERSION);
            VirtualDatabaseID vdbID = null;
            try {
                vdbID = RuntimeMetadataCatalog.getActiveVirtualDatabaseID(vdbName, vdbVersion);
            } catch (VirtualDatabaseDoesNotExistException e) {
                // Propagate message, don't care about stack trace
                throw new MetaMatrixAuthenticationException(e.getMessage());
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
        long uid = getUniqueSessionID();

        // Return a new session info object
        MetaMatrixSessionInfo newSession = new MetaMatrixSessionInfo(new MetaMatrixSessionID(uid),
        										authenticatedUserName,
        										creationTime,
        										creationTime,
        										applicationName,
        										MetaMatrixSessionState.ACTIVE,
                                                clusterName,
                                                productInfo,
        										productName, 
        										properties.getProperty(MMURL_Properties.CONNECTION.CLIENT_IP_ADDRESS), 
        										properties.getProperty(MMURL_Properties.CONNECTION.CLIENT_HOSTNAME));
        this.sessionCache.put(newSession.getSessionID(), newSession);
        return newSession;
	}
	
    private AuthenticationToken authenticateUser(String userName,
			Credentials credentials, Serializable trustedToken,
			String applicationName) throws SessionServiceException,
			MetaMatrixAuthenticationException {
		AuthenticationToken authenticatedToken = null;
		// Authenticate the principal ...
		try {
			authenticatedToken = (AuthenticationToken) this.membershipService.authenticateUser(userName,
							credentials, trustedToken, applicationName);
		} catch (ServiceException e) {
			String msg = PlatformPlugin.Util
					.getString("SessionServiceImpl.Unable_to_communicate_with_the_membership_service"); //$NON-NLS-1$
			SessionServiceException se = new SessionServiceException(e, msg);
			throw se;
		} catch (RemoteException e) {
			String msg = PlatformPlugin.Util
					.getString("SessionServiceImpl.Unable_to_communicate_with_the_membership_service"); //$NON-NLS-1$
			SessionServiceException se = new SessionServiceException(e, msg);
			throw se;
		} catch (MetaMatrixSecurityException e) {
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
			String msg = PlatformPlugin.Util
					.getString(
							"SessionServiceImpl.The_username_0_and/or_password_are_incorrect", params); //$NON-NLS-1$
			MetaMatrixAuthenticationException e = new MetaMatrixAuthenticationException(
					msg);
			LogManager.logError(LogSecurityConstants.CTX_SESSION, e, msg);
			throw e;
		}

		return authenticatedToken;
	}
	
	/**
	 * TODO: session id should be non-guessable
	 * @return
	 * @throws SessionServiceException
	 */
    protected long getUniqueSessionID() throws SessionServiceException {
        try {
            return DBIDGenerator.getID(DBIDGenerator.SESSION_ID);
        } catch (DBIDGeneratorException e) {
            throw new SessionServiceException(ErrorMessageKeys.SEC_SESSION_0107, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_SESSION_0107));
        }
    }

	@Override
	public int getActiveConnectionsCountForProduct(String product)
			throws SessionServiceException, ServiceException {
		if (product == null) {
			return 0;
		}
		int result = 0;
		for (MetaMatrixSessionInfo info : this.sessionCache.values()) {
			if (product.equalsIgnoreCase(info.getProductName())) {
				result++;
			}
		}
		return result;
	}

	@Override
	public Collection<MetaMatrixSessionInfo> getActiveSessions() throws SessionServiceException,
			ServiceException {
		return new ArrayList<MetaMatrixSessionInfo>(this.sessionCache.values());
	}

	@Override
	public int getActiveSessionsCount() throws SessionServiceException,
			ServiceException {
		return this.sessionCache.size();
	}

	@Override
	public long getPingInterval() {
		return this.clientPingInterval;
	}

	@Override
	public MetaMatrixPrincipal getPrincipal(MetaMatrixSessionID sessionID)
			throws InvalidSessionException, SessionServiceException,
			ServiceException {
        
        MetaMatrixSessionInfo sessionInfo = this.getSessionInfo(sessionID);

        try {
            return membershipService.getPrincipal(new MetaMatrixPrincipalName(sessionInfo.getUserName(), MetaMatrixPrincipal.TYPE_USER));
        } catch (ServiceException e) {
            throw new SessionServiceException(e, ErrorMessageKeys.SEC_SESSION_0004,PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_SESSION_0004));
        } catch (RemoteException e) {
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
	}

	@Override
	public boolean terminateSession(MetaMatrixSessionID terminatedSessionID,
			MetaMatrixSessionID adminSessionID) throws 
			AuthorizationException, SessionServiceException, ServiceException {
		Object[] params = {adminSessionID, terminatedSessionID};
		LogManager.logInfo(LogSecurityConstants.CTX_SESSION, PlatformPlugin.Util.getString( "SessionServiceImpl.terminateSession", params)); //$NON-NLS-1$
		try {
			closeSession(terminatedSessionID);
			return true;
		} catch (InvalidSessionException e) {
			LogManager.logWarning(LogSecurityConstants.CTX_SESSION,e,PlatformPlugin.Util.getString(LogMessageKeys.SEC_SESSION_0034, new Object[] {e.getMessage()}));
			return false;
		}
	}

	@Override
	public SessionToken validateSession(MetaMatrixSessionID sessionID)
			throws InvalidSessionException, SessionServiceException,
			ServiceException {
		MetaMatrixSessionInfo info = getSessionInfo(sessionID);
		return info.getSessionToken();
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

	void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

}
