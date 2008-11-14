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

package com.metamatrix.platform.admin.apiimpl;

import java.util.Collection;

import com.metamatrix.admin.api.server.AdminRoles;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.api.exception.security.InvalidSessionException;
import com.metamatrix.api.exception.security.SessionServiceException;
import com.metamatrix.dqp.internal.process.DQPWorkContext;
import com.metamatrix.platform.admin.api.SessionAdminAPI;
import com.metamatrix.platform.security.api.MetaMatrixPrincipal;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;
import com.metamatrix.platform.security.api.SessionToken;
import com.metamatrix.platform.security.api.service.SessionServiceInterface;
import com.metamatrix.platform.util.PlatformProxyHelper;

public class SessionAdminAPIImpl extends SubSystemAdminAPIImpl implements SessionAdminAPI {

    // Auth svc proxy
    private SessionServiceInterface sessAdmin;
    private static SessionAdminAPI sessionAdminAPI;

    /**
     * ctor
     */
    private SessionAdminAPIImpl() {
        
        sessAdmin = PlatformProxyHelper.getSessionServiceProxy(PlatformProxyHelper.ROUND_ROBIN_LOCAL);
    }
    
    public synchronized static SessionAdminAPI getInstance() {
        if (sessionAdminAPI == null) {
            sessionAdminAPI = new SessionAdminAPIImpl();
        }
        return sessionAdminAPI;
    }
    
    /**
     * Get the information for the account to which the specified session has been authenticated.
     *
     * @param userSessionID ID identifying session for which the account information is to be obtained
     * @return the information for the user account for which the <code>sessionToken</code> is logged into
     * @throws InvalidSessionException If the caller's session has expired or doesn't exist
     * @throws SessionNotFoundException If specified <code>userSessionID</code> is invalid or nonexistant
     * @throws AuthorizationException if the caller denoted by <code>callerSessionID</code>
     * does not have authority to access the account information for the <code>userSessionID</code> session
     * @throws MetaMatrixComponentException If couldn't find a component
     */
    public synchronized MetaMatrixPrincipal getPrincipal(MetaMatrixSessionID userSessionID)
    throws InvalidSessionException, AuthorizationException, MetaMatrixComponentException, SessionServiceException {
        return sessAdmin.getPrincipal(userSessionID);
    }
    
    public synchronized Collection getActiveSessions()
    throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException, SessionServiceException {
        return sessAdmin.getActiveSessions();
    }

    public synchronized void terminateSession(MetaMatrixSessionID userSessionID)
    throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException, SessionServiceException {
        // Validate caller's session
        SessionToken token = DQPWorkContext.getWorkContext().getSessionToken();
        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(token, AdminRoles.RoleName.ADMIN_SYSTEM, "SessionAdminAPIImpl.terminateSession(" + userSessionID + ")"); //$NON-NLS-1$ //$NON-NLS-2$
        sessAdmin.terminateSession(userSessionID, getSessionID());
    }

    public synchronized Boolean isSessionValid(MetaMatrixSessionID userSessionID)
    throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException, SessionServiceException {
        // Any administrator may call this read-only method - no need to validate role
        try {
        	sessAdmin.validateSession(userSessionID);
        	return Boolean.TRUE;
        } catch (InvalidSessionException e) {
        	
        }
        return Boolean.FALSE;
    }

    /**
     * Get the count of active user sessions on the system.
     * @return The count of all active users on
     * the system.
     */
    public synchronized int getActiveSessionsCount()
    throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException, SessionServiceException {
        return sessAdmin.getActiveSessionsCount();
    }

    /**
     * Get the count of all active connections to a product.
     * @return The count of all active connections to a product on
     * the system.
     */
    public synchronized int getActiveConnectionsCountForProduct(String product)
    throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException, SessionServiceException {
        return sessAdmin.getActiveConnectionsCountForProduct(product);
    }

}

