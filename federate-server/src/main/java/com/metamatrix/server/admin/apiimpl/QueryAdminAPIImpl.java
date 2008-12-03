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

package com.metamatrix.server.admin.apiimpl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.metamatrix.admin.api.server.AdminRoles;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.api.exception.security.InvalidSessionException;
import com.metamatrix.api.exception.server.InvalidRequestIDException;
import com.metamatrix.dqp.message.RequestID;
import com.metamatrix.platform.admin.apiimpl.AdminAPIHelper;
import com.metamatrix.platform.admin.apiimpl.SubSystemAdminAPIImpl;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;
import com.metamatrix.platform.security.api.SessionToken;
import com.metamatrix.platform.util.PlatformProxyHelper;
import com.metamatrix.server.admin.api.QueryAdminAPI;
import com.metamatrix.server.query.service.QueryServiceInterface;

public class QueryAdminAPIImpl extends SubSystemAdminAPIImpl implements QueryAdminAPI {

    private QueryServiceInterface queryAdmin;
    private static QueryAdminAPI queryAdminAPI;
    /**
     * ctor
     */
    private QueryAdminAPIImpl() throws MetaMatrixComponentException {

        queryAdmin = PlatformProxyHelper.getQueryServiceProxy(PlatformProxyHelper.ROUND_ROBIN_LOCAL);
    }

    public static synchronized QueryAdminAPI getInstance() throws MetaMatrixComponentException {
        if (queryAdminAPI == null) {
            queryAdminAPI = new QueryAdminAPIImpl();
        }
        return queryAdminAPI;
    }
    
    /**
     * Return all queries that are in the system.
     *
	 * @param callerSessionID ID of the caller's current session.
     * @return a collection of <code>Request</code> objects.
     * @throws MetaMatrixComponentException if an error occurs in communicating with a component.
     */
    public synchronized Collection getAllRequests()
        throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException {

        // Validate caller's session
        AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role

        List requests = new ArrayList();
        requests.addAll(queryAdmin.getAllQueries());
        return requests;
    }

    /**
     * Return all queries that are currently being processed for the user session.
     *
	 * @param callerSessionID ID of the caller's current session.
     * @param userSessionID the primary identifier for the user account.
     * @return a collection of <code>Request</code> objects.
     * @throws MetaMatrixComponentException if an error occurs in communicating with a component.
     */
    public synchronized Collection getRequestsForSession(MetaMatrixSessionID userSessionID)
        throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException {

        // Validate caller's session
        AdminAPIHelper.validateSession(getSessionID());
        // Validate user's session
        SessionToken userToken = AdminAPIHelper.validateSession(userSessionID);

        // Any administrator may call this read-only method - no need to validate role

        List requests = new ArrayList();
        requests.addAll(queryAdmin.getQueriesForSession(userToken));
        return requests;
    }


    /**
     * Cancel a single query for the user session.
     *
	 * @param callerSessionID ID of the caller's current session.
     * @param requestID the identifier of the query to be cancelled.
     * @throws InvalidRequestIDException if the <code>Request</code> specified by the ID does not exist.
     * @throws MetaMatrixComponentException if an error occurs in communicating with a component.
     */
    public synchronized void cancelRequest(RequestID requestID)
        throws AuthorizationException, InvalidSessionException, InvalidRequestIDException, MetaMatrixComponentException {

        // Validate caller's session
        SessionToken callerToken = AdminAPIHelper.validateSession(getSessionID());
        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(callerToken, AdminRoles.RoleName.ADMIN_PRODUCT, "QueryAdminAPIImpl.cancelRequest(" + requestID + ")"); //$NON-NLS-1$ //$NON-NLS-2$
        queryAdmin.cancelQuery(requestID, true);
    }
    
    /**
     * Cancel an atomic query for the user session.
     *
     * @param callerSessionID ID of the caller's current session.
     * @param requestID the identifier of the query that is running.
     * @param nodeid identifies the atomic query to be cancelled.
     * 
     * @throws InvalidRequestIDException if the <code>Request</code> specified by the ID does not exist.
     * @throws MetaMatrixComponentException if an error occurs in communicating with a component.
     */
    public synchronized void cancelRequest(RequestID requestID, int nodeID)
        throws AuthorizationException, InvalidSessionException, InvalidRequestIDException, MetaMatrixComponentException {

        // Validate caller's session
        SessionToken callerToken = AdminAPIHelper.validateSession(getSessionID());
        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(callerToken, AdminRoles.RoleName.ADMIN_PRODUCT, "QueryAdminAPIImpl.cancelRequest(" + requestID + ", " + nodeID + ")"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

        queryAdmin.cancelQuery(requestID, nodeID);
    }
    

    /**
     * Cancel all queries for the user session.
     *
	 * @param callerSessionID ID of the caller's current session.
     * @param userSessionID the primary identifier for the user account.
     * @throws MetaMatrixComponentException if an error occurs in communicating with a component.
     */
    public synchronized void cancelRequests(MetaMatrixSessionID userSessionID)
        throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException {

        // Validate caller's session
        SessionToken callerToken = AdminAPIHelper.validateSession(getSessionID());
        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(callerToken, AdminRoles.RoleName.ADMIN_PRODUCT, "QueryAdminAPIImpl.cancelRequests(" + userSessionID + ")"); //$NON-NLS-1$ //$NON-NLS-2$
        // Validate user's session
        SessionToken userToken = AdminAPIHelper.validateSession(userSessionID);

        try {
            queryAdmin.cancelQueries(userToken, true);
        } catch (InvalidRequestIDException e) {
            throw new MetaMatrixComponentException(e, e.getMessage());
        } 
    }


}
