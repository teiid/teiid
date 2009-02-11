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

package com.metamatrix.server.admin.api;

import java.util.*;

import com.metamatrix.platform.admin.api.SubSystemAdminAPI;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;

import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.api.exception.server.InvalidRequestIDException;
import com.metamatrix.dqp.message.RequestID;
     
public interface QueryAdminAPI extends SubSystemAdminAPI {

    /**
     * Return all queries that are in the system.
     *
     * @return a collection of <code>Request</code> objects.
	 * @throws AuthorizationException if caller is not authorized to perform this method.
	 * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException if an error occurs in communicating with a component.
     */
    Collection getAllRequests()
        throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException;

    /**
     * Return all queries that are currently being processed for the user session.
     *
     * @param userSessionID the primary identifier for the user account.
     * @return a collection of <code>Request</code> objects.
	 * @throws AuthorizationException if caller is not authorized to perform this method.
	 * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException if an error occurs in communicating with a component.
     */
    Collection getRequestsForSession(MetaMatrixSessionID userSessionID)
        throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException;


    /**
     * Cancel a single query.
     *
     * @param requestID the identifier of the query to be cancelled.
	 * @throws AuthorizationException if caller is not authorized to perform this method.
	 * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws InvalidRequestIDException if the <code>Request</code> specified by the ID does not exist.
     * @throws MetaMatrixComponentException if an error occurs in communicating with a component.
     */
    void cancelRequest(RequestID requestID)
        throws AuthorizationException, InvalidSessionException, InvalidRequestIDException, MetaMatrixComponentException;

    /**
     * Cancel multiple queries for the user session.
     *
     * @param userSessionID the primary identifier for the user account.
     * @param requestIDs the collection of identifiers of the queries to be cancelled.
	 * @throws AuthorizationException if caller is not authorized to perform this method.
	 * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws InvalidRequestIDException if the <code>Request</code> specified by the ID does not exist.
     * @throws MetaMatrixComponentException if an error occurs in communicating with a component.
     */
    //void cancelRequests(MetaMatrixSessionID userSessionID, Collection requestIDs)
        //throws AuthorizationException, InvalidSessionException, InvalidRequestIDException, MetaMatrixComponentException;

    /**
     * Cancel all queries for the user session.
     *
     * @param userSessionID the primary identifier for the user account.
	 * @throws AuthorizationException if caller is not authorized to perform this method.
	 * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException if an error occurs in communicating with a component.
     */
    void cancelRequests(MetaMatrixSessionID userSessionID)
        throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException;

    /**
     * Cancel an atomic query for the user session.
     *
     * @param callerSessionID ID of the caller's current session.
     * @param requestID the identifier of the query to be cancelled.
     * @param nodeID identifies the node in the query to cancel.
     * @throws AuthorizationException if caller is not authorized to perform this method.
     * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws InvalidRequestIDException if the <code>Request</code> specified by the ID does not exist.
     * @throws MetaMatrixComponentException if an error occurs in communicating with a component.
     */
    void cancelRequest(RequestID requestID, int nodeID)
    throws AuthorizationException, InvalidSessionException, InvalidRequestIDException, MetaMatrixComponentException;

}

