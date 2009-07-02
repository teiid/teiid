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

package com.metamatrix.platform.security.api.service;

import java.io.Serializable;
import java.util.Collection;
import java.util.Properties;

import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.api.exception.security.MetaMatrixAuthenticationException;
import com.metamatrix.api.exception.security.SessionServiceException;
import com.metamatrix.common.application.ApplicationService;
import com.metamatrix.platform.security.api.Credentials;
import com.metamatrix.platform.security.api.MetaMatrixPrincipal;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;
import com.metamatrix.platform.security.api.MetaMatrixSessionInfo;

/**
 * <p>
 * The session service deals with managing sessions; this involves creating
 * sessions, closing sessions, terminating sessions, and updating session
 * state.
 * </p>
 * <p>
 * A session has a timestamp, information about the principal owning the
 * session, and a "state" indicating whether it is actively in use, in use
 * but passivated, or has been rendered invalid by being closed or terminated,
 * or by expiring.
 * </p>
 * <p>
 * Note that this service does <i>not</i> deal with authentication explicitly,
 * but may use a membership service provider to authenticate some
 * requests.
 * </p>
 */
public interface SessionServiceInterface extends ApplicationService {
    public static String NAME = "SessionService"; //$NON-NLS-1$

    public static final String DEFAULT_MAX_SESSIONS = "5000"; //$NON-NLS-1$
    public static final String DEFAULT_SESSION_TIMEOUT = "600000"; //$NON-NLS-1$
    
    public static final String MAX_SESSIONS = "session.maxSessions"; //$NON-NLS-1$
    public static final String SESSION_TIMEOUT = "session.timeoutInMilli"; //$NON-NLS-1$
    
    /**
     * Create a session for the given user authenticating against the given <code>Credentials</code>.
     */
    public MetaMatrixSessionInfo createSession(String userName,
                                               Credentials credentials,
                                               Serializable trustedToken,
                                               String applicationName,
                                               Properties properties)
            throws MetaMatrixAuthenticationException, SessionServiceException;

    /**
     * Closes the specified session.
     *
     * @param sessionID The MetaMatrixSessionID identifying user's session
     * to be closed
     * @throws InvalidSessionException If sessionID identifies an invalid
     * session
     * @throws SessionServiceException
     */
    void closeSession(MetaMatrixSessionID sessionID)
    throws InvalidSessionException, SessionServiceException;

    /**
     * Terminates the specified session.  This is an administrative action.
     *
     * @param terminatedSessionID The MetaMatrixSessionID identifying user's session
     * to be terminated
     * @param adminSessionID The session id identifying session of administrator
     * @throws InvalidSessionException If terminatedSessionID identifies an invalid
     * session
     * @throws AuthorizationException if the caller denoted by <code>adminSessionID</code>
     * does not have authority to terminate the <code>terminatedSessionID</code> session
     * @throws SessionServiceException
     */
    boolean terminateSession(MetaMatrixSessionID terminatedSessionID, MetaMatrixSessionID adminSessionID)
    throws InvalidSessionException, AuthorizationException, SessionServiceException;

    /**
     * Get the collection of active user sessions on the system.
     * @return The collection of MetaMatrixSessionInfo objects of active users on
     * the system - possibly empty, never null.
     */
    Collection<MetaMatrixSessionInfo> getActiveSessions() throws SessionServiceException;

    /**
     * Get the number of active user sessions on the system.
     * @return int
     */
    int getActiveSessionsCount() throws SessionServiceException;

    /**
     * Returns a MetaMatrixPrincipal object describing the owner (user) of the
     * indicated session.
     * @param sessionID MetaMatrixSessionID representing the session
     * @return MetaMatrixPrincipal object describing the owner of the
     * indicated session.
     */
    MetaMatrixPrincipal getPrincipal(MetaMatrixSessionID sessionID)
    throws InvalidSessionException, SessionServiceException;

    /**
     * This method is intended to verify that the session is valid, and, if
     * need be, set the session in an active state, ready to be used.
     * @param sessionID MetaMatrixSessionID representing the session
     * @return SessionToken object identifying the session
     * @throws InvalidSessionException If sessionID identifies an invalid
     * session
     * @throws SessionServiceException
     */
    MetaMatrixSessionInfo validateSession(MetaMatrixSessionID sessionID)
    throws InvalidSessionException, SessionServiceException;
    
    /**
     * Get all <code>MetaMatrixSessionID</code>s that are in the ACTIVE state
     * and currently logged in to a VDB.
     * @param VDBName The name of the VDB.
     * @param VDBVersion The version of the VDB.
     * @throws SessionServiceException when transaction with database fails or unexpected exception happens
     */
    Collection<MetaMatrixSessionInfo> getSessionsLoggedInToVDB(String VDBName, String VDBVersion)
    throws SessionServiceException;

    /**
     * Periodically called by the client to indicate the client is still alive.
     *
     * @param sessionID - identifies the client
     */
    public void pingServer(MetaMatrixSessionID sessionID) throws InvalidSessionException;
    
    /**
     * Register a session listener
     * @param listener
     */
    public void register(SessionListener listener);
    
}
