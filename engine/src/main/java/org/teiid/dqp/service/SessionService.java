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

package org.teiid.dqp.service;

import java.util.Collection;
import java.util.Properties;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.client.security.InvalidSessionException;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.net.TeiidURL.CONNECTION.AuthenticationType;
import org.teiid.security.Credentials;


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
public interface SessionService {
    public static String NAME = "SessionService"; //$NON-NLS-1$

    public static final long DEFAULT_MAX_SESSIONS = 5000; 
    public static final long DEFAULT_SESSION_EXPIRATION = 0; 
    
    public static final String MAX_SESSIONS = "session.maxSessions"; //$NON-NLS-1$
    public static final String SESSION_EXPIRATION = "session.expirationTimeInMilli"; //$NON-NLS-1$
    
    /**
     * Create a session for the given user authenticating against the given <code>Credentials</code>.
     */
    public SessionMetadata createSession(String userName,
                                               Credentials credentials,
                                               String applicationName,
                                               Properties properties, boolean admin, boolean authenticate)
            throws LoginException, SessionServiceException;

    /**
     * Closes the specified session.
     *
     * @param sessionID The MetaMatrixSessionID identifying user's session
     * to be closed
     * @throws InvalidSessionException If sessionID identifies an invalid
     * session
     * @throws SessionServiceException
     */
    void closeSession(String sessionID) throws InvalidSessionException;

    /**
     * Terminates the specified session.  This is an administrative action.
     *
     * @param terminatedSessionID The MetaMatrixSessionID identifying user's session
     * to be terminated
     * @param adminSessionID The session id identifying session of administrator
     * @throws InvalidSessionException If terminatedSessionID identifies an invalid
     * session
     * does not have authority to terminate the <code>terminatedSessionID</code> session
     * @throws SessionServiceException
     */
    boolean terminateSession(String terminatedSessionID, String adminSessionID);

    /**
     * Get the collection of active user sessions on the system.
     * @return The collection of MetaMatrixSessionInfo objects of active users on
     * the system - possibly empty, never null.
     */
    Collection<SessionMetadata> getActiveSessions() throws SessionServiceException;

    /**
     * Get the number of active user sessions on the system.
     * @return int
     */
    int getActiveSessionsCount() throws SessionServiceException;

    /**
     * This method is intended to verify that the session is valid, and, if
     * need be, set the session in an active state, ready to be used.
     * @param sessionID MetaMatrixSessionID representing the session
     * @return SessionToken object identifying the session
     * @throws InvalidSessionException If sessionID identifies an invalid
     * session
     * @throws SessionServiceException
     */
    SessionMetadata validateSession(String sessionID)
    throws InvalidSessionException, SessionServiceException;
    
    /**
     * Get all <code>MetaMatrixSessionID</code>s that are in the ACTIVE state
     * and currently logged in to a VDB.
     * @param VDBName The name of the VDB.
     * @param VDBVersion The version of the VDB.
     * @throws SessionServiceException when transaction with database fails or unexpected exception happens
     */
    Collection<SessionMetadata> getSessionsLoggedInToVDB(String VDBName, int VDBVersion)
    throws SessionServiceException;

    /**
     * Periodically called by the client to indicate the client is still alive.
     *
     * @param sessionID - identifies the client
     */
    public void pingServer(String sessionID) throws InvalidSessionException;
    
    SessionMetadata getActiveSession(String sessionID);
    
	void setDqp(DQPCore dqp);
	
	LoginContext createLoginContext(String securityDomain, String user, String password) throws LoginException;

	AuthenticationType getAuthType();
	
	String getGssSecurityDomain();
	
	void associateSubjectInContext(String securityDomain, Subject subject); 
	
	Subject getSubjectInContext(String securityDomain);
}
