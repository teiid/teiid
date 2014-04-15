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

import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.client.security.InvalidSessionException;
import org.teiid.client.security.LogonException;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.net.socket.AuthenticationType;
import org.teiid.security.Credentials;
import org.teiid.security.SecurityHelper;


/**
 * <p>
 * The session service deals with managing sessions; this involves creating
 * sessions, closing sessions, terminating sessions, and updating session
 * state. Note that this service does <i>not</i> deal with authentication explicitly,
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
	public SessionMetadata createSession(String vdbName,
			String vdbVersion, AuthenticationType authType,
			String user, Credentials credential,
			String applicationName, Properties connProps) throws LoginException,
			SessionServiceException;

    /**
     * Closes the specified session.
     */
    void closeSession(String sessionID) throws InvalidSessionException;

    /**
     * Terminates the specified session.  This is an administrative action.
     *
     * @param terminatedSessionID The SessionID identifying user's session to be terminated
     * @param adminSessionID The session id identifying session of administrator
     */
    boolean terminateSession(String terminatedSessionID, String adminSessionID);

    /**
     * Get the collection of active user sessions on the system.
     * @return The collection of Session objects of active users on
     * the system - possibly empty, never null.
     */
    Collection<SessionMetadata> getActiveSessions();

    /**
     * Get the number of active user sessions on the system.
     * @return int
     */
    int getActiveSessionsCount() throws SessionServiceException;

    /**
     * This method is intended to verify that the session is valid, and, if
     * need be, set the session in an active state, ready to be used.
     * @param sessionID SessionID representing the session
     * @return Session object identifying the session
     */
    SessionMetadata validateSession(String sessionID)
    throws InvalidSessionException, SessionServiceException;
    
    /**
     * Get all Sessions that are in the ACTIVE state
     * and currently logged in to a VDB.
     * @param VDBName The name of the VDB.
     * @param VDBVersion The version of the VDB.
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

	AuthenticationType getAuthenticationType(String vdbName, String version, String user) throws LogonException;
	
	SecurityHelper getSecurityHelper();
	
	GSSResult neogitiateGssLogin(String user, String vdbName, String vdbVersion, byte[] serviceTicket) throws LoginException, LogonException;
}
