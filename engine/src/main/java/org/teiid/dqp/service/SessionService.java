/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.dqp.service;

import java.util.Collection;
import java.util.Properties;

import javax.security.auth.login.LoginException;

import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.client.security.InvalidSessionException;
import org.teiid.client.security.LogonException;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.net.socket.AuthenticationType;
import org.teiid.security.Credentials;
import org.teiid.security.GSSResult;
import org.teiid.security.SecurityHelper;
import org.teiid.vdb.runtime.VDBKey;


/**
 * <p>
 * The session service deals with managing sessions; this involves creating
 * sessions, closing sessions, terminating sessions, and updating session
 * state. Note that this service does <i>not</i> deal with authentication explicitly,
 * but may use a membership service provider to authenticate some
 * requests.
 *
 */
public interface SessionService {
    public static String NAME = "SessionService"; //$NON-NLS-1$

    public static final long DEFAULT_MAX_SESSIONS = 10000;
    public static final long DEFAULT_SESSION_EXPIRATION = 0;

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
     * @param vdbKey
     */
    Collection<SessionMetadata> getSessionsLoggedInToVDB(VDBKey vdbKey);

    /**
     * Periodically called by the client to indicate the client is still alive.
     *
     * @param sessionID - identifies the client
     */
    public void pingServer(String sessionID) throws InvalidSessionException;

    SessionMetadata getActiveSession(String sessionID);

    void setDqp(DQPCore dqp);

    AuthenticationType getAuthenticationType(String vdbName, String version, String user) throws LogonException;

    SecurityHelper getSecurityHelper();

    GSSResult neogitiateGssLogin(String user, String vdbName, String vdbVersion, byte[] serviceTicket) throws LoginException, LogonException;

    AuthenticationType getDefaultAuthenticationType();
}
