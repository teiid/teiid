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

package com.metamatrix.console.models;

import java.util.Collection;

import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.security.AuthorizationException;

import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.ui.ViewManager;
import com.metamatrix.console.util.ExternalException;

import com.metamatrix.platform.security.api.MetaMatrixSessionID;
import com.metamatrix.platform.security.api.MetaMatrixSessionInfo;

/**
 * Facade for GUI client to Session objects and Session-related functionality
 *of the remote system.  SessionManager maintains a collection of
 *UserSessionInfos for GUI client, manages refreshing of data (for
 *user-specified refresh rate) and exposes session-related business methods.
 */
public class SessionManager extends TimedManager{

    /**
     * This String is used as a message for a ModelChangedEvent, in the event
     *that a session is terminated
     * @see ModelChangedEvent
     * @see SessionManager#terminateSession
     */
    public static final String SESSION_TERMINATED= "A Session was terminated."; //$NON-NLS-1$

	public SessionManager(ConnectionInfo connection) {
		super(connection);
	}
	
    /**
     * Readies SessionManager to be used.  This object will initialize all
     *of its internal stuff and will inform listeners that it is ready for
     *use.
     */
    public void init() {
        super.init();
        super.setRefreshRate(10);
        setIsStale(true);
        super.setIsAutoRefreshEnabled(false);
    }

    public Collection /*<MetaMatrixSessionInfo >*/ getActiveSessions() throws Exception {
        Collection result = null;
        result = ModelManager.getSessionAPI(getConnection()).getActiveSessions();
        return result;
    }
    
    /**
     * Terminates, on remote system, the session specified by the parameter.
     *Fires a ModelChangedEvent with message
     *SessionManager.TERMINATE_SESSION and arg SessionToken.
     * @param userSessionInfo identifies session to be terminated
     */
    public void terminateSession(MetaMatrixSessionInfo userSessionInfo)
            throws ExternalException, AuthorizationException,
            ComponentNotFoundException {
        try {
            ViewManager.startBusy(); //ADJUSTS STATUS BAR
            ModelManager.getSessionAPI(getConnection()).terminateSession(
            		userSessionInfo.getSessionID());
        } catch (ComponentNotFoundException e) {
            throw e;
        } catch (AuthorizationException e) {
            throw e;
        } catch (Exception e) {
            throw new ExternalException(e);
        } finally {
            ViewManager.endBusy(); //ADJUSTS STATUS BAR
        }
        super.fireModelChangedEvent(SESSION_TERMINATED, userSessionInfo.getSessionToken());
    }

    public Collection /*<MetaMatrixSessionInfo>*/ getSessions() throws Exception {
        return getActiveSessions();
    }

    public boolean isSignedOnUserSessionValid()  {
        Boolean BIsValid = Boolean.FALSE;
        try {
            ViewManager.startBusy(); //ADJUSTS STATUS BAR
            MetaMatrixSessionID mmsidSessionID
                    = getConnection().getServerConnection().getLogonResult().getSessionID();

            BIsValid = ModelManager.getSessionAPI(getConnection())
                            .isSessionValid( mmsidSessionID );

        } catch (Exception e) {
            // no action, exception interpreted as FALSE
        } finally {
            ViewManager.endBusy(); //ADJUSTS STATUS BAR
        }

        return BIsValid.booleanValue();
    }
}

