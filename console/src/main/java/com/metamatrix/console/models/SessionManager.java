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

import com.metamatrix.admin.api.objects.Session;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.ui.ViewManager;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.platform.security.api.ILogon;

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
    
    private Collection<Session> sessions;
    private long lastReferesh;

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

    /**
     * Terminates, on remote system, the session specified by the parameter.
     *Fires a ModelChangedEvent with message
     *SessionManager.TERMINATE_SESSION and arg SessionToken.
     * @param userSessionInfo identifies session to be terminated
     */
    public void terminateSession(String userSessionInfo)
            throws ExternalException {
        try {
            ViewManager.startBusy(); //ADJUSTS STATUS BAR
            getConnection().getServerAdmin().terminateSession(userSessionInfo);
        } catch (Exception e) {
            throw new ExternalException(e);
        } finally {
            ViewManager.endBusy(); //ADJUSTS STATUS BAR
        }
        super.fireModelChangedEvent(SESSION_TERMINATED, userSessionInfo);
    }

    public Collection<Session> getSessions() throws Exception {
        return getSessions(false);
    }
    
    public Collection<Session> getSessions(boolean refresh) throws Exception {
    	if (refresh || lastReferesh + 30000 < System.currentTimeMillis()) {
    		this.sessions = getConnection().getServerAdmin().getSessions("*"); //$NON-NLS-1$
    	}
    	return this.sessions;
    }

    public boolean isSignedOnUserSessionValid()  {
        try {
            ViewManager.startBusy(); //ADJUSTS STATUS BAR
            getConnection().getServerConnection().getService(ILogon.class).ping();
            return true;
        } catch (Exception e) {
            // no action, exception interpreted as FALSE
        } finally {
            ViewManager.endBusy(); //ADJUSTS STATUS BAR
        }

        return false;
    }
}

