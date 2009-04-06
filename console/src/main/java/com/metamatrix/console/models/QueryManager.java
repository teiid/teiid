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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import javax.swing.Action;

import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.api.exception.server.InvalidRequestIDException;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.ui.ViewManager;
import com.metamatrix.console.util.AutoRefresher;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.console.util.InvalidRequestException;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;
import com.metamatrix.server.admin.api.QueryAdminAPI;
import com.metamatrix.server.serverapi.RequestInfo;

public class QueryManager extends TimedManager implements ManagerListener{

    private HashSet queryRequests;
    private MetaMatrixSessionID currentToken = null;
    private Action refreshAction;
    private AutoRefresher ar;

	public QueryManager(ConnectionInfo connection) {
		super(connection);
	}
	
    public void init() {
        super.init();
        super.setRefreshRate(5);
        setQueryRequests(new HashSet());
        setIsStale(true);
        ModelManager.getSessionManager(getConnection()).addManagerListener(this);
        super.setIsAutoRefreshEnabled(false);
    }
    
    private QueryAdminAPI getQueryAdminAPI() {
        return ModelManager.getQueryAPI(getConnection());
    }
    
    /**
     * The QueryManager listens for changes to the SessionManager.  If, for
     *example, a session is terminated, the QueryManager needs to know so
     *it can force itself to refresh.
     */
    public void modelChanged(ModelChangedEvent e){
        if (e.getMessage().equals(SessionManager.SESSION_TERMINATED)){
            this.refresh();
        }
    }

    public void cancelQueryRequests(Collection selected)
            throws ExternalException, AuthorizationException,
            InvalidRequestException, ComponentNotFoundException {
        try {
            ViewManager.startBusy();
            Iterator iterator = selected.iterator();
            RequestInfo q;
            // determine which request are a global query cancellation
            // versus just an atomic query.
            // If a global request and an atomic request for the same
            // query is select, then the global request will be submitted.
            List queries = new ArrayList(selected.size());
            while (iterator.hasNext()){
                q = (RequestInfo)iterator.next();
                if (!q.isAtomicQuery()) {
                    queries.add(q);
                }
            }
                
            iterator = selected.iterator();
            while (iterator.hasNext()){
                q = (RequestInfo)iterator.next();
                try{
                    if (q.isAtomicQuery()) {
                        if (!queries.contains(q)) {
                            getQueryAdminAPI().cancelRequest(q.getRequestID(), q.getNodeID());
                        }
                    } else {
                        getQueryAdminAPI().cancelRequest(q.getRequestID());
                    }
                } catch (AuthorizationException e) {
                    throw e;
                } catch (ComponentNotFoundException e) {
                    throw e;
                } catch (InvalidRequestIDException e) {
                    throw new InvalidRequestException(e);
                } catch (InvalidSessionException e) {
                    throw new InvalidRequestException(e);
                } catch (Exception e) {
                    throw new ExternalException(e);
                }
            }
            refresh();
        } finally {
            ViewManager.endBusy();
        }

    }

    public void cancelAllQueryRequests(MetaMatrixSessionID token)
            throws ComponentNotFoundException, InvalidRequestException,
            AuthorizationException, ExternalException {
        try{
            ViewManager.startBusy();
            try{
                getQueryAdminAPI().cancelRequests(token);
            } catch (ComponentNotFoundException e) {
                throw e;
            } catch (InvalidSessionException e) {
                throw new InvalidRequestException(e);
            } catch (AuthorizationException e) {
                throw e;
            } catch (Exception e) {
                throw new ExternalException(e);
            }
            refresh();
        } finally {
            ViewManager.endBusy();
        }
    }

    public Collection getAllRequests()
            throws AuthorizationException, ExternalException {
        loadRealData();
        return (Collection) getQueryRequests().clone();
    }

    public void setAutoRefresher(AutoRefresher autoRefresher){
        ar = autoRefresher;
    }

    private void loadRealData()
            throws ExternalException, AuthorizationException {
        boolean autoRefresh = (ar!=null && ar.isAutoRefreshEnabled());
        if (getIsStale() || autoRefresh || currentToken != null){
            try{
                currentToken = null;
                ViewManager.startBusy(); //ADJUSTS STATUS BAR
                //THIS COULD FAIL SILENTLY
                Collection col;
                try {
                    col = getQueryAdminAPI().getAllRequests();
                } catch (AuthorizationException e) {
                    throw e;
                } catch (Exception e) {
                    throw new ExternalException(e);
                }

                if (col!= null)
                    setQueryRequests(new HashSet(col));
                else
                    setQueryRequests(new HashSet(0));
                setIsStale(false);
                super.startTimer();
            } finally {
                ViewManager.endBusy();
            }
        }
    }

    //GETTERS-SETTERS

    private HashSet getQueryRequests(){
        return queryRequests;
    }

    private void setQueryRequests(HashSet set){
        queryRequests = set;
    }

    //OVERRIDING superclass implementation of refresh()
    public void refresh(){
        super.refresh();
        if(refreshAction != null){
            refreshAction.actionPerformed(null);
        }
    }

    public void setRefreshAction(Action refAction){
        refreshAction= refAction;
    }


}

