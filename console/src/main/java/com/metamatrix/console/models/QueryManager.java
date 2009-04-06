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
import java.util.Set;

import javax.swing.Action;

import com.metamatrix.admin.api.objects.AdminObject;
import com.metamatrix.admin.api.objects.Request;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.ui.ViewManager;
import com.metamatrix.console.util.AutoRefresher;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;

public class QueryManager extends TimedManager implements ManagerListener{

    private Collection<Request> queryRequests = new HashSet<Request>();
    private MetaMatrixSessionID currentToken = null;
    private Action refreshAction;
    private AutoRefresher ar;

	public QueryManager(ConnectionInfo connection) {
		super(connection);
	}
	
    public void init() {
        super.init();
        super.setRefreshRate(5);
        setIsStale(true);
        ModelManager.getSessionManager(getConnection()).addManagerListener(this);
        super.setIsAutoRefreshEnabled(false);
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

    public void cancelQueryRequests(Collection<Request> selected)
            throws ExternalException {
        try {
            ViewManager.startBusy();
            // determine which request are a global query cancellation
            // versus just an atomic query.
            // If a global request and an atomic request for the same
            // query is select, then the global request will be submitted.
            Set<String> queries = new HashSet<String>();
            for (Request q : selected) {
                if (!q.isSource()) {
                    queries.add(q.getIdentifier());
                }
            }
                
            for (Request q : selected) {
                try{
                    if (q.isSource()) {
                        if (!queries.contains(q.getSessionID() + '.' + q.getRequestID())) {
                            getConnection().getServerAdmin().cancelSourceRequest(q.getIdentifier());
                        }
                    } else {
                    	getConnection().getServerAdmin().cancelRequest(q.getIdentifier());
                    }
                } catch (Exception e) {
                    throw new ExternalException(e);
                }
            }
            refresh();
        } finally {
            ViewManager.endBusy();
        }

    }

    public Collection<Request> getAllRequests()
            throws ExternalException {
        loadRealData();
        return new ArrayList<Request>(getQueryRequests());
    }

    public void setAutoRefresher(AutoRefresher autoRefresher){
        ar = autoRefresher;
    }

    private void loadRealData()
            throws ExternalException {
        boolean autoRefresh = (ar!=null && ar.isAutoRefreshEnabled());
        if (getIsStale() || autoRefresh || currentToken != null){
            try{
                currentToken = null;
                ViewManager.startBusy(); //ADJUSTS STATUS BAR
                //THIS COULD FAIL SILENTLY
                try {
                	this.queryRequests = new ArrayList<Request>(this.getConnection().getServerAdmin().getRequests(AdminObject.WILDCARD));
                	this.queryRequests.addAll(this.getConnection().getServerAdmin().getSourceRequests(AdminObject.WILDCARD));
                } catch (Exception e) {
                    throw new ExternalException(e);
                }
                setIsStale(false);
                super.startTimer();
            } finally {
                ViewManager.endBusy();
            }
        }
    }

    //GETTERS-SETTERS

    private Collection<Request> getQueryRequests(){
        return queryRequests;
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

