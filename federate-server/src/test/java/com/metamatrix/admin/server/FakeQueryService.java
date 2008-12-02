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

package com.metamatrix.admin.server;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.metamatrix.admin.api.objects.Request;
import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.server.InvalidRequestIDException;
import com.metamatrix.common.comm.ClientServiceRegistry;
import com.metamatrix.common.config.api.DeployedComponentID;
import com.metamatrix.common.queue.WorkerPoolStats;
import com.metamatrix.dqp.message.RequestID;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;
import com.metamatrix.platform.security.api.SessionToken;
import com.metamatrix.platform.service.api.ServiceID;
import com.metamatrix.platform.service.api.exception.ServiceException;
import com.metamatrix.platform.service.api.exception.ServiceStateException;
import com.metamatrix.platform.vm.controller.VMControllerID;
import com.metamatrix.server.query.service.QueryServiceInterface;
import com.metamatrix.server.serverapi.RequestInfo;

public class FakeQueryService implements QueryServiceInterface {
    
    //Set<String>  Ids of cancelled queries.  
    protected static Set cancelledQueries = new HashSet();
    ServiceID id;
    
    protected static void clearState() {
        cancelledQueries.clear();
    }

    public FakeQueryService(ServiceID id) {
    	this.id = id;
    }
    /** 
     * @see com.metamatrix.server.query.service.BaseQueryServiceInterface#clearCache(com.metamatrix.platform.security.api.SessionToken)
     */
    public void clearCache(SessionToken sessionToken) throws ComponentNotFoundException,
                                                     ServiceStateException,
                                                     RemoteException {
    }

    /** 
     * @see com.metamatrix.server.query.service.BaseQueryServiceInterface#getAllQueries()
     */
    public Collection getAllQueries() throws ServiceStateException,
                                     RemoteException {
        List results = new ArrayList();
        
        RequestInfo info1 = new RequestInfo(new RequestID("1", 1L), "sql1", new Date(), new Date()); //$NON-NLS-1$
        SessionToken token1 = new SessionToken(new MetaMatrixSessionID(1), "user1"); //$NON-NLS-1$ //$NON-NLS-2$
        info1.setSessionToken(token1);
        results.add(info1);
        
        
        RequestInfo info2 = new RequestInfo(new RequestID("2", 2), "sql2", new Date(), new Date()); //$NON-NLS-1$
        SessionToken token2 = new SessionToken(new MetaMatrixSessionID(2), "user2"); //$NON-NLS-1$ //$NON-NLS-2$
        info2.setSessionToken(token2);
        results.add(info2);

        
        
        //SourceRequests
        RequestInfo info1A = new RequestInfo(new RequestID("1", 1), "sql1", new Date(), new Date()); //$NON-NLS-1$
        SessionToken token1A = new SessionToken(new MetaMatrixSessionID(1), "user1"); //$NON-NLS-1$ //$NON-NLS-2$
        info1A.setSessionToken(token1A);
        info1A.setConnectorBindingUUID("connectorBinding1");
        info1A.setNodeID(1);
        results.add(info1A);
        

        RequestInfo info2A = new RequestInfo(new RequestID("2", 2), "sql2", new Date(), new Date()); //$NON-NLS-1$
        SessionToken token2A = new SessionToken(new MetaMatrixSessionID(2), "user2"); //$NON-NLS-1$ //$NON-NLS-2$
        info2A.setSessionToken(token2A);
        info2A.setConnectorBindingUUID("connectorBinding2");
        info2A.setNodeID(2);
        
        results.add(info2A);

        
        
        return results; 
    }

    /** 
     * @see com.metamatrix.server.query.service.BaseQueryServiceInterface#getQueriesForSession(com.metamatrix.platform.security.api.SessionToken)
     */
    public Collection getQueriesForSession(SessionToken userToken) throws ServiceStateException,
                                                                  RemoteException {
        return null;
    }

    /** 
     * @see com.metamatrix.server.query.service.BaseQueryServiceInterface#cancelQueries(com.metamatrix.platform.security.api.SessionToken, boolean)
     */
    public void cancelQueries(SessionToken sessionToken,
                              boolean shouldRollback) throws InvalidRequestIDException,
                                                     MetaMatrixComponentException,
                                                     ServiceStateException,
                                                     RemoteException {
    }

    /** 
     * @see com.metamatrix.server.query.service.BaseQueryServiceInterface#cancelQuery(com.metamatrix.dqp.message.RequestID, boolean)
     */
    public void cancelQuery(RequestID requestID,
                            boolean shouldRollback) throws InvalidRequestIDException,
                                                   MetaMatrixComponentException,
                                                   ServiceStateException,
                                                   RemoteException {
        cancelledQueries.add(buildIdentifierFromRequestId(requestID, null));
    }

    /** 
     * @see com.metamatrix.server.query.service.BaseQueryServiceInterface#cancelQuery(com.metamatrix.dqp.message.RequestID, int)
     */
    public void cancelQuery(RequestID requestID,
                            int nodeID) throws InvalidRequestIDException,
                                       MetaMatrixComponentException,
                                       ServiceStateException,
                                       RemoteException {
        cancelledQueries.add(buildIdentifierFromRequestId(requestID, "" +nodeID)); //$NON-NLS-1$
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#die()
     */
    public void die() throws ServiceException,
                     RemoteException {
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#dieNow()
     */
    public void dieNow() throws ServiceException,
                        RemoteException {
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#checkState()
     */
    public void checkState() throws ServiceStateException,
                            RemoteException {
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#getProperties()
     */
    public Properties getProperties() throws RemoteException {
        return null;
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#getStartTime()
     */
    public Date getStartTime() throws RemoteException {
        return null;
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#getHostname()
     */
    public String getHostname() throws ServiceException,
                               RemoteException {
        return null;
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#getVMID()
     */
    public VMControllerID getVMID() throws ServiceException,
                                   RemoteException {
        return null;
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#isAlive()
     */
    public boolean isAlive() throws RemoteException {
        return false;
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#getServiceType()
     */
    public String getServiceType() throws RemoteException {
        return null;
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#getCurrentState()
     */
    public int getCurrentState() throws RemoteException {
        return 0;
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#getStateChangeTime()
     */
    public Date getStateChangeTime() throws RemoteException {
        return null;
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#getID()
     */
    public ServiceID getID() throws RemoteException {
        return null;
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#getQueueStatistics()
     */
    public Collection getQueueStatistics() throws RemoteException {
        List results = new ArrayList();
        WorkerPoolStats stats = new WorkerPoolStats();
        stats.name = "pool"; //$NON-NLS-1$
        stats.queued = (int)this.id.getID();
        stats.totalSubmitted = (int)this.id.getID();
        
        results.add(stats);
        return results;
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#getQueueStatistics(java.lang.String)
     */
    public WorkerPoolStats getQueueStatistics(String name) throws RemoteException {
    	return null;
    }

    /** 
     * @see com.metamatrix.platform.service.api.CacheAdmin#getCaches()
     */
    public Map getCaches() throws MetaMatrixComponentException,
                          RemoteException {
        return null;
    }

    /** 
     * @see com.metamatrix.platform.service.api.CacheAdmin#clearCache(java.lang.String, java.util.Properties)
     */
    public void clearCache(String name,
                           Properties props) throws MetaMatrixComponentException,
                                            RemoteException {
    }
    
    /**
     * Build the Identifer, as an array of its parts
     * @param requestId
     *  
     * @return the Identifer, as an array of its parts
     * @since 4.3
     */
    private static String buildIdentifierFromRequestId(RequestID requestId, String nodeId) {
        String connectionId = requestId.getConnectionID();
        String executionId = Long.toString(requestId.getExecutionID());
        
        StringBuffer buff = new StringBuffer();
        buff.append(connectionId);
        buff.append(Request.DELIMITER_CHAR);
        buff.append(executionId);
        if ( nodeId != null ) {
            buff.append(Request.DELIMITER_CHAR);
            buff.append(nodeId);
        }
        
        return buff.toString();
    }

	public void init(ServiceID id, DeployedComponentID deployedComponentID,
			Properties props, ClientServiceRegistry listenerRegistry)
			throws ServiceException, RemoteException {
	}

	public void setInitException(Throwable t) {
	}

	public void updateState(int state) {
	}

}
