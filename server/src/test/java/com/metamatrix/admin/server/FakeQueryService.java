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

package com.metamatrix.admin.server;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.transaction.xa.Xid;

import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.Transaction;

import com.metamatrix.admin.objects.MMAdminObject;
import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.server.InvalidRequestIDException;
import com.metamatrix.common.application.ClassLoaderManager;
import com.metamatrix.common.comm.ClientServiceRegistry;
import com.metamatrix.common.config.api.DeployedComponentID;
import com.metamatrix.common.queue.WorkerPoolStats;
import com.metamatrix.common.stats.ConnectionPoolStats;
import com.metamatrix.dqp.message.AtomicRequestID;
import com.metamatrix.dqp.message.RequestID;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;
import com.metamatrix.platform.security.api.SessionToken;
import com.metamatrix.platform.service.api.ServiceID;
import com.metamatrix.platform.service.controller.ServiceData;
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
    public void clearCache(SessionToken sessionToken) throws ComponentNotFoundException{
    }

    /** 
     * @see com.metamatrix.server.query.service.BaseQueryServiceInterface#getAllQueries()
     */
    public Collection getAllQueries() {
        List results = new ArrayList();
        
        RequestInfo info1 = new RequestInfo(new RequestID("1", 1L), "sql1", new Date(), new Date()); //$NON-NLS-1$ //$NON-NLS-2$
        SessionToken token1 = new SessionToken(new MetaMatrixSessionID(1), "user1"); //$NON-NLS-1$ 
        info1.setSessionToken(token1);
        results.add(info1);
        
        
        RequestInfo info2 = new RequestInfo(new RequestID("2", 2), "sql2", new Date(), new Date()); //$NON-NLS-1$ //$NON-NLS-2$
        SessionToken token2 = new SessionToken(new MetaMatrixSessionID(2), "user2"); //$NON-NLS-1$ 
        info2.setSessionToken(token2);
        results.add(info2);

        
        
        //SourceRequests
        RequestInfo info1A = new RequestInfo(new RequestID("1", 1), "sql1", new Date(), new Date()); //$NON-NLS-1$ //$NON-NLS-2$
        SessionToken token1A = new SessionToken(new MetaMatrixSessionID(1), "user1"); //$NON-NLS-1$ 
        info1A.setSessionToken(token1A);
        info1A.setConnectorBindingUUID("connectorBinding1");//$NON-NLS-1$ 
        info1A.setNodeID(1);
        results.add(info1A);
        

        RequestInfo info2A = new RequestInfo(new RequestID("2", 2), "sql2", new Date(), new Date()); //$NON-NLS-1$ //$NON-NLS-2$
        SessionToken token2A = new SessionToken(new MetaMatrixSessionID(2), "user2"); //$NON-NLS-1$ 
        info2A.setSessionToken(token2A);
        info2A.setConnectorBindingUUID("connectorBinding2");//$NON-NLS-1$ 
        info2A.setNodeID(2);
        
        results.add(info2A);

        
        
        return results; 
    }

    /** 
     * @see com.metamatrix.server.query.service.BaseQueryServiceInterface#getQueriesForSession(com.metamatrix.platform.security.api.SessionToken)
     */
    public Collection getQueriesForSession(SessionToken userToken) {
        return null;
    }

    /** 
     * @see com.metamatrix.server.query.service.BaseQueryServiceInterface#cancelQueries(com.metamatrix.platform.security.api.SessionToken, boolean)
     */
    public void cancelQueries(SessionToken sessionToken,
                              boolean shouldRollback) throws InvalidRequestIDException,
                                                     MetaMatrixComponentException{
    }

    /** 
     * @see com.metamatrix.server.query.service.BaseQueryServiceInterface#cancelQuery(com.metamatrix.dqp.message.RequestID, boolean)
     */
    public void cancelQuery(RequestID requestID,
                            boolean shouldRollback) throws InvalidRequestIDException,
                                                   MetaMatrixComponentException {
        cancelledQueries.add(requestID.getConnectionID() + MMAdminObject.DELIMITER_CHAR + requestID.getExecutionID());
    }
    
    @Override
    public void cancelQuery(AtomicRequestID ari)
    		throws InvalidRequestIDException, MetaMatrixComponentException {
    	cancelledQueries.add(ari.getRequestID().getConnectionID() + MMAdminObject.DELIMITER_CHAR + ari.getRequestID().getExecutionID()
    			+MMAdminObject.DELIMITER_CHAR + ari.getNodeID() + MMAdminObject.DELIMITER_CHAR + ari.getExecutionId());
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#die()
     */
    public void die() {
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#dieNow()
     */
    public void dieNow() {
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#checkState()
     */
    public void checkState(){
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#getProperties()
     */
    public Properties getProperties() {
        return null;
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#getStartTime()
     */
    public Date getStartTime() {
        return null;
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#getHostname()
     */
    public String getHostname() {
        return null;
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#getVMID()
     */
    public String getProcessName(){
        return null;
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#isAlive()
     */
    public boolean isAlive() {
        return false;
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#getServiceType()
     */
    public String getServiceType() {
        return null;
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#getCurrentState()
     */
    public int getCurrentState(){
        return 0;
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#getStateChangeTime()
     */
    public Date getStateChangeTime(){
        return null;
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#getID()
     */
    public ServiceID getID() {
        return null;
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#getQueueStatistics()
     */
    public Collection getQueueStatistics() {
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
    public WorkerPoolStats getQueueStatistics(String name) {
    	return null;
    }

    /** 
     * @see com.metamatrix.platform.service.api.CacheAdmin#getCaches()
     */
    public Map getCaches() throws MetaMatrixComponentException{
        return null;
    }

    /** 
     * @see com.metamatrix.platform.service.api.CacheAdmin#clearCache(java.lang.String, java.util.Properties)
     */
    public void clearCache(String name,
                           Properties props) throws MetaMatrixComponentException{
    }

	public void init(ServiceID id, DeployedComponentID deployedComponentID,
			Properties props, ClientServiceRegistry listenerRegistry, ClassLoaderManager clManager){
	}

	public void setInitException(Throwable t) {
	}

	public void updateState(int state) {
	}

	@Override
	public Throwable getInitException() {
		return null;
	}
	
	@Override
	public Collection<Transaction> getTransactions() {
		return Collections.emptyList();
	}
	
	@Override
	public void terminateTransaction(String transactionId, String sessionId)
			throws AdminException {
		
	}
	
	@Override
	public void terminateTransaction(Xid transactionId) throws AdminException {
		
	}
	
	@Override
	public ServiceData getServiceData() {
		return null;
	}

	@Override
	public Collection<ConnectionPoolStats> getConnectionPoolStats() {
		return null;
	}
	
	

}
