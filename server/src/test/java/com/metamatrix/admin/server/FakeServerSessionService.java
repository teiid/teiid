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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.api.exception.security.MetaMatrixAuthenticationException;
import com.metamatrix.api.exception.security.SessionServiceException;
import com.metamatrix.common.application.ClassLoaderManager;
import com.metamatrix.common.comm.ClientServiceRegistry;
import com.metamatrix.common.config.api.DeployedComponentID;
import com.metamatrix.common.queue.WorkerPoolStats;
import com.metamatrix.common.stats.ConnectionPoolStats;
import com.metamatrix.platform.security.api.Credentials;
import com.metamatrix.platform.security.api.MetaMatrixPrincipal;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;
import com.metamatrix.platform.security.api.MetaMatrixSessionInfo;
import com.metamatrix.platform.security.api.service.SessionServiceInterface;
import com.metamatrix.platform.service.api.ServiceID;
import com.metamatrix.platform.service.controller.ServiceData;
import com.metamatrix.platform.util.ProductInfoConstants;

public class FakeServerSessionService implements SessionServiceInterface {

    /**Set<String>  Contains MetaMatrixSessionID.toString() of terminated sessions.*/
    protected static Set terminatedSessions = new HashSet();
    
    protected static void clearState() {
        terminatedSessions.clear();
    }
    
    public MetaMatrixSessionInfo createSession(String userName,
    		Credentials credentials, Serializable trustedToken,
    		String applicationName, Properties properties)
    		throws MetaMatrixAuthenticationException, SessionServiceException {
    	return null;
    }

    /** 
     * @see com.metamatrix.platform.security.api.service.SessionServiceInterface#closeSession(com.metamatrix.platform.security.api.MetaMatrixSessionID)
     */
    public void closeSession(MetaMatrixSessionID sessionID) throws InvalidSessionException,
                                                           SessionServiceException {
    }

    /** 
     * @see com.metamatrix.platform.security.api.service.SessionServiceInterface#terminateSession(com.metamatrix.platform.security.api.MetaMatrixSessionID, com.metamatrix.platform.security.api.MetaMatrixSessionID)
     */
    public boolean terminateSession(MetaMatrixSessionID terminatedSessionID,
                                    MetaMatrixSessionID adminSessionID) throws InvalidSessionException,
                                                                       AuthorizationException,
                                                                       SessionServiceException {
        terminatedSessions.add(terminatedSessionID.toString());
        
        return false;
    }

    /** 
     * @see com.metamatrix.platform.security.api.service.SessionServiceInterface#getActiveSessions()
     */
    public Collection getActiveSessions() throws SessionServiceException {
        List sessions = new ArrayList();
        
        MetaMatrixSessionID id1 = new MetaMatrixSessionID(1); 
        Properties productInfo1 = new Properties();
        productInfo1.put(ProductInfoConstants.VIRTUAL_DB, "vdb1"); //$NON-NLS-1$
        productInfo1.put(ProductInfoConstants.VDB_VERSION, "1");//$NON-NLS-1$
        MetaMatrixSessionInfo info1 = new MetaMatrixSessionInfo(id1, "user1", 1, "app1", productInfo1, null,  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                                                                null); //$NON-NLS-1$
        sessions.add(info1);

        
        MetaMatrixSessionID id2 = new MetaMatrixSessionID(2);  
        Properties productInfo2 = new Properties();
        productInfo2.put(ProductInfoConstants.VIRTUAL_DB, "vdb2"); //$NON-NLS-1$
        productInfo2.put(ProductInfoConstants.VDB_VERSION, "2"); //$NON-NLS-1$
        MetaMatrixSessionInfo info2 = new MetaMatrixSessionInfo(id2, "user2", 2, "app2", productInfo2, null,  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        		null); //$NON-NLS-1$
        sessions.add(info2);
        
        return sessions;
    }

    /** 
     * @see com.metamatrix.platform.security.api.service.SessionServiceInterface#getActiveSessionsCount()
     */
    public int getActiveSessionsCount() throws SessionServiceException {
        return 0;
    }

    /** 
     * @see com.metamatrix.platform.security.api.service.SessionServiceInterface#getActiveConnectionsCountForProduct(java.lang.String)
     */
    public int getActiveConnectionsCountForProduct(String product) throws SessionServiceException{
        return 0;
    }

    /** 
     * @see com.metamatrix.platform.security.api.service.SessionServiceInterface#getPrincipal(com.metamatrix.platform.security.api.MetaMatrixSessionID)
     */
    public MetaMatrixPrincipal getPrincipal(MetaMatrixSessionID sessionID) throws InvalidSessionException,
                                                                          SessionServiceException {
        return null;
    }

    /** 
     * @see com.metamatrix.platform.security.api.service.SessionServiceInterface#getSessionsLoggedInToVDB(java.lang.String, java.lang.String)
     */
    public Collection getSessionsLoggedInToVDB(String VDBName,
                                               String VDBVersion) throws SessionServiceException {
        return null;
    }

    /** 
     * @see com.metamatrix.platform.security.api.service.SessionServiceInterface#pingServer(com.metamatrix.platform.security.api.MetaMatrixSessionID)
     */
    public void pingServer(MetaMatrixSessionID sessionID) {
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#die()
     */
    public void die() {
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#dieNow()
     */
    public void dieNow()  {
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#checkState()
     */
    public void checkState(){
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#getProperties()
     */
    public Properties getProperties(){
        return null;
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#getStartTime()
     */
    public Date getStartTime(){
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
    public String getProcessName() {
        return null;
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#isAlive()
     */
    public boolean isAlive(){
        return false;
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#getServiceType()
     */
    public String getServiceType(){
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
    public Date getStateChangeTime() {
        return null;
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#getID()
     */
    public ServiceID getID(){
        return null;
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#getQueueStatistics()
     */
    public Collection getQueueStatistics() {
        return null;
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#getQueueStatistics(java.lang.String)
     */
    public WorkerPoolStats getQueueStatistics(String name) {
        return null;
    }

    /** 
     * @see com.metamatrix.platform.security.api.service.SessionServiceInterface#validateSession(com.metamatrix.platform.security.api.MetaMatrixSessionID)
     */
    public MetaMatrixSessionInfo validateSession(MetaMatrixSessionID sessionID) throws InvalidSessionException,
                                                                      SessionServiceException {
        return null;
    }

	public long getPingInterval() {
		return 0;
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
	public ServiceData getServiceData() {
		return null;
	}

	@Override
	public Collection<ConnectionPoolStats> getConnectionPoolStats() {
		return null;
	}
	
	

}
