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

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.api.exception.security.InvalidSessionException;
import com.metamatrix.api.exception.security.MetaMatrixAuthenticationException;
import com.metamatrix.api.exception.security.SessionServiceException;
import com.metamatrix.common.comm.ClientServiceRegistry;
import com.metamatrix.common.config.api.DeployedComponentID;
import com.metamatrix.common.queue.WorkerPoolStats;
import com.metamatrix.platform.security.api.Credentials;
import com.metamatrix.platform.security.api.MetaMatrixPrincipal;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;
import com.metamatrix.platform.security.api.MetaMatrixSessionInfo;
import com.metamatrix.platform.security.api.SessionToken;
import com.metamatrix.platform.security.api.service.SessionServiceInterface;
import com.metamatrix.platform.service.api.ServiceID;
import com.metamatrix.platform.service.api.exception.ServiceException;
import com.metamatrix.platform.service.api.exception.ServiceStateException;
import com.metamatrix.platform.util.ProductInfoConstants;
import com.metamatrix.platform.vm.controller.VMControllerID;

public class FakeServerSessionService implements SessionServiceInterface {

    /**Set<String>  Contains MetaMatrixSessionID.toString() of terminated sessions.*/
    protected static Set terminatedSessions = new HashSet();
    
    protected static void clearState() {
        terminatedSessions.clear();
    }
    
    public MetaMatrixSessionInfo createSession(String userName,
    		Credentials credentials, Serializable trustedToken,
    		String applicationName, String productName, Properties properties)
    		throws MetaMatrixAuthenticationException, SessionServiceException,
    		ServiceException {
    	return null;
    }

    /** 
     * @see com.metamatrix.platform.security.api.service.SessionServiceInterface#closeSession(com.metamatrix.platform.security.api.MetaMatrixSessionID)
     */
    public void closeSession(MetaMatrixSessionID sessionID) throws InvalidSessionException,
                                                           SessionServiceException,
                                                           ServiceException {
    }

    /** 
     * @see com.metamatrix.platform.security.api.service.SessionServiceInterface#terminateSession(com.metamatrix.platform.security.api.MetaMatrixSessionID, com.metamatrix.platform.security.api.MetaMatrixSessionID)
     */
    public boolean terminateSession(MetaMatrixSessionID terminatedSessionID,
                                    MetaMatrixSessionID adminSessionID) throws InvalidSessionException,
                                                                       AuthorizationException,
                                                                       SessionServiceException,
                                                                       ServiceException {
        terminatedSessions.add(terminatedSessionID.toString());
        
        return false;
    }

    /** 
     * @see com.metamatrix.platform.security.api.service.SessionServiceInterface#getActiveSessions()
     */
    public Collection getActiveSessions() throws SessionServiceException,
                                         ServiceException {
        List sessions = new ArrayList();
        
        MetaMatrixSessionID id1 = new MetaMatrixSessionID(1); 
        Properties productInfo1 = new Properties();
        productInfo1.put(ProductInfoConstants.VIRTUAL_DB, "vdb1"); //$NON-NLS-1$
        productInfo1.put(ProductInfoConstants.VDB_VERSION, "1");//$NON-NLS-1$
        MetaMatrixSessionInfo info1 = new MetaMatrixSessionInfo(id1, "user1", 1, 1, "app1", 1, "cluster1",  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                                                                productInfo1, "product1", null, null); //$NON-NLS-1$
        sessions.add(info1);

        
        MetaMatrixSessionID id2 = new MetaMatrixSessionID(2);  
        Properties productInfo2 = new Properties();
        productInfo2.put(ProductInfoConstants.VIRTUAL_DB, "vdb2"); //$NON-NLS-1$
        productInfo2.put(ProductInfoConstants.VDB_VERSION, "2"); //$NON-NLS-1$
        MetaMatrixSessionInfo info2 = new MetaMatrixSessionInfo(id2, "user2", 2, 2, "app2", 2, "cluster2",  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        		productInfo2, "product2", null, null); //$NON-NLS-1$
        sessions.add(info2);
        
        return sessions;
    }

    /** 
     * @see com.metamatrix.platform.security.api.service.SessionServiceInterface#getActiveSessionsCount()
     */
    public int getActiveSessionsCount() throws SessionServiceException,
                                       ServiceException {
        return 0;
    }

    /** 
     * @see com.metamatrix.platform.security.api.service.SessionServiceInterface#getActiveConnectionsCountForProduct(java.lang.String)
     */
    public int getActiveConnectionsCountForProduct(String product) throws SessionServiceException,
                                                                  ServiceException {
        return 0;
    }

    /** 
     * @see com.metamatrix.platform.security.api.service.SessionServiceInterface#getPrincipal(com.metamatrix.platform.security.api.MetaMatrixSessionID)
     */
    public MetaMatrixPrincipal getPrincipal(MetaMatrixSessionID sessionID) throws InvalidSessionException,
                                                                          SessionServiceException,
                                                                          ServiceException {
        return null;
    }

    /** 
     * @see com.metamatrix.platform.security.api.service.SessionServiceInterface#getSessionsLoggedInToVDB(java.lang.String, java.lang.String)
     */
    public Collection getSessionsLoggedInToVDB(String VDBName,
                                               String VDBVersion) throws SessionServiceException,
                                                                 ServiceStateException {
        return null;
    }

    /** 
     * @see com.metamatrix.platform.security.api.service.SessionServiceInterface#pingServer(com.metamatrix.platform.security.api.MetaMatrixSessionID)
     */
    public void pingServer(MetaMatrixSessionID sessionID) throws ServiceStateException {
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
        return null;
    }

    /** 
     * @see com.metamatrix.platform.service.api.ServiceInterface#getQueueStatistics(java.lang.String)
     */
    public WorkerPoolStats getQueueStatistics(String name) throws RemoteException {
        return null;
    }

    /** 
     * @see com.metamatrix.platform.security.api.service.SessionServiceInterface#validateSession(com.metamatrix.platform.security.api.MetaMatrixSessionID)
     */
    public SessionToken validateSession(MetaMatrixSessionID sessionID) throws InvalidSessionException,
                                                                      SessionServiceException,
                                                                      ServiceException {
        return null;
    }

	public long getPingInterval() {
		return 0;
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
