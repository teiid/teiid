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

import java.util.Collection;

import org.teiid.dqp.internal.process.DQPWorkContext;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.extensionmodule.ExtensionModuleManager;
import com.metamatrix.core.util.SimpleMock;
import com.metamatrix.platform.admin.apiimpl.RuntimeStateAdminAPIHelper;
import com.metamatrix.platform.config.api.service.ConfigurationServiceInterface;
import com.metamatrix.platform.registry.ClusteredRegistryState;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;
import com.metamatrix.platform.security.api.SessionToken;
import com.metamatrix.platform.security.api.service.AuthorizationServiceInterface;
import com.metamatrix.platform.security.api.service.SessionServiceInterface;
import com.metamatrix.platform.service.api.ServiceID;
import com.metamatrix.platform.service.api.exception.ServiceException;
import com.metamatrix.server.query.service.QueryServiceInterface;


/**
 * Fake implementation that creates fake data for testing the Admin API. 
 * @since 4.3
 */
public class FakeServerAdminImpl extends ServerAdminImpl {

    FakeServerSessionService fsss = null;
    FakeConfigurationService fcs = null;
    AuthorizationServiceInterface fas = null;
    FakeQueryService fqs = null;
    FakeExtensionModuleManager femm = null;
    FakeRuntimeStateAdminAPIHelper frsaa = null;

    /**
     * constructor
     * @since 4.3
     */
    public FakeServerAdminImpl(ClusteredRegistryState registry) {
    	super(registry,null);
    	DQPWorkContext.setWorkContext(new DQPWorkContext());
    	DQPWorkContext.getWorkContext().setSessionToken(new SessionToken(new MetaMatrixSessionID(1), "fakeadminuser")); //$NON-NLS-1$
    }
    
    public void close() {        
    }
    
    protected synchronized SessionServiceInterface getSessionServiceProxy() throws ServiceException {
        if (fsss == null) {
            fsss =  new FakeServerSessionService();
        }
        return fsss;
    }
    
    protected synchronized ConfigurationServiceInterface getConfigurationServiceProxy() throws ServiceException {
        if (fcs == null) {
            fcs = new FakeConfigurationService();
        }
        return fcs;
    }
    
    protected synchronized AuthorizationServiceInterface getAuthorizationServiceProxy() throws ServiceException {
        if (fas == null) {
            fas = SimpleMock.createSimpleMock(AuthorizationServiceInterface.class);
        }
        return fas;
    }    
    
    protected synchronized QueryServiceInterface getQueryServiceProxy() throws ServiceException {
        if (fqs == null) {
            fqs = new FakeQueryService(new ServiceID(1, "dummy", "2")); //$NON-NLS-1$ //$NON-NLS-2$
        }
        return fqs; 
    }
    
    protected ExtensionModuleManager getExtensionSourceManager(){
        if (femm == null)  {
             femm = new FakeExtensionModuleManager();
        }
        return femm;
    }   
    
    protected RuntimeStateAdminAPIHelper getRuntimeStateAdminAPIHelper(){
        if (frsaa == null) {
            frsaa = new FakeRuntimeStateAdminAPIHelper(this.registry);
        }
        return frsaa;
    }  
    
    protected void waitForServicesToStart(Collection expectedServiceNames) throws MetaMatrixComponentException {
        //overridden to not wait
    }

    protected void waitForServicesToStop(Collection expectedServiceNames) throws MetaMatrixComponentException {
        //overridden to not wait
    }  
    
    
    
}