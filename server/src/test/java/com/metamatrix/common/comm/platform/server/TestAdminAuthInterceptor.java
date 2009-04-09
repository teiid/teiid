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

package com.metamatrix.common.comm.platform.server;

import java.lang.reflect.Proxy;
import java.util.HashSet;
import java.util.Set;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.dqp.internal.process.DQPWorkContext;

import com.metamatrix.admin.api.exception.AdminException;
import com.metamatrix.admin.api.exception.AdminProcessingException;
import com.metamatrix.admin.api.server.AdminRoles;
import com.metamatrix.admin.api.server.ServerAdmin;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.api.exception.security.AuthorizationMgmtException;
import com.metamatrix.common.comm.platform.socket.server.AdminAuthorizationInterceptor;
import com.metamatrix.core.util.SimpleMock;
import com.metamatrix.platform.admin.api.ExtensionSourceAdminAPI;
import com.metamatrix.platform.admin.apiimpl.ExtensionSourceAdminAPIImpl;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;
import com.metamatrix.platform.security.api.SessionToken;
import com.metamatrix.platform.security.api.service.AuthorizationServiceInterface;


/** 
 * @since 4.3
 */
public class TestAdminAuthInterceptor {

    @Before public void setUp() throws Exception {
    	DQPWorkContext.getWorkContext().setSessionToken(new SessionToken(new MetaMatrixSessionID(1), "gojo")); //$NON-NLS-1$
    }
    
    @After public void tearDown() throws Exception {
    	DQPWorkContext.setWorkContext(new DQPWorkContext());
    }
    
    @Test(expected=AdminProcessingException.class) public void testAddUserUDF_fail() throws AdminException {
        Set<String> userRoles = new HashSet<String>();
        ServerAdmin serverAdmin = getTestServerAdmin(userRoles, ServerAdmin.class);
    	serverAdmin.addUDF(null, null);
    }
    
	private <T> T getTestServerAdmin(final Set<String> userRoles, Class<T> iface) {
		return getTestServerAdmin(userRoles, iface, SimpleMock.createSimpleMock(iface));
	}

	private <T> T getTestServerAdmin(final Set<String> userRoles, Class<T> iface, T impl) {
		AuthorizationServiceInterface service = Mockito.mock(AuthorizationServiceInterface.class);
		try {
			Mockito.stub(service.isCallerInRole((SessionToken)Mockito.anyObject(), Mockito.argThat(new BaseMatcher<String>() {
				@Override
				public boolean matches(Object arg0) {
					return userRoles.contains(arg0);
				}
				
				@Override
				public void describeTo(Description arg0) {
					
				}
			}))).toReturn(Boolean.TRUE);
		} catch (AuthorizationMgmtException e) {
			throw new RuntimeException(e);
		}
        AdminAuthorizationInterceptor authInterceptor = new AdminAuthorizationInterceptor(service, impl);
        return (T)Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[] {iface}, authInterceptor);
	}
    
    @Test public void testAddUDF_succeed() throws Exception {
        Set<String> userRoles = new HashSet<String>();
        userRoles.add(AdminRoles.RoleName.ADMIN_SYSTEM);
        ServerAdmin serverAdmin = getTestServerAdmin(userRoles, ServerAdmin.class);
        serverAdmin.addUDF(null, null);
    }
    
    @Test public void testGetVDBs() throws Exception {
        Set<String> userRoles = new HashSet<String>();
        ServerAdmin serverAdmin = getTestServerAdmin(userRoles, ServerAdmin.class);
        serverAdmin.getVDBs("*"); //$NON-NLS-1$
    }
    
    @Test(expected=AdminProcessingException.class) public void testReadOnlyFails() throws Exception {
        Set<String> userRoles = new HashSet<String>();
        ServerAdmin serverAdmin = getTestServerAdmin(userRoles, ServerAdmin.class);
    	serverAdmin.getSessions("*"); //$NON-NLS-1$
    }
    
    @Test(expected=AuthorizationException.class) public void testSubsystemReadOnlyFails() throws Exception {
    	Set<String> userRoles = new HashSet<String>();
    	ExtensionSourceAdminAPI serverAdmin = getTestServerAdmin(userRoles, ExtensionSourceAdminAPI.class, ExtensionSourceAdminAPIImpl.getInstance());
    	serverAdmin.getSourceDescriptors();
    }
    
}
