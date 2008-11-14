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

package com.metamatrix.common.comm.platform.server;

import java.lang.reflect.Proxy;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import junit.framework.TestCase;

import com.metamatrix.admin.api.exception.AdminException;
import com.metamatrix.admin.api.server.AdminRoles;
import com.metamatrix.admin.api.server.ServerAdmin;
import com.metamatrix.admin.util.AdminMethodRoleResolver;
import com.metamatrix.common.comm.platform.FakeAdminHelper;
import com.metamatrix.common.comm.platform.socket.server.AdminAuthorizationInterceptor;
import com.metamatrix.core.util.SimpleMock;
import com.metamatrix.dqp.internal.process.DQPWorkContext;
import com.metamatrix.platform.admin.apiimpl.IAdminHelper;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;
import com.metamatrix.platform.security.api.SessionToken;


/** 
 * @since 4.3
 */
public class TestAdminAuthInterceptor extends TestCase {

    /**
     * Constructor for TestAdminMethodRoleResolver.
     * @param name
     */
    public TestAdminAuthInterceptor(String name) {
        super(name);
    }
    
    @Override
    protected void setUp() throws Exception {
    	DQPWorkContext.getWorkContext().setSessionToken(new SessionToken(new MetaMatrixSessionID(1), "NONE", "gojo", new Properties())); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Override
    protected void tearDown() throws Exception {
    	DQPWorkContext.setWorkContext(new DQPWorkContext());
    }
    
    public void testAddUserUDF_fail() throws AdminException {
        Set userRoles = new HashSet();
        ServerAdmin serverAdmin = getTestServerAdmin(userRoles);
        try {
        	serverAdmin.addUDF(null, null);
        } catch (AdminException err) {
        	
        }
    }

	private ServerAdmin getTestServerAdmin(Set userRoles) throws AdminException {
		IAdminHelper authHelper = new FakeAdminHelper("gojo", userRoles); //$NON-NLS-1$
        AdminMethodRoleResolver roleResolver = new AdminMethodRoleResolver();
        roleResolver.init();
        AdminAuthorizationInterceptor authInterceptor = new AdminAuthorizationInterceptor(authHelper, roleResolver, SimpleMock.createSimpleMock(ServerAdmin.class));
        ServerAdmin serverAdmin = (ServerAdmin)Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[] {ServerAdmin.class}, authInterceptor);
		return serverAdmin;
	}
    
    public void testAddUDF_succeed() throws Exception {
        Set userRoles = new HashSet();
        userRoles.add(AdminRoles.RoleName.ADMIN_SYSTEM);
        ServerAdmin serverAdmin = getTestServerAdmin(userRoles);
        serverAdmin.addUDF(null, null);
    }
    
    public void testGetVDBs() throws Exception {
        Set userRoles = new HashSet();
        ServerAdmin serverAdmin = getTestServerAdmin(userRoles);
        serverAdmin.getVDBs("*"); //$NON-NLS-1$
    }
    
    public void testReadOnlyFails() throws Exception {
        Set userRoles = new HashSet();
        ServerAdmin serverAdmin = getTestServerAdmin(userRoles);
        try {
        	serverAdmin.getSessions("*"); //$NON-NLS-1$
        } catch (AdminException e) {
        	
        }
    }
    
}
