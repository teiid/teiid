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

package com.metamatrix.admin.util;

import junit.framework.TestCase;

import com.metamatrix.admin.AdminPlugin;
import com.metamatrix.admin.api.exception.AdminException;
import com.metamatrix.admin.api.server.AdminRoles;


/**
 * @since 4.3
 */
public class TestAdminMethodRoleResolver extends TestCase {
    private AdminMethodRoleResolver methods;


    /**
     * Constructor for TestAdminMethodRoleResolver.
     * @param name
     */
    public TestAdminMethodRoleResolver(String name) {
        super(name);
    }

    /**
     * Get the role resolver.
     * @return the role resolver initializing as needed.
     * @throws AdminException
     * @since 4.3
     */
    private AdminMethodRoleResolver getAdminMethods() throws AdminException {
        if ( methods == null ) {
            AdminMethodRoleResolver theMethods = new AdminMethodRoleResolver();
            theMethods.init();
            methods = theMethods;
        }
        return methods;
    }

    /**
     *
     * @param method
     * @param expectedRoleName
     * @throws Exception
     * @since 4.3
     */
    private void helpTestExpectedRoles(String method, String expectedRoleName) throws Exception {
    	AdminMethodRoleResolver methodHolder = getAdminMethods();
        String roleName = methodHolder.getRoleNameForMethod(method);
        assertTrue("Did not receive the roles expected. Expected: " + expectedRoleName + " Recieved: " + roleName,  //$NON-NLS-1$ //$NON-NLS-2$
                   roleName.equals(expectedRoleName) );
        assertTrue("Did not receive the roles expected.  Expected: " + expectedRoleName + " Recieved: " + roleName,  //$NON-NLS-1$ //$NON-NLS-2$
                   expectedRoleName.equals(roleName) );
    }

    /**
     *
     * @throws Exception
     * @since 4.3
     */
    public void testInitWithUnknownMethod_fail() throws Exception {
        String methodName = "addBogus"; //$NON-NLS-1$
        try {
        	getAdminMethods().getRoleNameForMethod(methodName); 
        } catch (final AdminException err) {
            // Expected
            Object[] params = new Object[] {methodName};
            String msg = AdminPlugin.Util.getString("AdminMethodRoleResolver.Unknown_method", params); //$NON-NLS-1$

            assertEquals(msg, err.getMessage());
        }
    }

    /**
     *
     * @throws Exception
     * @since 4.3
     */
    public void testAddUser_succeed() throws Exception {
        String methodName = "addUser"; //$NON-NLS-1$
        helpTestExpectedRoles(methodName, AdminRoles.RoleName.ADMIN_SYSTEM);
    }

    /**
     *
     * @throws Exception
     * @since 4.3
     */
    public void testEnableHost_succeed() throws Exception {
        String methodName = "enableHost"; //$NON-NLS-1$
        helpTestExpectedRoles(methodName, AdminRoles.RoleName.ADMIN_SYSTEM);
    }
    
    public void testGetVdbs() throws Exception {
    	String methodName = "getVDBs"; //$NON-NLS-1$
        helpTestExpectedRoles(methodName, AdminMethodRoleResolver.ANONYMOUS_ROLE);
    }

}
