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

package com.metamatrix.platform.security.authorization.spi;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import junit.framework.TestCase;

import com.metamatrix.platform.security.api.AuthorizationActions;
import com.metamatrix.platform.security.api.AuthorizationPermission;
import com.metamatrix.platform.security.api.AuthorizationPolicy;
import com.metamatrix.platform.security.api.AuthorizationPolicyID;
import com.metamatrix.platform.security.api.MetaMatrixPrincipalName;
import com.metamatrix.platform.security.api.StandardAuthorizationActions;

/**
 * @since	  3.0
 */
public class TestFakeAuthorizationSource extends TestCase {

    /**
     * Constructor for TestFakeAuthorizationSource.
     * @param arg0
     */
    public TestFakeAuthorizationSource(String arg0) {
        super(arg0);
    }

    // =========================================================================
    //                      H E L P E R   M E T H O D S
    // =========================================================================

    public static void helpPopulate( final FakeAuthorizationSource source ) {

        final String realmName = AuthorizationTestUtil.METABASE_REALM_NAME;

        final AuthorizationPolicy policy1 = source.createPolicy("Policy1"); //$NON-NLS-1$
        final AuthorizationPolicy policy2 = source.createPolicy("Policy2"); //$NON-NLS-1$
        final AuthorizationPolicy policy3 = source.createPolicy("Policy3"); //$NON-NLS-1$

        final MetaMatrixPrincipalName u1 = AuthorizationTestUtil.createUserName("u1+p1"); //$NON-NLS-1$
        final MetaMatrixPrincipalName u2 = AuthorizationTestUtil.createUserName("u2+p1+p2"); //$NON-NLS-1$
        final MetaMatrixPrincipalName u3 = AuthorizationTestUtil.createUserName("u3+p2"); //$NON-NLS-1$
        final MetaMatrixPrincipalName u4 = AuthorizationTestUtil.createUserName("u4+p3"); //$NON-NLS-1$
        final MetaMatrixPrincipalName u5 = AuthorizationTestUtil.createUserName("u4+p1+p2+p3"); //$NON-NLS-1$

        final MetaMatrixPrincipalName g1 = AuthorizationTestUtil.createGroupName("g1+p1"); //$NON-NLS-1$

        AuthorizationTestUtil.addPrincipalsToPolicy(policy1,new MetaMatrixPrincipalName[]{u1,u2,g1,u5});
        AuthorizationTestUtil.addPrincipalsToPolicy(policy2,new MetaMatrixPrincipalName[]{u2,u3,u5});
        AuthorizationTestUtil.addPrincipalsToPolicy(policy3,new MetaMatrixPrincipalName[]{u4,u5});

        //  This is the resource tree
        //    f1
        //    f1/f11
        //    f1/f11/m111
        //    f1/f12
        //    f1/f12/m121
        //    f1/f12/m122
        //    f1/f12/m123
        //    f1/f12/m124

        // This is the authorized views of each policy
        //    Users->           [u1,u2,u5]  [u2,u3,u5]  [u4,u5]
        //    Resources         Policy1     Policy2     Policy3
        //    ----------------  ----------  ----------  ----------
        //    f1                r           cr          rud
        //    f1/f11            -           r           crud
        //    f1/f11/m111       -           ru          crud
        //    f1/f12            cr          r           crud
        //    f1/f12/m121       r           crud        rd
        //    f1/f12/m122       ru          r           rd
        //    f1/f12/m123       crud        -           -
        //    f1/f12/m124       crud        -           r

        // This is the authorized views of each user
        //    Resources          u1    u2    u3    u4    u5
        //    ----------------  ----  ----  ----  ----  ----
        //    f1                r     cr    cr    rud   crud
        //    f1/f11            -     r     r     crud  crud
        //    f1/f11/m111       -     ru    ru    crud  crud
        //    f1/f12            cr    cr    r     crud  crud
        //    f1/f12/m121       r     crud  crud  rd    crud
        //    f1/f12/m122       ru    ru    cr    rd    crud
        //    f1/f12/m123       crud  crud  -     -     crud
        //    f1/f12/m124       crud  crud  -     r     crud


        AuthorizationTestUtil.addPermissionToPolicy(policy1, realmName, "f1",          AuthorizationTestUtil.R); //$NON-NLS-1$
        AuthorizationTestUtil.addPermissionToPolicy(policy1, realmName, "f1/f12",      AuthorizationTestUtil.CR); //$NON-NLS-1$
        AuthorizationTestUtil.addPermissionToPolicy(policy1, realmName, "f1/f12/m121", AuthorizationTestUtil.R); //$NON-NLS-1$
        AuthorizationTestUtil.addPermissionToPolicy(policy1, realmName, "f1/f12/m122", AuthorizationTestUtil.RU); //$NON-NLS-1$
        AuthorizationTestUtil.addPermissionToPolicy(policy1, realmName, "f1/f12/m123", AuthorizationTestUtil.CRUD); //$NON-NLS-1$
        AuthorizationTestUtil.addPermissionToPolicy(policy1, realmName, "f1/f12/m124", AuthorizationTestUtil.CRUD); //$NON-NLS-1$

        AuthorizationTestUtil.addPermissionToPolicy(policy2, realmName, "f1",          AuthorizationTestUtil.CR); //$NON-NLS-1$
        AuthorizationTestUtil.addPermissionToPolicy(policy2, realmName, "f1/f11",      AuthorizationTestUtil.R); //$NON-NLS-1$
        AuthorizationTestUtil.addPermissionToPolicy(policy2, realmName, "f1/f11/m111", AuthorizationTestUtil.RU); //$NON-NLS-1$
        AuthorizationTestUtil.addPermissionToPolicy(policy2, realmName, "f1/f12",      AuthorizationTestUtil.R); //$NON-NLS-1$
        AuthorizationTestUtil.addPermissionToPolicy(policy2, realmName, "f1/f12/m121", AuthorizationTestUtil.CRUD); //$NON-NLS-1$
        AuthorizationTestUtil.addPermissionToPolicy(policy2, realmName, "f1/f12/m122", AuthorizationTestUtil.R); //$NON-NLS-1$

        AuthorizationTestUtil.addPermissionToPolicy(policy3, realmName, "f1",          AuthorizationTestUtil.RUD); //$NON-NLS-1$
        AuthorizationTestUtil.addPermissionToPolicy(policy3, realmName, "f1/f11",      AuthorizationTestUtil.CRUD); //$NON-NLS-1$
        AuthorizationTestUtil.addPermissionToPolicy(policy3, realmName, "f1/f11/m111", AuthorizationTestUtil.CRUD); //$NON-NLS-1$
        AuthorizationTestUtil.addPermissionToPolicy(policy3, realmName, "f1/f12",      AuthorizationTestUtil.CRUD); //$NON-NLS-1$
        AuthorizationTestUtil.addPermissionToPolicy(policy3, realmName, "f1/f12/m121", AuthorizationTestUtil.RD); //$NON-NLS-1$
        AuthorizationTestUtil.addPermissionToPolicy(policy3, realmName, "f1/f12/m122", AuthorizationTestUtil.RD); //$NON-NLS-1$

    }

    public void helpTestFindPolicies( final AuthorizationSourceTransaction source,
                                      final String[] userNames, final String[] groupNames,
                                      final String[] policyNames ) throws AuthorizationSourceConnectionException, AuthorizationSourceException {
        final Collection principals = new HashSet();
        for ( int i=0; i!=userNames.length; ++i ) {
            principals.add( AuthorizationTestUtil.createUserName(userNames[i]));
        }
        for ( int i=0; i!=groupNames.length; ++i ) {
            principals.add( AuthorizationTestUtil.createGroupName(groupNames[i]));
        }
        final Collection policyIDs = source.findPolicyIDs(principals, AuthorizationTestUtil.getRealm());
        final List policyNameList = Arrays.asList(policyNames);
        final Iterator iter = policyIDs.iterator();
        while (iter.hasNext()) {
            final AuthorizationPolicyID policyID = (AuthorizationPolicyID) iter.next();
            if ( !policyNameList.contains(policyID.getName()) ) {
                fail("Unable to find expected policy \"" + policyID.getName() + "\""); //$NON-NLS-1$ //$NON-NLS-2$
            } else {
                iter.next();
            }
        }
        if ( policyIDs.size() != 0 ) {
            fail("Found " + policyIDs.size() + " unexpected policies: " + policyIDs); //$NON-NLS-1$ //$NON-NLS-2$
        }
    }

    public void helpTestAuthorization( final FakeAuthorizationSource source,
                                       final String username,
                                       final AuthorizationActions actions,
                                       final boolean expectedAuthorization,
                                       final String resourceName ) throws AuthorizationSourceConnectionException, AuthorizationSourceException {
        final Collection principals = new HashSet();
        principals.add( AuthorizationTestUtil.createUserName(username) );

        // Create the permission ...
        final AuthorizationPermission perm = AuthorizationTestUtil.createPermission(
                                                            AuthorizationTestUtil.METABASE_REALM_NAME,
                                                            resourceName,actions);
        boolean authorized = false;
        final Collection policyIDs = source.findPolicyIDs(principals, AuthorizationTestUtil.getRealm());
        final Iterator iter = policyIDs.iterator();
        while (iter.hasNext()) {
            final AuthorizationPolicyID policyID = (AuthorizationPolicyID) iter.next();
            final AuthorizationPolicy policy = source.getPolicy(policyID);
            if ( policy.implies(perm) ) {
                authorized = true;
                break;
            }
        }
        if (authorized && !expectedAuthorization ) {
            fail("Unexpectedly authorized for resource \"" + resourceName + "\" [" + actions + "]"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        }
    }

    public void testFindPolicy() {
        final FakeAuthorizationSource source = new FakeAuthorizationSource();
        helpPopulate(source);
        final AuthorizationPolicy policy = source.findPolicy("Policy1"); //$NON-NLS-1$
        if ( !policy.getAuthorizationPolicyID().getName().equals("Policy1") ) { //$NON-NLS-1$
            fail("Unable to find the correct policy"); //$NON-NLS-1$
        }
    }

        //    Users->           [u1,u2,g1,u5]  [u2,u3,u5]  [u4,u5]
        //    Resources         Policy1         Policy2     Policy3

    public void testPolicyForPrincipals1() throws Exception {
        final FakeAuthorizationSource source = new FakeAuthorizationSource();
        helpPopulate(source);
        final String[] userNames   = new String[]{"u1"}; //$NON-NLS-1$
        final String[] groupNames  = new String[]{};
        final String[] policyNames = new String[]{"Policy1"}; //$NON-NLS-1$
        helpTestFindPolicies(source,userNames,groupNames,policyNames);
    }

    public void testPolicyForPrincipals2() throws Exception {
        final FakeAuthorizationSource source = new FakeAuthorizationSource();
        helpPopulate(source);
        final String[] userNames   = new String[]{"u1"}; //$NON-NLS-1$
        final String[] groupNames  = new String[]{"g1"}; //$NON-NLS-1$
        final String[] policyNames = new String[]{"Policy1"}; //$NON-NLS-1$
        helpTestFindPolicies(source,userNames,groupNames,policyNames);
    }

    public void testPolicyForPrincipals3() throws Exception {
        final FakeAuthorizationSource source = new FakeAuthorizationSource();
        helpPopulate(source);
        final String[] userNames   = new String[]{"u1"}; //$NON-NLS-1$
        final String[] groupNames  = new String[]{"g1"}; //$NON-NLS-1$
        final String[] policyNames = new String[]{"Policy1"}; //$NON-NLS-1$
        helpTestFindPolicies(source,userNames,groupNames,policyNames);
    }

    public void testPolicyForPrincipals4() throws Exception {
        final FakeAuthorizationSource source = new FakeAuthorizationSource();
        helpPopulate(source);
        final String[] userNames   = new String[]{"u2"}; //$NON-NLS-1$
        final String[] groupNames  = new String[]{};
        final String[] policyNames = new String[]{"Policy1","Policy2"}; //$NON-NLS-1$ //$NON-NLS-2$
        helpTestFindPolicies(source,userNames,groupNames,policyNames);
    }

    public void testPolicyForPrincipals5() throws Exception {
        final FakeAuthorizationSource source = new FakeAuthorizationSource();
        helpPopulate(source);
        final String[] userNames   = new String[]{"u5"}; //$NON-NLS-1$
        final String[] groupNames  = new String[]{};
        final String[] policyNames = new String[]{"Policy1","Policy2","Policy3"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        helpTestFindPolicies(source,userNames,groupNames,policyNames);
    }

        // This is the authorized views of each user
        //    Resources          u1    u2    u3    u4    u5
        //    ----------------  ----  ----  ----  ----  ----
        //    f1                r     cr    cr    rud   crud
        //    f1/f11            -     r     r     crud  crud
        //    f1/f11/m111       -     ru    ru    crud  crud
        //    f1/f12            cr    cr    r     crud  crud
        //    f1/f12/m121       r     crud  crud  rd    crud
        //    f1/f12/m122       ru    ru    cr    rd    crud
        //    f1/f12/m123       crud  crud  -     -     crud
        //    f1/f12/m124       crud  crud  -     r     crud

    public void testAuthorization1() throws Exception {
        final FakeAuthorizationSource source = new FakeAuthorizationSource();
        helpPopulate(source);
        final String userName      = "u5"; //$NON-NLS-1$
        final AuthorizationActions actions = StandardAuthorizationActions.ALL;
        helpTestAuthorization(source,userName,actions,true,"f1"); //$NON-NLS-1$
        helpTestAuthorization(source,userName,actions,true,"f1/f11"); //$NON-NLS-1$
        helpTestAuthorization(source,userName,actions,true,"f1/f11/m111"); //$NON-NLS-1$
        helpTestAuthorization(source,userName,actions,true,"f1/f12"); //$NON-NLS-1$
        helpTestAuthorization(source,userName,actions,true,"f1/f12/m121"); //$NON-NLS-1$
        helpTestAuthorization(source,userName,actions,true,"f1/f12/m122"); //$NON-NLS-1$
        helpTestAuthorization(source,userName,actions,true,"f1/f12/m123"); //$NON-NLS-1$
        helpTestAuthorization(source,userName,actions,true,"f1/f12/m124"); //$NON-NLS-1$
    }

    public void testAuthorization2() throws Exception {
        final FakeAuthorizationSource source = new FakeAuthorizationSource();
        helpPopulate(source);
        final String userName      = "u1"; //$NON-NLS-1$
        helpTestAuthorization(source,userName,AuthorizationTestUtil.R,   true,"f1"); //$NON-NLS-1$
        helpTestAuthorization(source,userName,AuthorizationTestUtil.CR,  true,"f1/f12"); //$NON-NLS-1$
        helpTestAuthorization(source,userName,AuthorizationTestUtil.R,   true,"f1/f12/m121"); //$NON-NLS-1$
        helpTestAuthorization(source,userName,AuthorizationTestUtil.RU,  true,"f1/f12/m122"); //$NON-NLS-1$
        helpTestAuthorization(source,userName,AuthorizationTestUtil.CRUD,true,"f1/f12/m123"); //$NON-NLS-1$
        helpTestAuthorization(source,userName,AuthorizationTestUtil.CRUD,true,"f1/f12/m124"); //$NON-NLS-1$

        helpTestAuthorization(source,userName,AuthorizationTestUtil.RU,  false,"f1"); //$NON-NLS-1$
        helpTestAuthorization(source,userName,AuthorizationTestUtil.RUD, false,"f1"); //$NON-NLS-1$
        helpTestAuthorization(source,userName,AuthorizationTestUtil.C,   false,"f1"); //$NON-NLS-1$
        helpTestAuthorization(source,userName,AuthorizationTestUtil.R,   false,"f1/f11"); //$NON-NLS-1$
    }

}
