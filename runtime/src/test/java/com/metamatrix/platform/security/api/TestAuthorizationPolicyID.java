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

package com.metamatrix.platform.security.api;

import junit.framework.TestCase;

/**
 * @author jcunningham
 *
 * To change the template for this generated type comment go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
public class TestAuthorizationPolicyID extends TestCase {

    /**
     * Constructor for TestAuthorizationPolicyID.
     * @param name
     */
    public TestAuthorizationPolicyID(String name) {
        super(name);
    }

    final public void testGetRealm() {
        AuthorizationRealm aRealm = new AuthorizationRealm("A VDB", "1", "A bogus realm."); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        AuthorizationPolicyID aPolicyID = new AuthorizationPolicyID("A new policy", "For a bogus realm", aRealm); //$NON-NLS-1$ //$NON-NLS-2$

        if ( !aPolicyID.getRealm().equals(aRealm) ) {
            fail(aPolicyID.getRealm() + " != " + aRealm + " but should be."); //$NON-NLS-1$ //$NON-NLS-2$
        }
    }

    /*
     * Test for boolean equals(Object)
     */
    final public void testEqualsObject() {
        AuthorizationPolicyID policyID_1 = new AuthorizationPolicyID("A Query Test", "A query test entitlement", new AuthorizationRealm("QueryTest", "1")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        AuthorizationPolicyID policyID_2 = new AuthorizationPolicyID("A Query Test", "A query test entitlement", new AuthorizationRealm("QueryTest", "001")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        AuthorizationPolicyID policyID_3 = new AuthorizationPolicyID("A Query Test", "A query test entitlement", new AuthorizationRealm("QueryTest", "000003")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        AuthorizationPolicyID policyID_4 = new AuthorizationPolicyID("A Query Test", "A query test entitlement", new AuthorizationRealm("QueryTest4", "1")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        AuthorizationPolicyID policyID_5 = new AuthorizationPolicyID("A Query Test", "A query test entitlement", new AuthorizationRealm("QueryTest", "001")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$

        // Self equality
        if ( !policyID_1.equals(policyID_1) ) {
            fail(policyID_1 + " != " + policyID_1 + " but should be."); //$NON-NLS-1$ //$NON-NLS-2$
        }
        if ( !policyID_2.equals(policyID_2) ) {
            fail(policyID_2 + " != " + policyID_2 + " but should be."); //$NON-NLS-1$ //$NON-NLS-2$
        }
        if ( !policyID_3.equals(policyID_3) ) {
            fail(policyID_3 + " != " + policyID_3 + " but should be."); //$NON-NLS-1$ //$NON-NLS-2$
        }
        if ( !policyID_4.equals(policyID_4) ) {
            fail(policyID_4 + " != " + policyID_4 + " but should be."); //$NON-NLS-1$ //$NON-NLS-2$
        }
        if ( !policyID_5.equals(policyID_5) ) {
            fail(policyID_5 + " != " + policyID_5 + " but should be."); //$NON-NLS-1$ //$NON-NLS-2$
        }

        // policyID_1 and policyID_2 should be equal
        if ( !policyID_1.equals(policyID_2) ) {
            fail(policyID_1 + " != " + policyID_2 + " but should be."); //$NON-NLS-1$ //$NON-NLS-2$
        }
        if ( !policyID_2.equals(policyID_1) ) {
            fail(policyID_2 + " != " + policyID_1 + " but should be."); //$NON-NLS-1$ //$NON-NLS-2$
        }

        // policyID_3 & policyID_4 not equal to anything
        if ( policyID_1.equals(policyID_3) ) {
            fail(policyID_1 + " == " + policyID_3 + " but should not be."); //$NON-NLS-1$ //$NON-NLS-2$
        }
        if ( policyID_2.equals(policyID_3) ) {
            fail(policyID_2 + " == " + policyID_3 + " but should not be."); //$NON-NLS-1$ //$NON-NLS-2$
        }
        if ( policyID_1.equals(policyID_4) ) {
            fail(policyID_1 + " == " + policyID_4 + " but should not be."); //$NON-NLS-1$ //$NON-NLS-2$
        }

        // policyID_2 is equal to policyID_5
        if ( !policyID_2.equals(policyID_5) ) {
            fail(policyID_2 + " != " + policyID_5 + " but should be."); //$NON-NLS-1$ //$NON-NLS-2$
        }
    }

}
