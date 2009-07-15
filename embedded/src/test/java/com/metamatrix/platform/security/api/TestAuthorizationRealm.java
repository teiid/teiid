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

public class TestAuthorizationRealm extends TestCase {

    /**
     * Constructor for TestAuthorizationRealm.
     * @param name
     */
    public TestAuthorizationRealm(String name) {
        super(name);
    }

    final public void testEquals() {
        AuthorizationRealm realm_1 = new AuthorizationRealm("QueryTest", "1"); //$NON-NLS-1$ //$NON-NLS-2$
        AuthorizationRealm realm_2 = new AuthorizationRealm("QueryTest", "001"); //$NON-NLS-1$ //$NON-NLS-2$
        AuthorizationRealm realm_3 = new AuthorizationRealm("QueryTest", "000003"); //$NON-NLS-1$ //$NON-NLS-2$
        AuthorizationRealm realm_4 = new AuthorizationRealm("QueryTest4", "1"); //$NON-NLS-1$ //$NON-NLS-2$
        AuthorizationRealm realm_5 = new AuthorizationRealm("QueryTest", "001"); //$NON-NLS-1$ //$NON-NLS-2$
        AuthorizationRealm realm_6 = new AuthorizationRealm("Querytest", "001"); //$NON-NLS-1$ //$NON-NLS-2$

        // Self equality
        if ( ! realm_1.equals(realm_1) ) {
            fail(realm_1 + " != " + realm_1 + " but should be."); //$NON-NLS-1$ //$NON-NLS-2$
        }
        if ( ! realm_2.equals(realm_2) ) {
            fail(realm_2 + " != " + realm_2 + " but should be."); //$NON-NLS-1$ //$NON-NLS-2$
        }
        if ( ! realm_3.equals(realm_3) ) {
            fail(realm_3 + " != " + realm_3 + " but should be."); //$NON-NLS-1$ //$NON-NLS-2$
        }
        if ( ! realm_4.equals(realm_4) ) {
            fail(realm_4 + " != " + realm_4 + " but should be."); //$NON-NLS-1$ //$NON-NLS-2$
        }
        if ( ! realm_5.equals(realm_5) ) {
            fail(realm_5 + " != " + realm_5 + " but should be."); //$NON-NLS-1$ //$NON-NLS-2$
        }

        // realm_1 and realm_2 should be equal
        if ( ! realm_1.equals(realm_2) ) {
            fail(realm_1 + " != " + realm_2 + " but should be."); //$NON-NLS-1$ //$NON-NLS-2$
        }
        if ( ! realm_2.equals(realm_1) ) {
            fail(realm_2 + " != " + realm_1 + " but should be."); //$NON-NLS-1$ //$NON-NLS-2$
        }

        // realm_3 & realm_4 not equal to anything
        if ( realm_1.equals(realm_3) ) {
            fail(realm_1 + " == " + realm_3 + " but should not be."); //$NON-NLS-1$ //$NON-NLS-2$
        }
        if ( realm_2.equals(realm_3) ) {
            fail(realm_2 + " == " + realm_3 + " but should not be."); //$NON-NLS-1$ //$NON-NLS-2$
        }
        if ( realm_1.equals(realm_4) ) {
            fail(realm_1 + " == " + realm_4 + " but should not be."); //$NON-NLS-1$ //$NON-NLS-2$
        }

        // realm_2 is equal to realm_5
        if ( ! realm_2.equals(realm_5) ) {
            fail(realm_2 + " != " + realm_5 + " but should be."); //$NON-NLS-1$ //$NON-NLS-2$
        }
        
        assertTrue(realm_5.equals(realm_6));
    }

}
