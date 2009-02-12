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
 * <p>Test cases for {@link StandardAuthorizationActions} class. </p>
 */
public class TestStandardAuthorizationActions extends TestCase {

    // Test case should succeed
    private static final boolean SUCCEED = true;

    // Test case should not succeed
    private static final boolean FAIL = false;

    // =========================================================================
    //                        T E S T     C O N T R O L
    // =========================================================================

    /** Construct test case. */
    public TestStandardAuthorizationActions( String name ) {
        super( name );
    }

    // =========================================================================
    //                      H E L P E R    M E T H O D S
    // =========================================================================

    /**
     * Test the expiration/effectivity info for licenses.
     */
    private static void helpTestImplies( int checkActions, int actionsInPlace,
                                         boolean shouldBeValid ) {

        AuthorizationActions authToCheck
            = StandardAuthorizationActions.getAuthorizationActions(checkActions);

        AuthorizationActions authInPlace
            = StandardAuthorizationActions.getAuthorizationActions(actionsInPlace);

        if ( shouldBeValid ) {
            assertTrue( "Authorization check failed, should have succeeded: " //$NON-NLS-1$
                + checkActions + " checked against " + actionsInPlace + ".", //$NON-NLS-1$ //$NON-NLS-2$
                authInPlace.implies(authToCheck) );
        } else {
            assertTrue( "Authorization check succeeded, should have failed: " //$NON-NLS-1$
                + checkActions + " checked against " + actionsInPlace + ".", //$NON-NLS-1$ //$NON-NLS-2$
                !authInPlace.implies(authToCheck) );
        }
    }

    // =========================================================================
    //                         T E S T     C A S E S
    // =========================================================================

    // POSITIVE EQUALITY TESTS
    
    /**
     * Positive test.
     */
    public void testPos_Implies_0_0() {
        helpTestImplies( 0, 0, SUCCEED );
    }

    /**
     * Positive test.
     */
    public void testPos_Implies_1_1() {
        helpTestImplies( 1, 1, SUCCEED );
    }

    /**
     * Positive test.
     */
    public void testPos_Implies_2_2() {
        helpTestImplies( 2, 2, SUCCEED );
    }

    /**
     * Positive test.
     */
    public void testPos_Implies_4_4() {
        helpTestImplies( 4, 4, SUCCEED );
    }

    /**
     * Positive test.
     */
    public void testPos_Implies_8_8() {
        helpTestImplies( 8, 8, SUCCEED );
    }

    /**
     * Positive test.
     */
    public void testPos_Implies_12_12() {
        helpTestImplies( 1|2, 1|2, SUCCEED );
    }

    /**
     * Positive test.
     */
    public void testPos_Implies_124_124() {
        helpTestImplies( 1|2|4, 1|2|4, SUCCEED );
    }

    /**
     * Positive test.
     */
    public void testPos_Implies_1248_1248() {
        helpTestImplies( 1|2|4|8, 1|2|4|8, SUCCEED );
    }

    // POSITIVE SUBSET TESTS
    
    /**
     * Positive test.
     */
    public void testPos_Implies_0_1() {
        helpTestImplies( 0, 1, SUCCEED );
    }

    /**
     * Positive test.
     */
    public void testPos_Implies_0_12() {
        helpTestImplies( 0, 1|2, SUCCEED );
    }

    /**
     * Positive test.
     */
    public void testPos_Implies_0_124() {
        helpTestImplies( 0, 1|2|4, SUCCEED );
    }

    /**
     * Positive test.
     */
    public void testPos_Implies_0_1248() {
        helpTestImplies( 0, 1|2|4|8, SUCCEED );
    }

    /**
     * Positive test.
     */
    public void testPos_Implies_1_12() {
        helpTestImplies( 1, 1|2, SUCCEED );
    }

    /**
     * Positive test.
     */
    public void testPos_Implies_1_14() {
        helpTestImplies( 1, 1|4, SUCCEED );
    }

    /**
     * Positive test.
     */
    public void testPos_Implies_1_18() {
        helpTestImplies( 1, 1|8, SUCCEED );
    }

    /**
     * Positive test.
     */
    public void testPos_Implies_1_124() {
        helpTestImplies( 1, 1|2|4, SUCCEED );
    }

    /**
     * Positive test.
     */
    public void testPos_Implies_1_128() {
        helpTestImplies( 1, 1|2|8, SUCCEED );
    }

    /**
     * Positive test.
     */
    public void testPos_Implies_1_148() {
        helpTestImplies( 1, 1|4|8, SUCCEED );
    }

    /**
     * Positive test.
     */
    public void testPos_Implies_1_1248() {
        helpTestImplies( 1, 1|2|4|8, SUCCEED );
    }

    /**
     * Positive test.
     */
    public void testPos_Implies_2_1248() {
        helpTestImplies( 2, 1|2|4|8, SUCCEED );
    }

    /**
     * Positive test.
     */
    public void testPos_Implies_4_1248() {
        helpTestImplies( 4, 1|2|4|8, SUCCEED );
    }

    /**
     * Positive test.
     */
    public void testPos_Implies_8_1248() {
        helpTestImplies( 8, 1|2|4|8, SUCCEED );
    }

    // NEGATIVE MISMATCHED SET TESTS
    
    /**
     * Negative test.
     */
    public void testNeg_Implies_1_2() {
        helpTestImplies( 1, 2, FAIL );
    }

    /**
     * Negative test.
     */
    public void testNeg_Implies_1_4() {
        helpTestImplies( 1, 4, FAIL );
    }

    /**
     * Negative test.
     */
    public void testNeg_Implies_1_8() {
        helpTestImplies( 1, 8, FAIL );
    }

    /**
     * Negative test.
     */
    public void testNeg_Implies_1_24() {
        helpTestImplies( 1, 2|4, FAIL );
    }

    /**
     * Negative test.
     */
    public void testNeg_Implies_1_28() {
        helpTestImplies( 1, 2|8, FAIL );
    }

    /**
     * Negative test.
     */
    public void testNeg_Implies_1_48() {
        helpTestImplies( 1, 4|8, FAIL );
    }

    /**
     * Negative test.
     */
    public void testNeg_Implies_2_14() {
        helpTestImplies( 2, 1|4, FAIL );
    }

    /**
     * Negative test.
     */
    public void testNeg_Implies_2_18() {
        helpTestImplies( 2, 1|8, FAIL );
    }

    /**
     * Negative test.
     */
    public void testNeg_Implies_2_48() {
        helpTestImplies( 2, 4|8, FAIL );
    }

    /**
     * Negative test.
     */
    public void testNeg_Implies_2_1() {
        helpTestImplies( 2, 1, FAIL );
    }

    /**
     * Negative test.
     */
    public void testNeg_Implies_4_1() {
        helpTestImplies( 4, 1, FAIL );
    }

    /**
     * Negative test.
     */
    public void testNeg_Implies_8_1() {
        helpTestImplies( 8, 1, FAIL );
    }

    /**
     * Negative test.
     */
    public void testNeg_Implies_24_1() {
        helpTestImplies( 2|4, 1, FAIL );
    }

    /**
     * Negative test.
     */
    public void testNeg_Implies_28_1() {
        helpTestImplies( 2|8, 1, FAIL );
    }

    /**
     * Negative test.
     */
    public void testNeg_Implies_48_1() {
        helpTestImplies( 4|8, 1, FAIL );
    }

    /**
     * Negative test.
     */
    public void testNeg_Implies_14_8() {
        helpTestImplies( 1|4, 8, FAIL );
    }

    /**
     * Negative test.
     */
    public void testNeg_Implies_124_8() {
        helpTestImplies( 1|2|4, 8, FAIL );
    }

    // NEGATIVE SUPERSET TESTS
    
    /**
     * Negative test.
     */
    public void testNeg_Implies_12_1() {
        helpTestImplies( 1|2, 1, FAIL );
    }

    /**
     * Negative test.
     */
    public void testNeg_Implies_14_1() {
        helpTestImplies( 1|4, 1, FAIL );
    }

    /**
     * Negative test.
     */
    public void testNeg_Implies_18_1() {
        helpTestImplies( 1|8, 1, FAIL );
    }

    /**
     * Negative test.
     */
    public void testNeg_Implies_128_1() {
        helpTestImplies( 1|2|8, 1, FAIL );
    }

    /**
     * Negative test.
     */
    public void testNeg_Implies_148_1() {
        helpTestImplies( 1|4|8, 1, FAIL );
    }

    /**
     * Negative test.
     */
    public void testNeg_Implies_1248_1() {
        helpTestImplies( 1|2|4|8, 1, FAIL );
    }

    /**
     * Negative test.
     */
    public void testNeg_Implies_18_8() {
        helpTestImplies( 1|8, 8, FAIL );
    }

    /**
     * Negative test.
     */
    public void testNeg_Implies_28_8() {
        helpTestImplies( 2|8, 8, FAIL );
    }

    /**
     * Negative test.
     */
    public void testNeg_Implies_48_8() {
        helpTestImplies( 4|8, 8, FAIL );
    }

    /**
     * Negative test.
     */
    public void testNeg_Implies_128_8() {
        helpTestImplies( 1|2|8, 8, FAIL );
    }

    /**
     * Negative test.
     */
    public void testNeg_Implies_148_8() {
        helpTestImplies( 1|4|8, 8, FAIL );
    }

    /**
     * Negative test.
     */
    public void testNeg_Implies_1248_8() {
        helpTestImplies( 1|2|4|8, 8, FAIL );
    }

} // END CLASS

