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

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.metamatrix.platform.security.util.RolePermissionFactory;

/**
 * <p>Test cases for {@link BasicAuthorizationPermission} class. </p>
 */
public class TestBasicAuthorizationPermission extends TestCase {

    // Factories
    private static BasicAuthorizationPermissionFactory bapFactory;
    private static RolePermissionFactory rpFactory;

    // Realms
    private static AuthorizationRealm realm_00;
    private static AuthorizationRealm realm_01;
    private static AuthorizationRealm realm_10;
//    private static AuthorizationRealm realm_11;

    // Resources
    private static String invariantResource = "Model.catalog.group.element_0"; //$NON-NLS-1$
    private static String longResource = invariantResource + ".fred"; //$NON-NLS-1$
    private static String groupResource = "Model.catalog.group"; //$NON-NLS-1$
    private static String groupRecursiveResource = "Model.catalog.group.*"; //$NON-NLS-1$
    private static String catalogResource = "Model.catalog"; //$NON-NLS-1$

    // Permissions
    // The permission to use as invariant
    private static AuthorizationPermission source;

    // Variant permissions
    private static AuthorizationPermission wrongInstancePerm;
    private static AuthorizationPermission wrongSuperRealmPerm;
    private static AuthorizationPermission wrongSubRealmPerm;
    private static AuthorizationPermission longResourcePerm;
    private static AuthorizationPermission catalogResourcePerm;
    private static AuthorizationPermission groupReadPerm;
    private static AuthorizationPermission groupRecursiveReadPerm;
    private static AuthorizationPermission memberOfGroupPerm;

    // Permission tests dealing with Actions
    private static AuthorizationPermission allActionPerm;
    private static AuthorizationPermission noneActionPerm;
    private static AuthorizationPermission createActionPerm;
    private static AuthorizationPermission readActionPerm;
    private static AuthorizationPermission updateActionPerm;
    private static AuthorizationPermission deleteActionPerm;

    // Test case should succeed
    private static final boolean SUCCEED = true;

    // Test case should not succeed
    private static final boolean FAIL = false;

    // =========================================================================
    //                         W O R K    M E T H O D S
    // =========================================================================

    /**
     * Setup data for tests
     */
    private static void initTestObjects() {
        // Create Realms
        realm_00 = new AuthorizationRealm("superRealm_0", "subRealm_0"); //$NON-NLS-1$ //$NON-NLS-2$
        realm_01 = new AuthorizationRealm("superRealm_0", "subRealm_1"); //$NON-NLS-1$ //$NON-NLS-2$
        realm_10 = new AuthorizationRealm("superRealm_1", "subRealm_0"); //$NON-NLS-1$ //$NON-NLS-2$
//        realm_11 = 
            new AuthorizationRealm("superRealm_1", "subRealm_1"); //$NON-NLS-1$ //$NON-NLS-2$

        // Create Factories
        bapFactory = new BasicAuthorizationPermissionFactory();
        rpFactory  = new RolePermissionFactory();

        // Create Permissions
        source                  = bapFactory.create(invariantResource, realm_00, StandardAuthorizationActions.ALL);

        allActionPerm           = bapFactory.create(invariantResource, realm_00, StandardAuthorizationActions.ALL);
        noneActionPerm          = bapFactory.create(invariantResource, realm_00, StandardAuthorizationActions.NONE);
        createActionPerm        = bapFactory.create(invariantResource, realm_00, StandardAuthorizationActions.DATA_CREATE);
        readActionPerm          = bapFactory.create(invariantResource, realm_00, StandardAuthorizationActions.DATA_READ);
        updateActionPerm        = bapFactory.create(invariantResource, realm_00, StandardAuthorizationActions.DATA_UPDATE);
        deleteActionPerm        = bapFactory.create(invariantResource, realm_00, StandardAuthorizationActions.DATA_DELETE);

        wrongInstancePerm       = rpFactory.create(invariantResource, realm_00, StandardAuthorizationActions.ALL);
        wrongSuperRealmPerm     = bapFactory.create(invariantResource, realm_10, StandardAuthorizationActions.ALL);
        wrongSubRealmPerm       = bapFactory.create(invariantResource, realm_01, StandardAuthorizationActions.ALL);
        longResourcePerm        = bapFactory.create(longResource, realm_00, StandardAuthorizationActions.ALL);
        catalogResourcePerm     = bapFactory.create(catalogResource, realm_00, StandardAuthorizationActions.ALL);

        groupRecursiveReadPerm  = bapFactory.create(groupRecursiveResource, realm_00, StandardAuthorizationActions.DATA_READ);
        groupReadPerm           = bapFactory.create(groupResource, realm_00, StandardAuthorizationActions.DATA_READ);
        memberOfGroupPerm       = bapFactory.create(invariantResource, realm_00, StandardAuthorizationActions.DATA_READ);
    }

    // =========================================================================
    //                        T E S T     C O N T R O L
    // =========================================================================

    /** Construct test case. */
    public TestBasicAuthorizationPermission( String name ) {
        super( name );
    }

    /**
     * Test suite, with one-time setup.
     */
    public static Test suite() {
        TestSuite suite = new TestSuite();
        suite.addTestSuite( TestBasicAuthorizationPermission.class );

        // One-time setup and teardown
        return new TestSetup(suite) {
            public void setUp() {
                initTestObjects();
            }
            public void tearDown() {
            }
        };
    }

    // =========================================================================
    //                      H E L P E R    M E T H O D S
    // =========================================================================

    /**
     * Test the expiration/effectivity info for licenses.
     */
    private static void helpTestImplies( AuthorizationPermission p1,
                                         AuthorizationPermission p2,
                                         boolean shouldImply ) {

        boolean implied = p1.implies(p2);

        if ( implied && shouldImply ) {
            assertTrue( "AuthorizationPermission.implies() check succeeded, should have succeeded: " //$NON-NLS-1$
                + p1 + " => " + p2 + " <> p1 => p2 ? " + implied + ": shouldBeValid? " + shouldImply, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                ( implied && shouldImply ) );
        } else if ( ! implied && ! shouldImply ) {
            assertTrue( "AuthorizationPermission.implies() check failed, should have failed: " //$NON-NLS-1$
                + p1 + " ! => " + p2 + " <> p1 => p2 ? " + implied + ": shouldBeValid? " + shouldImply, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                ( ! implied && ! shouldImply ) );
        } else if ( ! implied && shouldImply ) {
            assertTrue( "AuthorizationPermission.implies() check failed, should have succeeded: " //$NON-NLS-1$
                + p1 + " => " + p2 + " <> p1 => p2 ? " + implied + ": shouldBeValid? " + shouldImply, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                ( ! implied && ! shouldImply ) );
        } else if ( implied && ! shouldImply ) {
            assertTrue( "AuthorizationPermission.implies() check succeded, should have failed: " //$NON-NLS-1$
                + p1 + " ! => " + p2 + " <> p1 => p2 ? " + implied + ": shouldImply? " + shouldImply, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                ( implied && shouldImply ) );
        }

    }

    // =========================================================================
    //                         T E S T     C A S E S
    // =========================================================================

    // -----------------------------
    // POSITIVE EQUALITY TESTS
    // -----------------------------

    /**
     * Positive test.
     */
    public void testPos_RecursiveGroupAllowsElementRead() {
        helpTestImplies( groupRecursiveReadPerm, memberOfGroupPerm, SUCCEED );
    }

    /**
     * Positive test.
     */
    public void testPos_ElementAllowsGroupRead() {
        helpTestImplies( memberOfGroupPerm, groupReadPerm, SUCCEED );
    }

    /**
     * Positive test.
     */
    public void testPos_allImpliesAll() {
        helpTestImplies( allActionPerm, allActionPerm, SUCCEED );
    }

    /**
     * Positive test.
     */
    public void testPos_allImpliesCreate() {
        helpTestImplies( allActionPerm, createActionPerm, SUCCEED );
    }

    /**
     * Positive test.
     */
    public void testPos_allImpliesRead() {
        helpTestImplies( allActionPerm, readActionPerm, SUCCEED );
    }

    /**
     * Positive test.
     */
    public void testPos_allImpliesUpdate() {
        helpTestImplies( allActionPerm, updateActionPerm, SUCCEED );
    }

    /**
     * Positive test.
     */
    public void testPos_allImpliesDelete() {
        helpTestImplies( allActionPerm, deleteActionPerm, SUCCEED );
    }

    /**
     * Positive test.
     */
    public void testPos_createImpliesCreate() {
        helpTestImplies( createActionPerm, createActionPerm, SUCCEED );
    }

    /**
     * Positive test.
     */
    public void testPos_readImpliesRead() {
        helpTestImplies( readActionPerm, readActionPerm, SUCCEED );
    }

    /**
     * Positive test.
     */
    public void testPos_updateImpliesUpdate() {
        helpTestImplies( updateActionPerm, updateActionPerm, SUCCEED );
    }

    /**
     * Positive test.
     */
    public void testPos_deleteImpliesDelete() {
        helpTestImplies( deleteActionPerm, deleteActionPerm, SUCCEED );
    }

    // -----------------------------
    // NEGATIVE MISMATCHED SET TESTS
    // -----------------------------

    /**
     * Negative test.
     */
    public void testNeg_Implies_WrongInstance() {
        helpTestImplies( source, wrongInstancePerm, FAIL );
    }

    /**
     * Negative test.
     */
    public void testNeg_Implies_WrongSuperRealm() {
        helpTestImplies( source, wrongSuperRealmPerm, FAIL );
    }

    /**
     * Negative test.
     */
    public void testNeg_Implies_WrongSubRealm() {
        helpTestImplies( source, wrongSubRealmPerm, FAIL );
    }

    /**
     * Negative test.
     */
    public void testNeg_Implies_LongResource() {
        helpTestImplies( source, longResourcePerm, FAIL );
    }

    /**
     * Negative test.
     */
    public void testNeg_ElementImpliesCatalog() {
        helpTestImplies( source, catalogResourcePerm, FAIL );
    }

    /**
     * Negative test.
     */
    public void testNeg_ElementImpliesRecursiveGroupRead() {
        helpTestImplies( memberOfGroupPerm, groupRecursiveReadPerm, FAIL );
    }

    /**
     * Negative test.
     * Create !=> Delete.
     */
    public void testNeg_createImpliesDelete() {
        helpTestImplies( createActionPerm, deleteActionPerm, FAIL );
    }

    /**
     * Negative test.
     * Create ! => Read
     */
    public void testNeg_createImpliesRead() {
        helpTestImplies( createActionPerm, readActionPerm, FAIL );
    }

    /**
     * Negative test.
     * Create ! => Update
     */
    public void testNeg_createImpliesUpdate() {
        helpTestImplies( createActionPerm, updateActionPerm, FAIL );
    }

    /**
     * Negative test.
     * Create !=> All.
     */
    public void testNeg_createImpliesAll() {
        helpTestImplies( createActionPerm, allActionPerm, FAIL );
    }

    /**
     * Negative test.
     * Create !=> None.
     */
    public void testNeg_createImpliesNone() {
        helpTestImplies( createActionPerm, noneActionPerm, FAIL );
    }

    /**
     * Negative test.
     * Update ! => Create
     */
    public void testNeg_updateImpliesCreate() {
        helpTestImplies( updateActionPerm, createActionPerm, FAIL );
    }

    /**
     * Negative test.
     * Update ! => Read
     */
    public void testNeg_updateImpliesRead() {
        helpTestImplies( updateActionPerm, readActionPerm, FAIL );
    }

    /**
     * Negative test.
     * Update ! => Delete
     */
    public void testNeg_updateImpliesDelete() {
        helpTestImplies( updateActionPerm, deleteActionPerm, FAIL );
    }

    /**
     * Negative test.
     * Update !=> All.
     */
    public void testNeg_updateImpliesAll() {
        helpTestImplies( updateActionPerm, allActionPerm, FAIL );
    }

    /**
     * Negative test.
     * Update !=> None.
     */
    public void testNeg_updateImpliesNone() {
        helpTestImplies( updateActionPerm, noneActionPerm, FAIL );
    }

    /**
     * Negative test.
     * Read ! => Create
     */
    public void testNeg_readImpliesCreate() {
        helpTestImplies( readActionPerm, createActionPerm, FAIL );
    }

    /**
     * Negative test.
     * Read ! => Update
     */
    public void testNeg_readImpliesUpdate() {
        helpTestImplies( readActionPerm, updateActionPerm, FAIL );
    }

    /**
     * Negative test.
     * Read ! => Delete
     */
    public void testNeg_readImpliesDelete() {
        helpTestImplies( readActionPerm, deleteActionPerm, FAIL );
    }

    /**
     * Negative test.
     * Read !=> All.
     */
    public void testNeg_readImpliesAll() {
        helpTestImplies( readActionPerm, allActionPerm, FAIL );
    }

    /**
     * Negative test.
     * Read !=> None.
     */
    public void testNeg_readImpliesNone() {
        helpTestImplies( readActionPerm, noneActionPerm, FAIL );
    }

    /**
     * Negative test.
     * Delete !=> Create.
     */
    public void testNeg_deleteImpliesCreate() {
        helpTestImplies( deleteActionPerm, createActionPerm, FAIL );
    }

    /**
     * Negative test.
     * Delete !=> Read.
     */
    public void testNeg_deleteImpliesRead() {
        helpTestImplies( deleteActionPerm, readActionPerm, FAIL );
    }

    /**
     * Negative test.
     * Delete !=> Update.
     */
    public void testNeg_deleteImpliesUpdate() {
        helpTestImplies( deleteActionPerm, updateActionPerm, FAIL );
    }

    /**
     * Negative test.
     * Delete !=> All.
     */
    public void testNeg_deleteImpliesAll() {
        helpTestImplies( deleteActionPerm, allActionPerm, FAIL );
    }

    /**
     * Negative test.
     * Detete !=> None.
     */
    public void testNeg_deleteImpliesNone() {
        helpTestImplies( deleteActionPerm, noneActionPerm, FAIL );
    }

    /**
     * Negative test.
     * None !=> Create.
     */
    public void testNeg_noneImpliesCreate() {
        helpTestImplies( noneActionPerm, createActionPerm, FAIL );
    }

    /**
     * Negative test.
     * None !=> Read.
     */
    public void testNeg_noneImpliesRead() {
        helpTestImplies( noneActionPerm, readActionPerm, FAIL );
    }

    /**
     * Negative test.
     * None !=> Update.
     */
    public void testNeg_noneImpliesUpdate() {
        helpTestImplies( noneActionPerm, updateActionPerm, FAIL );
    }

    /**
     * Negative test.
     * None !=> Delete.
     */
    public void testNeg_noneImpliesDelete() {
        helpTestImplies( noneActionPerm, deleteActionPerm, FAIL );
    }

    /**
     * Negative test.
     * None !=> All.
     */
    public void testNeg_noneImpliesAll() {
        helpTestImplies( noneActionPerm, allActionPerm, FAIL );
    }

    /**
     * Negative test.
     * All !=> None
     */
    public void testNeg_allImpliesNone() {
        helpTestImplies( allActionPerm, noneActionPerm, FAIL );
    }

    /**
     * Negative test.
     * None !=> None.
     */
    public void testNeg_noneImpliesNone() {
        helpTestImplies( noneActionPerm, noneActionPerm, FAIL );
    }
}
