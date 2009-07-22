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

/*
 * Date: Jan 22, 2004
 * Time: 8:39:33 PM
 */
package com.metamatrix.platform.security.api;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.TestCase;

import com.metamatrix.core.id.IDGenerator;
import com.metamatrix.core.id.ObjectID;
import com.metamatrix.core.id.UUIDFactory;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.platform.admin.api.PermissionDataNodeDefinition;
import com.metamatrix.platform.admin.api.PermissionDataNodeTreeView;
import com.metamatrix.platform.admin.apiimpl.PermissionDataNodeDefinitionImpl;
import com.metamatrix.platform.admin.apiimpl.PermissionDataNodeImpl;
import com.metamatrix.platform.admin.apiimpl.PermissionDataNodeTreeViewImpl;

/**
 * JUnit test for AuthorizationObjectEditor
 */
public final class TestAuthorizationObjectEditor extends TestCase {
    // Permission factory
    private static BasicAuthorizationPermissionFactory bapFactory = new BasicAuthorizationPermissionFactory();

    // Realm (VDB)
    private static AuthorizationRealm aRealm = new AuthorizationRealm("VDB1", "Vers1", "The test realm"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

    // All resources in VDB
    private static String[] resources = new String[] {
        "Model_0.catalog_0.group_0.element_0", //$NON-NLS-1$
        "Model_0.catalog_0.group_0.element_1", //$NON-NLS-1$
        "Model_0.catalog_0.group_1.element_0", //$NON-NLS-1$
        "Model_0.catalog_1.group_0.element_0", //$NON-NLS-1$
        "Model_0.catalog_1.group_0.element_1", //$NON-NLS-1$
        "Model_0.catalog_1.group_0.element_2", //$NON-NLS-1$
        "Model_0.catalog_1.group_0.element_3", //$NON-NLS-1$
        "Model_0.catalog_1.group_0.element_4", //$NON-NLS-1$
        "Model_0.catalog_1.group_1.element_0", //$NON-NLS-1$
        "Model_0.catalog_1.group_1.element_1", //$NON-NLS-1$

        "Model_1.catalog_0.group_0.element_0", //$NON-NLS-1$
        "Model_1.catalog_0.group_0.element_1", //$NON-NLS-1$
        "Model_1.catalog_0.group_0.element_2", //$NON-NLS-1$
        "Model_1.catalog_0.group_0.element_3", //$NON-NLS-1$
        "Model_1.catalog_0.group_0.element_4", //$NON-NLS-1$
        "Model_1.catalog_0.group_1.element_0", //$NON-NLS-1$
        "Model_1.catalog_0.group_1.element_1", //$NON-NLS-1$
        "Model_1.catalog_0.group_1.element_2", //$NON-NLS-1$
        "Model_1.catalog_0.group_1.element_3", //$NON-NLS-1$
        "Model_1.catalog_0.group_1.element_4", //$NON-NLS-1$

        "Model_2.catalog_0.group_0.element_0", //$NON-NLS-1$
        "Model_2.catalog_0.group_1.element_0", //$NON-NLS-1$
        "Model_2.catalog_0.group_2.element_0", //$NON-NLS-1$
        "Model_2.catalog_0.group_3.element_0", //$NON-NLS-1$
        "Model_2.catalog_0.group_4.element_0", //$NON-NLS-1$
        "Model_2.catalog_0.group_5.element_0", //$NON-NLS-1$
        "Model_2.catalog_0.group_6.element_0", //$NON-NLS-1$
        "Model_2.catalog_0.group_7.element_0", //$NON-NLS-1$
        "Model_2.catalog_0.group_8.element_0", //$NON-NLS-1$
        "Model_2.catalog_0.group_9.element_0" //$NON-NLS-1$
    };

    // Permissions in policy some of which will be modified
    private Set policyPerms = new HashSet();
    // The perms to be modified
    private Set permsToModify = new HashSet();
    // TreeView has all resourecs as nodes some of which we mark modified
    private PermissionDataNodeTreeView treeView;
    // The test policy we will modify
    private AuthorizationPolicy policyToModify;

    // A lookup so that we can tell which PermissionDataNodes
    // to set modified when building the test PermissionDataNodeTreeView.
    private Set modifiedResources = new HashSet();

    // =========================================================================
    //                        F R A M E W O R K
    // =========================================================================

    /**
     * Constructor for TestAuthorizationObjectEditor.
     *
     * @param name
     */
    public TestAuthorizationObjectEditor(final String name) {
        super(name);
        oneTimeSetUp();
    }

    public void oneTimeSetUp() {
        setupPolicyAndPermsToModify();
        // must be set up after setupPolicyAndPermsToModify()
        setupTreeView();
    }

    public static void oneTimeTearDown() {
    }

    // =========================================================================
    //                            H E L P E R S
    // =========================================================================

    /**
     * All created perms go into policy.
     * <br>Save every other created perm in <i>permsToModify</i>.</br>
     */
    private void setupPolicyAndPermsToModify() {
        for ( int i = 0; i < resources.length; i++ ) {
            AuthorizationPermission aPerm = bapFactory.create(new DataAccessResource(resources[i]), aRealm,
                                                              StandardAuthorizationActions.ALL, null);
            policyPerms.add(aPerm);
            if ( i % 2 == 0 ) {
                permsToModify.add(aPerm);
                // modifiedResources is a lookup so that we can tell which PermissionDataNodes
                // to set modified when building the test PermissionDataNodeTreeView.
                modifiedResources.add(resources[i]);
            }
        }
        // Ceate test policy and add all created perms
        policyToModify = new AuthorizationPolicy(new AuthorizationPolicyID("Test policy", "A test policy", aRealm), //$NON-NLS-1$ //$NON-NLS-2$
                                                 null,
                                                 policyPerms);
    }

    private void setupTreeView() {
        List dataNodes = getResourcesAsList();

        // Get ID factory
        IDGenerator idGenerator = IDGenerator.getInstance();
        idGenerator.setDefaultFactory(new UUIDFactory());

        // The root resource has no ID. Create a fake ID for it.
        ObjectID fakeRootID = idGenerator.create();
        PermissionDataNodeImpl root = new PermissionDataNodeImpl(null,
                                                                 StandardAuthorizationActions.NONE,
                                                                 new PermissionDataNodeDefinitionImpl("root", //$NON-NLS-1$
                                                                                                      "root", //$NON-NLS-1$
                                                                                                      PermissionDataNodeDefinition.TYPE.UNKOWN), 
                                                                 false,
                                                                 fakeRootID);
        // Build the tree
        buildPermissionTree(root, dataNodes, policyToModify, idGenerator);
        treeView = new PermissionDataNodeTreeViewImpl(root);
   }

    private List getResourcesAsList() {
        List resourceList = new ArrayList(resources.length);
        for ( int i = 0; i < resources.length; i++ ) {
            String resource = resources[i];
            resourceList.add(resource);
        }
        return resourceList;
    }

    /**
     * Build depth-first tree of all nodes.
     *
     * @param root        The root of the node tree
     * @param nodes       The list of all resources (path).
     * @param policy      The policy from which to get permissions for nodes in this tree.
     * @param idGenerator The ID factory from which to convert the uuid String of each node
     *                    to an ObjectID that the tree requires.
     */
    private Map buildPermissionTree(PermissionDataNodeImpl root,
                                                   List nodes,
                                                   AuthorizationPolicy policy,
                                                   IDGenerator idGenerator) {
        Map parentChildMap = new HashMap();
        String sepStr = "."; //$NON-NLS-1$

        PermissionDataNodeImpl parent = root;
        parentChildMap.put(parent, new ArrayList());

        // Keep track of built PermissionNodes
        Map resourceToChild = new HashMap();
        Iterator pathItr = nodes.iterator();
//        System.out.println("\nBuilding Data entitlement tree..."); //$NON-NLS-1$
//        System.out.println("Policy: " + policy);
        while ( pathItr.hasNext() ) {
            String branch = (String) pathItr.next();
//            System.out.println("\n  Resource branch: " + branch); //$NON-NLS-1$
            List nodeList = StringUtil.split(branch, sepStr);

            Iterator nodeItr = nodeList.iterator();
            // Iterating over nodes in path
            while ( nodeItr.hasNext() ) {
                String nodeName = (String) nodeItr.next();
//                String pathName = (parent.equals(root) ? sepStr + nodeName : parent.getResourceName() + sepStr + nodeName);
                String pathName = (parent.equals(root) ? nodeName : parent.getResourceName() + sepStr + nodeName);
//                System.out.println("\n    Node: " + pathName); //$NON-NLS-1$
//                System.out.println("      Parent: " + parent.getResourceName()); //$NON-NLS-1$
                PermissionDataNodeImpl child = null;
                // If we've built this node before, use the same node.
                if ( resourceToChild.containsKey(pathName) ) {
                    child = (PermissionDataNodeImpl) resourceToChild.get(pathName);
//                    System.out.println("      Found node in cache: " + child.getResourceName()); //$NON-NLS-1$
                } else {
                    // Create the Resource for this node.
                    DataAccessResource theResource = new DataAccessResource(pathName);
//                    System.out.println("      Resource: " + theResource); //$NON-NLS-1$

                    // Find permission, if any
                    AuthorizationPermission aPermission = policy.findPermissionWithResource(theResource);
                    // Get the Actions for this resource - Default to NONE
                    AuthorizationActions actions = StandardAuthorizationActions.NONE;
                    if ( aPermission != null ) {
                        // If a permission found, us its Actions
                        actions = aPermission.getActions();
//                        System.out.println("      Policy had perm: ACTIONS - " + actions); //$NON-NLS-1$
                    }
//                    else {
//                        System.out.println("      Resource: " + theResource + " NOT FOUND IN POLICY!"); //$NON-NLS-1$ //$NON-NLS-2$
//                    }
                    // Create node definition
                    PermissionDataNodeDefinition nodeDef = new PermissionDataNodeDefinitionImpl(pathName,
                                                                                                nodeName,
                                                                                                PermissionDataNodeDefinition.TYPE.ELEMENT);
                    // Create PermissionNode
                    child = new PermissionDataNodeImpl(parent, actions, nodeDef, true, idGenerator.create());
                    if ( inModifiedRecources(child) ) {
//                        System.out.println("      Setting Resource: " + theResource + " MODIFIED"); //$NON-NLS-1$ //$NON-NLS-2$
                        child.setModified(true);
                    }
                    resourceToChild.put(pathName, child);
//                    System.out.println("      Created new node: " + child.getDisplayName() + " (" + child.getResourceName() + ")"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                }
                List children = (List) parentChildMap.get(parent);
                // Add this child to parent's child list if not already there
                if ( children != null && !children.contains(child) ) {
                    children.add(child);
                }
                // Make this child the parent of the next iteration (next node in current branch)
                if ( !parentChildMap.containsKey(child) ) {
                    parentChildMap.put(child, new ArrayList());
                }
                parent = child;
            }
            // Finished with branch. Start back at root
            parent = root;
        }

        return parentChildMap;
    }

    private boolean inModifiedRecources(PermissionDataNodeImpl aNode) {
        if ( modifiedResources.contains(aNode.getResourceName()) ) {
            return true;
        }
        return false;
    }

    /**
     * Validates that two collections contain <i>exactly</i> the same members.
     * @param collection_1
     * @param collection_2
     * @return <code>true</code> iff both collections contain the same and <i>only</i>
     * the same members.
     */
    private boolean helpTestPermissionCollectionsEqual(Collection collection_1, Collection collection_2) {
        boolean succeded = true;
        // IF - Both collections should contain the same number of elements...
        if ( collection_1.size() != collection_2.size() ) {
            return false;
        }
        // AND - collection_2 contain all members of collection_1...
        for ( Iterator permsToModifyItr = collection_1.iterator(); permsToModifyItr.hasNext(); ) {
            AuthorizationPermission aPermToModify = (AuthorizationPermission) permsToModifyItr.next();
            if ( !collection_2.contains(aPermToModify) ) {
                return false;
            }
        }
        // AND - collection_1 contain all members of collection_2
        for ( Iterator modifiedPermItr = collection_2.iterator(); modifiedPermItr.hasNext(); ) {
            AuthorizationPermission aModifiedPerm = (AuthorizationPermission) modifiedPermItr.next();
            if ( !collection_1.contains(aModifiedPerm) ) {
                return false;
            }
        }
        // THEN - both collections are equivelent
        return succeded;
    }

    // =========================================================================
    //                         T E S T   C A S E S
    // =========================================================================


    public void testModifyPermissions() {
        AuthorizationObjectEditor aoe = new AuthorizationObjectEditor();
        Collection modifiedPerms = aoe.modifyPermissions(treeView, policyToModify);
        assertTrue("Expected: " + permsToModify + " got: " + modifiedPerms, //$NON-NLS-1$ //$NON-NLS-2$
                   helpTestPermissionCollectionsEqual(permsToModify, modifiedPerms));
    }
}