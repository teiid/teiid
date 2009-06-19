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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.LogConstants;
import com.metamatrix.platform.admin.api.AuthorizationEditor;
import com.metamatrix.platform.admin.api.EntitlementMigrationReport;
import com.metamatrix.platform.admin.api.PermissionDataNode;
import com.metamatrix.platform.admin.api.PermissionTreeView;
import com.metamatrix.platform.admin.apiimpl.PermissionDataNodeImpl;

/**
 * Editor allows automatic creation of individual {@link com.metamatrix.common.actions.Actions}
 * in an enforcable way when making modifications to Authorizations.<br></br>
 * The actions can be submited to the Authorization Service to execute them in a transaction.
 */
public class AuthorizationObjectEditor extends AbstractAuthorizationObjectEditor implements AuthorizationEditor {

    public final static long serialVersionUID = 5799168432068176908L;
    
    /**
     * Create an instance of this editor, and specify whether actions are to be created
     * during modifications.  If actions are created, then each action is sent directly
     * to the destination at the time the action is created.
     * @param createActions flag specifying whether modification actions should be created
     * for each invocation to <code>modifyObject</code>
     */
    public AuthorizationObjectEditor( boolean createActions ) {
        super(createActions);
    }

    /**
     * Default ctor creates actions.
     */
    public AuthorizationObjectEditor() {
        super(true);
    }

    // ----------------------------------------------------------------------------------
    //                        C L O N E    M E T H O D S
    // ----------------------------------------------------------------------------------

    /**
     * Copy the <code>AuthorizationPermission</code>s from a source <code>AuthorizationPolicy</code>
     * to a target <code>AuthorizationPolicy</code> given an <code>AuthorizationRealm</code>.
     * @param sourcePolicy The source policy for cloning.
     * @param targetPolicy The source policy for cloning.
     * @param targetRealm The destination realm in which to place the permissions (may not be null).
     * @param allPaths Skip clone of any permission whose resource is not in this set. This set is
     * comprised of all allowable resources in the target realm.
     * @return The cloned policy.
     * @throws IllegalArgumentException if either the target <code>AuthorizationPolicy</code>
     * the source <code>AuthorizationPolicy</code> or the <code>AuthorizationRealm</code> is null.
     */
    public AuthorizationPolicy clonePolicyPermissions(AuthorizationPolicy sourcePolicy,
                                                      AuthorizationPolicy targetPolicy,
                                                      AuthorizationRealm targetRealm,
                                                      Set allPaths, EntitlementMigrationReport rpt) {
        if ( targetPolicy == null ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0002));
        }
        if ( sourcePolicy == null ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0006));
        }
        if ( targetRealm == null ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0004));
        }

        // Clone the permissions
        Set permissions = new HashSet();
        Iterator permItr = sourcePolicy.iterator();
        while ( permItr.hasNext() ) {
            AuthorizationPermission originalPerm = (AuthorizationPermission) permItr.next();
            String resourcePath = originalPerm.getResourceName();

            if ( BasicAuthorizationPermission.isRecursiveResource(resourcePath) ) {
                resourcePath = BasicAuthorizationPermission.removeRecursion(resourcePath);
            }
            
            if ( allPaths.contains(resourcePath) ) {
                // Fill in entry values
                if (rpt != null) {
                    rpt.addResourceEntry(SecurityPlugin.Util.getString("AuthorizationServiceImpl.Succeeded_migration"), //$NON-NLS-1$
                                 resourcePath,
                                 sourcePolicy.getAuthorizationPolicyID().getDisplayName(),
                                 targetPolicy.getAuthorizationPolicyID().getDisplayName(),
                                 StandardAuthorizationActions.getActionsString(originalPerm.getActions().getValue()),
                                 SecurityPlugin.Util.getString("AuthorizationServiceImpl.Migrated")); //$NON-NLS-1$
                }

                try {
                    // Clone for saftey
                    AuthorizationPermission newPerm  = (AuthorizationPermission) originalPerm.clone();
                    newPerm.setRealm(targetRealm);
                    permissions.add(newPerm);
                } catch(CloneNotSupportedException e) {
                    // They're all clonable but log anyway
                    final Object[] params = { originalPerm };
                    final String msg = SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0005, params);
                    LogManager.logError(LogConstants.CTX_AUTHORIZATION, e, msg);
                }
            } else {
                if (rpt != null) {
                    rpt.addResourceEntry(SecurityPlugin.Util.getString("AuthorizationServiceImpl.Failed_migration"), //$NON-NLS-1$
                                 resourcePath,
                                 sourcePolicy.getAuthorizationPolicyID().getDisplayName(),
                                 "", //$NON-NLS-1$
                                 StandardAuthorizationActions.getActionsString(originalPerm.getActions().getValue()),
                                 SecurityPlugin.Util.getString("AuthorizationServiceImpl.The_resource_for_this_permission_does_not_exist_in_the_target_VDB.")); //$NON-NLS-1$
                }

                final Object[] params = { originalPerm };
                final String msg = SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0011, params);
                LogManager.logWarning(LogConstants.CTX_AUTHORIZATION, msg);
            }
            
        }
        return this.addAllPermissions(targetPolicy, permissions);
    }

    // ----------------------------------------------------------------------------------
    //                  M O D I F I C A T I O N    M E T H O D S
    // ----------------------------------------------------------------------------------

    /**
     * Modifies permissions belonging to the given <code>AuthorizationPolicy</code>. All are found in
     * the given tree view of <code>PermissionDataNode</code>s.<br></br>
     * <strong>Note that if <code>showHidden</code> is set to <code>false</code> in <code>treeView</code>,
     * permissions WILL NOT be modified in those descendants.</strong>
     * @param treeView The permission tree view containg the modified nodes.
     * @param policy The <code>AuthorizationPolicy</code>, possibly new, on which to apply the new permissions.
     * @return The set of <code>PermissionDataNode</code>s that were modified.
     */
    public Collection modifyPermissions(PermissionTreeView treeView, AuthorizationPolicy policy) {
        // Make sure it's a valid policy
        if ( policy == null ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0012));
        }
        AuthorizationPolicyID thePolicyID = policy.getAuthorizationPolicyID();
        AuthorizationRealm realm = new AuthorizationRealm(thePolicyID.getVDBName(), thePolicyID.getVDBVersionString());

        Set permissionsToAdd = new HashSet();
        Set permissionsToRemove = new HashSet();
        List modifiedNodes = treeView.getModified();
        Set effectedNodes = new HashSet();
        AuthorizationPermissions currentPerms = policy.getAuthorizationPermissions();

        // Create permissions for all modified nodes in tree
        BasicAuthorizationPermissionFactory permFactory = new BasicAuthorizationPermissionFactory();

        LogManager.logTrace(LogConstants.CTX_AUTHORIZATION,"modifyPermissions: Starting..."); //$NON-NLS-1$
        Iterator nodeItr = modifiedNodes.iterator();
        while ( nodeItr.hasNext() ) {
            PermissionDataNodeImpl aNode = (PermissionDataNodeImpl) nodeItr.next();
            LogManager.logTrace(LogConstants.CTX_AUTHORIZATION,"modifyPermissions: Effected node: " + aNode); //$NON-NLS-1$
            AuthorizationActions theActions = aNode.getActions();
            String resourceName = aNode.getResourceName();
            AuthorizationPermission newPerm = null;
            AuthorizationPermission oldPerm = null;

            boolean isGroupNode = aNode.isGroupNode();

            if ( aNode.isLeafNode() || isGroupNode ) {
                LogManager.logTrace(LogConstants.CTX_AUTHORIZATION,"modifyPermissions: LEAF or GROUP - actions <" + theActions + ">"); //$NON-NLS-1$ //$NON-NLS-2$
                // Remove old permission on this node, if it exists
                oldPerm = getExistingPermission(aNode, policy);
                if ( oldPerm != null ) {
                    LogManager.logTrace(LogConstants.CTX_AUTHORIZATION,
                                        "modifyPermissions: Removing permission: " + oldPerm); //$NON-NLS-1$
                    currentPerms.remove(oldPerm);
                    permissionsToRemove.add(oldPerm);
                    effectedNodes.add(oldPerm);
                }
                if ( ! theActions.equals(StandardAuthorizationActions.NONE) ) {

                    if ( aNode.isLeafNode() ) {
                        LogManager.logTrace(LogConstants.CTX_AUTHORIZATION,"modifyPermissions: Creating LEAF permission."); //$NON-NLS-1$

                        // Create leaf permission
                        newPerm = permFactory.create(resourceName, realm, theActions);
                        LogManager.logTrace(LogConstants.CTX_AUTHORIZATION,"modifyPermissions: Adding new ELEMENT perm: <" + newPerm.getResourceName() + " - " + newPerm.getActions() + ">"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

                        permissionsToAdd.add(newPerm);

                    } else if (isGroupNode) {
                       	LogManager.logTrace(LogConstants.CTX_AUTHORIZATION,"modifyPermissions: Creating GROUP " + theActions + " permission."); //$NON-NLS-1$ //$NON-NLS-2$

                       	// Create new permissions
                       	newPerm = permFactory.create(resourceName, realm, theActions);
                       	LogManager.logTrace(LogConstants.CTX_AUTHORIZATION,"modifyPermissions: Adding new GROUP perm: <" + newPerm.getResourceName() + " - " + newPerm.getActions() + ">"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

                       	permissionsToAdd.add(newPerm);
                    }
                }

                // Add this node as effected
//                effectedNodes.add(aNode);
            }

            // Unmodify this node
            aNode.setModified(false, false);
        } // end modified nodes itr

        // Remove all old modified permissions
        this.removePermissions(policy, permissionsToRemove);

        // Add new/modified permissions
        this.addAllPermissions(policy, permissionsToAdd);

        return effectedNodes;
    }

    // ----------------------------------------------------------------------------------
    //                  H E L P E R    M E T H O D S
    // ----------------------------------------------------------------------------------

    private AuthorizationPermission getExistingPermission(PermissionDataNode aNode, AuthorizationPolicy policy) {
        AuthorizationPermission permToRemove = null;
        String resourceName = aNode.getResourceName();
        AuthorizationResource resource = new DataAccessResource(resourceName);
        permToRemove = policy.findPermissionWithResource(resource);
        return permToRemove;
    }

}



