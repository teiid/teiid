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
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.metamatrix.common.actions.AbstractObjectEditor;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.LogConstants;
import com.metamatrix.platform.admin.api.EntitlementMigrationReport;
import com.metamatrix.platform.admin.api.PermissionTreeView;
import com.metamatrix.platform.security.util.RolePermissionFactory;

public abstract class AbstractAuthorizationObjectEditor extends AbstractObjectEditor {
    public final static long serialVersionUID = -3690038844414207808L;


    /**
     * Create an instance of this editor, and specify whether actions are to be created
     * during modifications.  If actions are created, then each action is sent directly
     * to the destination at the time the action is created.
     * @param createActions flag specifying whether modification actions should be created
     * for each invocation to <code>modifyObject</code>
     */
    public AbstractAuthorizationObjectEditor( boolean createActions ) {
        super(createActions);

        if ( doCreateActions() ) {
            this.setDestination(new com.metamatrix.common.actions.BasicModificationActionQueue());
        }
    }

    /**
     * Default ctor creates actions.
     */
    public AbstractAuthorizationObjectEditor() {
        this(true);
    }

    // ----------------------------------------------------------------------------------
    //                  C R E A T E    M E T H O D S
    // ----------------------------------------------------------------------------------

    /**
     * Create a new AuthorizationPolicy.
     * @param policyID the new policyID from which to create the policy (may not be null).
     * @return The newly created policy.
     * @throws IllegalArgumentException if the target <code>AuthorizationPolicyID</code> is null.
     */
    public AuthorizationPolicy createAuthorizationPolicy(AuthorizationPolicyID policyID) {
        if ( policyID == null ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0001));
        }
        AuthorizationPolicyID targetID = (AuthorizationPolicyID) verifyTargetClass(policyID, AuthorizationPolicyID.class);
        AuthorizationPolicy policy = new AuthorizationPolicy(targetID);

        createCreationAction(targetID, policy);

        return policy;
    }

    // ----------------------------------------------------------------------------------
    //                      D E L E T E    M E T H O D S
    // ----------------------------------------------------------------------------------

    /**
     * Remove an <code>AuthorizationPolicy</code>.
     * @param policyID The target policy ID (may not be null).
     * @throws IllegalArgumentException if the target <code>AuthorizationPolicyIC</code> is null.
     */
    public void remove( AuthorizationPolicyID policyID ) {
        if ( policyID == null ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0001));
        }
        AuthorizationPolicyID targetID = (AuthorizationPolicyID) verifyTargetClass(policyID, AuthorizationPolicyID.class);

        createDestroyAction(targetID, targetID);

    }

    // ----------------------------------------------------------------------------------
    //                        C L O N E    M E T H O D S
    // ----------------------------------------------------------------------------------

    /**
     * Copy the <code>AuthorizationPermission</code>s from a source <code>AuthorizationPolicy</code>
     * to a target <code>AuthorizationPolicy</code> given an <code>AuthorizationRealm</code>.
     * @param targetPolicy The source policy for cloning.
     * @param sourcePerm The source permisison for cloning.
     * @param targetRealm The destination realm in which to place the permissions (may not be null).
     * @return The cloned policy.
     * @throws IllegalArgumentException if either the target <code>AuthorizationPolicy</code>
     * the source <code>AuthorizationPolicy</code> or the <code>AuthorizationRealm</code> is null.
     */
    public AuthorizationPolicy clonePermission(AuthorizationPolicy targetPolicy,
                                                      AuthorizationPermission sourcePerm,
                                                      AuthorizationRealm targetRealm) {
        if ( targetPolicy == null ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0002));
        }
        if ( sourcePerm == null ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0003));
        }
        if ( targetRealm == null ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0004));
        }

        AuthorizationPolicy theTargetPolicy = (AuthorizationPolicy) verifyTargetClass(targetPolicy, AuthorizationPolicy.class);
        AuthorizationRealm newRealm = (AuthorizationRealm) verifyTargetClass(targetRealm, AuthorizationRealm.class);

        // Clone the permission
        AuthorizationPermission newPerm = null;

        try {
            newPerm = (AuthorizationPermission) sourcePerm.clone();
        } catch(CloneNotSupportedException e) {
            // They're all clonable but log anyway
            final Object[] params = { sourcePerm };
            final String msg = SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0005,params);
            LogManager.logError(LogConstants.CTX_AUTHORIZATION, e, msg );
        }
        newPerm.setRealm(newRealm);

        theTargetPolicy = this.addPermission(theTargetPolicy, newPerm);

        return theTargetPolicy;
    }


    /**
     * Copy the <code>Principals</code>s from a source <code>AuthorizationPolicy</code>
     * to a target <code>AuthorizationPolicy</code>.
     * @param sourcePolicy The source policy for cloning.
     * @param targetPolicy The source policy for cloning.
     * @return The cloned policy.
     * @throws IllegalArgumentException if either the target <code>AuthorizationPolicy</code>
     * the source <code>AuthorizationPolicy</code> or the <code>AuthorizationRealm</code> is null.
     */
    public AuthorizationPolicy clonePolicyPrincipals(AuthorizationPolicy sourcePolicy,
                                                     AuthorizationPolicy targetPolicy) {
        return this.clonePolicyPrincipals(sourcePolicy, targetPolicy, null, null);
    }
    
    public AuthorizationPolicy clonePolicyPrincipals(AuthorizationPolicy sourcePolicy,
                                                     AuthorizationPolicy targetPolicy, Set allPrincipals, EntitlementMigrationReport rpt) {
        if ( targetPolicy == null ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0002));
        }
        if ( sourcePolicy == null ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0006));
        }

        AuthorizationPolicy theSourcePolicy = (AuthorizationPolicy) verifyTargetClass(sourcePolicy, AuthorizationPolicy.class);
        AuthorizationPolicy theTargetPolicy = (AuthorizationPolicy) verifyTargetClass(targetPolicy, AuthorizationPolicy.class);

        Set oldPrincipals = theTargetPolicy.getPrincipals();
        if ( oldPrincipals.size() > 0 ) {
            theTargetPolicy = removeAllPrincipals(theTargetPolicy);
        }

        // Clone the Principals
        Set principals = new HashSet(theSourcePolicy.getPrincipals());
        
        //filter principals
        //TODO: add reporting for principals
        if (allPrincipals != null) {
            for (Iterator i = principals.iterator(); i.hasNext();) {
                MetaMatrixPrincipalName principal = (MetaMatrixPrincipalName)i.next();
                if (!allPrincipals.contains(principal.getName())) {
                    i.remove();
                    LogManager.logWarning(LogConstants.CTX_AUTHORIZATION, SecurityPlugin.Util.getString("AbstractAuthorizationObjectEditor.missing_principal", new Object[] {sourcePolicy.getAuthorizationPolicyID().getDisplayName(), principal.getName()})); //$NON-NLS-1$
                }
            }
        }
        
        theTargetPolicy = this.addAllPrincipals(theTargetPolicy, principals);

        return theTargetPolicy;
    }

    /**
     * Set the description on the policy.
     * @param policy The target policy (may not be null).
     * @param description The new policy description may be null or empty.
     * @return The modified policy.
     * @throws IllegalArgumentException if the target <code>AuthorizationPolicy</code> is null.
     */
    public AuthorizationPolicy setDescription( AuthorizationPolicy policy, String description ) {
        if ( policy == null ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0002));
        }

        AuthorizationPolicy thePolicy = (AuthorizationPolicy) verifyTargetClass(policy, AuthorizationPolicy.class);
        AuthorizationPolicyID policyID = thePolicy.getAuthorizationPolicyID();
        // Keep old value for undo
        String oldDescription = policyID.getDescription();

        createExchangeAction(policyID,
                             AuthorizationModel.Attribute.DESCRIPTION,
                             oldDescription,
                             description );
        policyID.setDescription(description);

        return thePolicy;
    }

    /**
     * Set the description on the policy.
     * @param policyID The target policy (may not be null).
     * @param description The new policy description may be null or empty.
     * @return The modified policyID.
     * @throws IllegalArgumentException if the target <code>AuthorizationPolicy</code> is null.
     */
    public AuthorizationPolicyID setDescription( AuthorizationPolicyID policyID, String description ) {
        if ( policyID == null ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0001));
        }

        AuthorizationPolicyID thePolicyID = (AuthorizationPolicyID) verifyTargetClass(policyID, AuthorizationPolicyID.class);
        // Keep old value for undo
        String oldDescription = thePolicyID.getDescription();

        createExchangeAction(thePolicyID,
                             AuthorizationModel.Attribute.DESCRIPTION,
                             oldDescription,
                             description );
        thePolicyID.setDescription(description);

        return thePolicyID;
    }

    /**
     * Add a principal to the policy.
     * @param policy The target policy (may not be null).
     * @param principal The principal to add to the policy (no action taken if null).
     * @return The modified policy.
     * @throws IllegalArgumentException if the target <code>AuthorizationPolicy</code> is null.
     */
    public AuthorizationPolicy addPrincipal( AuthorizationPolicy policy, MetaMatrixPrincipalName principal ) {
        if ( policy == null ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0002));
        }
        if ( principal == null ) {
            return policy;
        }

        AuthorizationPolicy thePolicy = (AuthorizationPolicy) verifyTargetClass(policy, AuthorizationPolicy.class);

        createAddAction(thePolicy.getAuthorizationPolicyID(), AuthorizationModel.Attribute.PRINCIPAL_NAME, principal );

        thePolicy.addPrincipal(principal);

        return thePolicy;
    }

    /**
     * Add a set of principals to the policy.
     * @param policy The target policy (may not be null).
     * @param principals The set of principals to add to the policy (no action taken if null or empty).
     * @return The modified policy.
     * @throws IllegalArgumentException if the target <code>AuthorizationPolicy</code> is null.
     */
    public AuthorizationPolicy addAllPrincipals( AuthorizationPolicy policy, Set principals ) {
        if ( policy == null ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0002));
        }
        if ( principals == null || principals.size() == 0 ) {
            return policy;
        }

        AuthorizationPolicy thePolicy = (AuthorizationPolicy) verifyTargetClass(policy, AuthorizationPolicy.class);

        createAddAction(thePolicy.getAuthorizationPolicyID(),
                        AuthorizationModel.Attribute.PRINCIPAL_SET,
                        principals);

        thePolicy.addAllPrincipals(principals);

        return thePolicy;
    }

    /**
     * Remove a principal from the policy.
     * @param policy The target policy (may not be null).
     * @param principal The principal to remove from the policy (no action taken if null).
     * @return The modified policy.
     * @throws IllegalArgumentException if the target <code>AuthorizationPolicy</code> is null.
     */
    public AuthorizationPolicy removePrincipal( AuthorizationPolicy policy, MetaMatrixPrincipalName principal ) {
        if ( policy == null ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0002));
        }
        if ( principal == null ) {
            return policy;
        }

        AuthorizationPolicy thePolicy = (AuthorizationPolicy) verifyTargetClass(policy, AuthorizationPolicy.class);

        Set principals = thePolicy.getPrincipals();
        if ( ! principals.contains(principal) ) {
            return policy;
        }

        createRemoveAction(thePolicy.getAuthorizationPolicyID(),
                             AuthorizationModel.Attribute.PRINCIPAL_NAME,
                             principal );

        thePolicy.removePrincipal(principal);

        return thePolicy;
    }

    /**
     * Remove a set of principals from the policy.
     * @param policy The target policy (may not be null).
     * @param principals The set of principals to remove from the policy (no action taken if null or empty).
     * @return The modified policy.
     * @throws IllegalArgumentException if the target <code>AuthorizationPolicy</code> is null.
     */
    public AuthorizationPolicy removePrincipals( AuthorizationPolicy policy, Set principals ) {
        if ( policy == null ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0002));
        }
        if ( principals == null || principals.size() == 0 ) {
            return policy;
        }

        AuthorizationPolicy thePolicy = (AuthorizationPolicy) verifyTargetClass(policy, AuthorizationPolicy.class);

        Set oldPrincipals = thePolicy.getPrincipals();
        oldPrincipals.removeAll(principals);

        createRemoveAction(thePolicy.getAuthorizationPolicyID(),
                           AuthorizationModel.Attribute.PRINCIPAL_SET,
                           principals);

        // Set the policy's prinicpals to new ref.
        thePolicy.setPrincipals(oldPrincipals);

        return thePolicy;
    }

    /**
     * Remove all of the principals from the policy.
     * @param policy The target policy (may not be null).
     * @return The modified policy.
     * @throws IllegalArgumentException if the target <code>AuthorizationPolicy</code> is null.
     */
    public AuthorizationPolicy removeAllPrincipals( AuthorizationPolicy policy ) {
        if ( policy == null ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0002));
        }

        AuthorizationPolicy thePolicy = (AuthorizationPolicy) verifyTargetClass(policy, AuthorizationPolicy.class);
        // Keep old value for undo
        Set oldPrincipals = thePolicy.getPrincipals();

        if ( oldPrincipals.size() == 0 ) {
            return policy;
        }
        createRemoveAction(thePolicy.getAuthorizationPolicyID(),
                             AuthorizationModel.Attribute.PRINCIPAL_SET,
                             oldPrincipals);

        // Set the policy's prinicpals to empty set.
        thePolicy.setPrincipals(Collections.EMPTY_SET);

        return thePolicy;
    }

    /**
     * Modifies permissions belonging to the given <code>AuthorizationPolicy</code>. All are found in
     * the given tree view of <code>PermissionDataNode</code>s.<br></br>
     * <strong>Note that if <code>showHidden</code> is set to <code>false</code> in <code>treeView</code>,
     * permissions WILL NOT be modified in those descendants.</strong>
     * @param treeView The permission tree view containg the modified nodes.
     * @param policy The <code>AuthorizationPolicy</code>, possibly new, on which to apply the new permissions.
     * @return The set of <code>PermissionDataNode</code>s that were modified.
     */
    public abstract Collection modifyPermissions(PermissionTreeView treeView, AuthorizationPolicy policy);

    /**
     * Add an <code>AuthorizationPermission</code> to the policy.
     * @param policy The target policy (may not be null).
     * @param permission The permission to add to the policy (no action taken if null).
     * @return The modified policy.
     * @throws IllegalArgumentException if the target <code>AuthorizationPolicy</code> is null.
     */
    public AuthorizationPolicy addPermission( AuthorizationPolicy policy, AuthorizationPermission permission ) {
        if ( policy == null ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0002));
        }
        if ( permission == null ) {
            return policy;
        }

        AuthorizationPolicy thePolicy = (AuthorizationPolicy) verifyTargetClass(policy, AuthorizationPolicy.class);

        // Don't allow creation of roles
        AuthorizationRealm realm = permission.getRealm();
        if ( realm.getRealmName().equals(RolePermissionFactory.getRealmName()) ) {
            return thePolicy;
        }

        // If AuthorizationActions == NONE, just remove existing perm on this resource
        String resourceToRemove = null;
        if ( permission.getActions().equals(StandardAuthorizationActions.NONE) ) {
            resourceToRemove = permission.getResourceName();
        }

        Iterator itr = thePolicy.iterator();
        while ( itr.hasNext() ) {
            AuthorizationPermission tmpPerm = (AuthorizationPermission) itr.next();

            // See if there's a perm to remove...
            if ( resourceToRemove != null && tmpPerm.getResourceName().equals(resourceToRemove) ) {
                thePolicy = removePermission(thePolicy, tmpPerm);
                break;
            }

            // Don't do anything if permission is already present.
            if ( permission.equals(tmpPerm) ) {
                return thePolicy;
            }
        }

        createAddAction(thePolicy.getAuthorizationPolicyID(), AuthorizationModel.Attribute.PERMISSION, permission );

        thePolicy.addPermission(permission);

        return thePolicy;
    }

    /**
     * Add a set of <code>AuthorizationPermission</code>s to the policy.
     * @param policy The target policy (may not be null).
     * @param permissions The set of permissions to add to the policy (no action taken if null or empty).
     * @return The modified policy.
     * @throws IllegalArgumentException if the target <code>AuthorizationPolicy</code> is null.
     */
    protected AuthorizationPolicy addAllPermissions( AuthorizationPolicy policy, Set permissions ) {
        if ( policy == null ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0002));
        }
        if ( permissions == null || permissions.size() == 0 ) {
            return policy;
        }

        AuthorizationPolicy thePolicy = (AuthorizationPolicy) verifyTargetClass(policy, AuthorizationPolicy.class);

        Set permsToRemove = new HashSet();

        Iterator permItr = permissions.iterator();
        while ( permItr.hasNext() ) {
            AuthorizationPermission perm = (AuthorizationPermission) permItr.next();
            // If any new perm has AuthorizationActions == NONE, just remove existing perm from policy
            if ( perm.getActions().equals(StandardAuthorizationActions.NONE) ) {
                permsToRemove.add(perm);
            }

            // Don't allow creation of roles
             AuthorizationRealm realm = perm.getRealm();
            if ( realm.getRealmName().equals(RolePermissionFactory.getRealmName()) ) {
                return thePolicy;
            }
        }

        // Remove permissions whose Actions are being set to NONE
        if ( permsToRemove.size() > 0 ) {
            thePolicy = removePermissions(thePolicy, permsToRemove);
        }

        // Keep old value for undo
        AuthorizationPermissions oldPermissions = thePolicy.getAuthorizationPermissions();
        AuthorizationPermissions newPermissions = new AuthorizationPermissionsImpl();
        newPermissions.add(permissions);
        if ( oldPermissions != null && oldPermissions.size() > 0 ) {
            // Don't add any that are already in there
            newPermissions.removeAll(oldPermissions);
        }
// DEBUG:
/*if ( newPermissions.size() > 0 ) {
    System.out.println(" *** addAllPermissions: adding " + newPermissions.size() + " permissions" );
    Iterator itr = newPermissions.iterator();
    while ( itr.hasNext() ) {
        System.out.println("    " + itr.next());
    }
} else {
    System.out.println(" *** addAllPermissions: adding NO permissions");
}*/

        // NOTE: The Set of permissions has been converted to an AuthorizationPermissionsImpl obj.
        // The arg[0] of the action must be cast to that type.
        createAddAction(thePolicy.getAuthorizationPolicyID(), AuthorizationModel.Attribute.PERMISSIONS, newPermissions );

        thePolicy.addAllPermissions(newPermissions);

        return thePolicy;
    }

    /**
     * Remove an <code>AuthorizationPermission</code> from the policy.
     * @param policy The target policy (may not be null).
     * @param permission The permission to remove from the policy (no action taken if null).
     * @return The modified policy.
     * @throws IllegalArgumentException if the target <code>AuthorizationPolicy</code> is null.
     */
    public AuthorizationPolicy removePermission( AuthorizationPolicy policy, AuthorizationPermission permission ) {
        if ( policy == null ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0002));
        }
        if ( permission == null ) {
            return policy;
        }

        AuthorizationPolicy thePolicy = (AuthorizationPolicy) verifyTargetClass(policy, AuthorizationPolicy.class);
        // Keep old value for undo
        AuthorizationPermission oldPermission = null;
        AuthorizationPermissions oldPermissions = thePolicy.getAuthorizationPermissions();
        Iterator permissionItr = oldPermissions.iterator();
        while ( permissionItr.hasNext() ) {
            AuthorizationPermission tmpPermission = (AuthorizationPermission) permissionItr.next();
            if ( permission.equals( tmpPermission ) ) {
                oldPermission = tmpPermission;
                break;
            }
        }

        // If permission was not present, do nothing.
        if ( oldPermission == null ) {
            return policy;
        }

        createRemoveAction(thePolicy.getAuthorizationPolicyID(),
                             AuthorizationModel.Attribute.PERMISSION,
                             oldPermission);

        thePolicy.removePermission(permission);

        return thePolicy;
    }

    /**
     * Remove a <code>Set</code> of <code>AuthorizationPermission</code>s from the policy.
     * @param policy The target policy (may not be null).
     * @param permissions The set of permissions to remove from the policy (no action taken if null or empty).
     * @return The modified policy.
     * @throws IllegalArgumentException if the target <code>AuthorizationPolicy</code> is null.
     */
    public AuthorizationPolicy removePermissions( AuthorizationPolicy policy, Set permissions ) {
        if ( policy == null ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0002));
        }
        if ( permissions == null || permissions.size() == 0 ) {
            return policy;
        }

        AuthorizationPolicy thePolicy = (AuthorizationPolicy) verifyTargetClass(policy, AuthorizationPolicy.class);
        // Some/All of these will be removed
        AuthorizationPermissions oldPermissions = thePolicy.getAuthorizationPermissions();

// DEBUG:
/*System.out.println(" *** removePermissions: Old perms " + oldPermissions.size());
Iterator itr = oldPermissions.iterator();
while ( itr.hasNext() ) {
    System.out.println("    Old perm: " + itr.next());
}*/
// DEBUG:
/*System.out.println(" *** removePermissions: Perms to remove");
itr = permissions.iterator();
while ( itr.hasNext() ) {
    System.out.println("    Removing perm: " + itr.next());
}*/
        oldPermissions.removeAll(permissions);
        // Now left with subset of old permissions
// DEBUG:
/*System.out.println(" *** removePermissions: Perms left:");
itr = oldPermissions.iterator();
while ( itr.hasNext() ) {
    System.out.println("    Leaving perm: " + itr.next());
}*/

// DEBUG:
//System.out.println(" *** removePermissions: New size: " + oldPermissions.size() + "\n");

        createRemoveAction(thePolicy.getAuthorizationPolicyID(),
                             AuthorizationModel.Attribute.PERMISSION_SET,
                             permissions );

        thePolicy.setPermissions(oldPermissions);

        return thePolicy;
    }

    /**
     * Remove <emph>ALL</emph> <code>AuthorizationPermission</code>s from the
     * given <code>AuthorizationPolicy</code> that have the given resource and are
     * in the given <code>AuthorizationRealm</code>.
     * @param policy The target policy (may not be null).
     * @param resource The resource for which permissions will be removed.
     * @param realm The realm that permissions with given resource must belong to
     * to be removed.
     * @return The modified policy with permissions removed.
     * @throws IllegalArgumentException if the target <code>AuthorizationPolicy</code> or
     * <code>AuthorizationRealm</code> is null or if the resource is null or empty.
     */
    public AuthorizationPolicy removePermissions( AuthorizationPolicy policy, String resource, AuthorizationRealm realm ) {
        if ( policy == null ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0002));
        }
        if ( resource == null || resource.trim().length() == 0 ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0007));
        }
        if ( realm == null ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0008));
        }

        AuthorizationPolicy thePolicy = (AuthorizationPolicy) verifyTargetClass(policy, AuthorizationPolicy.class);
        AuthorizationRealm theRealm = (AuthorizationRealm) verifyTargetClass(realm, AuthorizationRealm.class);

        // Find candidate permissions for removal
        Set removePerms = new HashSet();
        Iterator permItr = thePolicy.iterator();
        while ( permItr.hasNext() ) {
            AuthorizationPermission perm = (AuthorizationPermission) permItr.next();

            if ( perm.getResourceName().equals(resource) && perm.getRealm().equals(theRealm) ) {
                removePerms.add(perm);
            }
        }

        // Remove the permissions
        if ( removePerms.size() > 0 ) {
            thePolicy = this.removePermissions(thePolicy, removePerms);
        }

        return thePolicy;
    }

    /**
     * Remove all of the permissions from the policy.
     * @param policy The target policy (may not be null).
     * @return The modified policy.
     * @throws IllegalArgumentException if the target <code>AuthorizationPolicy</code> is null.
     */
    public AuthorizationPolicy removeAllPermissions( AuthorizationPolicy policy ) {
        if ( policy == null ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0002));
        }

        AuthorizationPolicy thePolicy = (AuthorizationPolicy) verifyTargetClass(policy, AuthorizationPolicy.class);
        // Keep old value for undo
        AuthorizationPermissions oldPermissions = new AuthorizationPermissionsImpl(thePolicy.getAuthorizationPermissions());
        if ( oldPermissions.size() == 0 ) {
            return thePolicy;
        }

        createRemoveAction(thePolicy.getAuthorizationPolicyID(),
                             AuthorizationModel.Attribute.PERMISSIONS,
                             oldPermissions);

        // Sets the policy's permissions to new empty ref.
        thePolicy.removePermissions();

        return thePolicy;
    }
}
