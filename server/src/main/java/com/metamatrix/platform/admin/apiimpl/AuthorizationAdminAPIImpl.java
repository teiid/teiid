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

package com.metamatrix.platform.admin.apiimpl;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.admin.api.server.AdminRoles;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.api.exception.security.AuthorizationMgmtException;
import com.metamatrix.api.exception.security.MembershipServiceException;
import com.metamatrix.common.actions.ModificationActionQueue;
import com.metamatrix.platform.admin.api.AuthorizationAdminAPI;
import com.metamatrix.platform.admin.api.AuthorizationEditor;
import com.metamatrix.platform.security.api.AuthorizationObjectEditor;
import com.metamatrix.platform.security.api.AuthorizationPolicy;
import com.metamatrix.platform.security.api.AuthorizationPolicyID;
import com.metamatrix.platform.security.api.AuthorizationRealm;
import com.metamatrix.platform.security.api.MetaMatrixPrincipalName;
import com.metamatrix.platform.security.api.SessionToken;
import com.metamatrix.platform.security.api.service.AuthorizationServiceInterface;
import com.metamatrix.platform.security.api.service.MembershipServiceInterface;
import com.metamatrix.platform.security.util.RolePermissionFactory;
import com.metamatrix.platform.service.api.exception.ServiceException;
import com.metamatrix.platform.util.PlatformProxyHelper;

public class AuthorizationAdminAPIImpl extends SubSystemAdminAPIImpl implements AuthorizationAdminAPI {

    AuthorizationRealm roleRealm = RolePermissionFactory.getRealm();

    // Auth svc proxy
    private AuthorizationServiceInterface authAdmin;
    
    
    private static AuthorizationAdminAPI authAdminAPI;
    
    /**
     * ctor
     */
    private AuthorizationAdminAPIImpl() {
        authAdmin = PlatformProxyHelper.getAuthorizationServiceProxy(PlatformProxyHelper.ROUND_ROBIN_LOCAL);
    }

    public static synchronized AuthorizationAdminAPI getInstance() {
        if (authAdminAPI == null) {
            authAdminAPI = new AuthorizationAdminAPIImpl();
        }
        return authAdminAPI;
    }
    
    /**
     * Returns a <code>AuthorizationObjectEditor</code> to perform editing operations
     * on a entitlement type object.  The editing process will create actions for
     * each specific type of editing operation.  Those actions are what need to be
     * submitted to the <code>AuthorizationService</code> for actual updates to occur.
     * @return AuthorizationObjectEditor
     */
    public synchronized AuthorizationEditor createEditor()
            throws InvalidSessionException, AuthorizationException, MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(token, AdminRoles.RoleName.ADMIN_SYSTEM, "AuthorizationAdminAPIImpl.createEditor()"); //$NON-NLS-1$
        return new AuthorizationObjectEditor(true);
    }

    /**
     * Obtain the names of all of the realms known to the system.
     * @return the set of realm names
     * @throws InvalidSessionException if the <code>SessionToken</code> is not valid or is expired
     * @throws AuthorizationException if the caller is unable to perform this operation
     * @throws MetaMatrixComponentException if this service has trouble communicating.
     */
    public synchronized Collection getRealmNames()
            throws InvalidSessionException, AuthorizationException, MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role
        return authAdmin.getRealmNames(token);
    }

    public synchronized Map getRoleDescriptions()
            throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role
        return authAdmin.getRoleDescriptions(token);
    }

    public synchronized Collection getPrincipalsForRole(String roleName)
            throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role
        return authAdmin.getPrincipalsForRole(token, roleName);
    }

    /**
     * Returns a Collection of String names of MetaMatrix roles to which the
     * given principal is assigned.
     * @param principal <code>MetaMatrixPrincipalName</code> for which roles are sought
     * @return The <code>Collection</code> of role names the principal is assigned.
     * @throws InvalidSessionException if the administrative session is invalid
     * @throws AuthorizationException if administrator does not have the authority to perform the requested operation.
     * @throws MetaMatrixComponentException if this service has trouble communicating.
     */
    public synchronized Collection getRoleNamesForPrincipal(MetaMatrixPrincipalName principal)
            throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role
        return authAdmin.getRoleNamesForPrincipal(token, principal);
    }

    /**
     * Add the given set of principals to the given role.
     * @param principals Set of <code>MetaMatrixPrincipalName</code>s to which to add.
     * @param roleName The name of the role to which to add the principals.
     * @throws InvalidSessionException if the administrative session is invalid
     * @throws AuthorizationException if admninistrator does not have the authority to perform the requested operation.
     * @throws MetaMatrixComponentException if this service has trouble communicating.
     */
    public synchronized void addPrincipalsToRole(Set principals, String roleName)
            throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(token, AdminRoles.RoleName.ADMIN_SYSTEM, "AuthorizationAdminAPIImpl.addPrincipalsToRole(" + principals + ", " + roleName + ")"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

        AuthorizationPolicy role = authAdmin.getPolicy(token, new AuthorizationPolicyID(roleName, null, RolePermissionFactory.getRealm()));

        AuthorizationObjectEditor editor = new AuthorizationObjectEditor();
        role = editor.addAllPrincipals(role, principals);
        ModificationActionQueue queue = editor.getDestination();
        authAdmin.executeTransaction(token, queue.popActions());
    }

    /**
     * Add the given principal to the given roles.
     * @param principal The <code>MetaMatrixPrincipalName</code> to add
     * @param roleNames The <code>Collection</code> of <code>String</code> role names of which to add the principal.
     * @throws InvalidSessionException if the administrative session is invalid
     * @throws AuthorizationException if admninistrator does not have the authority to perform the requested operation.
     * @throws MetaMatrixComponentException if this service has trouble communicating.
     */
    public synchronized void addPrincipalToRoles(MetaMatrixPrincipalName principal, Collection roleNames)
            throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(token, AdminRoles.RoleName.ADMIN_SYSTEM, "AuthorizationAdminAPIImpl.addPrincipalToRoles(" + principal + ", " + roleNames + ")"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

        AuthorizationObjectEditor editor = new AuthorizationObjectEditor();
        Iterator roleItr = roleNames.iterator();
        while (roleItr.hasNext()) {
            String roleName = (String) roleItr.next();
            AuthorizationPolicy role = authAdmin.getPolicy(token,new AuthorizationPolicyID(roleName, null, RolePermissionFactory.getRealm()));
            role = editor.addPrincipal(role, principal);
        }
        ModificationActionQueue queue = editor.getDestination();
        authAdmin.executeTransaction(token, queue.popActions());
    }

    /**
     * Remove the given set of principals from the given role.
     * @param principals Set of <code>MetaMatrixPrincipalName</code>s to remove.
     * @param roleName The name of the role from which to remove the principals.
     * @throws InvalidSessionException if the administrative session is invalid
     * @throws AuthorizationException if admninistrator does not have the authority to perform the requested operation.
     * @throws MetaMatrixComponentException if this service has trouble communicating.
     */
    public synchronized void removePrincipalsFromRole(Set principals, String roleName)
            throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(token, AdminRoles.RoleName.ADMIN_SYSTEM, "AuthorizationAdminAPIImpl.removePrincipalsFromRole(" + principals + ", " + roleName + ")"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

        AuthorizationPolicy role = authAdmin.getPolicy(token, new AuthorizationPolicyID(roleName, null, RolePermissionFactory.getRealm()));

        AuthorizationObjectEditor editor = new AuthorizationObjectEditor();
        role = editor.removePrincipals(role, principals);
        ModificationActionQueue queue = editor.getDestination();

        authAdmin.executeTransaction(token, queue.popActions());
    }

    /**
     * Remove the policy with the specified ID.
     * @param policyID the ID of the policy that is to be removed.
     * @throws InvalidSessionException if the <code>sessionToken</code> is not valid or is expired
     * @throws AuthorizationException if the caller is unable to perform this operation
     * @throws MetaMatrixComponentException if this service is unable to locate resources required
     * for this operation
     */
    public synchronized void removePolicy(AuthorizationPolicyID policyID)
            throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(token, AdminRoles.RoleName.ADMIN_SYSTEM, "AuthorizationAdminAPIImpl.removePolicy(" + policyID +")"); //$NON-NLS-1$ //$NON-NLS-2$

        AuthorizationObjectEditor aoe = new AuthorizationObjectEditor(true);
        aoe.remove(policyID);
        ModificationActionQueue queue = aoe.getDestination();

        authAdmin.executeTransaction(token, queue.popActions());
    }

    /**
     * Get all policyIDs in the system except those that we want to filter from the console.
     */
    public synchronized Collection findAllPolicyIDs()
            throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role

        Collection allPolicyIDs = authAdmin.findAllPolicyIDs(token);

        Collection filteredPolicyIDs = new HashSet();
        // Filter the policies for all VDBs we don't want to see in the Console
        // Also, filter roles and MetaBase policyIDs
        Iterator policyIDItr = allPolicyIDs.iterator();
        while (policyIDItr.hasNext()) {
            AuthorizationPolicyID aPolicyID = (AuthorizationPolicyID) policyIDItr.next();
            AuthorizationRealm theRealm = aPolicyID.getRealm();

            if ( !theRealm.equals(roleRealm)) {
                filteredPolicyIDs.add(aPolicyID);
            }
        }
        return filteredPolicyIDs;
    }

    public synchronized Collection findPolicyIDs(Collection principals)
            throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role
        return authAdmin.findPolicyIDs(token, principals);
    }

    public synchronized Collection getPolicies(Collection policyIDs)
            throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role
        return authAdmin.getPolicies(token, policyIDs);
    }

    public synchronized Boolean containsPolicy(AuthorizationPolicyID policyID)
            throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role
        return new Boolean(authAdmin.containsPolicy(token, policyID));
    }

    public synchronized AuthorizationPolicy getPolicy(AuthorizationPolicyID policyID)
            throws AuthorizationException, AuthorizationMgmtException, InvalidSessionException, MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role
        return authAdmin.getPolicy(token, policyID);
    }

    public synchronized Set executeTransaction(List actions)
            throws AuthorizationException, AuthorizationMgmtException, InvalidSessionException, MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(token, AdminRoles.RoleName.ADMIN_SYSTEM, "AuthorizationAdminAPIImpl.executeTransaction(" + actions + ")"); //$NON-NLS-1$ //$NON-NLS-2$
        return authAdmin.executeTransaction(token, actions);
    }

    public synchronized Boolean removePrincipalFromAllPolicies(MetaMatrixPrincipalName principal)
            throws AuthorizationException, AuthorizationMgmtException, InvalidSessionException, MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(token, AdminRoles.RoleName.ADMIN_SYSTEM, "AuthorizationAdminAPIImpl.removePrincipalFromAllPolicies(" + principal +")"); //$NON-NLS-1$ //$NON-NLS-2$
        return new Boolean(authAdmin.removePrincipalFromAllPolicies(token, principal));
    }

    /**
     * Returns a <code>Collection</code> of <code>AuthorizationPolicyID</code>s
     * that have <code>AuthorizationPermission</code>s in the given <code>AuthorizationRealm</code>.<br>
     * <strong>NOTE:</strong> It is the responsibility of the caller to determine
     * which of the <code>AuthorizationPolicy</code>'s <code>AuthorizationPermission</code>s
     * are actually in the given <code>AuthorizationRealm</code>.  The <code>AuthorizationPolicy</code>
     * may span <code>AuthorizationRealm</code>s.
     * @param realm The realm in which to search for <code>AuthorizationPermission</code>s.
     * @return The collection of <code>AuthorizationPolicyID</code>s that have permissions
     * in the given realm - possibly empty but never null.
     * @throws AuthorizationException if admninistrator does not have the authority to preform the action.
     * @throws AuthorizationMgmtException if an error occurs in the Authorization store.
     * @throws MetaMatrixComponentException if this service has trouble communicating.
     */
    public synchronized Collection getPolicyIDsWithPermissionsInRealm(AuthorizationRealm realm)
            throws AuthorizationException, AuthorizationMgmtException, InvalidSessionException, MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role
        return authAdmin.getPolicyIDsWithPermissionsInRealm(token, realm);
    }

    /**
     * Returns a <code>Collection</code> of <code>AuthorizationPolicyID</code>s
     * in the given <code>AuthorizationRealm</code>.
     * <br>This method will only work for Data Access Authorizations because the realm
     * is encoded in a Data Access policy name.
     * <strong>NOTE:</strong> It is the responsibility of the caller to determine
     * which of the <code>AuthorizationPolicy</code>'s <code>AuthorizationPermission</code>s
     * are actually in the given <code>AuthorizationRealm</code>.  The <code>AuthorizationPolicy</code>
     * may span <code>AuthorizationRealm</code>s.
     * @param realm The realm in which to search for <code>AuthorizationPermission</code>s.
     * @return The collection of <code>AuthorizationPolicyID</code>s that have permissions
     * in the given realm - possibly empty but never null.
     * @throws AuthorizationException if admninistrator does not have the authority to preform the action.
     * @throws AuthorizationMgmtException if an error occurs in the Authorization store.
     * @throws MetaMatrixComponentException if this service has trouble communicating.
     */
    public synchronized Collection getPolicyIDsInRealm(AuthorizationRealm realm)
            throws AuthorizationException, AuthorizationMgmtException, InvalidSessionException, MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role
        return authAdmin.getPolicyIDsInRealm(token, realm);
    }

    /**
     * Returns a <code>Collection</code> of <code>AuthorizationPolicyID</code>s
     * that have <code>AuthorizationPermissions</code> that exist in the given
     * <emph>partial</emph> <code>AuthorizationRealm</code>.<br>
     * The implementation is such that all <code>AuthorizationPolicyID</code>s
     * whose <code>AuthorizationRealm</code> <emph>starts with</emph> the given
     * <code>AuthorizationRealm</code> are returned.
     * @param realm The <emph>partial</emph> realm in which to search for
     * <code>AuthorizationPermission</code>s whose realm name <emph>starts with</emph>
     * the given realm.
     * @return The collection of <code>AuthorizationPolicyID</code>s that have permissions
     * in the given partial realm - possibly empty but never null.
     * @throws AuthorizationException if admninistrator does not have the authority to preform the action.
     * @throws AuthorizationMgmtException if an error occurs in the Authorization store.
     * @throws MetaMatrixComponentException if this service has trouble communicating.
     */
    public synchronized Collection getPolicyIDsInPartialRealm(AuthorizationRealm realm)
            throws AuthorizationException, AuthorizationMgmtException, InvalidSessionException, MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role
        return authAdmin.getPolicyIDsInPartialRealm(token, realm);
    }

    /**
     * Returns a <code>Collection</code> of <code>AuthorizationPolicyID</code>s
     * that have <code>AuthorizationPermissions</code> on the given resource that
     * exists in the given <code>AuthorizationRealm</code>.<br>
     * @param realm The realm in which to search for <code>AuthorizationPermission</code>s.
     * @param resourceName The resource for which to search for <code>AuthorizationPermission</code>s.
     * @return The collection of <code>AuthorizationPolicyID</code>s that have permissions
     * on the given resource - possibly empty but never null.
     * @throws AuthorizationException if admninistrator does not have the authority to preform the action.
     * @throws AuthorizationMgmtException if an error occurs in the Authorization store.
     * @throws MetaMatrixComponentException if this service has trouble communicating.
     */
    public synchronized Collection getPolicyIDsForResourceInRealm(AuthorizationRealm realm, String resourceName)
            throws AuthorizationException, AuthorizationMgmtException, InvalidSessionException, MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role
        return authAdmin.getPolicIDsForResourceInRealm(token, realm, resourceName);
    }

    /**
     * Verify that caller is in the specified logical role.
     * @param caller The session token of the MetaMatrix principle involking an administrative method.
     * @return true if caller's session token is valid and he is a MetaMatrix administrator.
     * @throws AuthorizationMgmtException if this service has trouble connecting to services it uses.
     */
    public synchronized boolean isCallerInRole(SessionToken caller, String roleName)
            throws AuthorizationException, AuthorizationMgmtException, InvalidSessionException, MetaMatrixComponentException {
        // Validate caller's session
        AdminAPIHelper.validateSession(caller.getSessionID());
        // Any administrator may call this read-only method - no need to validate role
        return authAdmin.isCallerInRole(caller, roleName);
    }
    
    /**
     * Return true is given username is a super user
     * @param username - The user to verify as super user
     * @return true if given user is a super user 
     * @see com.metamatrix.platform.admin.api.AuthorizationAdminAPI#isSuperUser(java.lang.String)
     */
    public boolean isSuperUser(String username) throws ServiceException, MembershipServiceException, MetaMatrixComponentException {
        final MembershipServiceInterface membershipService = PlatformProxyHelper.getMembershipServiceProxy(PlatformProxyHelper.ROUND_ROBIN_LOCAL);
        return membershipService.isSuperUser(username);
    }

    /**
     * Returns a <code>List</code> of entitlements to the given fully qualified group name in the given realm.
     * <p>The list contains objects of type {@link com.metamatrix.platform.security.api.UserEntitlementInfo UserEntitlementInfo}
     * which will contain all user entitlement information for each group found. Each of these objects
     * will contain 1 or more objects of type {@link com.metamatrix.platform.security.api.GranteeEntitlementEntry GranteeEntitlementEntry}
     * which contain the <i>Grantee</i>'s name the entitlement <i>Grantor</i> or entity specifying the <i>Grantee</i>
     * is entitled and the <i>Allowed Actions</i> the <i>Grantee</i> is entitled to perform on the group.</p>
     * The attributes availible are:
     * <ol>
     *   <li value=1>VDB Name</li>
     *   <li>VDB Version</li>
     *   <li>Group Name (fully qualified)</li>
     *      <ul>
     *          <li>Grantee Name; Grantor Name; Allowed Actions (A <code>String[]</code> of one or more of {CREATE, READ, UPDATE, DELETE})</li>
     *          <li> ... </li>
     *      </ul>
     * </ol>
     * @param realm The realm in which the element must live.
     * @param fullyQualifiedGroupName The resource for which to look up permissions.
     * @return The <code>List</code> of entitlements to the given element in the
     * given realm - May be empty but never null.
     * @throws AuthorizationException if admninistrator does not have the authority to preform the action.
     * @throws AuthorizationMgmtException if an error occurs in the Authorization store.
     * @throws MetaMatrixComponentException if this service has trouble communicating.
     */
    public synchronized List getGroupEntitlements(AuthorizationRealm realm, String fullyQualifiedGroupName)
            throws AuthorizationException, AuthorizationMgmtException, InvalidSessionException, MetaMatrixComponentException {
        // Validate caller's session
//        SessionToken token =
        AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role
        return authAdmin.getGroupEntitlements(realm, fullyQualifiedGroupName);
    }

    /**
     * Returns a <code>List</code> of entitlements to the given element pattern in the given realm.
     * <p>The list contains objects of type {@link com.metamatrix.platform.security.api.UserEntitlementInfo UserEntitlementInfo}
     * which will contain all user entitlement information for each element found. Each of these objects
     * will contain 1 or more objects of type {@link com.metamatrix.platform.security.api.GranteeEntitlementEntry GranteeEntitlementEntry}
     * which contain the <i>Grantee</i>'s name the entitlement <i>Grantor</i> or entity specifying the <i>Grantee</i>
     * is entitled and the <i>Allowed Actions</i> the <i>Grantee</i> is entitled to perform on the element.</p>
     * The attributes availible are:
     * <ol>
     *   <li value=1>VDB Name</li>
     *   <li>VDB Version</li>
     *   <li>Group Name (fully qualified)</li>
     *   <li>Element Name (fully qualified)</li>
     *      <ul>
     *          <li>Grantee Name; Grantor Name; Allowed Actions (A <code>String[]</code> of one or more of {CREATE, READ, UPDATE, DELETE})</li>
     *          <li> ... </li>
     *      </ul>
     * </ol>
     * @param realm The realm in which the element must live.
     * @param elementNamePattern The resource for which to look up permissions. SQL '%' pattern matching may be used.
     * @return The <code>List</code> of entitlements to the given element in the
     * given realm - May be empty but never null.
     * @throws AuthorizationMgmtException if this service is unable to locate resources required
     * for this operation.
     */
    public synchronized List getElementEntitlements(AuthorizationRealm realm, String elementNamePattern)
            throws AuthorizationException, AuthorizationMgmtException, InvalidSessionException, MetaMatrixComponentException {
        // Validate caller's session
        AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role
        return authAdmin.getElementEntitlements(realm, elementNamePattern);
    }
}

