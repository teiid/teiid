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

package com.metamatrix.platform.admin.apiimpl;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.adminapi.AdminRoles;

import com.metamatrix.admin.RolesAllowed;
import com.metamatrix.admin.api.exception.security.InvalidSessionException;
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

@RolesAllowed(value=AdminRoles.RoleName.ADMIN_READONLY)
public class AuthorizationAdminAPIImpl implements AuthorizationAdminAPI {

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
    @RolesAllowed(value=AdminRoles.RoleName.ADMIN_SYSTEM)
    public AuthorizationEditor createEditor()
            throws InvalidSessionException, AuthorizationException, MetaMatrixComponentException {
        return new AuthorizationObjectEditor(true);
    }

    public Map getRoleDescriptions()
            throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession();
        return authAdmin.getRoleDescriptions(token);
    }

    public Collection getPrincipalsForRole(String roleName)
            throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession();
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
    public Collection getRoleNamesForPrincipal(MetaMatrixPrincipalName principal)
            throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession();
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
    @RolesAllowed(value=AdminRoles.RoleName.ADMIN_SYSTEM)
    public void addPrincipalsToRole(Set principals, String roleName)
            throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession();

        AuthorizationPolicy role = authAdmin.getPolicy(token, new AuthorizationPolicyID(roleName, null, RolePermissionFactory.getRealm()));

        AuthorizationObjectEditor editor = new AuthorizationObjectEditor();
        role = editor.addAllPrincipals(role, principals);
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
    @RolesAllowed(value=AdminRoles.RoleName.ADMIN_SYSTEM)
    public void removePrincipalsFromRole(Set principals, String roleName)
            throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession();

        AuthorizationPolicy role = authAdmin.getPolicy(token, new AuthorizationPolicyID(roleName, null, RolePermissionFactory.getRealm()));

        AuthorizationObjectEditor editor = new AuthorizationObjectEditor();
        role = editor.removePrincipals(role, principals);
        ModificationActionQueue queue = editor.getDestination();

        authAdmin.executeTransaction(token, queue.popActions());
    }

    /**
     * Get all policyIDs in the system except those that we want to filter from the console.
     */
    public Collection findAllPolicyIDs()
            throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession();

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

    public Boolean containsPolicy(AuthorizationPolicyID policyID)
            throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession();
        return new Boolean(authAdmin.containsPolicy(token, policyID));
    }

    public AuthorizationPolicy getPolicy(AuthorizationPolicyID policyID)
            throws AuthorizationException, AuthorizationMgmtException, InvalidSessionException, MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession();
        // Any administrator may call this read-only method - no need to validate role
        return authAdmin.getPolicy(token, policyID);
    }

    @RolesAllowed(value=AdminRoles.RoleName.ADMIN_SYSTEM)
    public Set executeTransaction(List actions)
            throws AuthorizationException, AuthorizationMgmtException, InvalidSessionException, MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession();
        return authAdmin.executeTransaction(token, actions);
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
    public Collection getPolicyIDsInRealm(AuthorizationRealm realm)
            throws AuthorizationException, AuthorizationMgmtException, InvalidSessionException, MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession();
        return authAdmin.getPolicyIDsInRealm(token, realm);
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
}

