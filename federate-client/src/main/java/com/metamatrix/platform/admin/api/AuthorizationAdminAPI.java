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

package com.metamatrix.platform.admin.api;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.api.exception.security.AuthorizationMgmtException;
import com.metamatrix.api.exception.security.InvalidSessionException;
import com.metamatrix.api.exception.security.MembershipServiceException;
import com.metamatrix.platform.security.api.AuthorizationPolicy;
import com.metamatrix.platform.security.api.AuthorizationPolicyID;
import com.metamatrix.platform.security.api.AuthorizationRealm;
import com.metamatrix.platform.security.api.MetaMatrixPrincipalName;
import com.metamatrix.platform.security.api.SessionToken;

public interface AuthorizationAdminAPI extends SubSystemAdminAPI {

    /**
     *  Returns a <code>AuthorizationObjectEditor</code> to perform editing operations
     *  on a entitlement type object.  The editing process will create actions for
     *  each specific type of editing operation.  Those actions are what need to be
     *  submitted to the <code>AuthorizationService</code> for actual updates to occur.
     *  @return AuthorizationObjectEditor
     */
   AuthorizationEditor createEditor()
   throws InvalidSessionException, AuthorizationException, MetaMatrixComponentException;

    /**
     * Obtain the names of all of the realms known to the system.
     * @return the set of realm names
     * @throws InvalidSessionException if the <code>SessionToken</code> is not valid or is expired
     * @throws AuthorizationException if the caller is unable to perform this operation
     * @throws MetaMatrixComponentException if this service has trouble communicating.
    */
    Collection getRealmNames()
    throws InvalidSessionException, AuthorizationException, MetaMatrixComponentException;

    /**
     * Obtain the names of all of the roles and their descriptions known to the system.
     * @return a Map of role descriptions key by the role's name.
     * @throws InvalidSessionException if the <code>SessionToken</code> is not valid or is expired
     * @throws AuthorizationException if the caller is unable to perform this operation
     * @throws MetaMatrixComponentException if this service has trouble communicating.
     */
    Map getRoleDescriptions()
    throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException;

    /**
     * Returns a collection <code>MetaMatrixPrincipalName</code> objects containing the name
     * of the principal along with its type which belong to the given role.
     * {@link com.metamatrix.security.api.MetaMatrixPrincipalName}
     * @param roleName String name of MetaMatrix role for which principals
     * are sought
     * @throws InvalidSessionException if the administrative session is invalid
     * @throws AuthorizationException if admninistrator does not have the authority to perform the requested operation.
     * @throws MetaMatrixComponentException if this service has trouble communicating.
     */
    Collection getPrincipalsForRole(String roleName)
    throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException;

    /**
     * Returns a Collection of String names of MetaMatrix roles to which the
     * given principal is assigned.
     * @param principal <code>MetaMatrixPrincipalName</code> for which roles are sought
     * @param explicitOnly If true, only return roles assigned directly to given principal.
     * If false, return all roles directly assigned and inherited.
     * @return The <code>Collection</code> of role names the principal is assigned.
     * @throws InvalidSessionException if the administrative session is invalid
     * @throws AuthorizationException if admninistrator does not have the authority to perform the requested operation.
     * @throws MetaMatrixComponentException if this service has trouble communicating.
     */
    Collection getRoleNamesForPrincipal(MetaMatrixPrincipalName principal)
    throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException;

    /**
     * Add the given set of principals to the given role.
     * @param principals Set of <code>MetaMatrixPrincipalName</code>s to which to add.
     * @param roleName The name of the role to which to add the principals.
     * @throws InvalidSessionException if the administrative session is invalid
     * @throws AuthorizationException if admninistrator does not have the authority to perform the requested operation.
     * @throws MetaMatrixComponentException if this service has trouble communicating.
     */
    void addPrincipalsToRole(Set principals, String roleName)
    throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException;

    /**
     *  Returns true if the given username matches the current membership security.membership.admin.username
     *  from the current config
     * @param username the username to compare to the current super user
     * @return
     * @throws ServiceException for generic service errors
     * @throws MembershipServiceException If there are issues within the membership service
     * @throws MetaMatrixComponentException if this service has trouble communicating.
     *
     */    
    boolean isSuperUser(String username) throws MembershipServiceException, MetaMatrixComponentException;


    /**
     * Remove the given set of principals from the given role.
     * @param principals Set of <code>MetaMatrixPrincipalName</code>s to remove.
     * @param roleName The name of the role from which to remove the principals.
     * @throws InvalidSessionException if the administrative session is invalid
     * @throws AuthorizationException if admninistrator does not have the authority to perform the requested operation.
     * @throws MetaMatrixComponentException if this service has trouble communicating.
     */
    void removePrincipalsFromRole(Set principals, String roleName)
    throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException;

    /**
     * Remove the policy with the specified ID.
     * @param policyID the ID of the policy that is to be removed.
     * @throws InvalidSessionException if the <code>sessionToken</code> is not valid or is expired
     * @throws AuthorizationException if the caller is unable to perform this operation
     * @throws MetaMatrixComponentException if this service is unable to locate resources required
     * for this operation
     */
    void removePolicy(AuthorizationPolicyID policyID)
    throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException;

    /**
     * Locate the IDs of all of the policies that are accessible by the caller.
     * @param caller the session token of the principal that is attempting to access the policies.
     * @return the set of all policy IDs
     * @throws InvalidSessionException if the <code>SessionToken</code> is not valid or is expired
     * @throws AuthorizationException if the caller is unable to perform this operation
     * @throws AuthorizationMgmtException if this service has trouble connecting to services it uses.
     * @throws MetaMatrixComponentException if this service has trouble communicating.
     */
    Collection findAllPolicyIDs()
    throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException;

    /**
     * Locate the IDs of all of the policies that apply to the specified principal and that are accessible by the caller.
     * @param principals the Set of UserGroupIDs and/or UserAccountIDs to whom the returned policies should apply to
     * (may not null, empty or invalid, all of which would result in an empty result)
     * @return the set of all policy IDs; never null but possibly empty
     * @throws InvalidSessionException if the <code>SessionToken</code> is not valid or is expired
     * @throws AuthorizationException if the caller is unable to perform this operation
     * @throws MetaMatrixComponentException if this service has trouble communicating.
     */
    Collection findPolicyIDs(Collection principals)
    throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException;

    /**
     * Locate the policies that have the specified IDs.  Any ID that is invalid is simply
     * ignored.
     * @param policyIDs the policy IDs for which the policies are to be obtained
     * @return the set of entitlements that correspond to those specified IDs that are valid
     * @throws InvalidSessionException if the <code>SessionToken</code> is not valid or is expired
     * @throws AuthorizationException if the caller is unable to perform this operation
     * @throws MetaMatrixComponentException if this service has trouble communicating.
     */
    Collection getPolicies(Collection policyIDs)
    throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException;

    /**
     * Return whether there is an existing policy with the specified ID.
     * @param id the ID that is to be checked
     * @return true if a policy with the specified ID exists
     * @throws InvalidSessionException if the <code>SessionToken</code> is not valid or is expired
     * @throws AuthorizationException if the caller is unable to perform this operation
     * @throws MetaMatrixComponentException if this service has trouble communicating.
     */
    Boolean containsPolicy(AuthorizationPolicyID policyID)
    throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException;

    /**
     * Locate the policy that has the specified ID.  Any ID that is invalid is simply
     * ignored.
     * @param policyID the ID of the policy to be obtained
     * @return the policy that correspond to the specified ID
     * @throws InvalidSessionException if the <code>SessionToken</code> is not valid or is expired
     * @throws AuthorizationException if the caller is unable to perform this operation
     * @throws MetaMatrixComponentException if this service has trouble communicating.
     */
    AuthorizationPolicy getPolicy(AuthorizationPolicyID policyID)
    throws AuthorizationException, AuthorizationMgmtException, InvalidSessionException, MetaMatrixComponentException;

    /**
     * Execute as a single transaction with the specified actions, and return
     * the set of IDs for the objects that were affected/modified by the action.
     * @param actions the ordered list of actions that are to be performed
     * on metamodel within the repository.
     * @return The set of objects that were affected by this transaction.
     * @throws InvalidSessionException if the <code>SessionToken</code> is not valid or has expired.
     * @throws AuthorizationException if the caller is unable to perform this operation.
     * @throws MetaMatrixComponentException if this service has trouble communicating.
     */
    Set executeTransaction(List actions)
    throws AuthorizationException, AuthorizationMgmtException, InvalidSessionException, MetaMatrixComponentException;

    /**
     * Remove given Principal from <emph>ALL</emph> <code>AuthorizationPolicies</code> to
     * which he belongs.
     * @param principal <code>MetaMatrixPrincipalName</code> which should be deleted.
     * @return true if at least one policy in which the principal had authorization
     * was found and deleted, false otherwise.
     * @throws AuthorizationException if admninistrator does not have the authority to preform the action.
     * @throws MetaMatrixComponentException if this service has trouble communicating.
     */
    Boolean removePrincipalFromAllPolicies(MetaMatrixPrincipalName principal)
    throws AuthorizationException, AuthorizationMgmtException, InvalidSessionException, MetaMatrixComponentException;

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
    Collection getPolicyIDsWithPermissionsInRealm(AuthorizationRealm realm)
    throws AuthorizationException, AuthorizationMgmtException, InvalidSessionException, MetaMatrixComponentException;

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
    Collection getPolicyIDsInRealm(AuthorizationRealm realm)
    throws AuthorizationException, AuthorizationMgmtException, InvalidSessionException, MetaMatrixComponentException;

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
    Collection getPolicyIDsInPartialRealm(AuthorizationRealm realm)
    throws AuthorizationException, AuthorizationMgmtException, InvalidSessionException, MetaMatrixComponentException;

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
    Collection getPolicyIDsForResourceInRealm(AuthorizationRealm realm, String resourceName)
    throws AuthorizationException, AuthorizationMgmtException, InvalidSessionException, MetaMatrixComponentException;

    /**
     * Verify that caller is in the specified logical role.
     * @param caller The session token of the MetaMatrix principle involking an administrative method.
     * @return true if caller's session token is valid and he is a MetaMatrix administrator.
     * @throws AuthorizationMgmtException if this service has trouble connecting to services it uses.
     */
    boolean isCallerInRole( SessionToken caller, String roleName )
    throws AuthorizationException, AuthorizationMgmtException, InvalidSessionException, MetaMatrixComponentException;

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
    List getGroupEntitlements(AuthorizationRealm realm, String fullyQualifiedGroupName)
    throws AuthorizationException, AuthorizationMgmtException, InvalidSessionException, MetaMatrixComponentException;

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
     * @throws ServiceStateException if the Authorization service is not taking requests.
     */
    List getElementEntitlements(AuthorizationRealm realm, String elementNamePattern)
    throws AuthorizationException, AuthorizationMgmtException, InvalidSessionException, MetaMatrixComponentException;
}

