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

package com.metamatrix.platform.admin.api;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.api.exception.security.AuthorizationMgmtException;
import com.metamatrix.api.exception.security.MembershipServiceException;
import com.metamatrix.platform.security.api.AuthorizationPolicy;
import com.metamatrix.platform.security.api.AuthorizationPolicyID;
import com.metamatrix.platform.security.api.AuthorizationRealm;
import com.metamatrix.platform.security.api.MetaMatrixPrincipalName;

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
}

