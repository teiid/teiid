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

package com.metamatrix.platform.security.authorization.spi;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.metamatrix.api.exception.security.AuthorizationMgmtException;
import com.metamatrix.common.connection.TransactionInterface;
import com.metamatrix.platform.security.api.AuthorizationPermission;
import com.metamatrix.platform.security.api.AuthorizationPolicy;
import com.metamatrix.platform.security.api.AuthorizationPolicyID;
import com.metamatrix.platform.security.api.AuthorizationRealm;
import com.metamatrix.platform.security.api.MetaMatrixPrincipalName;

public interface AuthorizationSourceTransaction extends TransactionInterface {

////////////////////////////////////////////////////////////////////////////////
// Metadata methods
////////////////////////////////////////////////////////////////////////////////
    /**
     * Returns a compound <code>List</code> of entitlements to the given fully qualified
     * group in the given realm.
     * The returned <code>List</code> will be comprised of a <code>List</code>s of 6 elements.<br>
     * They are, in order:
     * <ol>
     *   <li value=0>VDB Name</li>
     *   <li>VDB Version</li>
     *   <li>Group Name (fully qualified)</li>
     *   <li>Grantor</li>
     *   <li>Grantee</li>
     *   <li>Allowed Action (one or more of {CREATE, READ, UPDATE, DELETE})</li>
     * </ol>
     * @param realm The realm in which the group must live.
     * @param fullyQualifiedGroupName The resource for which to look up permissions.
     * @return The <code>List</code> of entitlements to the given group in the
     * given realm - May be empty but never null.
     * @throws AuthorizationSourceConnectionException if there is an error communicating with the source.
     * @throws AuthorizationSourceException if there is an unspecified error.
     */
    Map getGroupEntitlements(AuthorizationRealm realm, String fullyQualifiedGroupName)
    throws AuthorizationSourceConnectionException, AuthorizationSourceException;

    /**
     * Returns a compound <code>List</code> of entitlements to the given fully qualified
     * element in the given realm.
     * The returned <code>List</code> will be comprised of a <code>List</code>s of 7 elements.<br>
     * They are, in order:
     * <ol>
     *   <li value=0>VDB Name</li>
     *   <li>VDB Version</li>
     *   <li>Group Name (fully qualified)</li>
     *   <li>Element</li>
     *   <li>Grantor</li>
     *   <li>Grantee</li>
     *   <li>Allowed Action (one or more of {CREATE, READ, UPDATE, DELETE})</li>
     * </ol>
     * @param realm The realm in which the element must live.
     * @param elementNamePattern The resource for which to look up permissions.
     * @return The <code>List</code> of entitlements to the given element in the
     * given realm - May be empty but never null.
     * @throws AuthorizationSourceConnectionException if there is an error communicating with the source.
     * @throws AuthorizationSourceException if there is an unspecified error.
     */
    Map getElementEntitlements(AuthorizationRealm realm, String elementNamePattern)
    throws AuthorizationSourceConnectionException, AuthorizationSourceException;

////////////////////////////////////////////////////////////////////////////////
// Admin methods
////////////////////////////////////////////////////////////////////////////////

    /**
     * Obtain the names of all of the realms known to the system.
     * @return the collection of realm names
     */
     Collection getRealmNames()
     throws AuthorizationSourceConnectionException, AuthorizationSourceException;

    /**
     * Return whether there is an existing policy with the specified ID.
     * @param id the ID that is to be checked
     * @return true if a policy with the specified ID exists
     * @throws AuthorizationSourceConnectionException if there is an connection
     * or communication error with the data source, signifying that
     * the method should be retried with a different connection.
     * @throws AuthorizationSourceException if there is an unspecified or unknown
     * error with the data source.
     */
    boolean containsPolicy(AuthorizationPolicyID id)
    throws AuthorizationSourceConnectionException, AuthorizationSourceException;

    /**
     * Locate the IDs of all of the policies that are accessible by the caller.
     * @return the set of all policy IDs
     * @throws AuthorizationSourceConnectionException if there is an connection
     * or communication error with the data source, signifying that
     * the method should be retried with a different connection.
     * @throws AuthorizationSourceException if there is an unspecified or unknown
     * error with the data source.
     */
    Collection findAllPolicyIDs()
    throws AuthorizationSourceConnectionException, AuthorizationSourceException;

    /**
     * Locate the IDs of all of the policies that apply to the specified principal
     * and that are accessible by the caller in the given realm.
     * @param principals the Set of UserGroupIDs and/or UserAccountIDs to whom
     * the returned policies should apply to  (may not null, empty or invalid,
     * all of which would result in an empty result).
     * @param realm The applicable realm in which to search for policies.
     * @return the set of all policy IDs; never null but possibly empty
     * @throws AuthorizationSourceConnectionException if there is an connection
     * or communication error with the data source, signifying that
     * the method should be retried with a different connection.
     * @throws AuthorizationSourceException if there is an unspecified or unknown
     * error with the data source.
     */
    Collection findPolicyIDs(Collection principals, AuthorizationRealm realm)
    throws AuthorizationSourceConnectionException, AuthorizationSourceException;

    /**
     * Locate the IDs of all of the policies that apply to the specified principal
     * and that are accessible by the caller in all known realms.
     * @param principals the Set of UserGroupIDs and/or UserAccountIDs to whom
     * the returned policies should apply to  (may not null, empty or invalid,
     * all of which would result in an empty result).
     * @return the set of all policy IDs; never null but possibly empty
     * @throws AuthorizationSourceConnectionException if there is an connection
     * or communication error with the data source, signifying that
     * the method should be retried with a different connection.
     * @throws AuthorizationSourceException if there is an unspecified or unknown
     * error with the data source.
     */
    Collection findPolicyIDs(Collection principals)
    throws AuthorizationSourceConnectionException, AuthorizationSourceException;

    /**
     * Locate the policies that have the specified IDs.  Any ID that is invalid is simply
     * ignored.
     * @param policyIDs the policy IDs for which the policies are to be obtained
     * @return the set of entitlements that correspond to those specified IDs that are valid
     * @throws AuthorizationSourceConnectionException if there is an connection
     * or communication error with the data source, signifying that
     * the method should be retried with a different connection.
     * @throws AuthorizationSourceException if there is an unspecified or unknown
     * error with the data source.
     */
    Collection getPolicies(Collection policyIDs)
    throws AuthorizationSourceConnectionException, AuthorizationSourceException;

    /**
     * Locate the policy that has the specified ID.  Any ID that is invalid is simply
     * ignored.
     * specified policies
     * @param policyID the ID of the policy to be obtained
     * @return the policy that correspond to the specified ID
     * @throws AuthorizationSourceConnectionException if there is an connection
     * or communication error with the data source, signifying that
     * the method should be retried with a different connection.
     * @throws AuthorizationSourceException if there is an unspecified or unknown
     * error with the data source.
     */
    AuthorizationPolicy getPolicy(AuthorizationPolicyID policyID)
    throws AuthorizationSourceConnectionException, AuthorizationSourceException;

    /**
     * A method that is called before this transaction is closed, giving the
     * transaction a chance to release any resources.
     */
    void close();

    /**
     * Execute the actions on given object.
     * @param target The ID of the policy on which to execute the transactions.
     * @param actions The list of actions to execute.
     * @param grantor The principal name of the policy grantor.
     * @return The set of objects effected by this method.
     * @throws AuthorizationSourceConnectionException if there is an connection
     * or communication error with the data source, signifying that
     * the method should be retried with a different connection.
     * @throws AuthorizationSourceException if there is an unspecified or unknown
     * error with the data source.
     */
    Set executeActions(AuthorizationPolicyID target, List actions, String grantor)
    throws AuthorizationSourceConnectionException, AuthorizationSourceException, AuthorizationMgmtException;

    /**
     * Obtain the names of all of the roles and their descriptions known to the system.
     * @return a Map of role descriptions key by the role's name.
     * @throws AuthorizationSourceConnectionException if there is an connection
     * or communication error with the data source, signifying that
     * the method should be retried with a different connection.
     * @throws AuthorizationSourceException if there is an unspecified or unknown
     * error with the data source.
     */
    Map getRoleDescriptions()
    throws AuthorizationSourceConnectionException, AuthorizationSourceException;

    /**
     * Returns a collection <code>MetaMatrixPrincipalName</code> objects containing the name
     * of the principal along with its type which belong to the given role.
     * {@link com.metamatrix.platform.security.api.MetaMatrixPrincipalName}
     * @param roleName String name of MetaMatrix role for which principals
     * are sought
     * @return The collection of <code>MetaMatrixPrincipalName</code>s who are in the given role, possibly enpty, never null.
     * @throws AuthorizationSourceConnectionException if there is an connection
     * or communication error with the data source, signifying that
     * the method should be retried with a different connection.
     * @throws AuthorizationSourceException if there is an unspecified or unknown
     * error with the data source.
     */
    Collection getPrincipalsForRole(String roleName)
    throws AuthorizationSourceConnectionException, AuthorizationSourceException;

    /**
     * Returns a Collection of String names of MetaMatrix roles which the
     * given principal belongs to
     * @param principals <code>MetaMatrixPrincipalName</code>s of a principal and
     * any group memberships for which roles are sought
     * @return The collection of role names belonging to the given principal, possibly enpty, never null.
     * @throws AuthorizationSourceConnectionException if there is an connection
     * or communication error with the data source, signifying that
     * the method should be retried with a different connection.
     * @throws AuthorizationSourceException if there is an unspecified or unknown
     * error with the data source.
     */
    Collection getRoleNamesForPrincipal(Collection principals)
    throws AuthorizationSourceConnectionException, AuthorizationSourceException;

    /**
     * Remove given Principal from <emph>ALL</emph> <code>AuthorizationPolicies</code> to
     * which he belongs.
     * @param principal <code>MetaMatrixPrincipalName</code> which should be deleted.
     * @return true if at least one policy in which the principal had authorization
     * was found and deleted, false otherwise.
     * @throws AuthorizationSourceConnectionException if there is an connection
     * or communication error with the data source, signifying that
     * the method should be retried with a different connection.
     * @throws AuthorizationSourceException if there is an unspecified or unknown
     * error with the data source.
     */
    boolean removePrincipalFromAllPolicies(MetaMatrixPrincipalName principal)
    throws AuthorizationSourceConnectionException, AuthorizationSourceException;

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
     * @throws AuthorizationSourceConnectionException if there is an connection
     * or communication error with the data source, signifying that
     * the method should be retried with a different connection.
     * @throws AuthorizationSourceException if there is an unspecified or unknown
     * error with the data source.
     */
    Collection getPolicyIDsWithPermissionsInRealm(AuthorizationRealm realm)
    throws AuthorizationSourceConnectionException, AuthorizationSourceException;

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
     * @throws AuthorizationSourceConnectionException if there is an connection
     * or communication error with the data source, signifying that
     * the method should be retried with a different connection.
     * @throws AuthorizationSourceException if there is an unspecified or unknown
     * error with the data source.
     */
    Collection getPolicyIDsInRealm(AuthorizationRealm realm)
    throws AuthorizationSourceConnectionException, AuthorizationSourceException;

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
     * @throws AuthorizationSourceConnectionException if there is an connection
     * or communication error with the data source, signifying that
     * the method should be retried with a different connection.
     * @throws AuthorizationSourceException if there is an unspecified or unknown
     * error with the data source.
     */
    public Collection getPolicyIDsInPartialRealm(AuthorizationRealm realm)
    throws AuthorizationSourceConnectionException, AuthorizationSourceException;

    /**
     * Returns a <code>Collection</code> of <code>AuthorizationPolicyID</code>s
     * that have <code>AuthorizationPermissions</code> on the given resource that
     * exists in the given <code>AuthorizationRealm</code>.<br>
     * @param realm The realm in which to search for <code>AuthorizationPermission</code>s.
     * @param resourceName The resource for which to search for <code>AuthorizationPermission</code>s.
     * @return The collection of <code>AuthorizationPolicyID</code>s that have permissions
     * on the given resource - possibly empty but never null.
     * @throws AuthorizationSourceConnectionException if there is an connection
     * or communication error with the data source, signifying that
     * the method should be retried with a different connection.
     * @throws AuthorizationSourceException if there is an unspecified or unknown
     * error with the data source.
     */
    Collection getPolicyIDsForResourceInRealm(AuthorizationRealm realm, String resourceName)
    throws AuthorizationSourceConnectionException, AuthorizationSourceException;

    /**
     * Find and create all <code>AuthorizationPermissions</code> known to a policy.
     * @param policyID The policy indentifier.
     * @return The set of all permissions that belong to the given policy.
     */
    public Set getPermissionsForPolicy(AuthorizationPolicyID policyID)
    throws AuthorizationSourceConnectionException, AuthorizationSourceException;

    /**
     * Add the given resources as <code>AuthorizationPermission</code>s to existing
     * <code>AuthorizationPolicies</code> that have a permission with the given parent
     * as a resource. Use the parent's <code>AuthorizationActions</code> to create
     * the permission for each resource.
     * @param parent The uuid of the resource that will be the parent of the given
     * resources.
     * @param resources The uuids of the newly added resources.
     * @param realm Confine the resources to this realm.
     */
    void addPermissionsWithResourcesToParent(String parent, Collection resources, AuthorizationRealm realm)
    throws AuthorizationSourceConnectionException, AuthorizationSourceException;

    /**
     * Remove entries from AUTHREALM, AUTHPERMISSIONS, AUTHPOLICIES, AUTHPRINCIPALS
     * for the specified realm 
     * @param realm
     * @return
     * @throws AuthorizationSourceException
     * @since 4.3
     */
    void removePrincipalsAndPoliciesForRealm(AuthorizationRealm realm)
    throws AuthorizationSourceConnectionException, AuthorizationSourceException;    
    
    /**
     * Remove all permissions in the system that are on the given resources.
     * @param resources The IDs of the resources to be removed.
     * @param realm The <code>AuthorizationRealm</code> in which the resources reside.
     */
    void removePermissionsWithResources(Collection resources, AuthorizationRealm realm)
    throws AuthorizationSourceConnectionException, AuthorizationSourceException;

    /**
     * Get the collection of permissions whose resources are dependant on the given permision.
     * The returned collection will contain a permission for each dependant resource all having
     * the actions of the original request.  The search is scoped to the <code>AuthorizationRealm</code>
     * of the given request.
     * @param request The permission for which to find dependants.
     * @return A Collection of dependant permissions all with the actions of the given request.
     */
    Collection getDependantPermissions(AuthorizationPermission request)
            throws AuthorizationSourceConnectionException, AuthorizationSourceException;

}
