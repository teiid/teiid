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

package com.metamatrix.platform.security.api.service;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.adminapi.AdminOptions;

import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.admin.api.exception.security.MetaMatrixSecurityException;
import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.api.exception.security.AuthorizationMgmtException;
import com.metamatrix.api.exception.security.InvalidUserException;
import com.metamatrix.api.exception.security.MembershipServiceException;
import com.metamatrix.platform.admin.api.EntitlementMigrationReport;
import com.metamatrix.platform.admin.api.PermissionDataNode;
import com.metamatrix.platform.security.api.AuthorizationModel;
import com.metamatrix.platform.security.api.AuthorizationPermission;
import com.metamatrix.platform.security.api.AuthorizationPolicy;
import com.metamatrix.platform.security.api.AuthorizationPolicyID;
import com.metamatrix.platform.security.api.AuthorizationRealm;
import com.metamatrix.platform.security.api.MetaMatrixPrincipalName;
import com.metamatrix.platform.security.api.SessionToken;
import com.metamatrix.platform.service.api.ServiceInterface;
import com.metamatrix.platform.service.api.exception.ServiceStateException;

/**
 * This interface represents the API to the Authorization Service
 * and it defines the functionality that is accessible to clients.
 */
public interface AuthorizationServiceInterface extends ServiceInterface {
    public static String NAME = "AuthorizationService";
    
    
    /**
     * Return whether the specified account has authorization to access the specified resource.
     * This method returns false immediately upon encountering the first resource to which
     * the account does not have access.
     * @param sessionToken the session token of the principal whose access is being checked
     * @param contextName the name of the context for the caller (@see AuditContext)
     * @param request the permission that details the resource and the desired form of access
     * @return true if the specified principal is granted access to the requested resource,
     * or false otherwise
     * @throws InvalidSessionException if the session token for this cache is not valid
     * @throws AuthorizationMgmtException if this service is unable to locate resources required
     * for this operation
     */
    boolean checkAccess( SessionToken sessionToken, String contextName, AuthorizationPermission request )
    throws InvalidSessionException, AuthorizationMgmtException;

    /**
     * Return whether the specified account has authorization to access the specified resource
     * and all its dependent resources.
     * This method returns false immediately upon encountering the first resource to which
     * the account does not have access.
     * @param sessionToken the session token of the principal whose access is being checked
     * @param contextName the name of the context for the caller (@see AuditContext)
     * @param request the permission that details the resource and the desired form of access
     * @param fetchDependants If <code>true</code>, search authorization store for all dependent
     * Permissions of the given request. Access is checked for <i>all</i> resources - the given
     * request and all dependents.
     * @return true if the specified principal is granted access to the requested resources,
     * or false otherwise
     * @throws InvalidSessionException if the session token for this cache is not valid
     * @throws AuthorizationMgmtException if this service is unable to locate resources required
     * for this operation
     */
    boolean checkAccess( SessionToken sessionToken, String contextName, AuthorizationPermission request, boolean fetchDependants )
    throws InvalidSessionException, AuthorizationMgmtException;

    /**
     * Of those resources specified, return the subset for which the specified account does <i>not</i> have authorization
     * to access.
     * @param sessionToken the session token of the principal that is calling this method
     * @param contextName the name of the context for the caller (@see AuditContext)
     * @param requests the permissions that detail the resources and the desired form of access
     * @return the subset of <code>requests</code> that the account does <i>not</i> have access to
     * @throws InvalidSessionException if the session token for this cache is not valid
     * @throws AuthorizationMgmtException if this service is unable to locate resources required
     * for this operation
     */
    Collection getInaccessibleResources( SessionToken sessionToken, String contextName, Collection requests )
    throws InvalidSessionException, AuthorizationMgmtException;

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
     * @param groupNames the fully qualified group names - the resources - for which to look up permissions.
     * Collection of <code>String</code>.
     * @return The <code>List</code> of entitlements to the given element in the
     * given realm - May be empty but never null.
     * @throws AuthorizationMgmtException if this service is unable to locate resources required
     * for this operation.
     * @throws ServiceStateException if the Authorization service is not taking requests.
     */
    List getGroupEntitlements(AuthorizationRealm realm, Collection groupNames)
    throws AuthorizationMgmtException;

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
     * @param fullyQualifiedGroupName The resource for which to look up permissions.
     * @return The <code>List</code> of entitlements to the given element in the
     * given realm - May be empty but never null.
     * @throws AuthorizationMgmtException if this service is unable to locate resources required
     * for this operation.
     */
    List getGroupEntitlements(AuthorizationRealm realm, String fullyQualifiedGroupName)
    throws AuthorizationMgmtException;

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
     * @param elementNames The fully qualified element resource for which to look up permissions. Collection of <code>String</code>.
     * @return The <code>List</code> of entitlements to the given element in the
     * given realm - May be empty but never null.
     * @throws AuthorizationMgmtException if this service is unable to locate resources required
     * for this operation.
     */
    List getElementEntitlements(AuthorizationRealm realm, Collection elementNames)
    throws AuthorizationMgmtException;

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
     * @param elementName The fully qualified element resource for which to look up permissions.
     * @return The <code>List</code> of entitlements to the given element in the
     * given realm - May be empty but never null.
     * @throws AuthorizationMgmtException if this service is unable to locate resources required
     * for this operation.
     */
    List getElementEntitlements(AuthorizationRealm realm, String elementName)
    throws AuthorizationMgmtException;

////////////////////////////////////////////////////////////////////////////////
// Admin methods
////////////////////////////////////////////////////////////////////////////////
    /**
     * Obtain the names of all of the realms known to the system.
     * @param caller the session token of the principal that is attempting to access the realms.
     * @return the set of realm names
     * @throws InvalidSessionException if the <code>SessionToken</code> is not valid or is expired
     * @throws AuthorizationException if the caller is unable to perform this operation
     * @throws AuthorizationMgmtException if this service has trouble connecting to services it uses.
     */
    Collection getRealmNames(SessionToken caller)
    throws InvalidSessionException, AuthorizationException, AuthorizationMgmtException;

    /**
     * Return whether there is an existing policy with the specified ID.
     * @param caller the session token of the principal that is attempting to access the policies.
     * @param id the ID that is to be checked
     * @return true if a policy with the specified ID exists
     * @throws InvalidSessionException if the <code>SessionToken</code> is not valid or is expired
     * @throws AuthorizationException if the caller is unable to perform this operation
     * @throws AuthorizationMgmtException if this service has trouble connecting to services it uses.
     * @see AuthorizationDomain.containsPolicy
     */
    boolean containsPolicy(SessionToken caller, AuthorizationPolicyID id )
    throws InvalidSessionException, AuthorizationException, AuthorizationMgmtException;

    /**
     * Locate the IDs of all of the policies that are accessible by the caller.
     * @param caller the session token of the principal that is attempting to access the policies.
     * @return the set of all policy IDs
     * @throws InvalidSessionException if the <code>SessionToken</code> is not valid or is expired
     * @throws AuthorizationException if the caller is unable to perform this operation
     * @throws AuthorizationMgmtException if this service has trouble connecting to services it uses.
     * @see AuthorizationDomain.findAllPolicyIDs
     */
    Collection findAllPolicyIDs(SessionToken caller)
    throws InvalidSessionException, AuthorizationException, AuthorizationMgmtException;

    /**
     * Locate the IDs of all of the policies that apply to the specified principal and that are accessible by the caller.
     * @param caller the session token of the principal that is attempting to access the policies.
     * @param principals the Set of UserGroupIDs and/or UserAccountIDs to whom the returned policies should apply to
     * (may not null, empty or invalid, all of which would result in an empty result)
     * @return the set of all policy IDs; never null but possibly empty
     * @throws InvalidSessionException if the <code>SessionToken</code> is not valid or is expired
     * @throws AuthorizationException if the caller is unable to perform this operation
     * @throws AuthorizationMgmtException if this service has trouble connecting to services it uses.
     * @see AuthorizationModel.findAllPolicyIDs
     */
    Collection findPolicyIDs(SessionToken caller, Collection principals )
    throws InvalidSessionException, AuthorizationException, AuthorizationMgmtException;

    /**
     * Locate the policies that have the specified IDs.  Any ID that is invalid is simply
     * ignored.
     * @param caller the session token of the principal that is attempting to access the
     * specified policies
     * @param policyIDs the policy IDs for which the policies are to be obtained
     * @return the set of entitlements that correspond to those specified IDs that are valid
     * @throws InvalidSessionException if the <code>SessionToken</code> is not valid or is expired
     * @throws AuthorizationException if the caller is unable to perform this operation
     * @throws AuthorizationMgmtException if this service has trouble connecting to services it uses.
     * @see AuthorizationDomain.getPolicies
     */
    Collection getPolicies(SessionToken caller, Collection policyIDs)
    throws InvalidSessionException, AuthorizationException, AuthorizationMgmtException;

    /**
     * Locate the policy that has the specified ID.  Any ID that is invalid is simply
     * ignored.
     * @param caller the session token of the principal that is attempting to access the
     * specified policies
     * @param policyID the ID of the policy to be obtained
     * @return the policy that correspond to the specified ID
     * @throws InvalidSessionException if the <code>SessionToken</code> is not valid or is expired
     * @throws AuthorizationException if the caller is unable to perform this operation
     * @throws AuthorizationMgmtException if this service has trouble connecting to services it uses.
     * @see AuthorizationDomain.getPolicy
     */
    AuthorizationPolicy getPolicy(SessionToken caller, AuthorizationPolicyID policyID)
    throws InvalidSessionException, AuthorizationException, AuthorizationMgmtException;

    /**
     * Execute as a single transaction with the specified actions, and return
     * the set of IDs for the objects that were affected/modified by the action.
     * @param caller the session token of the principal that is attempting to access the policies.
     * @param actions the ordered list of actions that are to be performed
     * on metamodel within the repository.
     * @return The set of objects that were affected by this transaction.
     * @throws InvalidSessionException if the <code>SessionToken</code> is not valid or has expired.
     * @throws AuthorizationException if the caller is unable to perform this operation.
     * @throws AuthorizationMgmtException if there were errors with the SPI.  Causes rollback.
     * @throws IllegalArgumentException if the action is null.
     */
    Set executeTransaction(SessionToken caller, List actions)
    throws InvalidSessionException, AuthorizationException, AuthorizationMgmtException;

    /**
     * Verify that caller is in the specified logical role.
     * @param caller The session token of the MetaMatrix principle involking an administrative method.
     * @return true if caller's session token is valid and he is a MetaMatrix administrator.
     * @throws AuthorizationMgmtException if this service has trouble connecting to services it uses.
     */
    boolean isCallerInRole( SessionToken caller, String roleName )
    throws AuthorizationMgmtException;

    /**
     * Obtain the names of all of the roles and their descriptions known to the system.
     * @param caller the session token of the principal that is attempting to access the roles.
     * @return a Map of role descriptions key by the role's name.
     * @throws InvalidSessionException if the <code>SessionToken</code> is not valid or is expired
     * @throws AuthorizationException if the caller is unable to perform this operation
     * @throws AuthorizationMgmtException if this service has trouble connecting to services it uses.
     */
    Map getRoleDescriptions(SessionToken caller)
    throws InvalidSessionException, AuthorizationException, AuthorizationMgmtException;

    /**
     * Returns a collection <code>MetaMatrixPrincipalName</code> objects containing the name
     * of the principal along with its type which belong to the given role.
     * {@link com.metamatrix.security.api.MetaMatrixPrincipalName}
     * @param caller the session token of the principal that is attempting to access the roles.
     * @param roleName String name of MetaMatrix role for which principals
     * are sought
     * @throws InvalidSessionException if the administrative session is invalid
     * @throws MetaMatrixSecurityException if there is a problem internally with the MembershipService
     * @throws AuthorizationException if administrator does not have the authority to see the requested information
     * @throws ComponentNotFoundException if a component required by this method could not be found within the server
     */
    Collection getPrincipalsForRole(SessionToken caller, String roleName)
    throws InvalidSessionException, AuthorizationException, AuthorizationMgmtException;

    /**
     * Returns a Collection of String names of MetaMatrix roles to which the
     * given principal is assigned.
     * @param caller the session token of the principal that is attempting to access the roles.
     * @param principal <code>MetaMatrixPrincipalName</code> for which roles are sought
     * @return The <code>Collection</code> of role names the principal is assigned.
     * @throws InvalidSessionException if the administrative session is invalid
     * @throws MetaMatrixSecurityException if there is a problem internally with the MembershipService
     * @throws AuthorizationException if administrator does not have the authority to see the requested information
     * @throws ComponentNotFoundException if a component required by this method could not be found within the server
     */
    Collection getRoleNamesForPrincipal(SessionToken caller, MetaMatrixPrincipalName principal)
    throws InvalidSessionException, AuthorizationException, AuthorizationMgmtException;

    /**
     * Remove given Principal from <emph>ALL</emph> <code>AuthorizationPolicies</code> to
     * which he belongs.
     * @param caller the session token of the principal that is attempting to remove the Principal.
     * @param principal <code>MetaMatrixPrincipalName</code> which should be deleted.
     * @return true if at least one policy in which the principal had authorization
     * was found and deleted, false otherwise.
     * @throws AuthorizationException if administrator does not have the authority to perform the action.
     * @throws AuthorizationMgmtException if an error occurs in the Authorization store.
     */
    boolean removePrincipalFromAllPolicies(SessionToken caller, MetaMatrixPrincipalName principal)
    throws AuthorizationException, AuthorizationMgmtException;

    /**
     * Returns a <code>Collection</code> of <code>AuthorizationPolicyID</code>s
     * that have <code>AuthorizationPermission</code>s in the given <code>AuthorizationRealm</code>.<br>
     * <strong>NOTE:</strong> It is the responsibility of the caller to determine
     * which of the <code>AuthorizationPolicy</code>'s <code>AuthorizationPermission</code>s
     * are actually in the given <code>AuthorizationRealm</code>.  The <code>AuthorizationPolicy</code>
     * may span <code>AuthorizationRealm</code>s.
     * @param caller The session token of the principal that is attempting to retrieve the policies.
     * @param realm The realm in which to search for <code>AuthorizationPermission</code>s.
     * @return The collection of <code>AuthorizationPolicyID</code>s that have permissions
     * in the given realm - possibly empty but never null.
     * @throws AuthorizationException if administrator does not have the authority to perform the action.
     * @throws AuthorizationMgmtException if an error occurs in the Authorization store.
     */
    Collection getPolicyIDsWithPermissionsInRealm(SessionToken caller, AuthorizationRealm realm)
    throws AuthorizationException, AuthorizationMgmtException;

    /**
     * Returns a <code>Collection</code> of <code>AuthorizationPolicyID</code>s
     * in the given <code>AuthorizationRealm</code>.
     * <br>This method will only work for Data Access Authorizations because the realm
     * is encoded in a Data Access policy name.
     * <strong>NOTE:</strong> It is the responsibility of the caller to determine
     * which of the <code>AuthorizationPolicy</code>'s <code>AuthorizationPermission</code>s
     * are actually in the given <code>AuthorizationRealm</code>.  The <code>AuthorizationPolicy</code>
     * may span <code>AuthorizationRealm</code>s.
     * @param caller The session token of the principal that is attempting to retrieve the policies.
     * @param realm The realm in which to search for <code>AuthorizationPermission</code>s.
     * @return The collection of <code>AuthorizationPolicyID</code>s that have permissions
     * in the given realm - possibly empty but never null.
     * @throws AuthorizationException if administrator does not have the authority to perform the action.
     * @throws AuthorizationMgmtException if an error occurs in the Authorization store.
     */
    Collection getPolicyIDsInRealm(SessionToken caller, AuthorizationRealm realm)
    throws AuthorizationException, AuthorizationMgmtException;

    /**
     * Returns a <code>Collection</code> of <code>AuthorizationPolicy</code>s
     * that have <code>AuthorizationPermission</code>s in the given <code>AuthorizationRealm</code>.<br>
     * <strong>NOTE:</strong> It is the responsibility of the caller to determine
     * which of the <code>AuthorizationPolicy</code>'s <code>AuthorizationPermission</code>s
     * are actually in the given <code>AuthorizationRealm</code>.  The <code>AuthorizationPolicy</code>
     * may span <code>AuthorizationRealm</code>s.
     * @param caller The session token of the principal that is attempting to retrieve the policies.
     * @param realm The realm in which to search for <code>AuthorizationPermission</code>s.
     * @return The collection of <code>AuthorizationPolicy</code>s that have permissions
     * in the given realm - possibly empty but never null.
     * @throws AuthorizationException if administrator does not have the authority to perform the action.
     * @throws AuthorizationMgmtException if an error occurs in the Authorization store.
     */
    Collection getPoliciesInRealm(SessionToken caller, AuthorizationRealm realm)
    throws AuthorizationException, AuthorizationMgmtException;

    /**
     * Returns a <code>Collection</code> of <code>AuthorizationPolicyID</code>s
     * that have <code>AuthorizationPermissions</code> that exist in the given
     * <emph>partial</emph> <code>AuthorizationRealm</code>.<br>
     * The implementation is such that all <code>AuthorizationPolicyID</code>s
     * whose <code>AuthorizationRealm</code> <emph>starts with</emph> the given
     * <code>AuthorizationRealm</code> are returned.
     * @param caller The session token of the principal that is attempting to retrieve the policies.
     * @param realm The <emph>partial</emph> realm in which to search for
     * <code>AuthorizationPermission</code>s whose realm name <emph>starts with</emph>
     * the given realm.
     * @return The collection of <code>AuthorizationPolicyID</code>s that have permissions
     * in the given partial realm - possibly empty but never null.
     * @throws AuthorizationException if administrator does not have the authority to perform the action.
     * @throws AuthorizationMgmtException if an error occurs in the Authorization store.
     */
    public Collection getPolicyIDsInPartialRealm(SessionToken caller, AuthorizationRealm realm)
    throws AuthorizationException, AuthorizationMgmtException;

    /**
     * Returns a <code>Collection</code> of <code>AuthorizationPolicyID</code>s
     * that have <code>AuthorizationPermissions</code> on the given resource that
     * exists in the given <code>AuthorizationRealm</code>.<br>
     * @param caller The session token of the principal that is attempting to retrieve the policies.
     * @param realm The realm in which to search for <code>AuthorizationPermission</code>s.
     * @param resourceName The resource for which to search for <code>AuthorizationPermission</code>s.
     * @return The collection of <code>AuthorizationPolicyID</code>s that have permissions
     * on the given resource - possibly empty but never null.
     * @throws AuthorizationException if administrator does not have the authority to perform the action.
     * @throws AuthorizationMgmtException if an error occurs in the Authorization store.
     */
    Collection getPolicIDsForResourceInRealm(SessionToken caller, AuthorizationRealm realm, String resourceName)
    throws AuthorizationException, AuthorizationMgmtException;

    public boolean hasPolicy(SessionToken caller, AuthorizationRealm realm, String policyName) 
    	throws AuthorizationMgmtException, InvalidUserException, MembershipServiceException;
  
    
    public void migratePolicies(SessionToken token,
                                EntitlementMigrationReport rpt,
                                String targetVDBName, String targetVDBVersion, Set targetNodes,
                                Collection sourcePolicies, AdminOptions options) throws MetaMatrixComponentException,
                                                          InvalidSessionException,
                                                          AuthorizationException,
                                                          AuthorizationMgmtException;  

    /**
     * Takes a tree of <code>PermissionDataNodeImpl</code>s that have their <code>Resource</code>s
     * filled in and fills in all permissions on resources that are found in the given
     * <code>AuthorizationPolicyID</code>.<br></br>
     * If any permissions are found that have no corresponding data node, a <code>AuthorizationMgmtException</code>
     * is thrown noting the missing resource name(s).
     * @param root The node containing the resource (group or element full name)
     * for which to search for permission(s).
     * @param realm The realm in which to search.
     * @param tree The tree of PermissionDataNodes to fill in permissions for.
     * @return The root of the filled in tree.
     * If no permissions exist, the original is returned as the sole element in the list.
     * @throws AuthorizationMgmtException if there is a connection or communication error with the data source,
     * signifying that the method should be retried with a different connection; if there is an
     * unspecified or unknown error with the data source; or one or more permissions were found but
     * a corresponding <code>PermissionDataNodeImpl</code> could not be found.
     */
    PermissionDataNode fillPermissionNodeTree(PermissionDataNode root, AuthorizationPolicyID policyID)
    throws AuthorizationMgmtException;
}





