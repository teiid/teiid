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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.metamatrix.api.exception.security.AuthorizationMgmtException;
import com.metamatrix.common.connection.ManagedConnectionException;
import com.metamatrix.platform.security.api.AuthorizationPermission;
import com.metamatrix.platform.security.api.AuthorizationPolicy;
import com.metamatrix.platform.security.api.AuthorizationPolicyID;
import com.metamatrix.platform.security.api.AuthorizationRealm;
import com.metamatrix.platform.security.api.AuthorizationResource;
import com.metamatrix.platform.security.api.MetaMatrixPrincipalName;

/**
 * @since	  3.0
 */
public class FakeAuthorizationSource implements AuthorizationSourceTransaction {

    private boolean closed;
    private final Set realmNames;
    private final Map authPoliciesByID;             // <AuthorizationPolicyID,AuthorizationPolicy>

    /**
     * Constructor for FakeAuthorizationSource.
     */
    public FakeAuthorizationSource() {
        this.realmNames = new HashSet();
        this.authPoliciesByID = new HashMap();
    }

    /**
     * @see com.metamatrix.platform.security.authorization.spi.AuthorizationSourceTransaction#getGroupEntitlements(AuthorizationRealm, String)
     */
    public Map getGroupEntitlements( AuthorizationRealm realm, String fullyQualifiedGroupName)
            throws AuthorizationSourceConnectionException, AuthorizationSourceException {
        // No need to implement this for this test
        throw new UnsupportedOperationException("Method needed to support MetaMatrix JDBC driver"); //$NON-NLS-1$
    }

    /**
     * @see com.metamatrix.platform.security.authorization.spi.AuthorizationSourceTransaction#getElementEntitlements(AuthorizationRealm, String)
     */
    public Map getElementEntitlements( AuthorizationRealm realm, String elementNamePattern)
            throws AuthorizationSourceConnectionException, AuthorizationSourceException {
        // No need to implement this for this test
        throw new UnsupportedOperationException("Method needed to support MetaMatrix JDBC driver"); //$NON-NLS-1$
    }

    /**
     * Obtain the names of all of the realms known to the system.
     * @return the collection of realm names
     */
    public Collection getRealmNames()
        throws AuthorizationSourceConnectionException, AuthorizationSourceException {
        return realmNames;
    }

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
    public boolean containsPolicy(AuthorizationPolicyID id)
                throws AuthorizationSourceConnectionException, AuthorizationSourceException {
        return this.authPoliciesByID.containsKey(id);
    }

    /**
     * Locate the IDs of all of the policies that are accessible by the caller.
     * @return the set of all policy IDs
     */
    public Collection findAllPolicyIDs()
                throws AuthorizationSourceConnectionException, AuthorizationSourceException {
        return this.authPoliciesByID.keySet();
    }

    /**
     * Locate the IDs of all of the policies that apply to the specified principal
     * and that are accessible by the caller.
     * @param principals the Set of <code>MetaMatrixPrincipalName</code> to whom
     * the returned policies should apply to  (may not null, empty or invalid,
     * all of which would result in an empty result).
     * @param realm the <code>AuthorizationRealm</code> in which to find the principal's
     * permissions.  <i>Not currently used!</i>
     * @return the set of all policy IDs; never null but possibly empty
     * @throws AuthorizationSourceConnectionException if there is an connection
     * or communication error with the data source, signifying that
     * the method should be retried with a different connection.
     * @throws AuthorizationSourceException if there is an unspecified or unknown
     * error with the data source.
     */
    public Collection findPolicyIDs(Collection principals, AuthorizationRealm realm)
                throws AuthorizationSourceConnectionException, AuthorizationSourceException {

        final Set results = new HashSet();

        // Iterate over the policies ...
        final Iterator iter = this.authPoliciesByID.values().iterator();
        while (iter.hasNext()) {
            final AuthorizationPolicy policy = (AuthorizationPolicy) iter.next();
            final Set policyPrincipals = policy.getPrincipals();
            final Iterator principalIter = principals.iterator();
            while (principalIter.hasNext()) {
                final MetaMatrixPrincipalName principalName = (MetaMatrixPrincipalName) principalIter.next();
                if ( policyPrincipals.contains(principalName) ) {
                    results.add(policy.getAuthorizationPolicyID());
                    break;
                }
            }
        }
        return results;
    }

    /**
     * Locate the IDs of all of the policies that apply to the specified principal
     * and that are accessible by the caller.
     * @param principals the Set of <code>MetaMatrixPrincipalName</code> to whom
     * the returned policies should apply to  (may not null, empty or invalid,
     * all of which would result in an empty result).
     * @return the set of all policy IDs; never null but possibly empty
     * @throws AuthorizationSourceConnectionException if there is an connection
     * or communication error with the data source, signifying that
     * the method should be retried with a different connection.
     * @throws AuthorizationSourceException if there is an unspecified or unknown
     * error with the data source.
     */
    public Collection findPolicyIDs(Collection principals)
                throws AuthorizationSourceConnectionException, AuthorizationSourceException {

        final Set results = new HashSet();

        // Iterate over the policies ...
        final Iterator iter = this.authPoliciesByID.values().iterator();
        while (iter.hasNext()) {
            final AuthorizationPolicy policy = (AuthorizationPolicy) iter.next();
            final Set policyPrincipals = policy.getPrincipals();
            final Iterator principalIter = principals.iterator();
            while (principalIter.hasNext()) {
                final MetaMatrixPrincipalName principalName = (MetaMatrixPrincipalName) principalIter.next();
                if ( policyPrincipals.contains(principalName) ) {
                    results.add(policy.getAuthorizationPolicyID());
                    break;
                }
            }
        }
        return results;
    }

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
    public Collection getPolicies( Collection policyIDs)
                throws AuthorizationSourceConnectionException, AuthorizationSourceException {
        final Collection results = new HashSet();
        final Iterator iter = policyIDs.iterator();
        while(iter.hasNext()){
            final AuthorizationPolicyID policyID = (AuthorizationPolicyID) iter.next();
            final AuthorizationPolicy policy = this.getPolicy(policyID);
            results.add(policy);
        }
        return results;
    }

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
    public AuthorizationPolicy getPolicy( AuthorizationPolicyID policyID)
                throws AuthorizationSourceConnectionException, AuthorizationSourceException {

        // Look up the policy by ID ...
        final AuthorizationPolicy policy = (AuthorizationPolicy) this.authPoliciesByID.get(policyID);
        if ( policy != null ) {
            this.updateResourcesForPermissions(policy);
        }
        return policy;
    }

    /**
     * @see com.metamatrix.common.connection.TransactionInterface#close()
     */
    public void close() {
        this.closed = true;
    }

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
    public Set executeActions( AuthorizationPolicyID target, List actions, String grantor)
        throws AuthorizationSourceConnectionException, AuthorizationSourceException, AuthorizationMgmtException {

        // Only the AdminAPI and the VDB Delete utility should be calling this method
        throw new UnsupportedOperationException("Method is only called from AdminAPI and the VDB delete utility"); //$NON-NLS-1$
    }

    /**
     * @see com.metamatrix.platform.security.authorization.spi.AuthorizationSourceTransaction#getRoleDescriptions()
     */
    public Map getRoleDescriptions()
        throws AuthorizationSourceConnectionException, AuthorizationSourceException {

        // Only the AdminAPI should be calling this method
        throw new UnsupportedOperationException("Method is only called from AdminAPI"); //$NON-NLS-1$
    }

    /**
     * @see com.metamatrix.platform.security.authorization.spi.AuthorizationSourceTransaction#getPrincipalsForRole(String)
     */
    public Collection getPrincipalsForRole(String roleName)
        throws AuthorizationSourceConnectionException, AuthorizationSourceException {

        // Only the AdminAPI should be calling this method
        throw new UnsupportedOperationException("Method is only called from AdminAPI"); //$NON-NLS-1$
    }
    
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
    public Collection getRoleNamesForPrincipal(Collection principals)
        throws AuthorizationSourceConnectionException, AuthorizationSourceException {

        // Only the AdminAPI should be calling this method
        throw new UnsupportedOperationException("Method is only called from AdminAPI"); //$NON-NLS-1$
    }

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
    public boolean removePrincipalFromAllPolicies(MetaMatrixPrincipalName principal)
        throws AuthorizationSourceConnectionException, AuthorizationSourceException {
        boolean result = false;

        // Iterate through all the policies ...
        final Iterator iter = this.authPoliciesByID.values().iterator();
        while (iter.hasNext()) {
            final AuthorizationPolicy policy = (AuthorizationPolicy) iter.next();
            if ( policy.getPrincipals().contains(principal) ) {
                policy.removePrincipal(principal);
                result = true;
            }
        }
        return result;
    }

    /**
     * @see com.metamatrix.platform.security.authorization.spi.AuthorizationSourceTransaction#getPolicyIDsWithPermissionsInRealm(AuthorizationRealm)
     */
    public Collection getPolicyIDsWithPermissionsInRealm(AuthorizationRealm realm)
        throws AuthorizationSourceConnectionException, AuthorizationSourceException {
        // Only the AdminAPI and the VDB Delete utility should be calling this method
        throw new UnsupportedOperationException("Method is only called from AdminAPI and the VDB delete utility"); //$NON-NLS-1$
    }

    /**
     * @see com.metamatrix.platform.security.authorization.spi.AuthorizationSourceTransaction#getPolicyIDsInRealm(AuthorizationRealm)
     */
    public Collection getPolicyIDsInRealm(AuthorizationRealm realm)
        throws AuthorizationSourceConnectionException, AuthorizationSourceException {
        // Only the AdminAPI should be calling this method
        throw new UnsupportedOperationException("Method is only called from AdminAPI"); //$NON-NLS-1$
    }

    /**
     * @see com.metamatrix.platform.security.authorization.spi.AuthorizationSourceTransaction#getPolicyIDsInPartialRealm(AuthorizationRealm)
     */
    public Collection getPolicyIDsInPartialRealm(AuthorizationRealm realm)
        throws AuthorizationSourceConnectionException, AuthorizationSourceException {
        // Only the AdminAPI should be calling this method
        throw new UnsupportedOperationException("Method is only called from AdminAPI"); //$NON-NLS-1$
    }

    /**
     * @see com.metamatrix.platform.security.authorization.spi.AuthorizationSourceTransaction#getPolicyIDsForResourceInRealm(AuthorizationRealm, String)
     */
    public Collection getPolicyIDsForResourceInRealm( AuthorizationRealm realm, String resourceName)
        throws AuthorizationSourceConnectionException, AuthorizationSourceException {
        // Only the AdminAPI should be calling this method
        throw new UnsupportedOperationException("Method is only called from AdminAPI"); //$NON-NLS-1$
    }

    /**
     * Find and create all <code>AuthorizationPermissions</code> known to a policy.
     * @param policyID The policy indentifier.
     * @return The set of all permissions that belong to the given policy.
     */
    public Set getPermissionsForPolicy( AuthorizationPolicyID policyID)
                throws AuthorizationSourceConnectionException, AuthorizationSourceException {
        final AuthorizationPolicy policy = (AuthorizationPolicy) this.authPoliciesByID.get(policyID);
        if ( policy != null ) {
            return policy.getPermissions();
        }
        return Collections.EMPTY_SET;
    }

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
    public void addPermissionsWithResourcesToParent( String parent, Collection resources, AuthorizationRealm realm)
            throws AuthorizationSourceConnectionException, AuthorizationSourceException {
        // The service shouldn't be calling this method anymore!!!
        throw new UnsupportedOperationException("Service shouldn't be calling this method"); //$NON-NLS-1$
    }

    /**
     * Remove all permissions in the system that are on the given resources.
     * @param resources The resource names of the resources to be removed.
     * @param realm Confines the resource names to this realm.
     */
    public void removePermissionsWithResources( Collection resources, AuthorizationRealm realm)
            throws AuthorizationSourceConnectionException, AuthorizationSourceException {
        // Iterate through all the policies ...
        final Iterator iter = this.authPoliciesByID.values().iterator();
        while (iter.hasNext()) {
            final AuthorizationPolicy policy = (AuthorizationPolicy) iter.next();
            final Iterator permIter = policy.getPermissions().iterator();
            while ( permIter.hasNext() ) {
                final AuthorizationPermission perm = (AuthorizationPermission) permIter.next();
                final AuthorizationResource resource = perm.getResource();
                if ( resources.contains(resource) ) {
                    policy.removePermission(perm);
                }
            }
        }
    }


    /** 
     * @see com.metamatrix.platform.security.authorization.spi.AuthorizationSourceTransaction#removePrincipalsAndPoliciesForRealm(com.metamatrix.platform.security.api.AuthorizationRealm)
     * @since 4.3
     */
    public void removePrincipalsAndPoliciesForRealm(AuthorizationRealm realm) throws AuthorizationSourceConnectionException,
                                                                             AuthorizationSourceException {
    }

    /**
     * Get the collection of permissions whose resources are dependant on the given permision.
     * The returned collection will contain a permission for each dependant resource all having
     * the actions of the original request.  The search is scoped to the <code>AuthorizationRealm</code>
     * of the given request.
     *
     * @param request The permission for which to find dependants.
     * @return A Collection of dependant permissions all with the actions of the given request.
     */
    public Collection getDependantPermissions(AuthorizationPermission request) throws AuthorizationSourceConnectionException, AuthorizationSourceException {
        return null;  // TODO: Implement
    }

    /**
     * @see com.metamatrix.common.connection.TransactionInterface#commit()
     */
    public void commit() throws ManagedConnectionException {
        // do nothing
    }

    /**
     * @see com.metamatrix.common.connection.TransactionInterface#rollback()
     */
    public void rollback() throws ManagedConnectionException {
        // do nothing
    }

    /**
     * @see com.metamatrix.common.connection.TransactionInterface#isReadonly()
     */
    public boolean isReadonly() {
        return false;
    }

    /**
     * @see com.metamatrix.common.connection.TransactionInterface#isClosed()
     */
    public boolean isClosed() {
        return closed;
    }

    // --------------------------------------------------------------------------------------
    //    M E T H O D S   F O R   T H E   F A K E   I M P L E M E N T A T I O N   O N L Y
    // --------------------------------------------------------------------------------------

    public AuthorizationPolicy findPolicy( final String name ) {
        final AuthorizationPolicyID id = new AuthorizationPolicyID(name,""); //$NON-NLS-1$
        return (AuthorizationPolicy) this.authPoliciesByID.get(id);
    }

    public AuthorizationPolicy createPolicy( final String name ) {
        final AuthorizationPolicyID id = new AuthorizationPolicyID(name,""); //$NON-NLS-1$
        final AuthorizationPolicy policy = new AuthorizationPolicy(id);
        this.authPoliciesByID.put(id, policy);
        return policy;
    }

    public void removePolicy( final AuthorizationPolicy policy ) {
        this.authPoliciesByID.remove(policy.getAuthorizationPolicyID());
    }

    private void updateResourcesForPermissions( final AuthorizationPolicy policy )
                throws AuthorizationSourceConnectionException, AuthorizationSourceException {

        // Iterate over the permissions ...
        final Iterator iter = policy.getPermissions().iterator();
        while (iter.hasNext()) {
            final AuthorizationPermission permission = (AuthorizationPermission) iter.next();
            permission.getResource();

        }
    }

}
