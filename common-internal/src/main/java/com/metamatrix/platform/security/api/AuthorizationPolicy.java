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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * The AuthorizationPolicy class defines a set of permissions (i.e., permissions) that apply to a
 * set of principals (@see MetaMatrixPrincipalName).  Each permission defines a resource (or set of resources) that are to be
 * controlled, what actions are allowed, and possibly any additional restrictions that should
 * be placed upon the resource to limit content (i.e., content modifiers).
 */
public class AuthorizationPolicy implements Comparable, Serializable {

    /**
     * Contains principal name
     */
    private Set principals;

    private AuthorizationPermissions permissions;

    private AuthorizationPolicyID authorizationPolicyID;

    /**
     * Create an instance of an AuthorizationPolicy that has the specified ID.
     * @param id the ID of the policy
     */
    public AuthorizationPolicy( AuthorizationPolicyID id ) {
        if( id == null){
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0001));
        }
        this.authorizationPolicyID = id;
        this.principals = new LinkedHashSet();
        this.permissions = new AuthorizationPermissionsImpl();
    }

    /**
     * Create an instance of an AuthorizationPolicy that has the specified ID,
     * principal set and permissions.
     * @param id the ID of the policy
     * @param principals the set of <code>MetaMatrixPrincipalName</code>s to which this policy applies.
     * @param permissions the permissions that define the resource access for this policy.
     */
    public AuthorizationPolicy( AuthorizationPolicyID id, Set principals, Set permissions ) {
        this.authorizationPolicyID = id;
        if ( principals != null ) {
            this.principals = new LinkedHashSet(principals);
        } else {
            this.principals = new LinkedHashSet();
        }
        this.permissions = new AuthorizationPermissionsImpl();
        this.permissions.add(permissions);
    }

    /**
     * Create an instance of an AuthorizationPolicy from a copy of another.
     * @param orig the original policy that this new instance is to be based upon
     */
    public AuthorizationPolicy( AuthorizationPolicy orig ) {
        this.authorizationPolicyID = orig.authorizationPolicyID;
        this.principals = new LinkedHashSet( orig.principals );
        this.permissions = new AuthorizationPermissionsImpl();
        Iterator iter = orig.iterator();
        while ( iter.hasNext() ) {
            this.permissions.add( (AuthorizationPermission) iter.next() );
        }
    }

    /**
     * Get the AuthorizationPermissionsImpl of this policy.
     * @return The AuthorizationPermissionsImpl.
     */
    AuthorizationPermissions getAuthorizationPermissions() {
        return this.permissions;
    }

    /**
     * Get the given AuthorizationPermission.
     * @param permission The requested AuthorizationPermission.
     * @return The requested AuthorizationPermission (may be null if not found).
     */
    AuthorizationPermission getPermission(AuthorizationPermission permission) {
        AuthorizationPermission oldPermission = null;
        Iterator permItr = this.permissions.iterator();
        while ( permItr.hasNext() ) {
            oldPermission = (AuthorizationPermission) permItr.next();
            if ( oldPermission.equals(permission) ) {
                return oldPermission;
            }
        }
        return null;
    }

    /**
     * Given an <code>AuthorizationResource</code>, find the <code>AuthorizationPermission</code>,
     * if any, that supplies an <code>AuthorizationAction</code> for that resource.
     * @param resource The resource for which to find a permission.
     * @return The requested AuthorizationPermission (may be null if not found).
     */
    public AuthorizationPermission findPermissionWithResource(AuthorizationResource resource) {
        AuthorizationPermission permission = null;
        Iterator permItr = this.permissions.iterator();
        while ( permItr.hasNext() ) {
            permission = (AuthorizationPermission) permItr.next();
            AuthorizationResource theResource = permission.getResource();
            if ( theResource.isCannonicallyEquivalent(resource) ) {
                return permission;
            }
        }
        return null;
    }

    /**
     * Given an <code>AuthorizationResource</code>, find the <code>AuthorizationPermission</code>s,
     * if any, that are dependant on that resource. An <code>AuthorizationPermission</code> is
     * dependant on a resource if it has any <code>AuthorizationAction</code>s on that resource
     * or if it is part of a recursive permission involving the resource.
     * @param resource The resource for which to find a permission.
     * @return The Collections AuthorizationPermission (may be empty but not null).
     */
    public Collection getDependantPermissions(AuthorizationResource resource) {
        Collection dependantPerms = new ArrayList();
//        boolean recursive = resource.isRecursive();
        AuthorizationPermission permission = null;
        Iterator permItr = this.permissions.iterator();
        while ( permItr.hasNext() ) {
            permission = (AuthorizationPermission) permItr.next();
            AuthorizationResource theResource = permission.getResource();
            if ( theResource.isCannonicallyEquivalent(resource) ) {
                dependantPerms.add( permission );
            }
        }
        return dependantPerms;
    }

    /**
     * Obtain the identifier for this policy.
     * @return the policy's identifier.
     */
    public AuthorizationPolicyID getAuthorizationPolicyID() {
        return authorizationPolicyID;
    }

    /**
     * Obtain the set of principal names that this policy applies to.
     * @return the set of <code>MetaMatrixPrincipalName</code>s to which this
     * policy applies; never null but possibly empty
     */
    public Set getPrincipals() {
        return principals;
    }

    /**
     * Obtain the number of principals that this policy applies to.
     * @return the number of principals instances in this policy.
     */
    public int getPrincipalCount() {
        return principals.size();
    }

    /**
     * Obtain the number of permissions that this policy applies.
     * @return the number of permissions instances in this policy.
     */
    public int getPermissionCount() {
        return permissions.size();
    }

    /**
     * Obtain the permissions that this policy applies as a Set.
     * @return the permission instances in this policy.
     */
    public Set getPermissions() {
        Set perms = new LinkedHashSet();
        Iterator permItr = permissions.iterator();
        while ( permItr.hasNext() ) {
            perms.add(permItr.next());
        }
        return perms;
    }

    /**
     * Return whether this policy currently has at least one permission instance.
     * @return true if this policy contains at least one AuthorizationPermission instance
     * that it applies.
     */
    public boolean hasPermissions() {
        return permissions.size() > 0;
    }

    /**
     * Obtain the description for this policy which may be null.
     * @return the description. May be null.
     */
    public String getDescription() {
        return authorizationPolicyID.getDescription();
    }

    /**
     * Obtain an iterator over the AuthorizationPermission instances that this policy applies.
     * @return an iterator that can be used to access each of the AuthorizationPermission instances
     */
    public Iterator iterator() {
        return permissions.iterator();
    }

    /**
     * Return whether this policy currently has at least one principal reference.
     * @return true if this policy contains at least one principal instance
     * to which this policy is to be applied.
     */
    public boolean hasPrincipal() {
        return ! principals.isEmpty();
    }

    /**
     * Checks to see if any of the AuthorizationPermission instances in this policy
     * imply access to the resouces in the <i>permission</i> object.
     * @param permission the AuthorizationPermission object to check.
     * @return true if <i>permission</i> is implied by the AuthorizationPermission
     * instances in this policy, or false otherwise
     */
    public boolean implies(AuthorizationPermission permission) {
        return permissions.implies(permission);
    }

    /**
     * Returns a string describing this policy object.
     * The format is:
     * <pre>
     * super.toString() (
     *   // the authorization policy ID of the policy ...
     *   // the description of the policy ...
     *   // enumerate all the Principal
     *   // objects and call toString() on them,
     *   // one per line..
     *   // enumerate all the AuthorizationPermission
     *   // objects and call toString() on them,
     *   // one per line..
     * )</pre>
     *
     * <code>super.toString</code> is a call to the <code>toString</code>
     * method of this
     * object's superclass, which is Object. The result is
     * this object's type name followed by this object's
     * hashcode, thus enabling clients to differentiate different
     * AuthorizationPolicy objects, even if they contain the same permissions.
     */
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append('{');
        sb.append("ID=["); //$NON-NLS-1$
        sb.append(this.authorizationPolicyID);
        sb.append("] Principals=["); //$NON-NLS-1$
        Iterator iter = this.principals.iterator();
        while (iter.hasNext()) {
            try {
                sb.append(iter.next().toString() + ',');
            } catch (NoSuchElementException e){
            // ignore
            }
        }
        // Chop last ','
        if ( this.principals.size() > 0 ) {
            sb.setLength(sb.length()-1);
        }
        sb.append("]  Permissions=["); //$NON-NLS-1$
        sb.append(this.permissions);
        sb.append("]}"); //$NON-NLS-1$
        return sb.toString();
    }

    /**
     * Overrides Object hashCode method.
     * @return  a hash code value for this object.
     * @see     Object#hashCode()
     * @see     Object#equals(Object)
     */
    public int hashCode() {
        return this.authorizationPolicyID.hashCode();
    }

    /**
     * Returns true if the specified object is semantically equal to this instance.
     * Note:  this method is consistent with <code>compareTo()</code>.
     * <p>
     * @param obj the object that this instance is to be compared to.
     * @return whether the object is equal to this object.
     */
    public boolean equals(Object obj) {
        // Check if instances are identical ...
        if (this == obj) {
            return true;
        }

        // Check if object can be compared to this one
        // (this includes checking for null ) ...
        if (this.getClass().isInstance(obj)) {

        	// fail fast on different hash codes
            if (this.hashCode() != obj.hashCode()) {
                return false;
            }

        	// slower comparison
            return compare(this, (AuthorizationPolicy)obj) == 0;
        }

        // Otherwise not comparable ...
        return false;
    }

    /**
     * Compares this AuthorizationPolicy to another Object. If the Object is an AuthorizationPolicy,
     * this function compares the name.  Otherwise, it throws a
     * ClassCastException (as AuthorizationPolicy instances are comparable only to
     * other AuthorizationPolicy instances).  Note:  this method is consistent with
     * <code>equals()</code>.
     * <p>
     * @param o the object that this instance is to be compared to.
     * @return a negative integer, zero, or a positive integer as this object
     *      is less than, equal to, or greater than the specified object, respectively.
     * @throws ClassCastException if the specified object's type prevents it
     *      from being compared to this AuthorizationPolicy.
     */
    public int compareTo(Object o) throws ClassCastException {
        // Check if instances are identical ...
        if (this == o) {
            return 0;
        }
        if (o == null) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0018));
        }

        // Check if object cannot be compared to this one
        // (this includes checking for null ) ...
        if (!(this.getClass().isInstance(o))) {
            throw new ClassCastException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0019, o.getClass()));
        }

        // Check if everything else is equal ...
        return compare(this, (AuthorizationPolicy)o);
    }

    /**
     * Utility method to compare two AuthorizationPolicy instances.  Returns a negative integer, zero,
     * or a positive integer as this object is less than, equal to, or greater than
     * the specified object. <p>
     *
     * The comparison is based on the names of the user groups.<p>
     *
     * This method assumes that all type-checking has already been performed. <p>
     *
     * @param obj1 the first policyID to be compared
     * @param obj2 the second policyID to be compared
     * @return -1, 0, +1 based on whether obj1 is less than, equal to, or
     *         greater than obj2
     */
    static int compare(AuthorizationPolicy obj1, AuthorizationPolicy obj2) {

        // Compare policy IDs
        return (obj1.hashCode() == obj2.hashCode()) ? 0 :
                obj1.authorizationPolicyID.compareTo(obj2.authorizationPolicyID);

// JPC 04/25/03 - PolicyIDs are unique so doesn't seem like we need to compare all the rest of this stuff...
//        // Compare policy IDs
//        int compVal = AuthorizationPolicyID.compare(obj1.authorizationPolicyID, obj2.authorizationPolicyID);
//
//        // Compare principals
//        if ( compVal == 0 ) {
//            compVal += obj1.principals.equals(obj2.principals) ? 0 : -1;
//        }
//
//        // Compare permissions -
//        // One must imply all of the other AND other must imply all of the one
//        Iterator permItr = obj2.permissions.iterator();
//        while ( permItr.hasNext() && compVal == 0 ) {
//            AuthorizationPermission perm = (AuthorizationPermission) permItr.next();
//            try {
//                compVal += obj1.permissions.implies(perm) ? 0 : -1;
//            } catch ( MetaBaseResourceNotResolvedException e ) {
//                // Do nothing
//            }
//        }
//        permItr = obj1.permissions.iterator();
//        while ( permItr.hasNext() && compVal == 0 ) {
//            AuthorizationPermission perm = (AuthorizationPermission) permItr.next();
//            try {
//                compVal += obj2.permissions.implies(perm) ? 0 : -1;
//            } catch ( MetaBaseResourceNotResolvedException e ) {
//                // Do nothing
//            }
//        }
//
//        return compVal;
    }

    // =========================================================================
    //                 M O D I F I E R    M E T H O D S
    // =========================================================================

    /**
     * Define the set of description for this policy.
     * @param desc the new description for this policy.
     */
    public void setDescription(String desc) {
        this.authorizationPolicyID.setDescription(desc);
    }

    /**
     * Define the set of permissions that this policy is to apply.  Any existing
     * permissions are removed from the policy.  If the specified set is null,
     * this policy will have no governing permissions.
     * @param permissions the new permissions that this policy applies.
     */
    public void setPermissions(AuthorizationPermissions permissions) {
        if ( permissions != null ) {
            this.permissions = permissions;
        } else {
            this.permissions.clear();
        }
    }

    /**
     * Define the set of <code>MetaMatrixPrincipalName</code>s that this policy applies to.  Any existing
     * <code>MetaMatrixPrincipalName</code>s are removed from the policy.  If the specified set is null or empty,
     * this policy will apply to no principals.
     * @param principals the new set of <code>MetaMatrixPrincipalName</code>s to which this policy applies.
     */
    public void setPrincipals(Set principals) {
        if ( principals != null ) {
            this.principals = new LinkedHashSet(principals);
        } else {
            this.principals.clear();
        }
    }

    /**
     * Add to this policy's set of existing permissions a new permission that is to be applied by the policy.
     * @param permission the new permission that is to be added to this policy.  May not be null.
     * @return true if this policy changed as a result of the addition.
     * @throws IllegalArgumentException if the specified permission is null.
     */
    public boolean addPermission(AuthorizationPermission permission) {
        if ( permission == null ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0020));
        }
        return this.permissions.add(permission);
    }

    /**
     * Add to this policy's set of existing permissions a new set of permissions
     * that are to be applied by the policy.
     * @param permissions the new permissions that are to be added to this policy.  May not be null.
     * @return true if this policy changed as a result of the addition.
     * @throws IllegalArgumentException if the specified permission is null.
     */
    public boolean addAllPermissions(AuthorizationPermissions permissions) {
        if ( permissions == null || permissions.size() == 0 ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0020));
        }
        return this.permissions.add(permissions);
    }

    /**
     * Add to this policy's set of existing permissions a set of additional permissions that are to be applied by the policy.
     * Any permission that is added that has the same resource name as an existing permission overwrites the existing
     * permission.
     * @param permissions the set of new permissions that are to be added to this policy.  May not be null.
     * @return true if this policy changed as a result of the additions.
     * @throws IllegalArgumentException if the specified set of permissions is null or if the set contains a null value.
     */
    public boolean addAllPermissions(Set permissions) {
        if ( permissions == null ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0020));
        }
        if ( permissions.contains(null) ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0021));
        }
        return this.permissions.add(permissions);
    }

    /**
     * Remove from this policy's set of existing permissions the specified permission.  If the permission is not
     * currently in this policy, this method simply returns without performing any operation.
     * @param permission the permission that is to be removed from this policy.
     */
    public void removePermission(AuthorizationPermission permission) {
        this.permissions.remove(permission);
    }

    /**
     * Remove all of this policy's existing permissions.
     */
    public void removePermissions() {
        this.permissions.clear();
        this.permissions = new AuthorizationPermissionsImpl();
    }

    /**
     * Add to this policy's set of existing principals a new <code>MetaMatrixPrincipalName</code> name to whom this policy is to apply.
     * @param principal the new <code>MetaMatrixPrincipalName</code> that is to be added to this policy.  May not be null.
     * @return true if this policy changed as a result of the addition.
     * @throws IllegalArgumentException if the specified principal is null.
     */
    public boolean addPrincipal(MetaMatrixPrincipalName principal) {
        if ( principal == null ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0022));
        }
        return this.principals.add(principal);
    }

    /**
     * Add to this policy's set of existing principals a set of new <code>MetaMatrixPrincipalName</code> to whom this policy is to apply.
     * @param newPrincipals the set of new <code>MetaMatrixPrincipalName</code> that are to be added to this policy.  May not be null.
     * @return true if this policy changed as a result of the additions.
     * @throws IllegalArgumentException if the specified set of principals is null or if the set contains a null value.
     */
    public boolean addAllPrincipals(Set newPrincipals) {
        if ( newPrincipals == null ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0022));
        }
        if ( newPrincipals.contains(null) ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0023));
        }
        return this.principals.addAll(newPrincipals);
    }

    /**
     * Remove from this policy's set of existing principals the specified <code>MetaMatrixPrincipalName</code>.
     * If the principal is not in the existing set of principal names for this policy,
     * this method simply does nothing for that principal.
     * @param principal the <code>MetaMatrixPrincipalName</code> that is to be removed from this policy.
     */
    public void removePrincipal(MetaMatrixPrincipalName principal) {
        this.principals.remove(principal);
    }

    /**
     * Remove from this policy the entire set of existing <code>MetaMatrixPrincipalName</code> references.
     */
    public void clearPrincipals() {
        this.principals.clear();
    }


}




