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

/**
 * An authorization permission defines access permissions for a particular resource.  A rule determines what
 * action or actions can be performed on a resource, and is comprised of the resource,
 * the actions allowed, and an optional content modifier (which defines an additional criteria
 * that is to be placed upon the usage of the resource).
 */
public abstract class AuthorizationPermission implements Comparable, Serializable {

    /**
     * The default action for rules is NONE.
     */
    public static final AuthorizationActions DEFAULT_ACTIONS = StandardAuthorizationActions.NONE;

    /**
     * The resource of this permission; generally the name of the resource to which this rule applies.
     * This is a required attributed.
     */
    protected AuthorizationResource resource;

    /**
     * The realm in which this permission belongs.
     */
    private AuthorizationRealm realm;

    /**
     * The name of the factory that can create this permission.
     */
    private String factoryClassName;

    /**
     * The optional content modifier that should be used as an additional criteria for queries to this resource.
     */
    private String contentModifier;

    /**
     * The cached value of the hash code for this object.
     */
    protected int hashCode;
    protected int PRIME = 1000003;

    private AuthorizationActions actions;

    /**
     * Create a new authorization rule for the specified resource.
     * @param resource the resource to which this permission applies.
     * @param realm the name of the realm for this rule (may not be null, but may be empty)
     * @param actions the actions to apply to the resource
     * @param contentModifier the content modifier (may be null)
     * @param factoryClassName The factory class name that can instantiate this permission (may not be null)
     */
    protected AuthorizationPermission(AuthorizationResource resource, AuthorizationRealm realm, AuthorizationActions actions, String contentModifier, String factoryClassName) {
        if ( factoryClassName == null || factoryClassName.trim().length() == 0 ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0013));
        }
        if ( resource == null ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0014));
        }
        this.resource = resource;
        this.realm = realm;
        this.contentModifier = contentModifier;
        this.actions = ( actions != null ? actions : DEFAULT_ACTIONS );
        this.factoryClassName = factoryClassName;
        this.hashCode = this.computeHashCode();
    }

    /**
     * Create a new authorization rule for the specified resource.
     * @param resource The new resource
     * @param realm the name of the realm for this rule (may not be null, but may be empty)
     * @param factoryClassName The factory class name that can instantiate this permission (may not be null)
     */
    protected AuthorizationPermission(AuthorizationResource resource, AuthorizationRealm realm, String factoryClassName) {
        this(resource, realm, DEFAULT_ACTIONS, null, factoryClassName);
    }

    /**
     * Create a new authorization rule for the specified resource.
     * @param resource The new resource
     * @param realm the name of the realm for this rule (may not be null, but may be empty)
     * @param actions the actions for the resource
     * @param factoryClassName The factory class name that can instantiate this permission (may not be null)
     */
    protected AuthorizationPermission(AuthorizationResource resource, AuthorizationRealm realm, AuthorizationActions actions, String factoryClassName) {
        this(resource, realm, actions, null, factoryClassName);
    }

    /**
     * Make a deep copy of this object.
     * @return The newly copied object.
     */
    protected abstract Object clone() throws CloneNotSupportedException;

    /**
     * Obtain the name of the factory class for this rule.
     * @return the factory class name
     */
    public String getFactoryClassName() {
        return factoryClassName;
    }

    /**
     * Determin if this permission applies to all subnode resources.
     * @return <code>true</code> if this permission's resource is a node in
     * a subtree and the allowed Action applies to all subnodes, <code>false</code>
     * otherwise.
     */
    public boolean resourceIsRecursive() {
        return resource.isRecursive();
    }

    /**
     * Obtain the name of the resource for this rule.
     * @return the resource name
     */
    public String getResourceName() {
        return resource.getID();
    }

    /**
     * Obtain the resource object for this rule.
     * @return the resource
     */
    public AuthorizationResource getResource() {
        return resource;
    }

    /**
     * Determine whether this rule has a content modifier that should be used upon access to the resource.
     * @return true if this rule has a content modifier
     */
    public boolean hasContentModifier() {
        return this.contentModifier != null;
    }

    /**
     * Get the content modifier for this rule.  The content modifier should be used upon access to the resource.
     * @return the content modifier
     */
    public String getContentModifier() {
        return contentModifier;
    }

    /**
     * Get the name of the realm in which this permission belongs.
     * @return The realm name for this permission
     */
    public String getRealmName() {
        return realm.getRealmName();
    }

    /**
     * Get the <code>AuthorizationRealm</code> in which this permission belongs.
     * @return The realm for this permission
     */
    public AuthorizationRealm getRealm() {
        return realm;
    }

    /**
     * Set the realm in which this permission belongs.
     * @param realm The realm in which this permission should belong.
     */
    void setRealm(AuthorizationRealm realm) {
        this.realm = realm;
    }

    /**
     * Get the operations that the user may perform on the resource, as defined by this rule.
     * @return this rule's actions
     */
    public AuthorizationActions getActions() {
        return actions;
    }

    /**
     * Checks if the specified resource is ipmlied by this resource instance.
     * @param resource the AuthorizationPermission instance to be checked
     * @return true if the specified resource is implied by this object, false if not
     * @throws IllegalArgumentException if the specified resource is null or incomplete.
     */
    public abstract boolean implies(AuthorizationPermission resource);

    /**
     * Returns a string representing the current state of the object.
     */
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("[Realm=<"); //$NON-NLS-1$
        sb.append(this.realm);
        sb.append("> Resource=<"); //$NON-NLS-1$
        sb.append(this.resource.getID());
        sb.append("> Actions=<"); //$NON-NLS-1$
        sb.append(this.actions);
        sb.append("> Factory=<"); //$NON-NLS-1$
        sb.append(this.factoryClassName);
        sb.append(">]"); //$NON-NLS-1$
        return sb.toString();
    }

    /**
     * Overrides Object hashCode method.
     * @return  a hash code value for this object.
     * @see     Object#hashCode()
     * @see     Object#equals(Object)
     */
    public int hashCode() {
        return this.hashCode;
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
        if(obj instanceof AuthorizationPermission){
               return compare(this, (AuthorizationPermission)obj) == 0;
        }

        // Otherwise not comparable ...
        return false;
    }

    /**
     * Compares this AuthorizationPermission to another Object. If the Object is an AuthorizationPermission,
     * this function compares the name.  Otherwise, it throws a
     * ClassCastException (as policyID instances are comparable only to
     * other AuthorizationPermission instances).  Note:  this method is consistent with
     * <code>equals()</code>.
     * <p>
     * @param o the object that this instance is to be compared to.
     * @return a negative integer, zero, or a positive integer as this object
     *      is less than, equal to, or greater than the specified object, respectively.
     * @throws ClassCastException if the specified object's type prevents it
     *      from being compared to this AuthorizationPermission.
     */
    public int compareTo(Object o) throws ClassCastException {
        // Check if instances are identical ...
        if (this == o) {
            return 0;
        }
        if (o == null) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0015));
        }

        // Check if object cannot be compared to this one
        // (this includes checking for null ) ...
        if (!(o instanceof AuthorizationPermission)) {
            throw new ClassCastException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0016));
        }

        // Check if everything else is equal ...
        return compare(this, (AuthorizationPermission)o);
    }

    /**
     * Utility method to compare two AuthorizationPermission instances.  Returns a negative integer, zero,
     * or a positive integer as this object is less than, equal to, or greater than
     * the specified object. <p>
     *
     * Subclasses may not override this method because it is designed to enforce a
     * constraint placed on <emph>all</emph> <code>AuthorizationPermission</code>s.<p>
     *
     * This method assumes that all type-checking has already been performed. <p>
     *
     * @param obj1 the first policyID to be compared
     * @param obj2 the second policyID to be compared
     * @return -1, 0, +1 based on whether obj1 is less than, equal to, or
     *         greater than obj2
     */
    public static final int compare(AuthorizationPermission obj1, AuthorizationPermission obj2) {
        // Because the hash codes were computed using the attributes,
        // returning the difference in the hash code values will give a
        // consistent (but NOT lexicographical) ordering for both equals and compareTo.

        // If the hash codes are different, then simply return the difference
        // (this will probably be the case in most invocations) ...
        if (obj1.hashCode !=  obj2.hashCode ) {
            return obj1.hashCode - obj2.hashCode;
        }

        // If the hash codes are the same, then the resource names should be the same, so
        // so start comparing the rest of the attributes, starting with the most simplistic
        int resourceDiff = obj1.resource.compareTo(obj2.resource);
        if ( resourceDiff != 0 ) {
            return resourceDiff;
        }

        int actionDiff = obj1.actions.compareTo(obj2.actions);
        if ( actionDiff != 0) {
            return actionDiff;
        }

        int realmDiff = obj1.realm.compareTo(obj2.realm);
        if ( realmDiff != 0) {
            return realmDiff;
        }

        if (obj1.contentModifier == null && obj2.contentModifier == null ) {
            return 0;       // must compare both to return '0'
        }

        if ( obj1.contentModifier != null ) {
            return obj1.contentModifier.compareTo(obj2.contentModifier);
        }
        return obj2.contentModifier.compareTo(obj1.contentModifier);
    }

    /**
     * Compute the hash code value.
     */
    private int computeHashCode() {
        int result = 0;
        result = PRIME * result + this.actions.hashCode();
        result = PRIME * result + this.resource.hashCode();
        result = PRIME * result + this.realm.hashCode();
        if ( this.contentModifier != null ) {
            result = PRIME * result + this.contentModifier.hashCode();
        }
        return result;
    }

}





