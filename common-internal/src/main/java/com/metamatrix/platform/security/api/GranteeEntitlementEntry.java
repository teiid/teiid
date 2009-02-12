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
 * This object contains one entry for an entitlement entry in {@link UserEntitlementInfo}.
 * It comprises the triplet of <i>Grantee</i> the <i>Grantor</i> that assigned him the
 * <i>Allowed Actions</i> and the <i>Allowed Actions</i> on the <i>Resource</i> of the
 * entitlement.
 *
 * <p>This is not a standalone object.  These objects are returned as elements of
 * {@link UserEntitlementInfo#iterator}.  Specifically, the <i>Resource</i> and VDB name and
 * version are constant over iteration and are contained in {@link UserEntitlementInfo}.</p>
 */
public final class GranteeEntitlementEntry implements Serializable, Comparable {
    private MetaMatrixPrincipalName grantee;
    private String grantor;
    private AuthorizationActions allowedActions;
    // Identity
    private String identifier;

    /**
     * <br>ctor.</br>
     * Used when creating in the Authorization JDBC layer.
     * @param grantee
     * @param grantor
     * @param alloweActions
     */
    public GranteeEntitlementEntry(MetaMatrixPrincipalName grantee, String grantor, int allowedActions) {
        this.grantee = grantee;
        this.grantor = grantor;
        this.allowedActions = StandardAuthorizationActions.getAuthorizationActions(allowedActions);
        this.generateIdentity();
    }

    /**
     * <br>ctor.</br>
     * Uses another GranteeEntitlementEntry as a pattern to clone only changing the <i>Grantee</i>.
     * Used specifically when <i>clone</i> is a user group and grantee was determined to be one of
     * the group members.
     * @param grantee The new principal this object will represent.
     * @param clone All other inforamtion comes from this clone.
     */
    public GranteeEntitlementEntry(MetaMatrixPrincipalName grantee, GranteeEntitlementEntry clone) {
        this.grantee = grantee;
        this.grantor = clone.grantor;
        this.allowedActions = clone.allowedActions;
        this.generateIdentity();
    }

    /**
     * Get the <i>Grantee</i>.
     * @return The <i>Grantee</i>.
     */
    public String getGrantee() {
        return grantee.getName();
    }

    /**
     * Does this <i>Grantee</i> represent a user group?  If not, it's a user.
     * @return <code>true</code> if the <i>Grantee</i> represents a user group,
     * <code>false</code> if it's of type user.
     */
    public boolean isUserGroup() {
        return grantee.getType() == MetaMatrixPrincipal.TYPE_GROUP;
    }

    /**
     * Get the <i>Grantor</i>.
     * @return The <i>Grantor</i>.
     */
    public String getGrantor() {
        return grantor;
    }

    /**
     * Get the <i>Allowed Actions</i>.
     * @return The <i>Allowed Actions</i>.
     */
    public String[] getAllowedActions() {
        return allowedActions.getLabels();
    }

    /**
     * Overrides Object method of the same name
     */
    public int hashCode() {
        return this.identifier.hashCode();
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
        if (obj instanceof GranteeEntitlementEntry) {
            GranteeEntitlementEntry that = (GranteeEntitlementEntry) obj;

            return compare(this, that) == 0;
        }

        // Otherwise not comparable ...
        return false;
    }

    /**
     * Compares this GranteeEntitlementEntry to another Object. If the Object is an GranteeEntitlementEntry,
     * this function compares the name.  Otherwise, it throws a
     * ClassCastException (as GranteeEntitlementEntry instances are comparable only to
     * other GranteeEntitlementEntry instances).  Note:  this method is consistent with
     * <code>equals()</code>.
     * <p>
     * @param obj the object that this instance is to be compared to.
     * @return a negative integer, zero, or a positive integer as this object
     *      is less than, equal to, or greater than the specified object, respectively.
     * @throws ClassCastException if the specified object's type prevents it
     *      from being compared to this GranteeEntitlementEntry.
     */
    public int compareTo(Object o) throws ClassCastException {
        // Check if instances are identical ...
        if (this == o) {
            return 0;
        }
        if (o == null) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0045));
        }

        // Check if object cannot be compared to this one
        // (this includes checking for null ) ...
        if (!(o instanceof GranteeEntitlementEntry)) {
            throw new ClassCastException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0046, o.getClass()));
        }

        // Check if everything else is equal ...
        return compare(this, (GranteeEntitlementEntry)o);
    }

    /**
     * String representation of object.
     */
    public String toString() {
        StringBuffer buff = new StringBuffer(this.grantee.getName());
        buff.append(", "); //$NON-NLS-1$
        buff.append(this.grantor);
        buff.append(", {"); //$NON-NLS-1$
        String[] actions = this.allowedActions.getLabels();
        for ( int i=0; i<actions.length; i++ ) {
            buff.append(actions[i] + ", "); //$NON-NLS-1$
        }
        buff.setLength(buff.length() -2);
        buff.append("}"); //$NON-NLS-1$
        return buff.toString();
    }

    /**
     * Possibly replace actions with the logical OR of these actions
     * already in place and the given actions.
     * @param newActions The <code>AuthorizationActions</code> that may be added.
     */
    void addActions(AuthorizationActions newActions) {
        if (! this.allowedActions.implies(newActions) ) {
            this.allowedActions = StandardAuthorizationActions.getORedActions(newActions, this.allowedActions);
            this.generateIdentity();
        }
    }

    /**
     * Get the <i>Allowed Actions</i>.
     * @return The <i>Allowed Actions</i>.
     */
    AuthorizationActions getActions() {
        return allowedActions;
    }

    /**
     * Utility method to compare two GranteeEntitlementEntries.  Returns a negative integer, zero,
     * or a positive integer as this object is less than, equal to, or greater than
     * the specified object. <p>
     *
     * The comparison is based on the names of the user groups.<p>
     *
     * This method assumes that all type-checking has already been performed. <p>
     *
     * @param obj1 the first GranteeEntitlementEntry to be compared
     * @param obj2 the second GranteeEntitlementEntry to be compared
     * @return -1, 0, +1 based on whether obj1 is less than, equal to, or
     *         greater than obj2
     */
    static int compare(GranteeEntitlementEntry obj1, GranteeEntitlementEntry obj2) {
        return obj1.identifier.compareTo(obj2.identifier);
    }

    /**
     * Generate and set the immutable hashCode and the String identifier for this object.
     */
    private void generateIdentity() {
        // Gen ID String for comparing
        StringBuffer idBuff = new StringBuffer(this.grantee.getName());
        idBuff.append(this.grantor);
        String[] actions = this.allowedActions.getLabels();
        for ( int i=0; i<actions.length; i++ ) {
            idBuff.append(actions[i]);
        }
        this.identifier = idBuff.toString();
    }
}
