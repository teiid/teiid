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

import com.metamatrix.platform.security.util.RolePermissionFactory;

public class AuthorizationPolicyID implements Comparable, Serializable {
    public final static char DELIMITER= '!';

    public final static int DESCRIPTION_LEN = 250;

    /**
     * The immutable name for the policy.
     */
    private String name;

    /**
     * The policy description.
     */
    private String description;

    /**
     * Console display name for this policy.
     */
    private String displayName;

    /**
     * Get the <code>AuthorizationRealm</code> this policy
     * belongs in.
     * @return the policy's realm.
     */
    public AuthorizationRealm getRealm() {
        return realm;
    }

    /**
     * The AuthorizationRealm that this policy belongs in
     * (usually - but not limited to - a VDB version).
     */
    private AuthorizationRealm realm;

    /**
     * ctor
     * Meant to be used only by <code>JDBCAuthorizationTransaction</code> to
     * populate <code>AuthorizationPolicyID</code>s when retrieving
     * them from the authorization store.
     * Construct a policy ID with the specified name and description.
     * @param name the identifier (name) for the policy composed of
     * @param description the policy description.
     */
    public AuthorizationPolicyID(String name, String description) {
        parseAndSetName(name);
        this.setDescription(description);
    }

    /**
     * Construct a policy ID that is tied to an <code>AuthorizationRealm</code>.
     * @param theDisplayName the identifier that the Console will display.
     * @param description The policy description - may be <code>null</code>.
     * @param theRealm The <code>AuthorizationRealm</code> this policy should
     * be tied to - may <i>not</i> be <code>null</code>.
     */
    public AuthorizationPolicyID(String theDisplayName, String description, AuthorizationRealm theRealm) {
        if ( theDisplayName == null || theDisplayName.trim().length() == 0 ) {
            throw new IllegalArgumentException(
                    SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0024));
        }
        if ( theDisplayName.indexOf(DELIMITER) >= 0 ) {
            // The display name cannot contain any DELIMETERS
            throw new IllegalArgumentException(
                    SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0025, DELIMITER));
        }
        if ( theRealm == null ) {
            throw new IllegalArgumentException(
                    SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0026));
        }
        if ( theRealm.equals(RolePermissionFactory.getRealm()) ) {
            this.name = theDisplayName;
        } else {
            this.name = formName(theDisplayName, theRealm.getSuperRealmName(), theRealm.getSubRealmName());
        }
        this.realm = theRealm;
        this.displayName = theDisplayName;
        this.setDescription(description);
    }

    /**
     * Construct a policy ID that is tied to a VDB.
     * @param theDisplayName the identifier that the Console will display.
     * @param vdbName The name of the VDB this policy should be tied to.
     * @param vdbVersion The version of the VDB this policy should be tied to.
     */
    public AuthorizationPolicyID(String theDisplayName, String vdbName, int vdbVersion) {
        this(theDisplayName, vdbName, Integer.toString(vdbVersion));
    }

    /**
     * Construct a policy ID that is tied to a VDB.
     * @param theDisplayName the identifier that the Console will display.
     * @param vdbName The name of the VDB this policy should be tied to.
     * @param vdbVersion The version of the VDB this policy should be tied to.
     */
    public AuthorizationPolicyID(String theDisplayName, String vdbName, String vdbVersion) {
        if (theDisplayName == null || theDisplayName.trim().length() == 0) {
            throw new IllegalArgumentException(
                    SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0024));
        }
        if (vdbName == null || vdbName.trim().length() == 0) {
            throw new IllegalArgumentException(
                    SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0027));
        }
        this.displayName = theDisplayName;
        this.realm = new AuthorizationRealm(vdbName, vdbVersion);
        this.name = formName(theDisplayName, vdbName, vdbVersion);
        this.description = ""; //$NON-NLS-1$
    }

    /**
     * Returns the name for the policy.
     * @return the policy's name
     */
    public String getName() {
        return this.name;
    }

    /**
     * Obtain the description for this policy which may be null.
     * @return the description. May be null.
     */
    public String getDescription() {
        return description;
    }

    /**
     * Returns the Console display name for the policy.  May return the same as
     * <code>getName()</code>.
     * @return the Console display name
     */
    public String getDisplayName() {
        return this.displayName;
    }

    /**
     * Returns the Console's VDB name for the policy.  (Console limits policy to one version of one VDB.)
     * May be <code>null</code>.
     * @return The VDB name or <code>null</code> if the policy that this ID represents
     * is not tied to a VDB.
     */
    public String getVDBName() {
        return this.realm.getSuperRealmName();
    }

    /**
     * Return the Console's VDB version for the policy.  (Console limits policy to one version of one VDB.)
     * @return The VDB version or <code>-1</code> if the policy that this ID represents
     * is not tied to a VDB.
     */
    public int getVDBVersion() {
        String vdbVersion = this.realm.getSubRealmName();
        return (vdbVersion == null ? -1 : Integer.parseInt(vdbVersion));
    }

    /**
     * Return the Stringified Console's VDB version for the policy.  (Console limits policy to one
     * version of one VDB.)
     * @return The VDB version or <code>-1</code> if the policy that this ID represents
     * is not tied to a VDB.
     */
    public String getVDBVersionString() {
        String vdbVersion = this.realm.getSubRealmName();
        return (vdbVersion == null ? "-1" : vdbVersion); //$NON-NLS-1$
    }

    /**
     * Define the set of description for this policy.
     * @param desc the new description for this policy.
     */
    public void setDescription(String desc) {
        if ( desc != null ) {
            if (desc.length() > DESCRIPTION_LEN) {
                throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0028, DESCRIPTION_LEN));
            }
            this.description = desc;
        } else {
            this.description = ""; //$NON-NLS-1$
        }
    }

    /**
     * Returns a string representing the current state of the object.
     */
    public String toString() {
        StringBuffer buf = new StringBuffer("Name=<" + this.name); //$NON-NLS-1$
        buf.append("> Realm=<" + this.realm + '>'); //$NON-NLS-1$
        buf.append("> Desc=<" + this.description); //$NON-NLS-1$
        return buf.toString();
    }

    /**
     * Overrides Object hashCode method.
     * @return  a hash code value for this object.
     * @see     Object#hashCode()
     * @see     Object#equals(Object)
     */
    public int hashCode() {
        return this.name.hashCode();
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
        if (obj instanceof AuthorizationPolicyID) {

        	// fail fast on different hash codes
            if (this.hashCode() != obj.hashCode()) {
                return false;
            }

        	// slower comparison
            return compare(this, (AuthorizationPolicyID)obj) == 0;
        }

        // Otherwise not comparable ...
        return false;
    }

    /**
     * Compares this AuthorizationPolicyID to another Object. If the Object is an AuthorizationPolicyID,
     * this function compares the name.  Otherwise, it throws a
     * ClassCastException (as policyID instances are comparable only to
     * other policyID instances).  Note:  this method is consistent with
     * <code>equals()</code>.
     * <p>
     * @param o the object that this instance is to be compared to.
     * @return a negative integer, zero, or a positive integer as this object
     *      is less than, equal to, or greater than the specified object, respectively.
     * @throws ClassCastException if the specified object's type prevents it
     *      from being compared to this AuthorizationPolicyID.
     */
    public int compareTo(Object o) throws ClassCastException {
        // Check if instances are identical ...
        if (this == o) {
            return 0;
        }
        if (o == null) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0029));
        }

        // Check if object cannot be compared to this one
        // (this includes checking for null ) ...
        if (!(o instanceof AuthorizationPolicyID)) {
            throw new ClassCastException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0030, o.getClass()));
        }

        // Check if everything else is equal ...
        return compare(this, (AuthorizationPolicyID)o);
    }

    /**
     * Utility method to compare two policyIDs.  Returns a negative integer, zero,
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
    static int compare(AuthorizationPolicyID obj1, AuthorizationPolicyID obj2) {
        return (obj1.hashCode() == obj2.hashCode()) ? 0 : obj1.name.compareTo(obj2.name);
    }

    public static String parseRealm(AuthorizationRealm aRealm) {
        String superRealmName = aRealm.getSuperRealmName();
        String subRealmName = aRealm.getSubRealmName();
        if ( subRealmName == null ) {
            return superRealmName;
        }
        return formName("", superRealmName, subRealmName); //$NON-NLS-1$
    }

    public static String formName(String displayName, String vdbName, String vdbVersion) {
        String delimiterString = new String(new char[] {DELIMITER});
        StringBuffer name = new StringBuffer();
        if ( displayName != null && displayName.trim().length() > 0 ) {
            name.append(displayName);
        }
        if ( displayName.indexOf('.') < 0 ) {
            if ( vdbName != null && vdbName.trim().length() > 0 ) {
                name.append(delimiterString);
                name.append(vdbName);
            }
            if ( vdbVersion != null && vdbVersion.trim().length() > 0 ) {
                String versionString = vdbVersion;
                // left pad with zeros if needed
                while (versionString.length() < 3) {
                    versionString = "0" + versionString; //$NON-NLS-1$
                }
                name.append(delimiterString);
                name.append(versionString);
            }
        }
        return name.toString();
    }

    /**
     *
     */
    private void parseAndSetName(String idName) {
        this.name = idName;
        int firstDelimiterLoc = idName.indexOf(DELIMITER);
        String superRealmName = ""; //$NON-NLS-1$
        String subRealmName = ""; //$NON-NLS-1$
        if ( firstDelimiterLoc > 0 ) {
            this.displayName = idName.substring(0, firstDelimiterLoc);
            int secondDelimiterLoc = idName.lastIndexOf(DELIMITER);
            if ( secondDelimiterLoc > firstDelimiterLoc + 1 ) {
                superRealmName = idName.substring(firstDelimiterLoc + 1, secondDelimiterLoc);
                if ( secondDelimiterLoc < idName.length() ) {
                    subRealmName = idName.substring(secondDelimiterLoc + 1);
                }
            }
            this.realm = new AuthorizationRealm(superRealmName, subRealmName);
        } else {
            this.displayName = idName;
            this.realm = RolePermissionFactory.getRealm();
        }
    }
}


