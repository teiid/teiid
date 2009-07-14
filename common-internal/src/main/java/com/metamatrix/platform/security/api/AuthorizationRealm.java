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
import java.util.List;

import com.metamatrix.core.util.StringUtil;

/**
 * This class contains the realm and sub realm for an <code>AuthorizationPermission</code>.<br>
 * It may be composed of two divisions - a super realm and a sub realm.  The super
 * realm is required and is a major grouping for <code>AuthorizationPermission</code>s.
 * The sub realm is an optional and arbitrary tag that subdivides the super realm.<br>
 * This object will not change during the life of an <code>AuthorizationPermission</code>.
 */
public class AuthorizationRealm implements Comparable, Serializable {

    private static final String REALM_DELIMITER = "."; //$NON-NLS-1$
    private static final int NUMBER_OF_REALM_COMPONENTS = 2;

    // The Realm
    private String superRealmName;

    // The Sub Realm
    private String subRealmName;

    // Realm description
    private String description;

    /** Largest allowable description. Descriptions longer will be truncated. */
    public static final int MAX_DESCRIPTION_LEN = 550;

    /**
     * The cached value of the hash code for this object.
     */
    private int hashCode;

    /**
     * ctor
     * Meant to be used only by <code>JDBCAuthorizationTransaction</code> to
     * populate <code>AuthorizationPermission</code>s with their realm when retrieving
     * them from the database.
     * @param realmName The name of the realm to which an AuthorizationPermission
     * belongs - May not be null. May be the combined super and sub realm names,
     * in which case it will be parsed correctly.  Must contain no more than 2
     * realm components.
     * @throws IllegalArgumentException if <code>realmName</code> contains too many
     * components.
     */
    public AuthorizationRealm(String realmName) {
        if ( realmName == null || realmName.trim().length() == 0 ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0031));
        }
        if ( realmName.indexOf(REALM_DELIMITER) >= 0 ) {
            List realms = StringUtil.split(realmName, REALM_DELIMITER);
            if ( realms.size() > NUMBER_OF_REALM_COMPONENTS ) {
                throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0032, realmName));
            }

            init((String)realms.get(0), (String)realms.get(1), null);
        } else {
            init(realmName, null, null);
        }
    }

    /**
     * ctor
     * Must have at least a superRealmName and may have a subRealmName.
     * @param superRealmName The name of the realm to which an AuthorizationPermission
     * belongs - May not be null.
     * @param subRealmName An arbitrary subdivision of the given realm - May be null.
     * @throws IllegalArgumentException if <code>realmName</code> contains too many
     * components.
     */
    public AuthorizationRealm(String superRealmName, String subRealmName) {
        this(superRealmName, subRealmName, null);
    }

    /**
     * ctor
     * Must have at least a superRealmName and may have a subRealmName.
     * @param superRealmName The name of the realm to which an AuthorizationPermission
     * belongs - May not be null.
     * @param subRealmName An arbitrary subdivision of the given realm - May be null.
     * @param description May be null.
     * @throws IllegalArgumentException if <code>superRelamName</code> is null or empty
     * or if either superRealmName or subRealmName contain a realm delimeter char '.'.
     */
    public AuthorizationRealm(String superRealmName, String subRealmName, String description) {
        if ( superRealmName == null || superRealmName.trim().length() == 0 ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0031));
        }
        if ( superRealmName.indexOf(REALM_DELIMITER) >= 0 ||
            (subRealmName != null && subRealmName.indexOf(REALM_DELIMITER) >= 0) ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0033,
                    new Object[] {superRealmName, subRealmName}));
        }
        init(superRealmName, subRealmName, description);
    }

    /**
     * Must have at least a realmName and may have a subRealmName.
     * @param superRealmName The name of the realm to which an AuthorizationPermission
     * belongs - May not be null.
     * @param subRealmName An arbitrary subdivision of the given realm - May be null.
     */
    private void init(String superRealmName, String subRealmName, String description) {
        this.superRealmName = superRealmName;

        if ( subRealmName != null && subRealmName.trim().length() > 0 ) {
            // Remove left zero padding if needed
            char[] chars = subRealmName.toCharArray();
            int nonZeroIndex = 0;
            while ( chars[nonZeroIndex] == '0' && nonZeroIndex < chars.length ) {
                nonZeroIndex++;
            }
            if ( nonZeroIndex >= chars.length ) {
                throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0034));
            }
            this.subRealmName = subRealmName.substring(nonZeroIndex);
        }

        setDescription(description);

        hashCode = getRealmName().hashCode();
    }

    /**
     * Get the full realm name.
     * @return The full realm name.
     */
    public String getRealmName() {
        StringBuffer realm = new StringBuffer(this.superRealmName);
        if ( this.subRealmName != null ) {
            realm.append(REALM_DELIMITER);
            realm.append(this.subRealmName);
        }
        return realm.toString();
    }

    /**
     * Get the super realm name.
     * @return The realm name.
     */
    public String getSuperRealmName() {
        return this.superRealmName;
    }

    /**
     * Get the sub realm name.
     * @return The sub realm name - <strong>May be null</strong>.
     */
    public String getSubRealmName() {
        return this.subRealmName;
    }

    /**
     * Get the realm description.
     * @return The realm description.
     */
    public String getDescription() {
        return this.description;
    }

    /**
     * Set the realm description. The description argument will be ignored if
     * it's null or empty.
     * @param description The realm description.
     */
    public void setDescription(String description) {
        if ( description != null && description.trim().length() > 0 ) {
            this.description = StringUtil.truncString(description, MAX_DESCRIPTION_LEN);
        }
    }

    /**
     * Override Object method.
     */
    public String toString() {
        return getRealmName();
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
        if(obj instanceof AuthorizationRealm){
               return compare(this, (AuthorizationRealm)obj) == 0;
        }

        // Otherwise not comparable ...
        return false;
    }

    /**
     * Compares this AuthorizationRealm to another Object. If the Object is an AuthorizationRealm,
     * this function compares the name.  Otherwise, it throws a
     * ClassCastException (as AuthorizationRealm instances are comparable only to
     * other AuthorizationRealm instances).  Note:  this method is consistent with
     * <code>equals()</code>.
     * <p>
     * @param o the object that this instance is to be compared to.
     * @return a negative integer, zero, or a positive integer as this object
     *      is less than, equal to, or greater than the specified object, respectively.
     * @throws ClassCastException if the specified object's type prevents it
     *      from being compared to this AuthorizationRealm.
     */
    public int compareTo(Object o) throws ClassCastException {
        // Check if instances are identical ...
        if (this == o) {
            return 0;
        }
        if (o == null) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0035));
        }

        // Check if object cannot be compared to this one
        // (this includes checking for null ) ...
        if (!(o instanceof AuthorizationRealm)) {
            throw new ClassCastException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0036, o.getClass()));
        }

        // Check if everything else is equal ...
        return compare(this, (AuthorizationRealm)o);
    }

    /**
     * Utility method to compare two AuthorizationRealm instances.  Returns a negative integer, zero,
     * or a positive integer as this object is less than, equal to, or greater than
     * the specified object. <p>
     *
     * Subclasses may not override this method because it is designed to enforce a
     * constraint placed on <emph>all</emph> <code>AuthorizationRealm</code>s.<p>
     *
     * This method assumes that all type-checking has already been performed. <p>
     *
     * @param obj1 the first policyID to be compared
     * @param obj2 the second policyID to be compared
     * @return -1, 0, +1 based on whether obj1 is less than, equal to, or
     *         greater than obj2
     */
    static public final int compare(AuthorizationRealm obj1, AuthorizationRealm obj2) {
        return obj1.getRealmName().toLowerCase().compareTo(obj2.getRealmName().toLowerCase());
    }
}
