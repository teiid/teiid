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

//import com.metamatrix.common.log.LogManager;

//import com.metamatrix.platform.security.util.LogSecurityConstants;

/**
 * Defines a resource associated with a
 * {@link com.metamatrix.platform.security.api.BasicAuthorizationPermission BasicAuthorizationPermission}.
 */
public class DataAccessResource implements AuthorizationResource, Serializable {

    // --------------------------------------------------
    // Static constants related to the naming lexicon ...
    // --------------------------------------------------
//    private static final boolean IGNORE_CASE             = false;
    private static final String SEPARATOR                = "."; //$NON-NLS-1$
    public static final String RECURSIVE                 = "*"; //$NON-NLS-1$
    private static final String ALL_NODES                = RECURSIVE;
    public static final String SEPARATOR_WITH_RECURSIVE  = SEPARATOR + RECURSIVE;

    // The resource name
    private String name;
    // The resource's canonical name
    private String canonicalName;
    // Is this a recursive resource?
    private boolean isRecursive;

    /**
     * ctor
     * @param name The resource name
     */
    public DataAccessResource(String name) {
        this.name = name;
        init(name);
    }

    /**
     * Overrides method defined in <code>Object</code>.
     * @return The hashCode of this object.
     */
    public int hashCode() {
        return name.hashCode();
    }

    /**
     * Overrides method defined in <code>Object</code>.
     * Note:  this method is consistent with <code>compareTo()</code>.
     * @param obj The <code>Object</code> to compare.
     * @return <code>true</code> if two DataAccessResource instances are semantically equal.
     */
    public boolean equals(Object obj) {
        // Check if instances are identical ...
        if (this == obj) {
            return true;
        }

        // Check if object can be compared to this one
        // (this includes checking for null ) ...
        if (obj instanceof DataAccessResource) {
            return this.name.equals(((DataAccessResource)obj).name);
        }
        return false;
    }

    /**
     * Are these resources equal exception for recursion?
     * @param resource The resource to compare with this one disregarding recursion.
     * @return <code>true</code> if these two resources differ only be recursion.
     */
    public boolean isCannonicallyEquivalent(AuthorizationResource resource) {
        if (resource instanceof DataAccessResource) {
            return this.canonicalName.equals(((DataAccessResource)resource).canonicalName);
        }
        return false;
    }

    /**
     * Get the resource name.
     * @return The resource name.
     */
    public String getName() {
        return name;
    }

    /**
     * Get the identifier of this resource. <i>Will not</i> be <code>null</code>.
     * This is the identifier used to store and retrieve this resource from
     * the Authorization store.
     * @return The resource identifier.
     */
    public String getID() {
        return name;
    }

    /**
     * Get the UUID of this resource. May be <code>null</code>.
     * This is a payload of UUID for MetaBase authorization code.
     * @return The resource's UUID, if present, else <code>null</code>.
     */
    public String getUUID() {
        return null;
    }

    /**
     * Get the canonical name for this resource - used internally for comparing.
     * @return The resource's canonical name.
     */
    public String getCanonicalName() {
        return canonicalName;
    }

    /**
     * Determine if the Actions applies to this resource should be
     * applied recursively to sub resources.
     * @return Whether the actions are to be applied recursivly.
     */
    public boolean isRecursive() {
        return isRecursive;
    }

    /**
     * Package level method for use by the permission instance to
     * specify wheather this resoruce is recursive.
     * @param recursive
     */
    void setRecursive(boolean recursive) {
        isRecursive = recursive;
    }

    /**
     * Compares this DataAccessResource to another Object. If the Object is an DataAccessResource,
     * this function compares the name.  Otherwise, it throws a
     * ClassCastException (as DataAccessResource instances are comparable only to
     * other DataAccessResource instances).  Note:  this method is consistent with
     * <code>equals()</code>.
     * <p>
     * @param o the object that this instance is to be compared to.
     * @return a negative integer, zero, or a positive integer as this object
     *      is less than, equal to, or greater than the specified object, respectively.
     * @throws ClassCastException if the specified object's type prevents it
     *      from being compared to this DataAccessResource.
     */
    public int compareTo(Object o) throws ClassCastException {
        // Check if instances are identical ...
        if (this == o) {
            return 0;
        }
        if (o == null) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0043));
        }

        // Check if object cannot be compared to this one
        // (this includes checking for null ) ...
        if (!(o instanceof DataAccessResource)) {
            throw new ClassCastException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0044, o.getClass()));
        }

        // Check if everything else is equal ...
        return this.name.compareTo(((DataAccessResource)o).name);
    }

    /**
     * Does this resource <i>"imply"</i> another?
     */
    public boolean implies(AuthorizationResource thatResource) {
        if ( !(thatResource instanceof DataAccessResource) ) {
            return false;
        }
        DataAccessResource that = (DataAccessResource) thatResource;
// DEBUG:
//System.out.println(" *** implies: Permission is recursive? " + this.isRecursive);
        if ( isRecursive ) {
            // A recursive group implies access to its element
             if ( that.canonicalName.startsWith(this.canonicalName) ) {
// DEBUG:
//System.out.println(" *** implies: Permission is recursive and implied.");
//                LogManager.logTrace( LogSecurityConstants.CTX_AUTHORIZATION, "implies(): Recursive perm implies that: this [" +
//                    this.canonicalName + "] => that [" + that.canonicalName + "]");
                return true;
             }
        } else if ( ! that.isRecursive ) {
// DEBUG:
//System.out.println(" *** implies: Permission is NOT recursive.");
            // If this perms resource is an element of the requested perms resource (a group),
            // enforce policy that ANY element entitles its group.
            int lastSepIndex = this.canonicalName.lastIndexOf(SEPARATOR);
            if ( lastSepIndex > 0 && this.canonicalName.substring(0, lastSepIndex).equals(that.canonicalName) ) {
// DEBUG:
//System.out.println(" *** implies: Permission is NOT recursive and is implied.");
                return true;
            }
        }

//        LogManager.logTrace( LogSecurityConstants.CTX_AUTHORIZATION, "implies(): These resources equal? this [" +
//                this.canonicalName + "] == that [" + that.canonicalName + "]");
        return this.canonicalName.equals(that.canonicalName);
    }

    /**
     * This method is invoked by the constructors that take a string resource name, and is
     * to strip out any recursive or wildcard characters and return simple the name of the
     * node.
     */
    private void init( String resourceName ) {

        // If the resource name is the ALL_NODES resource ...
        if ( resourceName.equals(ALL_NODES) ) {
            isRecursive = true;
            this.canonicalName = "";      // resource name should be nothing //$NON-NLS-1$
        }

        // If the resource name includes the recursive parameter ...
        if ( resourceName.endsWith(SEPARATOR_WITH_RECURSIVE) ) {
            isRecursive = true;
            this.canonicalName = resourceName.substring(0, resourceName.length()-2);
        } else if ( resourceName.endsWith(RECURSIVE) ) {
            isRecursive = true;
            this.canonicalName = resourceName.substring(0, resourceName.length()-1);
        } else {
            // overkill since it is initialized to false, but
            // commented out here to remind us...
            //isRecursive = false;
            this.canonicalName = resourceName;
        }
        this.canonicalName = this.canonicalName.toLowerCase();
    }

    public String toString() {
        return name;
    }
}
