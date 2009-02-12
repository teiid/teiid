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

package com.metamatrix.platform.security.util;

import com.metamatrix.platform.security.api.AuthorizationActions;
import com.metamatrix.platform.security.api.AuthorizationPermission;
import com.metamatrix.platform.security.api.AuthorizationRealm;
import com.metamatrix.platform.security.api.AuthorizationResource;
import com.metamatrix.platform.security.api.SecurityMessagesKeys;
import com.metamatrix.platform.security.api.SecurityPlugin;
import com.metamatrix.platform.security.api.StandardAuthorizationActions;

/**
 * A RolePermission defines access permissions for Metadata resources (i.e., MetadataID instances).
 * This class extends the AuthorizationPermission abstract class and provides specialized
 * <code>implies</code>, <code>equals</code>, and <code>compareTo</code> method implementations.
 * <p>
 */
public class RolePermission extends AuthorizationPermission implements Cloneable {

    // -------------------------------------------------------------------
    // Public actions commonly associated with RolePermission instances ...
    // -------------------------------------------------------------------

    /**
     * Constant AuthorizationAction that allows management-related privileges to the Metadata resource(s)
     * (i.e., create, read, update and delete of the Metadata metadata resources) <i>and</i>
     * access to the data in the corresponding data source.
     */
    private static final AuthorizationActions ALL = StandardAuthorizationActions.ALL;

    /**
     * Create a new Metadata authorization permission for the specified resource.
     * @param resource the new resource name
     * @param realm the realm into which this role belongs
     */
    RolePermission(AuthorizationResource resource, AuthorizationRealm realm, String factoryClassName) {
        super( resource, realm, ALL, factoryClassName);
    }

    /**
     * Make a deep copy of this object.
     * @return The newly copied object.
     */
    protected Object clone() throws CloneNotSupportedException {
        return new RolePermission(this.getResource(),
                                  this.getRealm(),
                                  this.getFactoryClassName());
    }

    /**
     * Roles are not recursive.
     * @return <code>false</code> allways.
     */
    public boolean resourceIsRecursive() {
        return false;
    }

    /**
     * Determine whether the <code>official</code> AuthorizationPermission instance <i>implies</i>
     * the access requested by the <code>request</code> AuthorizationPermission instance.
     * <p>
     * The requested request is implied by the official request if <b><i>all</i></b> of the following
     * conditions are satisfied:
     * <p>
     * <li>the realm name of each request instance must be equivalent (since the realm
     * names are String instances, this is evaluated using the <code>String.equals()</code> method);</li>
     * <li>all of the actions of the requested request must be included in the actions
     * of the official request (@see AuthorizationActions.implies);</li>
     * <li>the number of atomic names in the official resource must be equal to or greater
     * than the number of atomic names in the requested resource;</li>
     * <li>if there is a recursive parameter in both resource names, and the
     * requested resource name is more restrictive than the official resource name;</li>
     * <li>if there is a recursive parameter in the official resource but not the
     * requested resource or neither resource contains a recursive parameter, and
     * the requested resource is contained in its entirety within the official resource
     * name (starting at the beginning of the official resource).</li>
     * Note: the content modifier of the permissions is not used in this algorithm.
     * <P>
     * @param request the request that is being requested or attempted, and which is in question
     * by the caller
     * @return true if the official request does imply access to the resource(s) specified
     * by the request
     */
    public boolean implies( AuthorizationPermission request ) {
        if (!(request instanceof RolePermission)) {
            return false;
        }

        // No actions to compare ...

        // ---------------------------
        // Compare the resource(s) ...
        // ---------------------------
	    RolePermission that = (RolePermission) request;
        return this.getResourceName().equals(that.getResourceName());
    }

    public boolean equals(Object obj) {
        // Check if instances are identical ...
        if (this == obj) {
            return true;
        }

        // Check if object can be compared to this one
        // (this includes checking for null ) ...
        if (obj instanceof RolePermission) {

            // slower comparison
            return compare(this, (RolePermission)obj) == 0;
        }

        // Otherwise not comparable ...
        return false;
    }

    public int compareTo(Object o) throws ClassCastException {
        // Check if instances are identical ...
        if (this == o) {
            return 0;
        }
        if (o == null) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_UTIL_0001));
        }

        // Check if object cannot be compared to this one
        // (this includes checking for null ) ...
        if (!(o instanceof RolePermission)) {
            throw new ClassCastException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_UTIL_0002, o.getClass()));
        }

        // Check if everything else is equal ...
        return compare(this, (RolePermission)o);
    }

}


