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

import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.LogConstants;

/**
 * A BasicAuthorizationPermission defines access permissions for Metadata resources (i.e., MetadataID instances).
 * This class extends the AuthorizationPermission abstract class and provides specialized
 * <code>implies</code>, <code>equals</code>, and <code>compareTo</code> method implementations.
 * <p>
 */
public class BasicAuthorizationPermission extends AuthorizationPermission implements Cloneable {

    public static final String RECURSIVE                 = DataAccessResource.RECURSIVE;
    public static final String SEPARATOR_WITH_RECURSIVE  = DataAccessResource.SEPARATOR_WITH_RECURSIVE;

    /**
     * Create a new Metadata authorization permission for the specified resource.
     * @param resource the resource
     * @param realm the name of the realm for this rule (may not be null, but may be empty)
     * @param actions the actions for the resource
     * @param contentModifier the content modifier (may be null)
     */
    BasicAuthorizationPermission(AuthorizationResource resource, AuthorizationRealm realm, AuthorizationActions actions, String contentModifier, String factoryClassName) {
        super( resource, realm, actions, contentModifier, factoryClassName);
    }

    /**
     * Create a new Metadata authorization permission for the specified resource.
     * @param resource the new resource
     * @param realmName the name of the realm for this rule (may not be null, but may be empty)
     * @param factoryClassName the name of the factory class used to create this permissions (may not be null, but may be empty)
     */
    BasicAuthorizationPermission(AuthorizationResource resource, AuthorizationRealm realmName, String factoryClassName) {
        super( resource, realmName, factoryClassName);
    }

    /**
     * Create a new Metadata authorization permission for the specified resource.
     * @param resource the new resource name
     * @param realm the name of the realm for this rule (may not be null, but may be empty)
     * @param actions the actions for the resource
     */
    BasicAuthorizationPermission(AuthorizationResource resource, AuthorizationRealm realm, AuthorizationActions actions, String factoryClassName) {
        super( resource, realm, actions, factoryClassName);
    }

    /**
     * Make a deep copy of this object.
     * @return The newly copied object.
     */
    protected Object clone() throws CloneNotSupportedException {
        return new BasicAuthorizationPermission(this.getResource(),
                                                this.getRealm(),
                                                this.getActions(),
                                                this.getContentModifier(),
                                                this.getFactoryClassName());
    }

    /**
     * Determine whether this <i>official</i> <code>AuthorizationPermission</code> instance <i>implies</i>
     * the access requested by the <code>request</code> <code>AuthorizationPermission</code> instance.  This
     * <i>official</i> permission is stored by the Authorization subsystem and is owned by a given
     * <code>Principal</code>.  The <code>request</code> permission has been created by a software component
     * on behalf of the given <code>Principal</code> wishing access to a resource controlled by the component.
     * <p>
     * The requested permission is implied by the official permission if <b><i>all</i></b> of the following
     * conditions are satisfied:</p>
     *
     * <li>The <code>request</code> permission must be of the same type (evaluated using
     * <code>instanceof</code>);</li>
     * <li>The {@link AuthorizationRealm} of each permission instance must be equivalent;</li>
     * <li>All of the actions of the requested permission must be included in the actions
     * of the official permission. See {@link AuthorizationActions#implies};</li>
     * <li>if there is a recursive parameter in both resource names, and the
     * requested resource name is more restrictive than the official resource name;</li>
     * <li>if there is a recursive parameter in the official resource but not the
     * requested resource or neither resource contains a recursive parameter, and
     * the requested resource is contained in its entirety within the official resource
     * name (starting at the beginning of the official resource).</li>
     * Note: the content modifier of the permissions is not used in this algorithm.
     *
     * @param request The permission that is being requested or attempted, and which is in question
     * by the caller
     * @return <code>true</code> if the official permission does imply access to the resource(s) specified
     * by the request
     */
    public boolean implies( AuthorizationPermission request ) {
//        LogManager.logTrace( LogSecurityConstants.CTX_AUTHORIZATION, "BasicAuthorizationPermission.implies(): this [" +
//                this.toString() + "] => that [" + request + "]");
        if (!(request instanceof BasicAuthorizationPermission)) {
//            LogManager.logDetail( LogSecurityConstants.CTX_AUTHORIZATION, "implies(): Permission not an instance of BasicAuthorizationPermission");
            return false;
        }

        // --------------------------------------
        // Actions NONE does not imply anything!
        // --------------------------------------
        if ( this.getActions().equals(StandardAuthorizationActions.NONE) ||
             request.getActions().equals(StandardAuthorizationActions.NONE) ) {
            return false;
        }

        // -----------------------
        // Compare the Realms ...
        // -----------------------
        if ( ! this.getRealm().equals(request.getRealm()) ) {
//            LogManager.logTrace( LogSecurityConstants.CTX_AUTHORIZATION, "implies(): Realms not equal: this [" +
//                    this.getRealm() + "] != that [" + request.getRealm() + "]");
            return false;
        }
        LogManager.logTrace( LogConstants.CTX_AUTHORIZATION, "implies(): Realms are equal"); //$NON-NLS-1$

        // -----------------------
        // Compare the actions ...
        // -----------------------
        if ( ! this.getActions().implies(request.getActions()) ) {
//            LogManager.logTrace( LogSecurityConstants.CTX_AUTHORIZATION, "implies(): Actions not implied: this [" +
//                    this.getActions() + "] != that [" + request.getActions() + "]");
            return false;
        }
        LogManager.logTrace( LogConstants.CTX_AUTHORIZATION, "implies(): Actions are implied"); //$NON-NLS-1$

	    BasicAuthorizationPermission that = (BasicAuthorizationPermission) request;
        // ---------------------------
        // Compare the resource(s) ...
        // ---------------------------
        DataAccessResource thisResource = (DataAccessResource) getResource();
        return thisResource.implies(that.getResource());
    }

    public boolean equals(Object obj) {
        // Check if instances are identical ...
        if (this == obj) {
            return true;
        }
        // Check if object can be compared to this one
        // (this includes checking for null ) ...
        if(obj instanceof BasicAuthorizationPermission){
               return compare(this, (BasicAuthorizationPermission)obj) == 0;
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
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0037));
        }

        // Check if object cannot be compared to this one
        // (this includes checking for null ) ...
        if (!(o instanceof BasicAuthorizationPermission)) {
            throw new ClassCastException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0038, o.getClass()));
        }

        // Check if everything else is equal ...
        return compare(this, (BasicAuthorizationPermission)o);
    }

    /**
     * Remove recursive attribute.
     * @param resourceName The resource name from which to remove recursion.
     * @return The non recursive resource name.
     */
    public static String removeRecursion(String resourceName) {
        return resourceName.substring(0, resourceName.indexOf(SEPARATOR_WITH_RECURSIVE));
    }

    /**
     * Does this resource have the recursive attribute.
     * @param resourceName The resource name to check for recursion.
     * @return <code>true</code> if the resource is recursive.
     */
    public static boolean isRecursiveResource(String resourceName) {
        return resourceName.endsWith(SEPARATOR_WITH_RECURSIVE);
    }

}


