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


/**
 * This interface defines a Resource on which can be granted one or more
 * {@link com.metamatrix.platform.security.api.AuthorizationActions Actions}.
 * Together these form an
 * {@link com.metamatrix.platform.security.api.AuthorizationPermission AuthorizationPermission}.
 */
public interface AuthorizationResource extends Comparable {
    /**
     * Get the name of this resource. May be <code>null</code>
     * if the resorce's ID has not been resolved.
     * @return The resource name.
     */
    String getName();

    /**
     * Get the identifier of this resource. <i>Will not</i> be <code>null</code>.
     * This is the identifier used to store and retrieve this resource from
     * the Authorization store.
     * @return The resource identifier.
     */
    String getID();

    /**
     * Get the UUID of this resource. May be <code>null</code>.
     * This is a payload of UUID for MetaBase authorization code.
     * @return The resource's UUID, if present, else <code>null</code>.
     */
    String getUUID();

    /**
     * Get the canonical name of this resource used for comparing.
     * May be <code>null</code> if the resource's ID has not been resolved.
     * @return The canonical resource name.
     */
    String getCanonicalName();

    /**
     * Determine if the Actions applies to this resource should be
     * applied recursively to sub resources.
     * @return Whether the actions are to be applied recursivly.
     */
    boolean isRecursive();

    /**
     * Does this resource imply another?
     * @param resource The other resource
     * @throws MetaBaseResourceNotResolvedException if implies is called bfore
     * the resource's ID has been resolved to a path.
     */
    boolean implies(AuthorizationResource resource);

    /**
     * Are these resources equal exception for recursion?
     * @param resource The resource to compare with this one disregarding recursion.
     * @return <code>true</code> if these two resources differ only be recursion.
     */
    boolean isCannonicallyEquivalent(AuthorizationResource resource);
}
