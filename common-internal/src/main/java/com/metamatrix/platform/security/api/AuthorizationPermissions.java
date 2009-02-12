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

/*
 * Date: Apr 24, 2003
 * Time: 12:51:40 PM
 */
package com.metamatrix.platform.security.api;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Set;

import com.metamatrix.platform.security.api.AuthorizationPermission;

/**
 * Interface AuthorizationPermissions.
 *
 * <p>This interface represents a collection of <code>AuthorizationPermission</code>s.</p>
 */
public interface AuthorizationPermissions extends Serializable {
    /**
     * Marks this AuthorizationPermissionCollection object as "readonly". After
     * a AuthorizationPermissionCollection object is marked as readonly, no new AuthorizationPermission
     * objects can be added to it using the {@link #add} method.
     */
    void setReadOnly();

    /**
     * Determine whether this AuthorizationPermissionCollection object is "readonly".  If it
     * is readonly, no new AuthorizationPermission objects can be added to it using the {@link #add} method.
     */
    boolean isReadOnly();

    /**
     * Adds a permission object to this object by adding it to the AuthorizationPermissionCollection
     * for the class the AuthorizationPermission belongs to.
     * This method creates
     * a new AuthorizationPermissionCollection object (and adds the permission to it)
     * if an appropriate collection does not yet exist. <p>
     *
     * @param permission the AuthorizationPermission object to add.
     * @return true if this collection changed as a result of the addition.
     * @throws java.lang.SecurityException if this AuthorizationPermission object is marked as readonly.
     * @see #isReadOnly()
     * @see #add(AuthorizationPermissions)
     * @see #add(Set)
     */
    boolean add(AuthorizationPermission permission);

    /**
     * Convenience method to add <code>AuthorizationPermissionsImpl</code> to this object by adding each
     * one to the AuthorizationPermissionCollection for the class that AuthorizationPermission instance belongs to.
     * This method creates new AuthorizationPermissionCollection object as required.
     *
     * @param permissions the set of AuthorizationPermission objects to add.
     * @return true if this collection changed as a result of the addition.
     * @throws java.lang.SecurityException if this AuthorizationPermission object is marked as readonly.
     * @see #isReadOnly()
     * @see #add(AuthorizationPermission)
     * @see #add(Set)
     */
    boolean add(AuthorizationPermissions permissions);

    /**
     * Convenience method to add a <code>Set</code> of permission objects to this object by adding each
     * one to the AuthorizationPermissionCollection for the class that AuthorizationPermission instance belongs to.
     * This method creates new AuthorizationPermissionCollection object as required.
     *
     * @param permissions the set of AuthorizationPermission objects to add.
     * @return true if this collection changed as a result of the addition.
     * @throws java.lang.SecurityException if this AuthorizationPermission object is marked as readonly.
     * @see #isReadOnly()
     * @see #add(AuthorizationPermission)
     * @see #add(AuthorizationPermissions)
     */
    boolean add(Set permissions);

    /**
     * Remove from this collection's set of existing permissions the specified permission.  If the permission is not
     * currently in this collection, this method simply returns without performing any operation.
     * @param permission the permission that is to be removed from this policy.
     */
    boolean remove(AuthorizationPermission permission);

    /**
     * Remove from this collection's set of existing permissions all of the
     * set of specified permissions.  If any of the permissions are not
     * currently in this collection, that permission is ignored.
     * @param permissions the set of permissions that are to be removed from this policy.
     */
    boolean removeAll(Set permissions);

    /**
     * Remove from this collection's set of existing permissions all of the
     * set of specified permissions.  If any of the permissions are not
     * currently in this collection, that permission is ignored.
     * @param permissions The AuthorizationPermissionsImpl that are to be removed from this policy.
     */
    boolean removeAll(AuthorizationPermissions permissions);

    /**
     * Remove from this collection the entire set of existing permissions.
     */
    void clear();

    /**
     * Obtain an iterator over the AuthorizationPermission instances in this collection.
     * @return an iterator that can be used to access each of the instances in this
     * collection.
     */
    Iterator iterator();

    /**
     * Checks to see if the AuthorizationPermissionCollection in this object that corresponds
     * to the specified permission's type contains permissions that imply access to the
     * resouces in the <i>permission</i> object.
     * @param permission the AuthorizationPermission object to check.
     * @return true if <i>permission</i> is implied by the permissions in the AuthorizationPermissionCollection it
     * belongs to, false if not.
     */
    boolean implies(AuthorizationPermission permission);

    /**
     * Determine the number of AuthorizationPermission instances represented by this object.
     * @return the number of permissions within this object.
     */
    int size();
}
