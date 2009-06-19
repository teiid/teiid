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

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.LogConstants;

/**
 * This class represents a heterogeneous set of AuthorizationPermission instances.  Like the AuthorizationPermission
 * class, this class (and all AuthorizationPermissionCollection classes) also has an <code>implies</code>
 * method that can be used to determine whether a particular AuthorizationPermission is allowed by the
 * permissions contained within an AuthorizationPermissionsImpl instance.
 * <p>
 * The different AuthorizationPermission instances contained by an AuthorizationPermissionsImpl instance are
 * organized into homogeneous AuthorizationPermissionCollection objects contained in the AuthorizationPermissionsImpl
 * object; an AuthorizationPermission object added to the AuthorizationPermissionsImpl object is automatically
 * placed into the appropriate AuthorizationPermissionCollection object for that type (or realm) of permission
 * (as prescribed by the result of the <code>newAuthorizationPermissionCollection()</code> method on the AuthorizationPermission
 * subclass). If no special container is specified, a default container (which has some optimizations for the
 * <code>implies</code> method, based upon the AuthorizationPermission object's <code>hashCode()</code> method) is used.
 */
public final class AuthorizationPermissionsImpl implements AuthorizationPermissions {

    private Set thePermissions;
    // Used to provide permissions collection synchronization
    private Object lockObj = new Object();

    private boolean readOnly;

    /**
     * Creates a new AuthorizationPermissionsImpl object containing no AuthorizationPermission objects.
     */
    public AuthorizationPermissionsImpl() {
        this.readOnly = false;
	    this.thePermissions = new HashSet();
    }

    /**
     * Create a new AuthorizationPermissionsImpl object that is a copy of the original.
     * Make a deep copy of the orig.
     * @param orig The original to be copied.
     */
    public AuthorizationPermissionsImpl( AuthorizationPermissions orig ) {
	    this();
        synchronized (this.lockObj) {
            Iterator permItr = orig.iterator();
            while ( permItr.hasNext() ) {
                AuthorizationPermission aPerm = (AuthorizationPermission) permItr.next();
                if ( aPerm != null ) {
                    try {
                        this.thePermissions.add(aPerm.clone());
                    } catch ( CloneNotSupportedException e ) {
                        // They're all clonable but log anyway
                        final Object[] params = { aPerm };
                        final String msg = SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0005, params);
                        //I18nLogManager.logError(LogSecurityConstants.CTX_AUTHORIZATION,SecurityMessagesKeys.SEC_API_0005,e,aPerm);
                        LogManager.logError(LogConstants.CTX_AUTHORIZATION, e, msg);
                    }
                }
            }
        }
    }

    /**
     * Marks this AuthorizationPermissionCollection object as "readonly". After
     * a AuthorizationPermissionCollection object is marked as readonly, no new AuthorizationPermission
     * objects can be added to it using the <code>add</code>.
     */
    public void setReadOnly() {
        this.readOnly = true;
    }

    /**
     * Determine whether this AuthorizationPermissionCollection object is "readonly".  If it
     * is readonly, no new AuthorizationPermission objects can be added to it using the <code>add</code>.
     */
    public boolean isReadOnly() {
        return this.readOnly;
    }

    /**
     * Adds a permission object to this object by adding it to the AuthorizationPermissionCollection
     * for the class the AuthorizationPermission belongs to.
     * This method creates
     * a new AuthorizationPermissionCollection object (and adds the permission to it)
     * if an appropriate collection does not yet exist. <p>
     *
     * @param permission the AuthorizationPermission object to add.
     * @return true if this collection changed as a result of the addition.
     * @throws SecurityException if this AuthorizationPermission object is marked as readonly.
     */
    public boolean add(AuthorizationPermission permission) {
        if ( this.isReadOnly() ) {
            throw new SecurityException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0017));
        }
        boolean result = false;
        if ( permission != null ) {
            synchronized (this.lockObj) {
                result = this.thePermissions.add(permission);
            }
        }
        return result;
    }

    /**
     * Convenience method to add <code>AuthorizationPermissionsImpl</code> to this object by adding each
     * one to the AuthorizationPermissionCollection for the class that AuthorizationPermission instance belongs to.
     * This method creates new AuthorizationPermissionCollection object as required.
     *
     * @param permissions the set of AuthorizationPermission objects to add.
     * @return true if this collection changed as a result of the addition.
     * @throws SecurityException if this AuthorizationPermission object is marked as readonly.
     * @see #isReadOnly()
     */
    public boolean add(AuthorizationPermissions permissions) {
        if ( isReadOnly() ) {
            throw new SecurityException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0017));
        }
        boolean result = false;
        if ( permissions != null ) {
            result = this.addPermissions(permissions.iterator());
        }
        return result;
    }

    /**
     * Convenience method to add a <code>Set</code> of permission objects to this object by adding each
     * one to the AuthorizationPermissionCollection for the class that AuthorizationPermission instance belongs to.
     * This method creates new AuthorizationPermissionCollection object as required.
     *
     * @param permissions the set of AuthorizationPermission objects to add.
     * @return true if this collection changed as a result of the addition.
     * @throws SecurityException if this AuthorizationPermission object is marked as readonly.
     * @see #isReadOnly()
     */
    public boolean add(Set permissions) {
        if ( isReadOnly() ) {
            throw new SecurityException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0017));
        }
        boolean result = false;
        if ( permissions != null ) {
            result = this.addPermissions(permissions.iterator());
        }
        return result;
    }

    /**
     * Remove from this collection's set of existing permissions the specified permission.  If the permission is not
     * currently in this collection, this method simply returns without performing any operation.
     * @param permission the permission that is to be removed from this policy.
     */
    public boolean remove(AuthorizationPermission permission) {
        boolean result = false;
        synchronized (this.lockObj) {
            result = this.thePermissions.remove(permission);
        }
        return result;
    }

    /**
     * Remove from this collection's set of existing permissions all of the
     * set of specified permissions.  If any of the permissions are not
     * currently in this collection, that permission is ignored.
     * @param permissions the set of permissions that are to be removed from this policy.
     */
    public boolean removeAll(Set permissions) {
        boolean result = false;
        if ( permissions != null ) {
            result = this.removePermissions(permissions.iterator());
        }
        return result;
    }

    /**
     * Remove from this collection's set of existing permissions all of the
     * set of specified permissions.  If any of the permissions are not
     * currently in this collection, that permission is ignored.
     * @param permissions The AuthorizationPermissionsImpl that are to be removed from this policy.
     */
    public boolean removeAll(AuthorizationPermissions permissions) {
        boolean result = false;
        if ( permissions != null ) {
            result = this.removePermissions(permissions.iterator());
        }
        return result;
    }

    /**
     * Remove from this collection the entire set of existing permissions.
     */
    public void clear() {
        this.thePermissions.clear();
    }

    /**
     * Obtain an iterator over the AuthorizationPermission instances in this collection.
     * @return an iterator that can be used to access each of the instances in this
     * collection.
     */
    public Iterator iterator() {
        Iterator permItr = Collections.EMPTY_SET.iterator();
        synchronized (this.lockObj) {
            permItr = this.thePermissions.iterator();
        }
        return permItr;
    }

    /**
     * Checks to see if the AuthorizationPermissionCollection in this object that corresponds
     * to the specified permission's type contains permissions that imply access to the
     * resouces in the <i>permission</i> object.
     * @param permission the AuthorizationPermission object to check.
     * @return true if <i>permission</i> is implied by the permissions in the AuthorizationPermissionCollection it
     * belongs to, false if not.
     */
    public boolean implies(AuthorizationPermission permission) {
        if ( permission == null ) {
            return false;
        }
        Iterator permIter = thePermissions.iterator();
        while (  permIter.hasNext() ) {
            AuthorizationPermission aPerm = (AuthorizationPermission) permIter.next();
            if ( aPerm.implies(permission) ) {
                return true;
            }
        }
        return false;
    }

    /**
     * Determine the number of AuthorizationPermission instances represented by this object.
     * @return the number of permissions within this object.
     */
    public int size() {
        return this.thePermissions.size();
    }

    /**
     * Returns a string representation of the object. In general, the
     * <code>toString</code> method returns a string that
     * "textually represents" this object. The result should
     * be a concise but informative representation that is easy for a
     * person to read.
     * @return  a string representation of the object.
     */
    public String toString() {
        StringBuffer sb = new StringBuffer();
        Iterator iter = this.iterator();
        while ( iter.hasNext() ) {
            try {
                sb.append(iter.next().toString() + ',');
            } catch ( NoSuchElementException e ) {
                // ignore
            }
        }
        // Chop last ','
        int sbLen = sb.length();
        if ( sbLen > 0 ) {
            sb.setLength(sb.length() - 1);
        }
        return sb.toString();
    }

    /**
     * Add the <code>AuthorizationPermission</code>s from the given iterator.
     * <br><code>null</code> permissions are ignored.</br>
     * @param permIter The Iterator that contains permissions to add.
     * @return <code>true</code> if the permissions were changed as a result
     * of adding the permisions.
     */
    private boolean addPermissions(Iterator permIter) {
        boolean result = false;
        synchronized (this.lockObj) {
            while ( permIter.hasNext() ) {
                AuthorizationPermission aPerm = (AuthorizationPermission) permIter.next();
                if ( aPerm != null && this.thePermissions.add(aPerm) && !result ) {
                    result = true;
                }
            }
        }
        return result;
    }

    /**
     * Remove the <code>AuthorizationPermission</code>s in the given iterator.
     * <br><code>null</code> permissions are ignored.</br>
     * @param permIter The Iterator that contains permissions to remove.
     * @return <code>true</code> if the permissions were changed as a result
     * of removing the permisions.
     */
    private boolean removePermissions(Iterator permIter) {
        boolean result = false;
        synchronized (this.lockObj) {
            while ( permIter.hasNext() ) {
                AuthorizationPermission aPerm = (AuthorizationPermission) permIter.next();
                if ( aPerm != null && this.thePermissions.remove(aPerm) && !result ) {
                    result = true;
                }
            }
        }
        return result;
    }
}
