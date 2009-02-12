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

package com.metamatrix.platform.admin.apiimpl;

import com.metamatrix.admin.AdminMessages;
import com.metamatrix.common.object.ObjectDefinitionImpl;
import com.metamatrix.platform.admin.api.PermissionDataNodeDefinition;
import com.metamatrix.platform.security.api.SecurityPlugin;

public class PermissionDataNodeDefinitionImpl extends ObjectDefinitionImpl implements PermissionDataNodeDefinition  {

    private int type;

    /**
     * Create a definition object with the specified set of attributes.
     * @param name the fully-qualified path name.
     * @param displayName the name by which to display the node.
     * @param nodeType the <i>sub</i>type of this node.
     */
    public PermissionDataNodeDefinitionImpl(String name, String displayName, int nodeType) {
        super(name,displayName);
        if (nodeType < PermissionDataNodeDefinition.TYPE.LOWER_BOUND ||
            nodeType > PermissionDataNodeDefinition.TYPE.UPPER_BOUND) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(AdminMessages.ADMIN_0038,  nodeType));
        }
        this.type = nodeType;
    }

    /**
     * Get the type of <code>PermissionDataNode</code>.
     * @return The type of this <code>PermissionDataNode</code>.
     * @see PermissionDataNodeDefinition.TYPE
     */
    public int getType() {
        return this.type;
    }

    /**
     * Compares this object to another. If the specified object is
     * an instance of the FolderDefinition class, then this
     * method compares the contents; otherwise, it throws a
     * ClassCastException (as instances are comparable only to
     * instances of the same
     *  class).
     * <p>
     * Note:  this method <i>is</i> consistent with
     * <code>equals()</code>, meaning that
     * <code>(compare(x, y)==0) == (x.equals(y))</code>.
     * <p>
     * @param obj the object that this instance is to be compared to.
     * @return a negative integer, zero, or a positive integer as this object
     *      is less than, equal to, or greater than the specified object, respectively.
     * @throws IllegalArgumentException if the specified object reference is null
     * @throws ClassCastException if the specified object's type prevents it
     *      from being compared to this instance.
     */
    public int compareTo(Object obj) {
        PermissionDataNodeDefinitionImpl that = (PermissionDataNodeDefinitionImpl)obj; // May throw ClassCastException
        if (obj == null) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(AdminMessages.ADMIN_0039));
        }

        return this.getName().compareTo(that.getName());
    }

    /**
     * Returns true if the specified object is semantically equal
     * to this instance.
     *   Note:  this method is consistent with
     * <code>compareTo()</code>.
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
        if (this.getClass().isInstance(obj)) {
            PermissionDataNodeDefinitionImpl that = (PermissionDataNodeDefinitionImpl)obj;
            return this.getName().equals(that.getName());
        }

        // Otherwise not comparable ...
        return false;
    }

}


