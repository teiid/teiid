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

package com.metamatrix.common.tree.directory;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.object.ObjectDefinitionImpl;
import com.metamatrix.common.util.ErrorMessageKeys;

public class FileDefinitionImpl extends ObjectDefinitionImpl implements FileDefinition  {

    /**
     * Create an empty definition object with all defaults.
     */
    public FileDefinitionImpl() {
        super();
    }

    /**
     * Create a definition object with the specified set of attributes.
     * @param name the name
     */
    public FileDefinitionImpl(String name) {
        super(name);
    }

    /**
     * Create a definition object with the specified set of attributes.
     * @param name the name
     */
    public FileDefinitionImpl(String name, String displayName) {
        super(name,displayName);
    }

    /**
     * Compares this object to another. If the specified object is
     * an instance of the FileDefinition class, then this
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
        FileDefinitionImpl that = (FileDefinitionImpl)obj; // May throw ClassCastException
        if (obj == null) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0036));
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
            FileDefinitionImpl that = (FileDefinitionImpl)obj;
            return this.getName().equals(that.getName());
        }

        // Otherwise not comparable ...
        return false;
    }

}


