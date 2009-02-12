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

package com.metamatrix.common.tree;

import com.metamatrix.common.object.ObjectDefinition;
import com.metamatrix.common.object.PropertiedObject;

/**
 * This interface represents a single entry on a hierarchical system.
 */
public interface TreeNode extends Comparable, PropertiedObject {

    /**
     * Return whether this TreeNode represents an existing resource.
     * @return true the entry exists, or false otherwise.
     */
    boolean exists();

    /**
     * Obtain the name of this TreeNode. The name does not contain the
     * namespace but is unique within the namespace of the TreeNode.
     * @return the name of the node; never null or zero-length
     */
    String getName();

    /**
     * Obtain the full name of the TreeNode which is unique within the TreeView.
     * The full name is the concatenation of the namespace, separator, and
     * name of this TreeNode.
     * @return the fully qualified name of this node; never null
     */
    String getFullName();

    /**
     * Obtain the namespace to which this tree node belongs. The separator
     * character is used between each of the components of the namespace.
     * @return the string that represents the namespace of this node; never null
     */
    String getNamespace();

    /**
     * Get this type for this tree node.
     * @return the entity's type; never null
     */
    ObjectDefinition getType();

    /**
     * Obtain the character that is used to separate names in a path sequence for
     * the abstract path.  This character is completely dependent upon the implementation.
     * @return the charater used to delimit names in the abstract path.
     */
    char getSeparatorChar();

    /**
     * Obtain the character (as a String) that is used to separate names in a path sequence for
     * the abstract path.
     * @return the string containing the charater used to delimit names in the abstract path; never null
     */
    String getSeparator();

    /**
     * Return whether this node has undergone changes.  The time from which changes are
     * maintained is left to the implementation.
     * @return true if this TreeNode has changes, or false otherwise.
     */
    boolean isModified();

    /**
     * Compares this object to another. If the specified object is
     * an instance of the TreeNode class, then this
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
    int compareTo(Object par1);
    /**
     * Returns true if the specified object is semantically equal
     * to this instance.
     *   Note:  this method is consistent with
     * <code>compareTo()</code>.
     * @param obj the object that this instance is to be compared to.
     * @return whether the object is equal to this object.
     */
    boolean equals(Object par1);

    /**
     * Return the string form of this TreeNode.
     * @return the stringified abstract path, equivalent to <code>getPath</code>
     */
    String toString();

}
