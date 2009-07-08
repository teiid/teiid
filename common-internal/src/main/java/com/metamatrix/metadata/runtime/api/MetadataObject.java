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

package com.metamatrix.metadata.runtime.api;

import java.util.Properties;


public interface MetadataObject extends java.io.Serializable{

   /**
     * Return the identifier for this object.  The returned type will be an instance of the BaseID subclass
     * which corresponds to the class of this node.
     * @return the specialized BaseID instance for this node.
     */
    MetadataID getID();


    /**
     * Returns the name for this instance of the object.  If you are using
     * the dot notation for a naming conventions, this will return the last
     * node in name.
     * @return the name
     *
     * @see getFullName
     */
    String getName();

    /**
     * Returns the full name for this instance of the object.
     * @return the name
     */
    String getFullName();

    /**
     * Compares this object to another. If the specified object is an instance of
     * the same class, then this method compares the name; otherwise, it throws a
     * ClassCastException (as instances are comparable only to instances of the same
     * class).  Note:  this method is consistent with <code>equals()</code>.
     * <p>
     * @param obj the object that this instance is to be compared to.
     * @return a negative integer, zero, or a positive integer as this object
     *      is less than, equal to, or greater than the specified object, respectively.
     * @throws ClassCastException if the specified object's type prevents it
     *      from being compared to this instance.
     */
    int compareTo(Object obj);

    /**
     * Returns a string representing the current state of the object.
     * @return the string representation of this instance.
     */
    String toString();

    /**
     * Returns true if the specified object is semantically equal to this instance.
     * Note:  this method is consistent with <code>compareTo()</code>.
     * <p>
     * @param obj the object that this instance is to be compared to.
     * @return whether the object is equal to this object.
     */
    boolean equals(Object obj);

    /**
     * Return a deep cloned instance of this object.  Subclasses must override
     * this method.
     * @return the object that is the clone of this instance.
     * @throws CloneNotSupportedException if this object cannot be cloned (i.e., only objects in
     * {@link com.metamatrix.metadata.api.Defaults Defaults} cannot be cloned).
     */
    public Object clone() throws CloneNotSupportedException;

    /**
     * returns the VirtualDatabaseID that identifies the VirtualDatabase this metadata object resides.
     * @return VirtualDatabaseID
     */
    VirtualDatabaseID getVirtualDatabaseID();


    /**
     * returns the user defined properties for this metadata object.
     * @return Properties
     */
    Properties getProperties() throws VirtualDatabaseException;
}

