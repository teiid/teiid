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

import java.io.Serializable;
import java.util.List;


/**
 * The MetadataID class serves as the interface class for identifiers
 * of objects.  This class provides the method signatures
 * common to all ID classes as well as the majority of the implementation, and
 * defines an identification name as a list of one or more non-zero length atomic name components
 * delimeted by a '.' (similar to JNDI names).  
 * <p>
 * These classes are shipped between the client and RuntimeMetadata, so
 * the MetadataID class is serializable.  To speed serialization and decrease
 * the overhead of shipping MetadataID across the network using RMI, several
 * instance variables that may not be required by all users are made transient
 * and recomputed as needed.
 * <p>
 * Additionally, because IDs are designed
 * to be used as primary keys, the <code>hashCode</code>, <code>equals</code>
 * and <code>compareTo</code> methods are all consistent and optimized for
 * fast performance.  This is in part accomplished by caching the hash code value,
 * which is tolerable since all MetadataID subclasses are <i>immutable</i>: they
 * cannot be modified after they have been created.
 * <p>
 * Finally, several key methods that are very commonly used and that will not be
 * overridden in subclasses are marked as <code>final</code> as an inlining hint to the
 * compiler.
 */
public interface MetadataID extends Cloneable, Comparable, Serializable {

    /**
     * Obtain the full name for the object that this identifier represents.
     * @return the full name for this identifier.
     */
    public  String getFullName() ;
    /**
     * Obtain the last name component this identifier.  This last name component
     * is the logical name for the object that this identifier represents.
     * @return the name for this identifier.
     */
    public  String getName();

    /**
     * Obtain the path for this component.
     * @return the path
     */
     public String getPath();

    /**
     * Obtain the specified component of the name.
     * @param the index of the atomic name component to return; must be less than
     * the result of the method <code>size</code> in order to be valid.
     * @return the full name for this identifier.
     * @throws IndexOutOfBoundsException if the index is not valid and is out of the bounds of this name.
     */
    public  String getNameComponent( int index ) ;
    /**
     * Obtain the list of atomic name components for this ID.
     * @return the unmodifiable list of String objects.
     * @throws IndexOutOfBoundsException if the index is not valid and is out of the bounds of this name.
     */
    public List getNameComponents();
    /**
     * Return the number of atomic name components in this identifier.
     * @return the size of this identifier.
     */
    public int size();
    /**
     * Returns true if the specified object is semantically equal to this instance.
     * Note:  this method is consistent with <code>compareTo()</code>.
     * <p>
     * @param obj the object that this instance is to be compared to.
     * @return whether the object is equal to this object.
     */
    public boolean equals(Object obj);

    /**
     * Compares this object to another. If the specified object is an instance of
     * the MetadataID class, then this method compares the name; otherwise, it throws a
     * ClassCastException (as instances are comparable only to instances of the same
     * class).
     * Note:  this method <i>is</i> consistent with <code>equals()</code>, meaning
     * that <code>(compare(x, y)==0) == (x.equals(y))</code>.
     * <p>
     * The algorithm that this method follows is based primarily upon the
     * hash code.  When two instances of MetadataID, objects A and B, are being compared,
     * this method first compares the (cached) hash code of the two objects.  If the
     * two hash codes are not equal, the method returns the difference in the hash
     * codes (namely <code>A.hashCode() - B.hashCode()</code>).
     * If, however, the two hash code values are equivalent, then the
     * two MetadataID instances are <i>potentially</i> equivalent, and the
     * full names of the BaseIDs are compared (ignoring case) to determine <i>actual</i> result.
     * <p>
     * @param obj the object that this instance is to be compared to.
     * @return a negative integer, zero, or a positive integer as this object
     *      is less than, equal to, or greater than the specified object, respectively.
     * @throws IllegalArgumentException if the specified object reference is null
     * @throws ClassCastException if the specified object's type prevents it
     *      from being compared to this instance.
     */
    public int compareTo(Object obj);

    /**
     * Compares this object to another lexicographically. If the specified object is an instance of
     * the same class, then this method compares the name; otherwise, it throws a
     * ClassCastException (as instances are comparable only to instances of the same
     * class).  Note:  this method is consistent with <code>equals()</code>.
     * <p>
     * @param obj the object that this instance is to be compared to.
     * @return a negative integer, zero, or a positive integer as this object
     *      is less than, equal to, or greater than the specified object, respectively.
     * @throws IllegalArgumentException if the specified object reference is null
     * @throws ClassCastException if the specified object's type prevents it
     *      from being compared to this instance.
     */
    public int compareToByName(Object obj);

    /**
     * Returns the hash code value for this object.
     *  @return a hash code value for this object.
     */
    public  int hashCode();

    /**
     * Returns a string representing the current state of the object.
     * @return the string representation of this instance.
     */
    public  String toString();
    /**
     * Return a deep cloned instance of this object.  Subclasses must override
     * this method.
     * @return the object that is the clone of this instance.
     * @throws CloneNotSupportedException if this object cannot be cloned (i.e., only objects in
     * {@link com.metamatrix.metadata.api.Defaults Defaults} cannot be cloned).
     */
    public Object clone() throws CloneNotSupportedException;
    /**
     * Return the full name of the parent.  This is a convenience method to return
     * the list of atomic name components that excludes this ID's last atomic name component.
     * @return the full name of the parent, or null if this ID has no parent.
     */
    public  String getParentFullName() ;

}


