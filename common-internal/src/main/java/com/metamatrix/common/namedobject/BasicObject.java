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

package com.metamatrix.common.namedobject;

import java.io.Serializable;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.core.MetaMatrixRuntimeException;

/**
 * This class represents the basic implementation of MetadataObject, which is
 * the foundation for all classes that are used to capture metadata.
 * This abstract class is immutable, although it is intended that subclasses
 * are mutable.  Additionally, although this class is thread safe, subclasses
 * do not have to be thread safe, since the framework for update and modifying
 * these objects must guarantee proper concurrent access.
 * <p>
 * These classes are shipped between the client and Metadata Service, so
 * this class is serializable.
 * <p>
 * Also, the <code>hashCode</code>, <code>equals</code>
 * and <code>compareTo</code> methods are all consistent and optimized for
 * fast performance.  This is in part accomplished by caching the hash code value
 * which identifies quickly that two objects are <i>not</i> equal.
 * <p>
 * This class and all of its subclasses are designed to be publicly immutable.
 * That is, no component outside of the Configuration Service changes these objects
 * once they are created.
 */
public abstract class BasicObject implements BaseObject, Serializable {

    // To accomplish the public immutability, the BasicObject class has
    // <code>setXXX</code> methods that visible to this package (where the
    // {@link com.metamatrix.common.config.model.BasicConfigurationObjectEditor }
    // class exists).  Finally, a protected <code>updateHashCode</code> method that can be invoked
    // by subclasses within the <code>setXXX</code> methods and that in-turn invokes the
    // specialized <code>computeHashCode</code> method overridden by each subclass, obtains
    // the new hash code value, and sets the internal hash code value.  This framework
    // provides a relatively simply template that simplifies the responsibility of
    // developers as they provide new subclasses.

    /**
     * The ID for this object.  Never null.
     * @label ID
     * @link aggregation
     * @supplierCardinality 1
     */
    private BaseID id;


    /**
     * Create a new instance with the specified ID.
     * @param id the ID for this object (may not be null).
     * @throws IllegalArgumentException if either the ID or data source ID is null.
     */
    protected BasicObject(BaseID id) {
        if (id == null) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.NAMEDOBJECT_ERR_0004));
        }
        this.id = id;
    }

    /**
     * Get the ID for this metadata object.  The ID can never change in an object,
     * so it is an immutable field.
     * @return the identifier for this metadata object.
     */
    public BaseID getID() {
        return this.id;
    }

    /**
     * Returns the name for this instance of the object.  If you are using
     * the dot notation for a naming conventions, this will return the last
     * node in name.
     * @return the name
     *
     * @see getFullName
     */
    public String getName(){
	    return getID().getName();
    }

    /**
     * Returns the full name for this instance of the object.
     * @return the name
     */
    public String getFullName() {
      return getID().getFullName();
    }


    /**
    * Sets the id for this objects
    * @param newID is of type BaseID
    */
    protected void setID(BaseID newID) {
        this.id = newID;
    }

    /**
     * Overrides Object hashCode method. Note that the
     * hash code is computed purely from the ID, so two distinct instances that
     * have the same identifier (i.e., full name) will have the same hash code
     * value.
     * <p>
     * This hash code must be consistent with the <code>equals</code> method.
     * defined by subclasses.
     * @return the hash code value for this metadata object.
     */
    public int hashCode(){
	return this.id.hashCode();
    }

    /**
     * Returns true if the specified object is semantically equal to this instance.
     * Note:  this method is consistent with <code>compareTo()</code>.
     * <p>
     * @param obj the object that this instance is to be compared to.
     * @return whether the object is equal to this object.
     */
    public boolean equals(Object obj) {
        // Check if instances are identical ...
        if ( this == obj ) {
            return true;
        }

        // Check if object can be compared to this one
        // (this includes checking for null ) ...
        if ( this.getClass().isInstance(obj) ) {

        	// Do quick hash code check first
        	if( this.hashCode() != obj.hashCode() ) {
        		return false;
		}

            BasicObject that = (BasicObject) obj;
            return this.getID().equals( that.getID() );
        }

        // Otherwise not comparable ...
        return false;
    }
    /**
     * Compares this object to another. If the specified object is an instance of
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
    public int compareTo(Object obj) {
        // Check if instances are identical...
        if ( this == obj ) {
            return 0;
        }

        // Check if object can be compared to this one...
        // (this includes checking for null ) ...
        if ( this.getClass().isInstance(obj) ) {
            BasicObject that = (BasicObject) obj;
            return this.getID().compareTo( that.getID() );
        }

        // Otherwise not comparable ...
        throw new ClassCastException(CommonPlugin.Util.getString(ErrorMessageKeys.NAMEDOBJECT_ERR_0005, obj.getClass(), this.getClass()) );
    }
    /**
     * Returns a string representing the current state of the object.
     * @return the string representation of this instance.
     */
    public String toString(){
	    return this.id.toString();
    }
    
    @Override
    public Object clone() {
    	try {
			return super.clone();
		} catch (CloneNotSupportedException e) {
			throw new MetaMatrixRuntimeException(e);
		}
    }

}





