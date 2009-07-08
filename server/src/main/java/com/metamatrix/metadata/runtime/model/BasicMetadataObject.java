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

package com.metamatrix.metadata.runtime.model;

import java.io.Serializable;
import java.util.Properties;

import org.teiid.metadata.RuntimeMetadataPlugin;

import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.metadata.runtime.api.MetadataID;
import com.metamatrix.metadata.runtime.api.MetadataObject;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseException;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.metadata.util.ErrorMessageKeys;

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
abstract public class BasicMetadataObject implements MetadataObject, Serializable {

    // To accomplish the public immutability, the BasicObject class has
    // <code>setXXX</code> methods that visible to this package (where the
    // {@link com.metamatrix.common.config.model.BasicConfigurationObjectEditor }
    // class exists).  Finally, a protected <code>updateHashCode</code> method that can be invoked
    // by subclasses within the <code>setXXX</code> methods and that in-turn invokes the
    // specialized <code>computeHashCode</code> method overridden by each subclass, obtains
    // the new hash code value, and sets the internal hash code value.  This framework
    // provides a relatively simply template that simplifies the responsibility of
    // developers as they provide new subclasses.

    private MetadataID id;
    private BasicVirtualDatabaseID virtualDatabaseID = null;
    private Properties properties;
//    private boolean hasProperties = true;

    /**
     * Call constructor to instantiate a BasicVirtualDatabase runtime object by passing the BasicVirtualDatabaseID.
     * @param virtualDBID the VirtualDatabaseID for this object (may not be null).
     * @throws IllegalArgumentException if either the ID or data source ID is null.
     */
    protected BasicMetadataObject(BasicVirtualDatabaseID virtualDBID) {
        if (virtualDBID == null) {
            throw new MetaMatrixRuntimeException (ErrorMessageKeys.BMO_0001, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.BMO_0001) );
        }
        id = virtualDBID;
	    this.virtualDatabaseID = virtualDBID;
    }

    /**
     * Call constructor to instantiate a runtime object by passing the BasicRuntimeID that identifies the entity and the BasicVirtualDatabaseID that identifes the Virtual Database the object will be contained.
     * @param metadataID the MetadataID for this object (may not be null).
     * @param virtualDBID the VirtualDatabaseID for this object (may not be null).
     * @throws IllegalArgumentException if either the ID or data source ID is null.
     */
    protected BasicMetadataObject(MetadataID metadataID, BasicVirtualDatabaseID virtualDBID) {
        if (virtualDBID == null || metadataID == null) {
            throw new MetaMatrixRuntimeException (ErrorMessageKeys.BMO_0001, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.BMO_0001) );
        }
        id = metadataID;
        this.virtualDatabaseID = virtualDBID;
    }

    /**
     * returns the id for the Virtual Database for which this runtime object is contained within.  This method will be overridden by BasicVirtualDatabase so that it calls the super method getID to return as its <code>VirtualDatabaseID</code>.
     */
    public VirtualDatabaseID getVirtualDatabaseID() {
	      return virtualDatabaseID;
    }

    public String getNameInSource() {
        String alias = getAlias();
        if(alias != null)
	        return alias;
        return getName();
    }

	/**
	 * Returns whether the name-in-soure is defined for this element.
	 * @return true if this element has the name in source; false otherwise.
	 */
    public boolean hasNameInSource(){
    	return getAlias() != null;
	}

    public String getAlias(){
        return null;
    }

    public String getPath() {
	      return null;
    }

    public Properties getProperties() throws VirtualDatabaseException {
//        if(this.hasProperties){
//            if(properties == null){
//                properties = RuntimeMetadataCatalog.getVirtualDatabaseMetadata(this.virtualDatabaseID).getProperties(this.id);
//                if(properties == null)
//                    this.hasProperties = false;
//            }
//        }
        return properties;
    }

    public Properties getCurrentProperties(){
        return properties;
    }

    public void setProperties(Properties definedProperties) {
	      this.properties = definedProperties;
    }

    /**
     * Get the ID for this metadata object.  The ID can never change in an object,
     * so it is an immutable field.
     * @return the identifier for this metadata object.
     */
    public MetadataID getID() {
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
    * @param newID is of type MetadataID
    */
    protected void setID(MetadataID newID) {
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

        if (obj instanceof BasicMetadataObject) {
            BasicMetadataObject that = (BasicMetadataObject) obj;
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
            BasicMetadataObject that = (BasicMetadataObject) obj;
            return this.getID().compareTo( that.getID() );
        }

        // Otherwise not comparable ...
        throw new MetaMatrixRuntimeException (ErrorMessageKeys.BMO_0002, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.BMO_0002, obj.getClass(), this.getClass()));
    }
    /**
     * Returns a string representing the current state of the object.
     * @return the string representation of this instance.
     */
    public String toString(){
	    return this.id.toString();
    }

    /**
     * Return a deep cloned instance of this object.  Subclasses must override
     * this method.
     * @return the object that is the clone of this instance.
     * @throws CloneNotSupportedException if this object cannot be cloned (i.e., only objects in
     * {@link com.metamatrix.metadata.api.Defaults Defaults} cannot be cloned.
     */
    public Object clone() throws CloneNotSupportedException {
          throw new CloneNotSupportedException("Cannot clone this immutable object.");
    }

    public void addProperty(String name, String value){
        if(this.properties == null)
            this.properties = new Properties();
        this.properties.setProperty(name, value);
    }
}

