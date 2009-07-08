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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.teiid.connector.metadata.runtime.MetadataConstants;
import org.teiid.metadata.RuntimeMetadataPlugin;

import com.metamatrix.common.namedobject.IDVerifier;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.metadata.runtime.api.ElementID;
import com.metamatrix.metadata.runtime.api.KeyID;
import com.metamatrix.metadata.runtime.api.MetadataID;
import com.metamatrix.metadata.runtime.util.RuntimeIDParser;
import com.metamatrix.metadata.util.ErrorMessageKeys;

/**
 * <p>The BasicRuntimeID is utilized by the server side code only.  This implementation is meant to hide the UID used by the server for performance gains when reading from the database. </p>
 * <p>This class should only be instantiated when the data is read from the database.  The assumption is that all data loaded into the Runtime Catalog will have this type instantiated for the RuntimeID.</p>
 */
public class BasicMetadataID implements MetadataID {

    public static final long NOT_DEFINED = MetadataConstants.NOT_DEFINED_LONG;

    private long uid=NOT_DEFINED;

    public static String FAKE_NAME = "fName";

    /**
     * The character that delimits the atomic components of the name
     */
    public static final String DELIMITER = new String( new char[] { IDVerifier.DELIMITER_CHARACTER } );
    /**
     * The wildcard character.
     */
    public static final String WILDCARD = "*";
    /**
     * The fully-qualified name of the node.  Will never be null.
     */
    private String fullName;
    /**
     * The short name of the node.  Will never be null.
     */
    private String shortName;
    /**
     * The hash code for the fully-qualified name of the node.  It should be set
     * when the name is set.  Since this object is immutable, it should never have
     * to be updated.  It can be used for a fail-fast equals check (where unequal
     * hash codes signify unequal objects, but not necessarily vice versa) as well.
     */
    private int hashCode;
    /**
     * Ordered list of atomic name components.  This is transient to
     * reduce the overhead of passing superfluous information in an RMI request.
     */
    private transient List atomicNames;

    private transient Object lock;

    /**
     * Create an instance with the specified full name.  The full name must be one or more atomic
     * name components delimited by this class' delimeter character.
     * @param fullName the string form of the full name from which this object is to be created;
     * never null and never zero-length.
     * @throws IllegalArgumentException if the full name is null
     */
    public BasicMetadataID(String fullName) {
        this(fullName, NOT_DEFINED);
    }
    protected BasicMetadataID(String fullName, long internalUniqueID ) {
        if ( fullName == null ) {
            throw new MetaMatrixRuntimeException (ErrorMessageKeys.BMDID_0001, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.BMDID_0001) );
        }
        if ( fullName.trim().length() == 0 ) {
            throw new MetaMatrixRuntimeException (ErrorMessageKeys.BMDID_0002, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.BMDID_0002) );
        }
        this.fullName = fullName;
        this.uid = internalUniqueID;
        this.updateHashCode();
        this.lock = new Object();
    }

    protected BasicMetadataID(String parentName, String name, long internalUniqueID ) {
        if ( name == null ) {
            throw new MetaMatrixRuntimeException (ErrorMessageKeys.BMDID_0001, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.BMDID_0001) );
        }
        if ( name.trim().length() == 0 ) {
            throw new MetaMatrixRuntimeException (ErrorMessageKeys.BMDID_0002, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.BMDID_0002) );
        }
        this.shortName = name;
        if(parentName == null){
        	this.fullName = name;
        }else{
        	this.fullName = parentName + DELIMITER + name;
        }
        this.uid = internalUniqueID;
        this.updateHashCode();
        this.lock = new Object();
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        this.lock = new Object();
    }

    /**
     * Obtain the full name for the object that this identifier represents.
     * @return the full name for this identifier.
     */
    public final String getFullName() {
        return this.fullName;
    }
    /**
     * Obtain the last name component this identifier.  This last name component
     * is the logical name for the object that this identifier represents.
     * @return the name for this identifier.
     */
    public final String getName() {
    	if(shortName != null){
    		return shortName;
    	}
        final List nameComponents = new ArrayList(this.getNameComponents());
        int nameComponentCount = nameComponents.size();
        return (String)nameComponents.get(nameComponentCount - 1);
//        int nameComponentCount = this.getNameComponents().size();
//        return (String)this.getNameComponents().get(nameComponentCount - 1);
    }
    /**
     * Obtain the specified component of the name.
     * @param the index of the atomic name component to return; must be less than
     * the result of the method <code>size</code> in order to be valid.
     * @return the full name for this identifier.
     * @throws IndexOutOfBoundsException if the index is not valid and is out of the bounds of this name.
     */
    public final String getNameComponent( int index ) {
        return (String) this.getNameComponents().get(index);
    }
    /**
     * Obtain the list of atomic name components for this ID.
     * @return the unmodifiable list of String objects.
     * @throws IndexOutOfBoundsException if the index is not valid and is out of the bounds of this name.
     */
    public final List getNameComponents() {
        if (this.atomicNames == null) {
            synchronized(this.lock) {
                // Check if the atomicNames list was populated while acquiring the lock
                if (this.atomicNames != null) {
                    return this.atomicNames;
                }
                if ( this.fullName.indexOf(IDVerifier.DELIMITER_CHARACTER) != -1 ) {
                    this.atomicNames = StringUtil.split(this.fullName, DELIMITER);
                } else {
                    this.atomicNames = new ArrayList(1);
                    this.atomicNames.add(this.fullName);
                }
                this.atomicNames = Collections.unmodifiableList(this.atomicNames);
            }
        }
        return this.atomicNames;
    }
    /**
     * Return the number of atomic name components in this identifier.
     * @return the size of this identifier.
     */
    public final int size() {
	    return this.getNameComponents().size();
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
        //if ( this.getClass().isInstance(obj) ) {
        if ( obj instanceof BasicMetadataID ) {

        	// Do quick hash code check first
        	if( this.hashCode() != obj.hashCode() ) {
        		return false;
		      }

            // If the types aren't the same, then fail
            BasicMetadataID that = (BasicMetadataID) obj;
            if ( this.getClass() != that.getClass() ) {
                return false;
            }

            //If the uids are the same, return true
            long thisUID = this.getUID();
            long thatUID = that.getUID();
            if(thisUID !=  Long.MIN_VALUE && thatUID != Long.MIN_VALUE){
                return thisUID == thatUID;
            }

            return this.getFullName().equalsIgnoreCase(that.getFullName() );
        }

        // Otherwise not comparable ...
        return false;
    }
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
    public int compareTo(Object obj) {
        BasicMetadataID that = (BasicMetadataID) obj;     // May throw ClassCastException
        if ( obj == null ) {
            throw new MetaMatrixRuntimeException (ErrorMessageKeys.GEN_0005, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.GEN_0005) );
        }

        int diff = this.hashCode() - that.hashCode();
        if ( diff != 0 ) {
            return diff;
        }

        if ( this.getClass() != that.getClass() ) {
            diff = this.getClass().hashCode() - that.getClass().hashCode();
            return diff;
        } //else{
          //  if((this.getUID() != NOT_DEFINED) && (this.getUID() == that.getUID()))
          //      return 0;
        //}

        return this.fullName.compareToIgnoreCase( that.fullName );
    }
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
    public int compareToByName(Object obj) {
        BasicMetadataID that = (BasicMetadataID) obj;     // May throw ClassCastException
        if ( obj == null ) {
            throw new MetaMatrixRuntimeException (ErrorMessageKeys.GEN_0005, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.GEN_0005) );
        }

        return this.fullName.compareToIgnoreCase( that.fullName );
    }
    /**
     * Returns the hash code value for this object.
     *  @return a hash code value for this object.
     */
    public final int hashCode() {
        return this.hashCode;
    }

    /**
     * Returns a string representing the current state of the object.
     * @return the string representation of this instance.
     */
    public final String toString() {
        return this.fullName;
    }
    /**
     * Return a deep cloned instance of this object.  Subclasses must override
     * this method.
     * @return the object that is the clone of this instance.
     * @throws CloneNotSupportedException if this object cannot be cloned (i.e., only objects in
     * {@link com.metamatrix.metadata.api.Defaults Defaults} cannot be cloned).
     */
    public Object clone() throws CloneNotSupportedException{
	      return super.clone();
    }
    /**
     * Return the full name of the parent.  This is a convenience method to return
     * the list of atomic name components that excludes this ID's last atomic name component.
     * @return the full name of the parent, or null if this ID has no parent.
     */
    public final String getParentFullName() {
	    int lastDelim = this.fullName.lastIndexOf(IDVerifier.DELIMITER_CHARACTER);
	    if ( lastDelim == -1 ) {
		    return null;
	    }
		  return this.fullName.substring(0,lastDelim);
    }

    public final boolean hasParent() {
        boolean result = false;
        if ( this.atomicNames != null ) {
            result = ( this.atomicNames.size() > 1 );
        } else {
            int lastDelim = this.fullName.indexOf(IDVerifier.DELIMITER_CHARACTER);
            if ( lastDelim != -1 ) {
                result = true;
            }
        }
        return result;
    }
    /**
     * Update the currently cached hash code value with a newly computed one.
     * This method should be called in any subclass method that updates any field.
     * This includes constructors.
     * <p>
     * The implementation of this method invokes the <code>computeHashCode</code>
     * method, which is likely overridden in subclasses.
     */
    protected final void updateHashCode() {
        this.hashCode = computeHashCode();
    }
    /**
     * Return a new hash code value for this instance.  If subclasses provide additional
     * and non-transient fields, they should override this method using the following
     * template:
     * <pre>
     * protected int computeHashCode() {
     *      int result = super.computeHashCode();
     *      result = HashCodeUtil.hashCode(result, ... );
     *      result = HashCodeUtil.hashCode(result, ... );
     *      return result;
     * }
     * </pre>
     * Any specialized implementation must <i>not<i> rely upon the <code>hashCode</code>
     * method.
     * <p>
     * Note that this method does not and cannot actually update the hash code value.
     * Rather, this method is called by the <code>updateHashCode</code> method.
     * @return the new hash code for this instance.
     */
    protected int computeHashCode() {
        return this.fullName.toLowerCase().hashCode();
    }
    /**
     * Helper method that instantiates the atomic name list if null.
     */
//    private final void buildAtomicNames() {
//        if (this.atomicNames == null) {
//            this.atomicNames = StringUtil.split(this.fullName, DELIMITER);
//            this.atomicNames = Collections.unmodifiableList(this.atomicNames);
//        }
//    }
/**
 * return the path.
 *  @return String
 */
    public String getPath() {
	      return RuntimeIDParser.getPath(this);
    }

/**
 * return the internal unique identifier that makes this object unique within its type.
 *  @return int
 */
    public long getUID() {
              return uid;
    }
    public void setUID(long uid) {
              this.uid = uid;
    }
    /** This method should never be called after the MetadataID is added to the metadata cache */
    public void setName(String name, boolean isShortName) {
        if(getName().equals(FAKE_NAME)){
            int lastComponentIndex = fullName.lastIndexOf(DELIMITER) + 1;
            if(lastComponentIndex == 0) {
                this.fullName = name;
            } else {
                this.fullName = this.fullName.substring(0, lastComponentIndex) + name;
            }
            this.atomicNames = null;
            if(isShortName){
            	shortName = name;
            }
            updateHashCode();
        }
    }

    /** This method should never be called after the MetadataID is added to the metadata cache */
    public void setFullName(String name) {
        this.fullName = name;
        this.atomicNames = null;
        updateHashCode();
    }

    public void setShortName(String name) {
        this.shortName = name;
    }

    /** This method should never be called after the MetadataID is added to the metadata cache */
    public void setModelName(String modelName){
        if(getNameComponents().size() > 1 && getNameComponent(0).equals(FAKE_NAME)){
            int firstComponentIndex = fullName.indexOf(DELIMITER);
            this.fullName = modelName + fullName.substring(firstComponentIndex);
        }
        this.atomicNames = null;
        updateHashCode();
    }

    /** This method should never be called after the MetadataID is added to the metadata cache */
    public void setGroupFullName(String newName){
        if(this instanceof ElementID || this instanceof KeyID){
            int index = fullName.lastIndexOf(".");
            this.fullName = newName + this.fullName.substring(index);
            this.atomicNames = null;
            updateHashCode();
        }
    }
}

