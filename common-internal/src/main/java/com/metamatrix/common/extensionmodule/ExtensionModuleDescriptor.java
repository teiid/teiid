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

package com.metamatrix.common.extensionmodule;

import java.io.Serializable;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.core.util.Assertion;

/**
 * <p>This interface describes an immutable lightweight serializable object
 * for describing an extension module as it exists in the Platform at a
 * given point in time.</p>
 *
 * <p>If ordered (using Collections.sort or something else based on their
 * {@link #compareTo} method), they should be ordered according to their
 * {@link #getPosition search position}.  Equality should, therefore, also
 * be based on their search position - the search position should
 * be a unique integer across all extension modules <i>at a given point
 * in time</i> (if position is changed, the entire list should be
 * refreshed).  Therefore, one should never mix ExtensionModuleDescriptor
 * instances that were retrieved from the server at different points
 * in time. </p>
 *
 * <p>(Note: Classes which implement this interface should also explicitly
 * implement the java.io.Serializable interface.)</p>
 */
public class ExtensionModuleDescriptor implements Comparable, Serializable {

    protected String name;
    protected String type;
    protected int position;
    protected boolean enabled;
    protected String desc;
    protected String createdBy;
    protected String creationDate;
    protected String lastUpdatedBy;
    protected String lastUpdatedDate;
    protected long checksum;
	
    public ExtensionModuleDescriptor() {}
    
    public ExtensionModuleDescriptor( String name, String type, int position, boolean enabled, String desc, String createdBy, String creationDate, String lastUpdatedBy, String lastUpdatedDate, long checksum) {
        this.name = name;        
        this.type = type;
        this.position = position;
        this.enabled = enabled;
        this.desc = desc;
        this.createdBy = createdBy;
        this.creationDate = creationDate;
        this.lastUpdatedBy = lastUpdatedBy;
        this.lastUpdatedDate = lastUpdatedDate;
        this.checksum = checksum;
    }

    
    public ExtensionModuleDescriptor(ExtensionModuleDescriptor clone) {
        clone.name = this.name;        
        clone.type = this.type;
        clone.position = this.position;
        clone.enabled = this.enabled;
        clone.desc = this.desc;
        clone.createdBy = this.createdBy;
        clone.creationDate = this.creationDate;
        clone.lastUpdatedBy = this.lastUpdatedBy;
        clone.lastUpdatedDate = this.lastUpdatedDate;
        clone.checksum = this.checksum;
    }
    
    /**
     * Name (e.g. filename) of this extension module
     * @return name of extension module
     */
    public String getName() {
    	return this.name;
    }

    /**
     * Type of this extension module
     * @return type of this extension module
     */
    public String getType() {
    	return this.type;
    }

    /**
     * <p>Position of this extension module in search order  - zero based.
     * This number itself is not important, what is imporant is the
     * <i>relative</i> ordering of all extension modules, which this
     * attribute indicates.</p>
     *
     * <p>The position attribute does <i>not</i> have to
     * be the same integer for a given extension module over time.  In
     * particular, when the list of modules is re-ordered, any extension
     * module may be given a new position.  Furthermore, as extension modules
     * are added and deleted, the list of positions does not have to be
     * a continuous sequence - there can be gaps, and there doesn't
     * necessarily have to be a zero-th extension module (even though
     * the search position is zero-based.)</p>
     *
     * @return position in search order
     */
    public int getPosition() {
    	return this.position;
    }

    /**
     * Whether this extension module is enabled for searching or not
     * (it may exist yet be disabled; it will not be searched)
     * @return enabled for searching
     */
    public boolean isEnabled() {
    	return this.enabled;
    }

    /**
     * Optional description of this extension module - may be null.
     * @return description
     */
    public String getDescription() {
    	return this.desc;
    }

    /**
     * Principal name who created this extension module
     * @return name of principal who created the extension module
     */
    public String getCreatedBy() {
    	return this.createdBy;
    }

    /**
     * String date this extension module was created (in String form
     * using {@link com.metamatrix.core.util.DateUtil DateUtil}).
     * @return date of the creation
     */
    public String getCreationDate() {
    	return this.creationDate;
    }

    /**
     * Principal name who last updated this extension module
     * @return name of principal who last updated the extension module
     */
    public String getLastUpdatedBy() {
    	return this.lastUpdatedBy;
    }

    /**
     * String date this extension module was last updated (in String form
     * using {@link com.metamatrix.core.util.DateUtil DateUtil}).
     * @return date of last update
     */
    public String getLastUpdatedDate() {
    	return this.lastUpdatedDate;
    }

    /**
     * The checksum of the contents of the extension module - this can be
     * used to quickly compare if the contents stored are the same as,
     * for example, the contents of a local file.  This checksum will have been
     * generated with a java.util.zip.CRC32 instance, using the entire
     * byte array of the extension module as it was added.
     * @return checksum of the contents of the extension module
     */
    public long getChecksum() {
    	return this.checksum;
    }

    /**
     * <p>Compares this object to another. If the specified object is
     * an instance of the ExtensionModuleDescriptor class, then this
     * method compares the contents; otherwise, it throws a
     * ClassCastException (as instances are comparable only to
     * instances of the same
     *  class).
     * </p>
     * <p>Note:  this method <i>should be</i> consistent with
     * <code>{@link #equals equals()}</code>, meaning that
     * <code>(compare(x, y)==0) == (x.equals(y))</code>.
     * </p>
     * @param obj the object that this instance is to be compared to.
     * @return a negative integer, zero, or a positive integer as this object
     * is less than, equal to, or greater than the specified object, respectively.
     * @throws AssertionError if the specified object reference is null
     * @throws ClassCastException if the specified object's type prevents it
     * from being compared to this instance.
     */
    public int compareTo(Object obj){
        ExtensionModuleDescriptor that = (ExtensionModuleDescriptor) obj;     // May throw ClassCastException
        if(obj == null){
            Assertion.isNotNull(obj,CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0045));
        }
        
        if ( obj == this ) {
            return 0;
        }
        return this.position - that.getPosition();
    }
    /**
     * Returns true if the specified object is semantically equal
     * to this instance.
     * Note:  this method should be consistent with
     * <code>{@link #compareTo compareTo()}</code>.
     * @param obj the object that this instance is to be compared to.
     * @return whether the object is equal to this object.
     */
    public boolean equals(Object obj){
        // Check if instances are identical ...
        if ( this == obj ) {
            return true;
        }

        // Check if object can be compared to this one
        // (this includes checking for null ) ...
        if ( obj instanceof ExtensionModuleDescriptor ) {
            ExtensionModuleDescriptor that = (ExtensionModuleDescriptor) obj;
            return ( this.position == that.getPosition() );
        }

        // Otherwise not comparable ...
        return false;
    }

    /**
     * Return the string form of this ExtensionModuleDescriptor.
     * @return the string form of this ExtensionModuleDescriptor
     */
    public String toString(){
        return "Extension Source: " + this.name; //$NON-NLS-1$
    }
    
    public void setName(String newName){name=newName;}
    public void setType(String newType){type=newType;}
    public void setPosition(int pos){position=pos;}
    public void setEnabled(boolean isEnabled){enabled=isEnabled;}
    public void setDescription(String description){desc=description;}
    public void setCreatedBy(String by){createdBy=by;}
    public void setCreationDate(String date){creationDate=date;}
    public void setLastUpdatedBy(String by){lastUpdatedBy=by;}
    public void setLastUpdatedDate(String date){lastUpdatedDate=date;}
    public void setChecksum(long sum){checksum=sum;}    
} 
