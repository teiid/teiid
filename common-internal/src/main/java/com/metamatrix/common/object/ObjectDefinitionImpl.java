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

package com.metamatrix.common.object;

import java.io.Serializable;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.core.util.StringUtil;

/**
 * This class captures the definition of a generic property, and can be used
 * to determine appropriate user-interface representations to present and capture
 * property values.
 */
public class ObjectDefinitionImpl implements Serializable, Comparable, Cloneable, ObjectDefinition {

    private String name = ""; //$NON-NLS-1$
    private String pluralDisplayName = ""; //$NON-NLS-1$
    private String shortDisplayName = ""; //$NON-NLS-1$
    private String displayName = ""; //$NON-NLS-1$
    private String shortDescription = ""; //$NON-NLS-1$

    /**
     * Create an empty property definition object with all defaults.
     */
    public ObjectDefinitionImpl() {
    }

    /**
     * Create a definition object with the specified set of attributes.
     * @param name the name
     * @param displayName the displayable name for this property; or null
     * if the display name is the same as the property name.
     * @param multiplicity the instance of Multiplicity that captures the allowable
     * range of the cardinality of property values; if null, the default
     * multiplicity of "1" is used.     */
    public ObjectDefinitionImpl(String name) {
        this.setName(name);
        this.setDisplayName(name);
    }

    /**
     * Create a definition object with the specified set of attributes.
     * @param name the name
     * @param displayName the displayable name for this property; or null
     * if the display name is the same as the property name.
     */
    public ObjectDefinitionImpl(String name, String displayName) {
        this.setName(name);
        this.setDisplayName(displayName);
    }

    /**
     * Create a property definition object that is a copy of the specified instance.
     * @param defn the definition that is to be copied; may not be null
     */
    public ObjectDefinitionImpl(ObjectDefinition defn) {
        if (defn == null) {
            throw new AssertionError(CommonPlugin.Util.getString(ErrorMessageKeys.OBJECT_ERR_0014));
        }
        this.setName(defn.getName());
        this.setDisplayName(defn.getDisplayName());
        this.setShortDisplayName(defn.getShortDisplayName());
        this.setPluralDisplayName(defn.getPluralDisplayName());
        this.setShortDescription(defn.getShortDescription());
    }

    /**
     * Get the actual or programmatic name of this property.
     * @return the property's name (never null)
     */
    public String getName() {
        return name;
    }

    /**
     * Set the actual or programmatic name of this property.
     * @param name the new property name; or null if there is no name for this
     * property definition
     */
    public void setName(String name) {
        this.name = (name != null ? name : ""); //$NON-NLS-1$
    }

    /**
     * Get the localized display name of this property.
     * @return the displayable name for this property (never null)
     */
    public String getPluralDisplayName() {
        return pluralDisplayName;
    }

    /**
     * Set the localized display name of this property.
     * @param displayName the displayable name for this property; or null
     * if the display name is the same as the property name.
     */
    public void setPluralDisplayName(String displayName) {
        this.pluralDisplayName = (displayName != null ? displayName : this.displayName + "s"); //$NON-NLS-1$
    }

    /**
     * Get the localized display name of this property.
     * @return the displayable name for this property (never null)
     */
    public String getShortDisplayName() {
        return shortDisplayName;
    }

    /**
     * Set the localized display name of this property.
     * @param displayName the displayable name for this property; or null
     * if the display name is the same as the property name.
     */
    public void setShortDisplayName(String displayName) {
        this.shortDisplayName = (displayName != null ? displayName : this.displayName);
    }

    /**
     * Get the localized display name of this property.
     * @return the displayable name for this property (never null)
     */
    public String getDisplayName() {
        return displayName;
    }

    /**
     * Set the localized display name of this property.
     * @param displayName the displayable name for this property; or null
     * if the display name is the same as the property name.
     */
    public void setDisplayName(String displayName) {
        this.displayName = (displayName != null ? displayName : this.name);
    }

    /**
     * Get the short description of this property.
     * @return the short description for this property (never null)
     */
    public String getShortDescription() {
        return shortDescription;
    }

    /**
     * Set the short description of this property.
     * @param shortDescription the short description for this property, or
     * null if there is no short description.
     */
    public void setShortDescription(String shortDescription) {
        this.shortDescription = (shortDescription != null ? shortDescription : ""); //$NON-NLS-1$
    }

    /**
     * Compares this object to another. If the specified object is
     * an instance of the MetaMatrixSessionID class, then this
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
        ObjectDefinitionImpl that = (ObjectDefinitionImpl)obj; // May throw ClassCastException
        if (obj == null) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.OBJECT_ERR_0007, this.getClass().getName()));
        }

        return this.name.compareTo(that.name);
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
            ObjectDefinitionImpl that = (ObjectDefinitionImpl)obj;
            return this.getName().equals(that.getName());
        }

        // Otherwise not comparable ...
        return false;
    }

    /**
     * Returns the hash code value for this object.
     * @return a hash code value for this object.
     */
    public int hashCode() {
        return this.name.hashCode();
    }

    /**
     * Returns a string representing the current state of the object.
     * @return the string representation of this instance.
     */
    public String toString() {
        return this.displayName;
    }

    public Object clone() {
        return new ObjectDefinitionImpl(this);
    }

    /**
     * This method attempts to compute and set the display name using the name,
     * which is expected to be in "humpback" notation (e.g., "aName"). <i>Note that
     * this implementation is not expected to work in all situations.</i>
     * <p>This method first ensures that the first character is uppercase.
     * Then, the method inserts before any remaining uppercase characters a
     * space.</p>
     */
    public void computeDisplayName() {
        String result = StringUtil.computeDisplayableForm( this.getName(), null );
        if ( result != null ) {
            this.setDisplayName(result);    // don't change it if result is null
        }
    }

    /**
     * This method attempts to compute and set the plural display name from the
     * display name, using a simple pluralization rules. <i>Note that this implementation
     * is not expected to work in all situations.</i>
     * <p> Pluralization rules are as follows: <
     *    <li>Strings ending in -ss are appended with "es".</li>
     *    <li>Strings ending in -y will have the y replaced with "ies".</li>
     *    <li>All other Strings will be appended with "s".</li>
     */
    public void computePluralDisplayName() {
        String displayName = this.getDisplayName();
        if ( displayName == null || displayName.length() == 0 ) {
            displayName = StringUtil.computeDisplayableForm( this.getName(), null );
        }
        if ( displayName != null ) {
            String result = StringUtil.computePluralForm( displayName, null );
            if ( result != null ) {
                this.setPluralDisplayName(result);    // don't change it if result is null
            }
        }
    }
}





