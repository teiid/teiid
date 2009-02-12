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

package com.metamatrix.common.config.api;

import java.io.Serializable;
import java.util.List;

import com.metamatrix.common.object.Multiplicity;
import com.metamatrix.common.object.PropertyDefinition;
import com.metamatrix.common.object.PropertyDefinitionImpl;
import com.metamatrix.common.object.PropertyType;

/**
 * ComponentTypePropDefn is an unmodifiable wrapper for the PropertyDefinition.
 * Notice that this contains an origPropertyDefinition from which it returns
 * values.  This is done so that an instance of this class
 * can be instantiated and passed a PropertyDefinition.
 */
public class ComponentTypePropDefn implements PropertyDefinition, Serializable {

    private PropertyDefinition origPropertyDefinition;

    public ComponentTypePropDefn() {
        origPropertyDefinition = new PropertyDefinitionImpl();
    }

    public ComponentTypePropDefn(PropertyDefinition definition) {
        if (definition == null) {
            throw new IllegalArgumentException("Unable to make a copy of a null property definition object"); //$NON-NLS-1$
        }

        origPropertyDefinition = definition;
    }

    public ComponentTypePropDefn(String name, String displayName, PropertyType type,
                        Multiplicity multiplicity,  String shortDescription,
                        String defaultValue, List allowedValues, String valueDelimiter,
                        boolean isHidden, boolean isPreferred, boolean isExpert, boolean isModifiable) {

        origPropertyDefinition = new PropertyDefinitionImpl(name, displayName, type, multiplicity, shortDescription,
                                                            defaultValue, allowedValues, valueDelimiter, isHidden, isPreferred,
                                                            isExpert, isModifiable);
    }

    public ComponentTypePropDefn(String name, String displayName, PropertyType type,
                        Multiplicity multiplicity) {
        origPropertyDefinition = new PropertyDefinitionImpl(name,displayName,type,multiplicity);
    }


    /**
     * Get the actual or programmatic name of this property.
     * @return the property's name (never null)
     */
    public String getName() {
        return origPropertyDefinition.getName();
    }

    /**
     * Get the localized display name of this property.
     * @return the displayable name for this property (never null)
     */
    public String getDisplayName() {
        return origPropertyDefinition.getDisplayName();
    }

    /**
     * Get the short description of this property.
     * @return the short description for this property (never null)
     */
    public String getShortDescription() {
        return origPropertyDefinition.getShortDescription();
    }

    /**
     * Get the multiplicity specification for this property.
     * @return the instance of Multiplicity that captures the allowable
     * range of the cardinality of property values; never null
     */
    public Multiplicity getMultiplicity() {
        return origPropertyDefinition.getMultiplicity();
    }


    /**
     * The "required" flag is used to identify features that require at least
     * one value (possibly a default value) by the consumer of the property.  Whether
     * a property definition is required or not can be determined entirely from the
     * multiplicity: if the multiplicity includes '0', then the property is
     * not required.
     * <p>
     * Whether a property is required by the consumer is unrelated to whether
     * there is a default value, which only simplifies the task of the property
     * provider.  A property may be required, meaning it must have at least one
     * value, but that same property definition may or may not have a default.
     * The combination of required and whether it has a default will determine
     * whether the user must supply a value.
     * @return true if this property requires at least one value.
     */
    public boolean isRequired() {
        return origPropertyDefinition.isRequired();
    }

    /**
     * The "hidden" flag is used to identify features that are intended only for tool
     * use, and which should not be exposed to humans.
     * @return true if this property is marked with the hidden
     * flag, or false otherwise.
     */
    public boolean isHidden() {
        return origPropertyDefinition.isHidden();
    }

    
    /** 
     * @see com.metamatrix.common.object.PropertyDefinition#getRequiresRestart()
     * @since 4.3
     */
    public boolean getRequiresRestart() {
        return origPropertyDefinition.getRequiresRestart();
    }

    /**
     * The "masked" flag is used to tell whether the value should be masked
     * when displayed to users.
     * @return true if this property value is to be masked,
     * or false otherwise.
     */
    public boolean isMasked(){
        return origPropertyDefinition.isMasked();
    }

    /**
     * The modifiable flag is used to identify features that may not be changed once
     * they are set.
     * @return true if this property is marked with the modifyable
     * flag, or false otherwise.
     */
    public boolean isModifiable(){
        return origPropertyDefinition.isModifiable();
    }


    /**
     * The "preferred" flag is used to identify features that are particularly important
     * for presenting to humans.
     * @return true if this property is marked with the preferred
     * flag, or false otherwise.
     */
    public boolean isPreferred() {
        return origPropertyDefinition.isPreferred();
    }

    /**
     * The "expert" flag is used to distinguish between features that are
     * intended for expert users from those that are intended for normal users.
     * @return true if this property is to be marked with the expert flag,
     * or false otherwise.
     */
    public boolean isExpert() {
        return origPropertyDefinition.isExpert();
    }

    /**
     * Get the type for values of this property.
     * @return the type that best describes the values of this property.
     */
    public PropertyType getPropertyType() {
        return origPropertyDefinition.getPropertyType();
    }


    /**
     * Get the default value for values of this property, or an empty String if
     * there is no default value.
     * @see #hasDefaultValue
     * @return the default value for this property, or null
     * if there is no default value.
     */
    public Object getDefaultValue() {
        return origPropertyDefinition.getDefaultValue();
    }

    /**
     * Get the allowed values for this property.
     * @return the unmodifiable list of allowed values for this property, or an empty
     * set if the values do not have to conform to a fixed set.
     * @see #hasAllowedValues
     */
    public List getAllowedValues() {
        return origPropertyDefinition.getAllowedValues();
    }

    /**
     * Return whether there is a default value for this property.
     * @see #getDefaultValue
     * @return true if this property has a default value or false if there is
     * no default value.
     */
    public boolean hasDefaultValue() {
        return origPropertyDefinition.hasDefaultValue();
    }


    /**
     * Return whether the value or values for this property are constrained to be only
     * those in the AllowedValues list.
     * @see #hasAllowedValues
     * @see #getAllowedValues
     * @return true if this property's value must be with the list of AllowedValues.
     */
    public boolean isConstrainedToAllowedValues(){
        return origPropertyDefinition.isConstrainedToAllowedValues();
    }


    /**
     * Return whether there is a prescribed set of values that all property values
     * should be selected from.
     * @see #getAllowedValues
     * @return true if this property has a set from which all values must be
     * selected, or false if the property values may be any value.
     */
    public boolean hasAllowedValues() {
        return origPropertyDefinition.hasAllowedValues();
    }

    /**
     * Return the text expression that is used to delimit multiple values
     * within a single String value.
     * @return the delimiter String; may be null only if the multiplicity
     * has a maximum value of 1.
     */
    public String getValueDelimiter() {
        return origPropertyDefinition.getValueDelimiter();
    }

    /**
     * Get the localized display name of this property.
     * @return the displayable name for this property (never null)
     */
    public String getShortDisplayName(){
        return origPropertyDefinition.getShortDisplayName();
    }

    /**
     * Get the localized display name of this property.
     * @return the displayable name for this property (never null)
     */
    public String getPluralDisplayName(){
        return origPropertyDefinition.getPluralDisplayName();
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
        //return origPropertyDefinition.compareTo(obj);

        ComponentTypePropDefn that = (ComponentTypePropDefn)obj; // May throw ClassCastException
        if (obj == null) {
            throw new IllegalArgumentException("Attempt to compare null"); //$NON-NLS-1$
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
        //return origPropertyDefinition.equals(obj);
        
        // Check if instances are identical ...
        if (this == obj) {
            return true;
        }

        // Check if object can be compared to this one
        // (this includes checking for null ) ...
        if (this.getClass().isInstance(obj)) {
            ComponentTypePropDefn that = (ComponentTypePropDefn)obj;
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
        return origPropertyDefinition.hashCode();
    }

    /**
     * Returns a string representing the current state of the object.
     * @return the string representation of this instance.
     */
    public String toString() {
        return origPropertyDefinition.toString();
    }

    /**
     * Returns a clone of this ComponentTypePropDefn; also, a copy is made
     * of the inner, wrapped PropertyDefinition instance.
     */
    /*
    public Object clone() {
        PropertyDefinition newInnerDefn = new PropertyDefinitionImpl(origPropertyDefinition);
        return new ComponentTypePropDefn(newInnerDefn);
    }
    //PropertyDefinition instances are unchangeable, so isn't a clone method
    //unnecessary?

    */

    /**
     * Convert the specified values to a stringified form.  This method uses the
     * <code>toString</code> method on the values.
     * @param values the array of values that this definition describes; may not be null
     * @return the stringified form of the values; never null
     */
    public String getValuesAsString( Object[] values ) {
        return origPropertyDefinition.getValuesAsString(values);
    }

    /**
     * Convert the stringified form to an array of String values.
     * @param stringifiedValue the stringified form of the values
     * @return the array of String values; never null, but may by empty
     */
    public Object[] getValuesFromString( String stringifiedValues ) {
        return origPropertyDefinition.getValuesFromString(stringifiedValues);
    }
    
    /**
     * Convert the specified values to a stringified form.  This method uses the
     * <code>toString</code> method on the values.
     * @param values the array of values that this definition describes; may not be null
     * @param delim the delimiter to use, overriding the property definition's
     * set of values; if null, the property definition's delimiter will be used, or
     * if there is no delimiter defined for the property definition, the default delimiter of ','
     * @return the stringified form of the values; never null
     */
    public String getValuesAsString( Object[] values, String delim ) {
        return origPropertyDefinition.getValuesAsString(values, delim);
    }

    /**
     * Convert the stringified form to an array of String values.
     * @param stringifiedValue the stringified form of the values
     * @param delim the delimiter to use, overriding the property definition's
     * set of values; if null, the property definition's delimiter will be used, or
     * if there is no delimiter defined for the property definition, the default delimiter of ','
     * @return the array of String values; never null, but may by empty
     */
    public Object[] getValuesFromString( String stringifiedValues, String delim ) {
        return origPropertyDefinition.getValuesFromString(stringifiedValues, delim);
    }
    
}
