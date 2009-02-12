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

import java.util.List;

/**
 * Defines the type of property that will be placed in a detail panel or table
 */
public interface PropertyDefinition extends ObjectDefinition {

    /**
     * Get the type for values of this property.
     * @return the type that best describes the values of this property.
     */
    PropertyType getPropertyType();

    /**
     * Get the default value for values of this property, or an empty String if
     * there is no default value.
     * @see #hasDefaultValue
     * @return the default value for this property, or an empty String
     * if there is no default value.
     */
    Object getDefaultValue();

    /**
     * Get the allowed values for this property.
     * @return the unmodifiable list of allowed values for this property, or an empty
     * set if the values do not have to conform to a fixed set.
     * @see #hasAllowedValues
     */
    List getAllowedValues();

    /**
     * Get whether this property requires the system to be restarted before it takes effect.
     * @return true if this property requires the system to be restarted before it takes effect.
     */
    public boolean getRequiresRestart();
    
    /**
     * The modifiable flag is used to identify features that may not be changed once
     * they are set.
     * @return true if this property is marked with the modifyable
     * flag, or false otherwise.
     */
    boolean isModifiable();

    /**
     * Return whether the value or values for this property are constrained to be only
     * those in the AllowedValues list.
     * @see #hasAllowedValues
     * @see #getAllowedValues
     * @return true if this property's value must be with the list of AllowedValues.
     */
    boolean isConstrainedToAllowedValues();

    /**
     * Return whether there is a default value for this property.
     * @see #getDefaultValue
     * @return true if this property has a default value or false if there is
     * no default value.
     */
    boolean hasDefaultValue();

    /**
     * Return whether there is a prescribed set of values that all property values
     * should be selected from.
     * @see #getAllowedValues
     * @return true if this property has a set from which all values must be
     * selected, or false if the property values may be any value.
     */
    boolean hasAllowedValues();

    /**
     * Return the text expression that is used to delimit multiple values
     * within a single String value.
     * @return the delimiter String; may be null only if the multiplicity
     * has a maximum value of 1.
     */
    String getValueDelimiter();
    /**
     * The "expert" flag is used to distinguish between features that are
     * intended for expert users from those that are intended for normal users.
     * @return true if this property is to be marked with the expert flag,
     * or false otherwise.
     */
    boolean isExpert();
    /**
     * The "preferred" flag is used to identify features that are particularly important
     * for presenting to humans.
     * @return true if this property is marked with the preferred
     * flag, or false otherwise.
     */
    boolean isPreferred();
    /**
     * The "hidden" flag is used to identify features that are intended only for tool
     * use, and which should not be exposed to humans.
     * @return true if this property is marked with the hidden
     * flag, or false otherwise.
     */
    boolean isHidden();
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
    boolean isRequired();
    /**
     * The "masked" flag is used to tell whether the value should be masked
     * when displayed to users.
     * @return true if this property value is to be masked,
     * or false otherwise.
     */
    boolean isMasked();

    /**
     * Get the multiplicity specification for this property.
     * @return the instance of Multiplicity that captures the allowable
     * range of the cardinality of property values; never null
     */
    Multiplicity getMultiplicity();

    /**
     * Convert the specified values to a stringified form.  This method uses the
     * <code>toString</code> method on the values.
     * @param values the array of values that this definition describes; may not be null
     * @return the stringified form of the values; never null
     */
    String getValuesAsString( Object[] values );

    /**
     * Convert the specified values to a stringified form.  This method uses the
     * <code>toString</code> method on the values.
     * @param values the array of values that this definition describes; may not be null
     * @param delim the delimiter to use, overriding the property definition's
     * set of values; if null, the property definition's delimiter will be used, or
     * if there is no delimiter defined for the property definition, the default delimiter of ','
     * @return the stringified form of the values; never null
     */
    String getValuesAsString( Object[] values, String delim );

    /**
     * Convert the stringified form to an array of String values.
     * @param stringifiedValue the stringified form of the values
     * @return the array of String values; never null, but may by empty
     */
    Object[] getValuesFromString( String stringifiedValues );

    /**
     * Convert the stringified form to an array of String values.
     * @param stringifiedValue the stringified form of the values
     * @param delim the delimiter to use, overriding the property definition's
     * set of values; if null, the property definition's delimiter will be used, or
     * if there is no delimiter defined for the property definition, the default delimiter of ','
     * @return the array of String values; never null, but may by empty
     */
    Object[] getValuesFromString( String stringifiedValues, String delim );
}


