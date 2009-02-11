/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.core.util.StringUtil;

/**
 * Prototype implementation of PropertyDefinition
 */
public class PropertyDefinitionImpl extends ObjectDefinitionImpl implements PropertyDefinition {

    public static final List BOOLEAN_ALLOWED_VALUES = Collections.unmodifiableList( Arrays.asList( new Object[]{Boolean.FALSE.toString(),Boolean.TRUE.toString()} ) );

    public static final PropertyType DEFAULT_TYPE = PropertyType.STRING;
    public static final String DEFAULT_DELIMITER = ","; //$NON-NLS-1$
    public static final Object DEFAULT_VALUE = null;
    public static final boolean DEFAULT_IS_EXPERT = false;
    public static final boolean DEFAULT_IS_PREFERRED = false;
    public static final boolean DEFAULT_IS_HIDDEN = false;
    public static final boolean DEFAULT_IS_MASKED = false;
    public static final boolean DEFAULT_IS_CONSTRAINED = true;
    public static final boolean DEFAULT_IS_MODIFIABLE = true;
    public static final boolean DEFAULT_REQUIRES_RESTART = false;
    public static final String DEFAULT_MULTIPLICITY = "0..1"; //$NON-NLS-1$
    public static final String DEFAULT_DEFAULT_VALUE = null; 
    public static final String DEFAULT_DISPLAY_NAME = null; 
    public static final String DEFAULT_SHORT_DESCRIPTION = null; 

    private boolean modifiable = DEFAULT_IS_MODIFIABLE;
    private boolean constrained = DEFAULT_IS_CONSTRAINED;
    private boolean hidden = DEFAULT_IS_HIDDEN;
    private boolean preferred = DEFAULT_IS_PREFERRED;
    private boolean expert = DEFAULT_IS_EXPERT;
    private boolean masked = DEFAULT_IS_MASKED;
    private boolean requiresRestart = DEFAULT_REQUIRES_RESTART;

    private Object defaultValue = DEFAULT_VALUE;
    private List allowedValues = new ArrayList();
    private String valueDelimiter = DEFAULT_DELIMITER;
    private PropertyType type = DEFAULT_TYPE;

    /**
     * @clientCardinality 0..1
     * @supplierCardinality 1
     */
    private Multiplicity multiplicity;

    /**
     * Create an empty property definition object with all defaults.
     */
    public PropertyDefinitionImpl() {
        super();
        this.multiplicity = Multiplicity.getInstance();
    }

    /**
     * Create a property definition object with the specified set of attributes.
     * The object is created without a short description, with no default value,
     * with the default value-delimiter, with no prescribed allowable values,
     * and as not required, not hidden, not preferred, and not expert.
     * @param name the new property name; or null if there is no name for this
     * property definition
     * @param type the new property type; if null, the default type
     * (PropertyType.STRING) is used.
     * @param multiplicity the instance of Multiplicity that captures the allowable
     * range of the cardinality of property values; if null, the default
     * multiplicity of "1" is used.
     */
    public PropertyDefinitionImpl(String name, PropertyType type, Multiplicity multiplicity) {
        super(name);
        this.setPropertyType(type);
        this.setMultiplicity(multiplicity);
    }

    /**
     * Create a property definition object with the specified set of attributes.
     * The object is created without a short description, with no default value,
     * with the default value-delimiter, with no prescribed allowable values,
     * and as not required, not hidden, not preferred, and not expert.
     * @param name the new property name; or null if there is no name for this
     * property definition
     * @param displayName the displayable name for this property; or null
     * if the display name is the same as the property name.
     * @param type the new property type; if null, the default type
     * (PropertyType.STRING) is used.
     * @param multiplicity the instance of Multiplicity that captures the allowable
     * range of the cardinality of property values; if null, the default
     * multiplicity of "1" is used.
     */
    public PropertyDefinitionImpl(String name, String displayName, PropertyType type,
                        Multiplicity multiplicity) {
        super(name,displayName);
        this.setPropertyType(type);
        this.setMultiplicity(multiplicity);
    }
    /**
     * Create a property definition object with the fully-specified set of attributes.
     * @param name the new property name; or null if there is no name for this
     * property definition
     * @param displayName the displayable name for this property; or null
     * if the display name is the same as the property name.
     * @param type the new property type; if null, the default type
     * (PropertyType.STRING) is used.
     * @param multiplicity the instance of Multiplicity that captures the allowable
     * range of the cardinality of property values; if null, the default
     * multiplicity of "1" is used.
     * @param shortDescription the short description for this property, or
     * null if there is no short description.
     * @param defaultValue the new default value for this property, or null
     * if there is to be no default value.
     * @param allowedValues the list of allowable values for this property,
     * or an empty set or null reference if there is no prescribed set of values.
     * @param valueDelimiter the delimiter String; may be null only if the multiplicity
     * has a maximum value of 1.  The default delimiter expression is a
     * single comma.
     * @param isHidden true if this property definition is intended only for tool
     * use, and which should not be exposed to humans.
     * @param isPreferred true if this property definition is particularly important
     * for presenting to humans.
     * @param isExpert true if this property definition is intended for expert users
     * an not for normal users.
     */
    public PropertyDefinitionImpl(String name, String displayName, PropertyType type,
                        Multiplicity multiplicity,  String shortDescription,
                        Object defaultValue, List allowedValues, String valueDelimiter,
                        boolean isHidden, boolean isPreferred, boolean isExpert ) {
        super(name,displayName);
        this.setPropertyType(type);
        this.setMultiplicity(multiplicity);
        this.setShortDescription(shortDescription);
        this.setDefaultValue(defaultValue);
        this.setAllowedValues(allowedValues);
        this.setHidden(isHidden);
        this.setPreferred(isPreferred);
        this.setExpert(isExpert);
        this.setValueDelimiter(valueDelimiter);
    }

    public PropertyDefinitionImpl(String name, String displayName, PropertyType type,
                        Multiplicity multiplicity,  String shortDescription,
                        Object defaultValue, List allowedValues, String valueDelimiter,
                        boolean isHidden, boolean isPreferred, boolean isExpert, boolean isModifiable) {
        this(name,displayName,type,multiplicity,shortDescription,defaultValue,allowedValues,valueDelimiter,
             isHidden,isPreferred,isExpert);
        this.setModifiable(isModifiable);
    }
    /**
     * Create a property definition object that is a copy of the specified
     * property definition.
     * @param defn the definition that is to be copied; may not be null
     */
    public PropertyDefinitionImpl(PropertyDefinition defn) {
        super(defn);
        this.setDefaultValue(defn.getDefaultValue());
        this.setPropertyType(defn.getPropertyType());
        this.setMultiplicity(defn.getMultiplicity());
        this.setAllowedValues(defn.getAllowedValues());
        this.setHidden(defn.isHidden());
        this.setPreferred(defn.isPreferred());
        this.setExpert(defn.isExpert());
        this.setValueDelimiter(defn.getValueDelimiter());
        this.setModifiable(defn.isModifiable());
        this.setMasked(defn.isMasked());
        this.setConstrainedToAllowedValues(defn.isConstrainedToAllowedValues());
        this.setRequiresRestart(defn.getRequiresRestart());
    }
    
    
    
    /** 
     * @see com.metamatrix.common.object.PropertyDefinition#getRequiresRestart()
     * @since 4.3
     */
    public boolean getRequiresRestart () {
        return this.requiresRestart;
    }

    /**
     * Set whether this property requires the system to be restarted before it takes effect.
     */
    public void setRequiresRestart(boolean flag) {
        this.requiresRestart = flag;
    }
    
    /**
     * The modifiable flag is used to identify features that may not be changed once
     * they are set.
     * @return true if this property is marked with the modifyable
     * flag, or false otherwise.
     */
    public boolean isModifiable() {
        return this.modifiable;
    }

    public void setModifiable(boolean flag) {
        this.modifiable = flag;
    }

    /**
     * Return whether the value or values for this property are constrained to be only
     * those in the AllowedValues list.
     * @see #hasAllowedValues
     * @see #getAllowedValues
     * @return true if this property's value must be with the list of AllowedValues.
     */
    public boolean isConstrainedToAllowedValues() {
        return this.constrained;
    }

    public void setConstrainedToAllowedValues(boolean flag) {
        this.constrained = flag;
    }
    /**
     * The "hidden" flag is used to identify features that are intended only for tool
     * use, and which should not be exposed to humans.
     * @return true if this property is marked with the hidden
     * flag, or false otherwise.
     */
    public boolean isHidden() {
        return this.hidden;
    }
    /**
     * The "hidden" flag is used to identify features that are intended only for tool
     * use, and which should not be exposed to humans.
     * @param hidden true if this property is to be marked with the hidden flag,
     * or false otherwise.
     */
    public void setHidden(boolean hidden) {
        this.hidden = hidden;
    }
    /**
     * The "preferred" flag is used to identify features that are particularly important
     * for presenting to humans.
     * @return true if this property is marked with the preferred
     * flag, or false otherwise.
     */
    public boolean isPreferred() {
        return this.preferred;
    }
    /**
     * The "preferred" flag is used to identify features that are particularly important
     * for presenting to humans.
     * @param preferred true if this property is to be marked with the preferred
     * flag, or false otherwise.
     */
    public void setPreferred(boolean preferred) {
        this.preferred = preferred;
    }
    /**
     * The "masked" flag is used to tell whether the value should be masked
     * when displayed to users.
     * @return true if this property value is to be masked,
     * or false otherwise.
     */
    public boolean isMasked() {
        return this.masked;
    }
    /**
     * The "masked" flag is used to tell whether the value should be masked
     * when displayed to users.
     * @param masked true if this property is to be masked,
     * or false otherwise.
     */
    public void setMasked(boolean masked) {
        this.masked = masked;
    }
    /**
     * The "expert" flag is used to distinguish between features that are
     * intended for expert users from those that are intended for normal users.
     * @return true if this property is to be marked with the expert flag,
     * or false otherwise.
     */
    public boolean isExpert() {
        return this.expert;
    }
    /**
     * The "expert" flag is used to distinguish between features that are intended for
     * expert users from those that are intended for normal users.
     * @param expert true if this property is to be marked with the expert flag,
     * or false otherwise.
     */
    public void setExpert(boolean expert) {
        this.expert = expert;
    }
    /**
     * Get the type for values of this property.
     * @return the type that best describes the values of this property.
     */
    public PropertyType getPropertyType() {
        return this.type;
    }
    /**
     * Set the type for values of this property.
     * @param type the new property type; if null, the default type
     * (PropertyType.STRING) is used.
     */
    public void setPropertyType(PropertyType type) {
        this.type = (type != null ? type : DEFAULT_TYPE);
    }
    /**
     * Get the default value for values of this property, or an empty String if
     * there is no default value.
     * @see #hasDefaultValue
     * @return the default value for this property, or null if there is no default value.
     */
    public Object getDefaultValue() {
        // If this property definition can ONLY have one value ...
        if ( this.multiplicity != null && this.multiplicity.getMaximum() > 1 ) {
            return null;
        }
        return this.defaultValue;
    }
    /**
     * Set the default value fo values of this property.
     * @param defaultValue the new default value for this property, or null
     * if there is to be no default value.
     */
    public void setDefaultValue(Object defaultValue) {
        if ( PropertyType.BOOLEAN.getClassName().equals(this.getPropertyType().getClassName()) && defaultValue instanceof String ) {
            this.defaultValue = Boolean.valueOf((String)defaultValue);
        } else {
            this.defaultValue = defaultValue;
        }
    }

    /**
     * Get the allowed values for this property.
     * @return the unmodifiable list of allowed values for this property, or an empty
     * set if the values do not have to conform to a fixed set.
     * @see #hasAllowedValues
     */
    public List getAllowedValues() {
        return this.allowedValues;
    }
    /**
     * Set the list of allowed values for this property.
     * @param allowedValues the new list of allowable values for this property,
     * or an empty set or null reference if there is no prescribed set of values.
     */
    public void setAllowedValues(List allowedValues) {
        this.allowedValues = (allowedValues != null ? allowedValues : new ArrayList());
        this.allowedValues = Collections.unmodifiableList(this.allowedValues);
    }
    /**
     * Return the text expression that is used to delimit multiple values
     * within a single String value.
     * @return the delimiter String; may be null only if the multiplicity
     * has a maximum value of 1.
     */
    public String getValueDelimiter() {
        return this.valueDelimiter;
    }
    /**
     * Set the text expression that is used to delimit multiple values
     * within a single String value.
     * @param delim the delimiter String; may be null only if the multiplicity
     * has a maximum value of 1.  The default delimiter expression is a
     * single comma.
     */
    public void setValueDelimiter(String delim) {
        this.valueDelimiter = delim;
    }
    /**
     * Return whether there is a default value for this property.
     * @see #getDefaultValue
     * @return true if this property has a default value or false if there is
     * no default value.
     */
    public boolean hasDefaultValue() {
        // If this property definition can ONLY have one value ...
        if ( this.multiplicity != null && this.multiplicity.getMaximum() > 1 ) {
            return false;
        }
        if ( this.defaultValue == null ) {
            return false;
        }
        return true;
    }
    /**
     * Return whether there is a prescribed set of values that all property values
     * should be selected from.
     * @see #getAllowedValues
     * @return true if this property has a set from which all values must be
     * selected, or false if the property values may be any value.
     */
    public boolean hasAllowedValues() {
        return (!this.allowedValues.isEmpty());
    }

    /**
     * Get the multiplicity specification for this property.
     * @return the instance of Multiplicity that captures the allowable
     * range of the cardinality of property values; never null
     */
    public Multiplicity getMultiplicity() {
        return this.multiplicity;
    }

    /**
     * Set the multiplicity specification for this property.
     * @param multiplicity the instance of Multiplicity that captures the allowable
     * range of the cardinality of property values; if null, the default
     * multiplicity of "1" is used.
     */
    public void setMultiplicity(Multiplicity multiplicity) {
        if (multiplicity == null) {
            this.multiplicity = Multiplicity.getInstance();
        } else {
            this.multiplicity = multiplicity;
        }
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
        return !this.multiplicity.isIncluded(0);
    }

    public Object clone() {
        return new PropertyDefinitionImpl(this);
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
        PropertyDefinitionImpl that = (PropertyDefinitionImpl)obj; // May throw ClassCastException
        if (obj == null) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.OBJECT_ERR_0007, this.getClass().getName()));
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
        // Check if instances are identical ...
        if (this == obj) {
            return true;
        }

        // Check if object can be compared to this one
        // (this includes checking for null ) ...
        if (this.getClass().isInstance(obj)) {
            PropertyDefinitionImpl that = (PropertyDefinitionImpl)obj;
            return this.getName().equals(that.getName());
        }

        // Otherwise not comparable ...
        return false;
    }

    /**
     * Convert the specified values to a stringified form.  This method uses the
     * <code>toString</code> method on the values.
     * @param values the array of values that this definition describes; may not be null
     * @return the stringified form of the values; never null
     */
    public String getValuesAsString( Object[] values ) {
		ArgCheck.isNotNull(values);
        return getValuesAsString(values,this.getValueDelimiter());
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
    	ArgCheck.isNotNull(values);

        // Make sure there is a delimiter of some form!
        if ( delim == null ) {
            delim = this.getValueDelimiter();
        }
        if ( delim == null ) {
            delim = PropertyDefinitionImpl.DEFAULT_DELIMITER;
        }
        return StringUtil.join(Arrays.asList(values),delim);
    }

    /**
     * Convert the stringified form to an array of String values.
     * @param stringifiedValue the stringified form of the values
     * @return the array of String values; never null, but may by empty
     */
    public Object[] getValuesFromString( String stringifiedValues ) {
        return getValuesFromString(stringifiedValues,this.getValueDelimiter());
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
        // Shortcut this method ...
        if ( stringifiedValues == null || stringifiedValues.length() == 0 ) {
            return new Object[0];
        }

        // Make sure there is a delimiter of some form!
        if ( delim == null ) {
            delim = this.getValueDelimiter();
        }
        if ( delim == null ) {
            delim = PropertyDefinitionImpl.DEFAULT_DELIMITER;
        }
        List result = StringUtil.getTokens(stringifiedValues,delim);
        return result.toArray();
    }
}


