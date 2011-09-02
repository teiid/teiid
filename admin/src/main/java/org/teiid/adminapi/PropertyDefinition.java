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

package org.teiid.adminapi;

import java.util.Collection;

/**
 * Information about a property of an AdminObject
 */
public interface PropertyDefinition extends AdminObject {

	public enum RestartType {
		NONE,
		SERVICE,
		PROCESS,
		ALL_PROCESSES,
		CLUSTER
	} 
    /**
     * The value of the maximum multiplicity if the multiplicity is considered unbounded.
     */
    public static final int UNBOUNDED_VALUE = Integer.MAX_VALUE;
    
    
    /**
     * Get the localized display name of this property.
     * @return the displayable name for this property
     */
    String getDisplayName();
    
    /**
     * Get the description of this property.
     * @return the description for this property
     */
    String getDescription();
    
    
    /**
     * Get the name of the java class that best represents the property type.
     * @return the name of the java class that best represents the property type.
     */
    String getPropertyTypeClassName();

    
    /**
     * Get the default value for values of this property, or an empty String if
     * there is no default value.
     * @return the default value for this property, or an empty String
     * if there is no default value.
     */
    Object getDefaultValue();

    /**
     * Get the allowed values for this property.
     * @return the list of allowed values for this property, or an empty
     * set if the values do not have to conform to a fixed set.
     */
    Collection getAllowedValues();

    /**
     * Get whether this property requires the system to be restarted before it takes effect.
     * @return true if this property requires the system to be restarted before it takes effect.
     */
    public RestartType getRequiresRestart();
    
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
     * @see #getAllowedValues
     * @return true if this property's value must be with the list of AllowedValues.
     */
    boolean isConstrainedToAllowedValues();

    /**
     * The "expert" flag is used to distinguish between features that are
     * intended for expert users from those that are intended for normal users.
     * @return true if this property is to be marked with the expert flag,
     * or false otherwise.
     */
    boolean isAdvanced();
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

    
    
}


