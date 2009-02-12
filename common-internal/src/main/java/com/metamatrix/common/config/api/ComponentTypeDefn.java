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

import java.util.List;

import com.metamatrix.common.namedobject.BaseObject;
import com.metamatrix.common.object.PropertyDefinition;
import com.metamatrix.common.object.PropertyType;

/**
* The ComponentTypeDefn is a definition defined for a specific ComponentType.
* One or more ComponentTypeDefns will be defined for a ComponentType.
*/
public interface ComponentTypeDefn extends BaseObject {

    /**
     * This is the default value for the
     * {@link #isEffectiveImmediately effective immediately} attribute of
     * a ComponentTypeDefn. It should be used to create a
     * ComponentTypeDefn is the value is unknown.
     */
    public static final boolean DEFAULT_IS_EFFECTIVE_IMMEDIATELY = false;

    /**
    * Returns the id for ComponentType this definition applies to;
    * @return ComponentTypeID
    */
    ComponentTypeID getComponentTypeID();

    /**
    * Returns an unmodifiable PropertyDefinition
    * @return unmodifiable PropertyDefinition
    */
    PropertyDefinition getPropertyDefinition();

    /**
    * Returns a modifiable PropertyDefinition.  To save the changes
    * made to the property definition, must call the modify method
    * on the ObjectEditor.
    * @return modifiable PropertyDefinition
    */
    PropertyDefinition getClonedPropertyDefinition();

    /**
    * Returns a boolean indicating if this definition has allowed values
    * @return boolean of true if allowed values exist
    */
    boolean hasAllowedValues();

    /**
    * Returns a list of PropDefnAllowedValue objects
    * @return allowed values of type PropDefnAllowedValue
    */
    List getAllowedValues();

    /**
    * Returns the property type
    * @return PropertyType
    */
    PropertyType getPropertyType();

    /**
    * Returns boolean indicating if this type definition is deprecated or not
    * @return boolean is true if this definition is deprecated
    */
    boolean isDeprecated();

    /**
    * Returns boolean indicating if this definition is required for this
    * component type.
    * @return boolean is true is the definition is required
    */
    boolean isRequired();
    
    /**
    * Are changes to the property that this ComponentTypeDefn represents effective
    * immediately or only on next startup of the server.
    *
    * @return true if changes are effective immediately.
    */
    boolean isEffectiveImmediately();
    
}
