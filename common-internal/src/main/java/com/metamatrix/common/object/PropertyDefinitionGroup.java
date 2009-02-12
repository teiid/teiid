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

import com.metamatrix.core.util.ArgCheck;

/**
 * PropertyDefinitionGroup is a nestable list of PropertyDefinitions that are defined to
 * be used together.  The data structure also contains a name, displayName, and other
 * ObjectDefinition properties.
 */
public class PropertyDefinitionGroup extends ObjectDefinitionImpl {

    private List propertyDefinitions;
    private List subGroups;

    /**
     * Create an instance of the PropertyDefinitionGroup, specifying the list of
     * PropertyDefinition instances to be used.
     * @param name the identifier of this instance.
     * @param displayName the displayable name for this property; or null
     * if the display name is the same as the property name.
     * @param shortDescription the short description for this property, or
     * null if there is no short description.
     * @param propertyDefinitions the list of PropertyDefinition instances; the list
     * is assumed to be immutable, and may never be null
     */
    public PropertyDefinitionGroup(String name, String displayName, String shortDescription,
            List propertyDefinitions) {
        this(name, displayName, shortDescription, propertyDefinitions, null);
    }

    /**
     * Create an instance of the PropertyDefinitionGroup, specifying the list of
     * PropertyDefinition instances to be used and an optional List of subgroup definitions
     * @param name the identifier of this instance.
     * @param displayName the displayable name for this property; or null
     * if the display name is the same as the property name.
     * @param shortDescription the short description for this property, or
     * null if there is no short description.
     * @param propertyDefinitions the list of PropertyDefinition instances; the list
     * is assumed to be immutable, and may never be null
     * @param subPropertyDefinitionGroups the list of PropertyDefinitionGroup instances that are
     * contained by this instance; the list is assumed to be immutable, may be null or an empty list.
     */
    public PropertyDefinitionGroup(String name, String displayName, String shortDescription,
            List propertyDefinitions, List subPropertyDefinitionGroups) {
        super(name, displayName);
		ArgCheck.isNotNull(propertyDefinitions);
        this.setShortDescription(shortDescription);
        this.propertyDefinitions = propertyDefinitions;
        this.subGroups = subPropertyDefinitionGroups;
    }

    /**
     * obtain the List of PropertyDefinitions for this instance.
     * @return the list of PropertyDefinitions.
     */
    public List getPropertyDefinitions() {
        return this.propertyDefinitions;
    }

    /**
     * determine if this PropertiedObjectGroup has subgroups.
     * @return true if subgroups exist, false if not.
     */
    public boolean hasSubgroups() {
        boolean result = false;
        if ( subGroups != null && ! subGroups.isEmpty() ) {
            result = true;
        }
        return result;
    }

    /**
     * obtain the List of PropertyDefinitionGroups that are contained by this instance.
     * @return the subgroups, may be null if there are none.
     */
    public List getSubGroups() {
        return this.subGroups;
    }


}

