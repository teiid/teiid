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

package com.metamatrix.common.config.model;

import java.util.List;

import com.metamatrix.common.config.api.ComponentTypeDefn;
import com.metamatrix.common.config.api.ComponentTypeDefnID;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.ComponentTypePropDefn;
import com.metamatrix.common.namedobject.BasicObject;
import com.metamatrix.common.object.PropertyDefinition;
import com.metamatrix.common.object.PropertyType;

public class BasicComponentTypeDefn extends BasicObject implements ComponentTypeDefn{
    private ComponentTypeID componentTypeID;
    private ComponentTypePropDefn unModifiablePropertyDefinition;
    private PropertyDefinition propertyDefinition;
    private boolean isDeprecated;
    private boolean effectiveImmediately;

    public BasicComponentTypeDefn(ComponentTypeDefnID id,
                              ComponentTypeID typeID,
                              PropertyDefinition propertyDefinition,
                              boolean deprecated, boolean effectiveImmediately) {
    super(id);
        this.componentTypeID = typeID;
        this.setPropertyDefinition(propertyDefinition);
        this.isDeprecated = deprecated;
        this.effectiveImmediately = effectiveImmediately;
    }
    
    /**
     * This constructor is deprecated as of 2.0. should try to use constructor
     * with effectiveImmediately.
     *
     * @deprecated as of 2.0 beta 1, use
     * {@link #BasicComponentTypeDefn(ComponentTypeDefnID, ComponentTypeID, PropertyDefinition, boolean, boolean) BasicComponentTypeDefn}
     */
    public BasicComponentTypeDefn(ComponentTypeDefnID id,
                              ComponentTypeID typeID,
                              PropertyDefinition propertyDefinition,
                              boolean deprecated) {
        this(id, typeID, propertyDefinition, deprecated, ComponentTypeDefn.DEFAULT_IS_EFFECTIVE_IMMEDIATELY);
    } 


    BasicComponentTypeDefn(BasicComponentTypeDefn defn) {
        super(defn.getID());
        this.componentTypeID = defn.getComponentTypeID();
        this.isDeprecated = defn.isDeprecated();
        this.effectiveImmediately = defn.isEffectiveImmediately();
        this.setPropertyDefinition(defn.getClonedPropertyDefinition());
    }


    public ComponentTypeID getComponentTypeID() {
        return componentTypeID;
    }

    public PropertyDefinition getPropertyDefinition() {
        return unModifiablePropertyDefinition;
    }

    public PropertyDefinition getClonedPropertyDefinition() {
        return propertyDefinition;
    }

    public PropertyType getPropertyType() {
        return propertyDefinition.getPropertyType();
    }

    public boolean isDeprecated() {
        return isDeprecated;
    }

    public boolean isRequired() {
        return this.propertyDefinition.isRequired();
    }
    
    public boolean isEffectiveImmediately() {
        return effectiveImmediately;
    }

    public boolean hasAllowedValues() {
        return this.unModifiablePropertyDefinition.isConstrainedToAllowedValues();
    }

    public List getAllowedValues() {
        return unModifiablePropertyDefinition.getAllowedValues();
    }
    

    void setComponentTypeID(ComponentTypeID type) {
        this.componentTypeID = type;
    }

    void setIsEffectiveImmediately(boolean effectiveImmediately) {
        this.effectiveImmediately = effectiveImmediately;
    }
    
    void setIsDeprecated(boolean deprecated) {
        this.isDeprecated = deprecated;
    }

    void setPropertyDefinition(PropertyDefinition propertyDefinition) {
        this.unModifiablePropertyDefinition = new ComponentTypePropDefn(propertyDefinition);
        this.propertyDefinition = propertyDefinition;
    }

    /**
     * Return a deep cloned instance of this object.  Subclasses must override
     *  this method.
     *  @return the object that is the clone of this instance.
     */
   public synchronized Object clone() {
    	return new BasicComponentTypeDefn(this);
    }


}
