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

import java.util.Date;
import java.util.Properties;

import com.metamatrix.common.config.model.ConfigurationVisitor;
import com.metamatrix.common.namedobject.BaseID;
import com.metamatrix.common.namedobject.BaseObject;

/**
 * A ComponentObject represents a physical piece of the configuration and deployment tree.  
 * The basic information contained in it is - id (name), type and properties. 
 */
public interface ComponentObject extends BaseObject {


    
/**
 * Returns the name
 * @return String name of component
 */
    String getName();
/**
 * Returns the current unmodifiable properties for the component
 * @return Properties
 */
    Properties getProperties();

/**
 * Returns a property value for the given property name
 * @param name of the property value to obtain
 * @return String property value
 */
    String getProperty(String name);
/**
 * Returns the component type
 * @return ComponentType of this component
 */
    ComponentTypeID getComponentTypeID();
    /**
     * Returns the description of this component definition
     * @return String description of this component definition
     */
    String getDescription();
    
    
	/**
     * Returns the principal who created this type
     * @return String principal name, or null if this object has only
     * been created locally
     */
    String getCreatedBy();

    /**
     * Returns the Date this type was created
     * @return Date this type was created
     */
    Date getCreatedDate();

    /**
     * Returns the principal who last modified this type
     * @return String principal name
     */
    String getLastChangedBy();

    /**
     * Returns the Date this type was last changed
     * @return Date this type was last changed
     */
    Date getLastChangedDate();
    
    

/**
 *  Returns true if this object is dependent upon the specified <code>ComponentObjectID</code>
 *  @param componentObjectId is the id to check for dependencies for
 *  @return boolean true if this object is dependent
 */
    boolean isDependentUpon(BaseID componentObjectId);


    void accept(ConfigurationVisitor visitor);

}

