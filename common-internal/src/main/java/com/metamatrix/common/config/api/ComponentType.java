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

import java.util.Collection;
import java.util.Date;
import java.util.Properties;

import com.metamatrix.common.config.model.ConfigurationVisitor;
import com.metamatrix.common.namedobject.BaseObject;

public interface ComponentType extends BaseObject {
    
    public static final int CONFIGURATION_COMPONENT_TYPE_CODE = 0;
    public static final int SERVICE_COMPONENT_TYPE_CODE = 1;
    public static final int CONNECTOR_COMPONENT_TYPE_CODE = 2;
    public static final int PRODUCT_COMPONENT_TYPE_CODE = 3;
    public static final int RESOURCE_COMPONENT_TYPE_CODE = 4;      
    public static final int VM_COMPONENT_TYPE_CODE = 5;

//    public static final int PSC_COMPONENT_TYPE_CODE = 6;
    public static final int HOST_COMPONENT_TYPE_CODE = 7;
    public static final int DEPLOYED_COMPONENT_TYPE_CODE = 8;
    
    public static final int AUTHPROVIDER_COMPONENT_TYPE_CODE =11;
   
    
    public static final int SHARED_RESOURCE_COMPONENT_TYPE_CODE = 10;
    

    /**
     * Returns the <code>Collection</code> of <code>ComponentTypeDefn</code>s
     * that are defined for this component type
     * @return Set of ComponentTypeDefn
     */
    Collection getComponentTypeDefinitions();
    
    
    /**
     * Returns the <code>ComponentTypeDefn</code>
     * for the specified name
     * @return ComponentTypeDefn of ComponentTypeDefn
     */
    ComponentTypeDefn getComponentTypeDefinition(String name);    
    

    /**
     * Returns the default values for this component type.
     * Note, it does not return the defaults from its super
     * component type.  To obtain all default properties, call
     * @link ConfigurationModelContainer.getDefaultPropertyValues(); 
     * @return
     * @since 4.3
     */
    Properties getDefaultPropertyValues();
    
    Properties getDefaultPropertyValues(Properties props);
    
    /**
     * Returns the List <String> of properties that are defined as 
     * masked for this component type.
     * 
     */
    Collection getMaskedPropertyNames();
    
    
    /**
     * Returns the <code>String</code> representation of the default
     * value for the requested property. 
     * @param propertyName
     * @return
     * @since 4.2
     */
    String getDefaultValue(String propertyName);

    /**
     * Returns the parent component type id
     * @return ComponentType parent
     */
    ComponentTypeID getParentComponentTypeID();

    /**
     * Returns the super component type id
     * @return ComponentType parent
     */
    ComponentTypeID getSuperComponentTypeID();

    /**
     * Returns int indicating the component type code;
     * @return int
     */
    int getComponentTypeCode();

    /**
     * Returns true is the component type is considered deployable within
     * a configuration.
     * @return boolean true if the component type is deployable
     */
    boolean isDeployable();

    /**
     * Returns true if this component type is no longer used
     * @return boolean true if deprecated
     */
    boolean isDeprecated();

    /**
     * Returns true if this component type is monitored
     * @return boolean true if monitored
     */
    boolean isMonitored(); 
    
    /**
     * Returns true if this component type is a connector type
     * @return boolean true if a connector type
     */
    boolean isOfTypeConnector();   

    /**
     * Returns the principal who created this type
     * @return String principal name, or null if this object has only
     * been created locally
     */
    String getCreatedBy();

    /**
     * Returns a string version of the Date this type was created
     * @return Date this type was created, or null if this object has only
     * been created locally
     */
    Date getCreatedDate();

    /**
     * Returns the principal who last modified this type
     * @return String principal name, or null if this object has only
     * been created locally
     */
    String getLastChangedBy();

    /**
     * Returns a string version of the this type was last changed
     * @return Date this type was last changed, or null if this object has only
     * been created locally
     */
    Date getLastChangedDate();
    
    /**
     * Returns the description, if it has one, of the component type
     * @return String description
     * @since 4.2
     */
    String getDescription();
    
   void accept(ConfigurationVisitor visitor);

}

