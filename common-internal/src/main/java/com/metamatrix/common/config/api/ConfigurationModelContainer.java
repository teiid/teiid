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
import java.util.Map;
import java.util.Properties;

import com.metamatrix.common.config.api.exceptions.ConfigurationException;


/**
 * Created on Sep 18, 2002
 *
 * The ConfigurationModelContainer represents the logical view
 * to obtaining configuration related information.
 * 
 * The persistent layer will build this container 
 */
public interface ConfigurationModelContainer extends Cloneable {
        
    Configuration getConfiguration();
    
    ConfigurationID getConfigurationID();
    
    /**
     * Return all the objects that make up this model.  
     * <b>Note</b>, objects that are modeled under the context of
     * a configuration will be contained in the Configuration
     * object.  The following are the types that are not modeled 
     * under on configuration:
     * <li>Host</li>
     * <li>ComponentTypes,ProductTypes</li>
     * <li>ConnectorBindings</li>
     * <li>Resources</li>
     * @return Collection of objects
     */
    Collection getAllObjects();
    
    
    /**
     * Return a collection of objects of type <code>Host</code>.
     * These are all the hosts defined in this configuration.
     * @return
     * @since 4.2
     */
    Collection getHosts();
    
    /**
     * Return a host based on the host name. 
     * @param fullName
     * @return
     * @since 4.2
     */
    Host getHost(String fullName);
    
    /**
     * Return a map of the component types.
     * <key>type full name <value>ComponentType 
     * @return map of ComponentTypes
     * @since 4.1
     */
    Map getComponentTypes();
    
    /**
     * Return a collection of the <code>ComponentTypeDefinition<code>s for
     * the request component type 
     * @param typeID
     * @return Collection of ComponentTypeDefinitions
     * @since 4.1
     */
    Collection getAllComponentTypeDefinitions(ComponentTypeID typeID);
       
    /**
     * Return a <code>ComponentTypeDefn</code> for a give typeID and defnName.
     * @param typeID identifies the specific @link ComponentType to look for
     * @param defnName idenfities the @link ComponentTypeDefn to look for
     *      in the componentType. 
     * @return ComponentTypeDefn
     * @since 4.1
     */
    ComponentTypeDefn getComponentTypeDefinition(ComponentTypeID typeID, String defnName);
    
    /**
     * Return the ComponentType for the specified name
     * @param fullName
     * @return ComponentType
     * @since 4.1
     */
    ComponentType getComponentType(String fullName);
    
    /**
     * Return a properties object that contains the default properties
     * defined for the specified component type
     * @param componentTypeID
     * @return default Properties for the ComponentType
     * @since 4.1
     */
    Properties getDefaultPropertyValues(ComponentTypeID componentTypeID) ;
    
    Properties getDefaultPropertyValues(Properties defaultProperties, ComponentTypeID componentTypeID) ;
    
    
    /**
     * Return a collection of objects of type <code>ResourceDescriptor</code>.
     * These are all the hosts defined in this configuration.
     * @return
     * @since 4.2
     */
    
    Collection getConnectionPools();
      
    /**
     * Return a collection of objects of type <code>ProductType</code>.
     * These are all the hosts defined in this configuration.
     * @return
     * @since 4.2
     */
 //   Collection getProductTypes();
    
    /**
     * Return a <code>ProductType</code> based on the specified name.
     * @return
     * @since 4.2
     */
 //   ProductType getProductType(String fullname);    
    
    SharedResource getResource(String resourceName);
    
    Collection getResources();
    
	           
    Object clone();
    
}
