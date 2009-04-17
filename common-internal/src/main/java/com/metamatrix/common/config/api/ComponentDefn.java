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


public interface ComponentDefn extends
                              ComponentObject {

    /*
     * The resource descriptor and shared resource  
     */
    public static final int CONFIGURATION_COMPONENT_CODE = ComponentType.CONFIGURATION_COMPONENT_TYPE_CODE;
    public static final int SERVICE_COMPONENT_CODE = ComponentType.SERVICE_COMPONENT_TYPE_CODE;
    public static final int CONNECTOR_COMPONENT_CODE = ComponentType.CONNECTOR_COMPONENT_TYPE_CODE;
    public static final int PRODUCT_COMPONENT_CODE = ComponentType.PRODUCT_COMPONENT_TYPE_CODE;
    public static final int VM_COMPONENT_CODE = ComponentType.VM_COMPONENT_TYPE_CODE;
    public static final int RESOURCE_DESCRIPTOR_COMPONENT_CODE = ComponentType.RESOURCE_COMPONENT_TYPE_CODE;  
    public static final int HOST_COMPONENT_CODE = ComponentType.HOST_COMPONENT_TYPE_CODE;
    public static final int SHARED_RESOURCE_COMPONENT_CODE = ComponentType.SHARED_RESOURCE_COMPONENT_TYPE_CODE;  
    public static final int DEPLOYED_COMPONENT_CODE = ComponentType.DEPLOYED_COMPONENT_TYPE_CODE;   
    public static final int AUTHPROVIDER_COMPONENT_CODE = ComponentType.AUTHPROVIDER_COMPONENT_TYPE_CODE;   
    
    
    /**
     * Returns the <code>ConfigurationID</code> that identifies the configuation this component belongs to.
     * 
     * @return ConfigurationID
     */
    ConfigurationID getConfigurationID();
    
    
    /**
     * Returns true if the component object is enabled for use.   
     * @return
     * @since 4.2
     */
    boolean isEnabled();
    
    
    /**
     * Indicates if this component is essenstial to the server running 
     * @return
     * @since 5.5
     */
    boolean isEssential();

}

