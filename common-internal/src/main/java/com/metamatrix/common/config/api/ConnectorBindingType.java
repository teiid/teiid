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



/**
* The ConnectorBindingType represents the connector ComponentType.
*/
public interface ConnectorBindingType extends ServiceComponentType {

    public static final String COMPONENT_TYPE_NAME = "Connector"; //$NON-NLS-1$
    public static final ComponentTypeID CONNECTOR_TYPE_ID = new ComponentTypeID(COMPONENT_TYPE_NAME);
     
    /*
     * NOTE: These attributes must match the ConnectorAPI property names in org.teiid.connector.internal.ConnectorPropertyNames
     */
    public interface Attributes {
        public static final String IS_XA = "IsXA"; //$NON-NLS-1$
        public static final String CONNECTOR_CLASS = "ConnectorClass";//$NON-NLS-1$
        public static final String MM_JAR_PROTOCOL = "extensionjar"; //$NON-NLS-1$   
        public static final String CONNECTOR_TYPE_CLASSPATH = "ConnectorTypeClassPath"; //$NON-NLS-1$
        
        public static final String CONNECTOR_CLASSPATH_PRE_6 = "ConnectorClassPath"; //$NON-NLS-1$

    }
    
    /**
     * Get the list of extension modules needed by this Connector Binding Type. 
     * @return String[] list of extension modules; never null
     */
    public String[] getExtensionModules();
} 
