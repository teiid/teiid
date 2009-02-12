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

package com.metamatrix.platform.service.controller;

import com.metamatrix.common.config.api.ServiceComponentType;

public interface ServicePropertyNames {

    public static final String SERVER_URL = "ServerURL"; //$NON-NLS-1$
    public static final String SERVICE_NAME = "ServiceName"; //$NON-NLS-1$
    public static final String SERVICE_ROUTING_ID = ServiceComponentType.SERVICE_ROUTING_ID; 
    public static final String INSTANCE_NAME = "InstanceName"; //$NON-NLS-1$
    public static final String SERVICE_CLASS_NAME = "ServiceClassName"; //$NON-NLS-1$
    public static final String COMPONENT_TYPE_NAME = "ComponentTypeName"; //$NON-NLS-1$
    public static final String SERVICE_ESSENTIAL = ServiceComponentType.SERVICE_ESSENTIAL ; 

    /**
     * Whether service monitoring is enabled.  For example, if true the ConnectorService checks the underlying data sources.  
     * Should be "true" or "false".  Default is "true". 
     */
    public static final String SERVICE_MONITORING_ENABLED      = "ServiceMonitoringEnabled"; //$NON-NLS-1$

    
}
