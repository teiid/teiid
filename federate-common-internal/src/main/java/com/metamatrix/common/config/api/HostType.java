/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
* The ConnectorComponentType represents the connector ComponentType.
*/
public interface HostType extends ServiceComponentType {

    public static final String COMPONENT_TYPE_NAME = "Host"; //$NON-NLS-1$
    
    public static final String PORT_NUMBER = com.metamatrix.admin.api.objects.Host.PORT_NUMBER;
    
    public static final String INSTALL_DIR = com.metamatrix.admin.api.objects.Host.INSTALL_DIR;
    
    public static final String LOG_DIRECTORY = com.metamatrix.admin.api.objects.Host.LOG_DIRECTORY;
    
    public static final String DATA_DIRECTORY = com.metamatrix.admin.api.objects.Host.DATA_DIRECTORY;
    
    public static final String HOST_DIRECTORY = com.metamatrix.admin.api.objects.Host.HOST_DIRECTORY;
    
    public static final String HOST_ENABLED = com.metamatrix.admin.api.objects.Host.HOST_ENABLED;

    /**
     * When specified, indicates what address the host will be bound to.  If this
     * is not specified, then the host (logical) name will be used.
     */
    public static final String HOST_BIND_ADDRESS = com.metamatrix.admin.api.objects.Host.HOST_BIND_ADDRESS;
    
    /**
     * The physical address represents a resolvable address for which the host can
     * be found.  This may or may not be the same as the host name.
     */
    public static final String HOST_PHYSICAL_ADDRESS = com.metamatrix.admin.api.objects.Host.HOST_PHYSICAL_ADDRESS;
    
   
    /**
     * The java executable property that defines what java to run when starting a process.
     */
    public static final String JAVA_EXEC = "metamatrix.host.java"; //$NON-NLS-1$
} 