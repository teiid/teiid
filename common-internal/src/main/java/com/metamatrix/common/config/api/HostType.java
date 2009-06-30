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
* The ConnectorComponentType represents the connector ComponentType.
*/
public interface HostType extends ServiceComponentType {

    public static final String COMPONENT_TYPE_NAME = "Host"; //$NON-NLS-1$
    
    public static final String INSTALL_DIR = org.teiid.adminapi.Host.INSTALL_DIR;
    
    public static final String LOG_DIRECTORY = org.teiid.adminapi.Host.LOG_DIRECTORY;
    
    public static final String HOST_DIRECTORY = org.teiid.adminapi.Host.HOST_DIRECTORY;
    
    public static final String HOST_ENABLED = org.teiid.adminapi.Host.HOST_ENABLED;

    /**
     * When specified, indicates what address the host will be bound to.  If this
     * is not specified, then the host (logical) name will be used.
     */
    public static final String HOST_BIND_ADDRESS = org.teiid.adminapi.Host.HOST_BIND_ADDRESS;
    
    /**
     * The physical address represents a resolvable address for which the host can
     * be found.  This may or may not be the same as the host name.
     */
    public static final String HOST_PHYSICAL_ADDRESS = org.teiid.adminapi.Host.HOST_PHYSICAL_ADDRESS;
    
  
} 