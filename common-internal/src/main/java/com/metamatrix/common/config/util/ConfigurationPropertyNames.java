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

package com.metamatrix.common.config.util;

public interface ConfigurationPropertyNames {

    static final String APPLICATION_CREATED_BY = "ApplicationCreatedBy"; //$NON-NLS-1$
    static final String APPLICATION_VERSION_CREATED_BY = "ApplicationVersion"; //$NON-NLS-1$
    static final String USER_CREATED_BY = "UserCreatedBy"; //$NON-NLS-1$
    static final String CONFIGURATION_VERSION = "ConfigurationVersion"; //$NON-NLS-1$
    static final String SYSTEM_VERSION = "SystemVersion"; //$NON-NLS-1$
    static final String TIME = "Time"; //$NON-NLS-1$    
    
    // NOTE: if the latest config version is changed, then the com.metamatrix.vdb.internal.def.VDBDefPropertyNames 
    // needs to be change, which will impact the importing of .DEF files.
    static final double CONFIG_CURR_VERSION_DBL = 6.0;
    static final String CONFIG_CURR_VERSION = "6.0";
        
}
