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
    static final String METAMATRIX_SYSTEM_VERSION = "MetaMatrixSystemVersion"; //$NON-NLS-1$
    static final String TIME = "Time"; //$NON-NLS-1$

    // at 4.2 is where the configuration format changes, so anything prior
    // to this version will use the old (3.0) import/export utility
    static final String MM_CONFIG_4_2_VERSION = "4.2"; //$NON-NLS-1$
    static final String MM_CONFIG_3_0_VERSION = "3.0"; //$NON-NLS-1$
    
    
    // NOTE: if the latest config version is changed, then the com.metamatrix.vdb.internal.def.VDBDefPropertyNames 
    // needs to be change, which will impact the importing of .DEF files.
    static final double MM_LATEST_CONFIG_VERSION = 4.2;
    
    
    public static final String MEMBERSHIP_ADMIN_PASSWORD = "security.membership.admin.password"; //$NON-NLS-1$
    public static final String MEMBERSHIP_ADMIN_USERNAME = "security.membership.admin.username"; //$NON-NLS-1$
    public static final String MEMBERSHIP_SECURITY_ENABLED = "security.membership.security.enabled"; //$NON-NLS-1$
    public static final String MEMBERSHIP_DOMAIN_ORDER = "security.membership.security.DomainOrder"; //$NON-NLS-1$
    
}
