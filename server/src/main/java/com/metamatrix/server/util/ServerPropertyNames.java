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

package com.metamatrix.server.util;

import com.metamatrix.common.util.CommonPropertyNames;

/**
 * Class that defines property constants for MetaMatrix
 */
public final class ServerPropertyNames {

	public static final String SERVER_SITE_ID           = CommonPropertyNames.DOMAIN_ID;
    public static final String SERVER_PLATFORM          = CommonPropertyNames.SERVER_PLATFORM;
    public static final String STANDALONE_PLATFORM      = CommonPropertyNames.STANDALONE_PLATFORM;
    public static final String SERVER_INSTALL_DATE      = "metamatrix.installDate"; //$NON-NLS-1$
    public static final String SERVER_VERSION           = "metamatrix.version"; //$NON-NLS-1$
    
    public static final String SYSTEM_VDB_URL           = "metamatrix.server.metadata.systemURL"; //$NON-NLS-1$

    
    
    /**
     * Whether ConnectorService should cache ClassLoaders.  Should be "true" or "false".  Default is "true". 
     */
    public static final String CACHE_CLASS_LOADERS      = "metamatrix.server.cacheConnectorClassLoaders"; //$NON-NLS-1$
   
	public static final String COMMON_EXTENSION_CLASPATH = "metamatrix.extension.CommonClasspath"; //$NON-NLS-1$
}
