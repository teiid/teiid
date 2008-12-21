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

package com.metamatrix.core.util;


/**
 * Contains constants for code base versioning and license checking
 * such as code base version number and product names.
 * 
 * @since 4.2
 */
public class MetaMatrixProductVersion {
    
    /**
     * The {@link #VERSION_NUMBER} indicates the current major.minor version of the product.  
     * If the product is in a patch version (i.e., 4.2.1) release, that will not be indicated.
     */
    public static final String VERSION_NUMBER = "6.0"; //$NON-NLS-1$
        
    // Changing any of these will force a change to config.xml and data_mmproducts_insert.sql
    // (see build.kits.server.common.config and build.kits.server.common.config.sql repectively)
    public static final String PLATFORM_TYPE_NAME = "Platform"; //$NON-NLS-1$
    public static final String METAMATRIX_SERVER_TYPE_NAME = "Integration Server"; //$NON-NLS-1$
    public static final String CONNECTOR_PRODUCT_TYPE_NAME = "Connectors"; //$NON-NLS-1$
    public static final String MODELER_PRODUCT_TYPE_NAME = "Modeler"; //$NON-NLS-1$
    
    public String getReleaseNumber() {
		return "6.0"; //$NON-NLS-1$
	}
	
	public String getBuildNumber() {
		return "0.1"; //$NON-NLS-1$
	}
	
	public String getCopyright() {
		return "Copyright (C) 2008 Red Hat, Inc"; //$NON-NLS-1$
	}
	
	public String getBuildDate() {
		return "pre-relese anyday"; //$NON-NLS-1$
	}

}
