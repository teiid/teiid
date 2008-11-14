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
    public static final String VERSION_NUMBER = "5.6"; //$NON-NLS-1$
    /**********************************************************
     * TODO: VERSION NUMBER - NEEDS UPDATING WITH EACH NEW CODEBASE
     *********************************************************/
    
    // Names stored within model and VDB files as the producer
    public static final String PRODUCER_NAME = "MetaMatrix"; //$NON-NLS-1$
        
    // Changing any of these will force a change to config.xml and data_mmproducts_insert.sql
    // (see build.kits.server.common.config and build.kits.server.common.config.sql repectively)
    public static final String PLATFORM_TYPE_NAME = "Platform"; //$NON-NLS-1$
    public static final String METAMATRIX_QUERY_NAME = "MetaMatrix Query"; //$NON-NLS-1$
    public static final String METAMATRIX_DIMENSION_NAME = "MetaMatrix Dimension"; //$NON-NLS-1$
    public static final String METAMATRIX_ENTERPRISE_NAME = "MetaMatrix Enterprise"; //$NON-NLS-1$
    public static final String METAMATRIX_SERVER_TYPE_NAME = "Integration Server"; //$NON-NLS-1$
    public static final String METADATA_SERVER_TYPE_NAME = "Repository"; //$NON-NLS-1$
    public static final String CONNECTOR_PRODUCT_TYPE_NAME = "Connectors"; //$NON-NLS-1$
    public static final String MODELER_PRODUCT_TYPE_NAME = "Modeler"; //$NON-NLS-1$

}
