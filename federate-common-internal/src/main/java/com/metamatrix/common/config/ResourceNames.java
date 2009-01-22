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

package com.metamatrix.common.config;

/**
*   ResourceNames defines the different resources that require
*   connection properties.  These properties are loaded up by
*   the {@link CurrentConfiguration} and made availble by calling
*   the method {@link #getResourceProperties}.  The following
*   are the basis for the names:
*     -  CompTypes, excluding connectors (e.g., Config Service, Session Service, etc).
*     -  internal operations (e.g., Runtime Metadata, logging, cursors, etc.)
*/
public interface ResourceNames {


    public static final String RUNTIME_METADATA_SERVICE = "RuntimeMetadataService"; //$NON-NLS-1$
    public static final String MEMBERSHIP_SERVICE = "MembershipService"; //$NON-NLS-1$
    // Txn Mgr properties for Transactional Server product
    public static final String XA_TRANSACTION_MANAGER = "XATransactionManager"; //$NON-NLS-1$
    public static final String INDEXING_SERVICE = "IndexingService"; //$NON-NLS-1$
    public static final String WEB_SERVICES = "WebServices"; //$NON-NLS-1$
    public static final String JGROUPS = "JGroups"; //$NON-NLS-1$
    public static final String SSL = "SSL"; //$NON-NLS-1$

    
}
