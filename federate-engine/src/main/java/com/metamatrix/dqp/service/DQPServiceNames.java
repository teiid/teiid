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

package com.metamatrix.dqp.service;

/**
 * Defines constants used for naming DQP services.
 */
public interface DQPServiceNames {

    /** Buffer service - provides access to buffer management */
    public static final String BUFFER_SERVICE = "dqp.buffer"; //$NON-NLS-1$

    /** Metadata service - provides acecss to runtime metadata */
    public static final String METADATA_SERVICE = "dqp.metadata"; //$NON-NLS-1$

    /** Data service - provides access to data via connectors */
    public static final String DATA_SERVICE = "dqp.data"; //$NON-NLS-1$

    /** Authorization service - provides access to entitlements info */
    public static final String AUTHORIZATION_SERVICE = "dqp.authorization"; //$NON-NLS-1$

    /** VDB service - provides access to vdb information */
    public static final String VDB_SERVICE = "dqp.vdb"; //$NON-NLS-1$

    /** Tracking service - provides access to vdb information */
    public static final String TRACKING_SERVICE = "dqp.tracking"; //$NON-NLS-1$

    /** Transaction service - provides access to MMTtransactionManager */
    public static final String TRANSACTION_SERVICE = "dqp.transaction"; //$NON-NLS-1$

    /** Configuration Service - provides access to Configuration*/
    public static final String CONFIGURATION_SERVICE = "dqp.configuration"; //$NON-NLS-1$
    
    public static final String REGISTRY_SERVICE = "platform.registry"; //$NON-NLS-1$

    /**
     * Array of all services a DQP may use.
     */
    public static final String[] ALL_SERVICES = new String[] {
        CONFIGURATION_SERVICE,
        TRACKING_SERVICE,
        BUFFER_SERVICE,
        AUTHORIZATION_SERVICE,
        TRANSACTION_SERVICE,                                           
        VDB_SERVICE,
        METADATA_SERVICE,
        DATA_SERVICE,
    };
}
