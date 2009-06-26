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

package com.metamatrix.dqp.service;

import com.metamatrix.dqp.util.LogConstants;
import com.metamatrix.platform.security.api.service.MembershipServiceInterface;
import com.metamatrix.platform.security.api.service.SessionServiceInterface;


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

    /** Transaction service - provides access to MMTtransactionManager */
    public static final String TRANSACTION_SERVICE = "dqp.transaction"; //$NON-NLS-1$

    /** Configuration Service - provides access to Configuration*/
    public static final String CONFIGURATION_SERVICE = "dqp.configuration"; //$NON-NLS-1$
    
    public static final String REGISTRY_SERVICE = "platform.registry"; //$NON-NLS-1$
    
    public static final String SESSION_SERVICE = "dqp.session"; //$NON-NLS-1$
    
    public static final String MEMBERSHIP_SERVICE = "dqp.membership"; //$NON-NLS-1$
    

    /**
     * Array of all services a DQP may use.
     */
    public static final String[] ALL_SERVICES = new String[] {
        CONFIGURATION_SERVICE,
        BUFFER_SERVICE,
        AUTHORIZATION_SERVICE,
        TRANSACTION_SERVICE,                                           
        VDB_SERVICE,
        METADATA_SERVICE,
        DATA_SERVICE,
        SESSION_SERVICE,
        MEMBERSHIP_SERVICE
    };
    
    public static final Class[] ALL_SERVICE_CLASSES = new Class[] {
    	ConfigurationService.class,
        BufferService.class,
        AuthorizationService.class,
        TransactionService.class,                                           
        VDBService.class,
        MetadataService.class,
        DataService.class,
        SessionServiceInterface.class,
        MembershipServiceInterface.class
    };
    
    public static final String[] SERVICE_LOGGING_CONTEXT = new String[] {
    	null,
        null,
        null,
        LogConstants.CTX_TXN_LOG,                                           
        null,
        null,
        null,
        null,
        null
    };
}
