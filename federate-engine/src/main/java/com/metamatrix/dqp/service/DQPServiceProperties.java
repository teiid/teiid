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

/*
 */
package com.metamatrix.dqp.service;

public interface DQPServiceProperties {
    
    public interface BufferService {
        /**
         * Property prefix for DQP Buffer Service properties.
         */
        public static final String BUFFER_PREFIX = "dqp.buffer"; //$NON-NLS-1$
    
        /**
         * Determines whether buffer management should be all-memory (if false)
         * or mixed memory and disk access (if true).  Default value is false.
         */
        public static final String DQP_BUFFER_USEDISK = BUFFER_PREFIX + ".usedisk"; //$NON-NLS-1$
    
        /**
         * Determines the directory to use if buffer management is using disk.
         * This property is not used if DQP_BUFFER_USEDISK = true.  Default value
         * is ".".
         */
        public static final String DQP_BUFFER_DIR = BUFFER_PREFIX + ".dir"; //$NON-NLS-1$
        
        /**
         * Determines amount of memory to use in-memory before buffering to 
         * disk.  This property is not used if DQP_BUFFER_USEDISK = true.  The 
         * value is in megabytes.  Default value is 32.   
         */
        public static final String DQP_BUFFER_MEMORY = BUFFER_PREFIX + ".memory"; //$NON-NLS-1$
    }

    public interface DataService {
        /**
         * Property prefix for DQP Data Service properties.
         */
        public static final String DATA_PREFIX = "dqp.data"; //$NON-NLS-1$
    }
        
    public interface MetadataService {
        /**
         * Property prefix for DQP Metadata Service properties.
         */
        public static final String METADATA_PREFIX = "dqp.metadata"; //$NON-NLS-1$

        public static final String SYSTEM_VDB_URL = METADATA_PREFIX + ".systemURL"; //$NON-NLS-1$
        
        /**
         * Property name used to reference a URL object.  The URL object is used to interpret the SYSTEM_VDB_URL string.
         */
        public static final String SYSTEM_VDB_CONTEXT_URL = METADATA_PREFIX + ".systemContextURL"; //$NON-NLS-1$
    }    
        
    public interface AuthorizationService {
        /**
         * Property prefix for DQP Metadata Service properties.
         */
        public static final String AUTHORIZATION_PREFIX = "dqp.authorization"; //$NON-NLS-1$
    }    
        
    public interface VDBService {
        /**
         * Property prefix for DQP Metadata Service properties.
         */
        public static final String VDB_PREFIX = "dqp.vdb"; //$NON-NLS-1$
    }
        
    public interface TrackingService {
        /**
         * Property prefix for DQP Metadata Service properties.
         */
        public static final String TRACKING_PREFIX = "dqp.tracking"; //$NON-NLS-1$
        
        /**
         * Fully-qualified class name of Command Logger service provider.
         */
        public static final String COMMAND_LOGGER_CLASSNAME = TRACKING_PREFIX + ".commandLoggerClassname"; //$NON-NLS-1$        
    }
}
