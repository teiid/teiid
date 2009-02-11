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

package com.metamatrix.dqp.embedded;

/**
 */
public interface DQPEmbeddedProperties {

    public static final String DQP_LOGFILE = "dqp.logFile"; //$NON-NLS-1$
    public static final String DQP_LOGLEVEL = "dqp.logLevel"; //$NON-NLS-1$
    public static final String DQP_CAPTURE_SYSTEM_PRINTSTREAMS = "dqp.captureSystemStreams"; //$NON-NLS-1$
    public static final String DQP_SERVICE_METADATA = "dqp.service.metadata"; //$NON-NLS-1$
    public static final String DQP_SERVICE_DATA = "dqp.service.data"; //$NON-NLS-1$ 
    public static final String DQP_CLASSPATH = "dqp.classpath"; //$NON-NLS-1$
    public static final String DQP_EXTENSIONS = "dqp.extensions"; //$NON-NLS-1$
    public static final String DQP_CONFIGFILE = "dqp.configFile"; //$NON-NLS-1$
    public static final String DQP_METADATA_SYSTEMURL = "dqp.metadata.systemURL"; //$NON-NLS-1$    
    public static final String VDB_DEFINITION = "vdb.definition"; //$NON-NLS-1$
    public static final String USER_DEFINED_FUNCTIONS = "dqp.userDefinedFunctionsFile"; //$NON-NLS-1$
    public static final String USER_DEFINED_FUNCTIONS_CLASPATH = "dqp.userDefinedFunctionsClasspath"; //$NON-NLS-1$
    public static final String DQP_KEYSTORE = "dqp.keystore"; //$NON-NLS-1$
    public static final String DQP_IDENTITY = "dqp.identity"; //$NON-NLS-1$
    public static final String DQP_TMPDIR = "mm.io.tmpdir"; //$NON-NLS-1$
    
     // Holds the value of the DQP Embedded configuration properties file.
    public static final String DQP_BOOTSTRAP_PROPERTIES_FILE = "dqp.propertiesFile"; //$NON-NLS-1$


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
        
        /**
         * The maximum number of rows a processor batch should contain. Default is 2000.
         */
        public static final String DQP_PROCESSOR_BATCH_SIZE = BUFFER_PREFIX + ".processorBatchSize"; //$NON-NLS-1$
        
        /**
         * The maximum number of rows a connector batch should contain. Default is 2000.
         */
        public static final String DQP_CONNECTOR_BATCH_SIZE = BUFFER_PREFIX + ".connectorBatchSize"; //$NON-NLS-1$
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
    }
}
