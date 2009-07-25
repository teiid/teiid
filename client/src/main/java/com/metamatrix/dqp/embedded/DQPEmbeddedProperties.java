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

package com.metamatrix.dqp.embedded;

/**
 */
public interface DQPEmbeddedProperties {
    public static final String DQP_LOGDIR = "dqp.logdir"; //$NON-NLS-1$
    public static final String DQP_EXTENSIONS = "dqp.extensions"; //$NON-NLS-1$
    public static final String DQP_CONFIGFILE = "dqp.configFile"; //$NON-NLS-1$
    public static final String DQP_METADATA_SYSTEMURL = "dqp.metadata.systemURL"; //$NON-NLS-1$    
    public static final String VDB_DEFINITION = "vdb.definition"; //$NON-NLS-1$
    public static final String USER_DEFINED_FUNCTIONS = "dqp.userDefinedFunctionsFile"; //$NON-NLS-1$
    public static final String COMMON_EXTENSION_CLASPATH = "dqp.extension.CommonClasspath"; //$NON-NLS-1$
    public static final String DQP_WORKDIR = "dqp.workdir"; //$NON-NLS-1$
    public static final String DQP_DEPLOYDIR = "dqp.deploydir"; //$NON-NLS-1$
    public static final String DQP_LIBDIR = "dqp.lib"; //$NON-NLS-1$
    public static final String PROCESSNAME = "processName"; //$NON-NLS-1$
    public static final String CLUSTERNAME = "clusterName"; //$NON-NLS-1$

    // socket specific
    public static final String BIND_ADDRESS = "server.bindAddress"; //$NON-NLS-1$
    public static final String SERVER_PORT = "server.portNumber"; //$NON-NLS-1$
    public static final String MAX_THREADS = "server.maxSocketThreads"; //$NON-NLS-1$
    public static final String INPUT_BUFFER_SIZE = "server.inputBufferSize"; //$NON-NLS-1$       
    public static final String OUTPUT_BUFFER_SIZE = "server.outputBufferSize"; //$NON-NLS-1$       
    
    //derived properties
    public static final String DQP_TMPDIR = "mm.io.tmpdir"; //$NON-NLS-1$
    public static final String BOOTURL = "bootURL"; //$NON-NLS-1$
    public static final String ENABLE_SOCKETS = "sockets.enabled"; //$NON-NLS-1$
    public static final String HOST_ADDRESS = "hostAddress"; //$NON-NLS-1$
    public static final String DQP_BOOTSTRAP_FILE = "bootstrapFile"; //$NON-NLS-1$
    public static final String TEIID_HOME = "teiid.home"; //$NON-NLS-1$
    
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
