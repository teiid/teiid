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

package com.metamatrix.common.buffer;

/**
 * This class holds constants for all the buffer manager properties.
 */
public final class BufferManagerPropertyNames {

    /**
     * Optional property - the class name of the memory manager to use (must be an implementation
     * of {@link com.metamatrix.buffer.impl.BufferIDCreator}.
     */
    public static final String ID_CREATOR = "metamatrix.buffer.idCreator"; //$NON-NLS-1$

    /**
     * Optional property - the amount of memory (in megabytes) that buffer management should use.  
     * This value should be a positive integer (less than the the max heap size) and defaults to 
     * 128.
     */
    public static final String MEMORY_AVAILABLE = "metamatrix.buffer.memoryAvailable"; //$NON-NLS-1$

    /**
     * Optional property - the percent of buffer management memory that a particular session 
     * can use.  This property can be used to prevent a single user from consuming too many 
     * resources.  This value should be in the range [1..100] and defaults to 50.
     */
    public static final String SESSION_USE_PERCENTAGE = "metamatrix.buffer.sessionUsePercentage"; //$NON-NLS-1$

    /**
     * Optional property - the percent of buffer management memory that serves as a threshold
     * for active memory management. Below the threshold, no active memory management occurs
     * and no data is moved between memory and persistent storage in the background.  Above
     * the threshold, a background thread attempts to clean the in-memory buffers using an LRU
     * algorithm.  This value should be in the range [1..100] and defaults to 75.
     */
    public static final String ACTIVE_MEMORY_THRESHOLD = "metamatrix.buffer.activeMemoryThreshold"; //$NON-NLS-1$

    /**
     * Optional property - the period between checking whether active memory clean up
     * should occur, in milliseconds.  This value should be a millisecond value and defaults
     * to 500 ms.  A value of 0 indicates that no active management should occur.
     */
    public static final String MANAGEMENT_INTERVAL = "metamatrix.buffer.managementInterval"; //$NON-NLS-1$

    /**
     * Optional property - the max size of a batch sent between connector and query service.
     * Making batches larger reduces communication overhead between connector and query service
     * but increases the granularity of memory management on those batches.  This value should 
     * be a positive integer and defaults to 1000.
     */
    public static final String CONNECTOR_BATCH_SIZE = "metamatrix.buffer.connectorBatchSize"; //$NON-NLS-1$

    /**
     * Optional property - the max size of a batch sent internally within the query processor.
     * In general, these batches should be smaller than the connector batch size as there are 
     * no communication costs with these batches.  Smaller batches typically allow a user to 
     * get their first results quicker and allow fine-grained buffer management on intermediate
     * results.  This value should be a positive integer and defaults to 100.
     */
    public static final String PROCESSOR_BATCH_SIZE = "metamatrix.buffer.processorBatchSize"; //$NON-NLS-1$

    /**
     * Optional property - this value specifies the location to store temporary buffers to
     * large to fit in memory.  Temporary buffer files will be created and destroyed in this
     * directory.  This value should be a string specifying an absolute directory path.
     */
    public static final String BUFFER_STORAGE_DIRECTORY = "metamatrix.buffer.storageDirectory"; //$NON-NLS-1$

    /**
     * Optional property - this values specifies how many open file descriptors should be cached
     * in the storage directory.  Increasing this value in heavy load may improve performance
     * but will use more file descriptors, which are a limited system resource.  The default
     * is 10.
     */
    public static final String MAX_OPEN_FILES = "metamatrix.buffer.maxOpenFiles"; //$NON-NLS-1$
    /**
     * Optional property - this values specifies the maximum size in MegaBytes that a buffer file can reach.
     * The default is 2048 MB (i.e. 2GB).
     */
    public static final String MAX_FILE_SIZE = "metamatrix.buffer.maxFileSize"; //$NON-NLS-1$
    
    /**
     * Optional property - this value specifies how often the buffer statistics should be 
     * collected and logged.  This is primarily useful during debugging and defaults to a value
     * of 0, which indicates no stats logging.  This value should be either 0 to indicate no
     * logging or a positive integer indicating the period in milliseconds between logging.
     */
    public static final String LOG_STATS_INTERVAL = "metamatrix.buffer.logStatsInterval"; //$NON-NLS-1$

    /**
     * The environment property name for the class that is to be used for the MetadataConnectionFactory implementation.
     * This property is required (there is no default).
     */
    public static final String CONNECTION_FACTORY = "metamatrix.buffer.connection.Factory"; //$NON-NLS-1$

    /**
     * The environment property name for the class of the driver.
     * This property is optional.
     */
    public static final String CONNECTION_DRIVER = "metamatrix.buffer.connection.Driver"; //$NON-NLS-1$

    /**
     * The environment property name for the protocol for connecting to the metadata store.
     * This property is optional.
     */
    public static final String CONNECTION_PROTOCOL = "metamatrix.buffer.connection.Protocol"; //$NON-NLS-1$

    /**
     * The environment property name for the name of the metadata store database.
     * This property is optional.
     */
    public static final String CONNECTION_DATABASE = "metamatrix.buffer.connection.Database"; //$NON-NLS-1$

    /**
     * The environment property name for the username that is to be used for connecting to the metadata store.
     * This property is optional.
     */
    public static final String CONNECTION_USERNAME = "metamatrix.buffer.connection.User"; //$NON-NLS-1$

    /**
     * The environment property name for the password that is to be used for connecting to the metadata store.
     * This property is optional.
     */
    public static final String CONNECTION_PASSWORD = "metamatrix.buffer.connection.Password"; //$NON-NLS-1$

    /**
     * The environment property name for the maximum number of milliseconds that a metadata connection
     * may remain unused before it becomes a candidate for garbage collection.
     * This property is optional.
     */
    public static final String CONNECTION_POOL_MAXIMUM_AGE = "metamatrix.buffer.connection.MaximumAge"; //$NON-NLS-1$

    /**
     * The environment property name for the maximum number of concurrent users of a single metadata connection.
     * This property is optional.
     */
    public static final String CONNECTION_POOL_MAXIMUM_CONCURRENT_USERS = "metamatrix.buffer.connection.MaximumConcurrentReaders"; //$NON-NLS-1$


    // Can't construct
    private BufferManagerPropertyNames() {
    }
}
