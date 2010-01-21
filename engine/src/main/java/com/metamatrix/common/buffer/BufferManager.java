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

import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;

/**
 * The buffer manager controls how memory is used and how data flows through 
 * the system.  It uses {@link StorageManager storage managers}
 * to retrieve data, store data, and 
 * transfer data.  The buffer manager has algorithms that tell it when and 
 * how to store data.  The buffer manager should also be aware of memory 
 * management issues.
 */
public interface BufferManager {
	
	public enum TupleSourceType {
		/**
		 * Indicates that a tuple source is use during query processing as a 
		 * temporary results.
		 */
		PROCESSOR,
		/**
		 * Indicates that a tuple source represents a query's final results.
		 */
		FINAL
	}

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
	 * but will use more file descriptors, which are a limited system resource.
	 */
	public static final String MAX_OPEN_FILES = "metamatrix.buffer.maxOpenFiles"; //$NON-NLS-1$
	/**
	 * Optional property - this values specifies the maximum size in MegaBytes that a buffer file can reach.
	 * The default is 2048 MB (i.e. 2GB).
	 */
	public static final String MAX_FILE_SIZE = "metamatrix.buffer.maxFileSize"; //$NON-NLS-1$
	/**
	 * Optional property - the max number of batches to process at once in algorithms such as sorting.
	 */
	public static final String MAX_PROCESSING_BATCHES = "metamatrix.buffer.maxProcessingBatches"; //$NON-NLS-1$
	
	public static int DEFAULT_CONNECTOR_BATCH_SIZE = 2048;
	public static int DEFAULT_PROCESSOR_BATCH_SIZE = 1024;
	public static int DEFAULT_MAX_PROCESSING_BATCHES = 8;
	public static int DEFAULT_RESERVE_BUFFERS = 64;

    /**
     * Get the batch size to use during query processing.  
     * @return Batch size (# of rows)
     */
    int getProcessorBatchSize();

    /**
     * Get the batch size to use when reading data from a connector.  
     * @return Batch size (# of rows)
     */
    int getConnectorBatchSize();
    
	TupleBuffer createTupleBuffer(List elements, String groupName, TupleSourceType tupleSourceType) 
    throws MetaMatrixComponentException;
	
	/**
	 * Return the maximum number of batches that can be temporarily held potentially 
	 * across even a blocked exception.
	 * @return
	 */
    int getMaxProcessingBatches();
    
    /**
     * Creates a new {@link FileStore}.  See {@link FileStore#setCleanupReference(Object)} to
     * automatically cleanup the underlying resources.
     * @param name
     * @return
     */
    FileStore createFileStore(String name);
    
    /**
     * Reserve up to count buffers for use.  Wait will cause the process to block until
     * all of the requested or half of the total buffers are available.
     * @param count
     * @param wait
     * @return
     * @throws MetaMatrixComponentException
     */
    int reserveBuffers(int count, boolean wait) throws MetaMatrixComponentException;
    
    /**
     * Releases the buffers reserved by a call to {@link BufferManager#reserveBuffers(int, boolean)}
     * @param count
     */
    void releaseBuffers(int count);
    
}
