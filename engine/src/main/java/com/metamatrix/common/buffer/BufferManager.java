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
import java.util.Properties;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.lob.LobChunk;

/**
 * The buffer manager controls how memory is used and how data flows through 
 * the system.  It uses {@link StorageManager storage managers}
 * to retrieve data, store data, and 
 * transfer data.  The buffer manager has algorithms that tell it when and 
 * how to store data.  The buffer manager should also be aware of memory 
 * management issues.
 */
public interface BufferManager {
	
	public enum TupleSourceStatus {
		/**
		 * Indicates the status of a {@link TupleSource} is active; the
		 * TupleSource is itself currently still receiving data.
		 * @see #getStatus
		 * @see #setStatus
		 */
		ACTIVE,
		/**
		 * Indicates the status of a {@link TupleSource} is full; the
		 * TupleSource has loaded all of its tuples.
		 * @see #getStatus
		 * @see #setStatus
		 */
		FULL
	}
	
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
	 * Prompts this implementation to initialize itself with the supplied
	 * Properties.  The implementation Class should document what properties
	 * are necessary, if any.
     * @param lookup Class used to determine identity and lookup other managers
     * @param properties Properties required to initialize the Buffer Manager
     * @throws BufferException if there was a problem initializing
	 */
	void initialize(String location, Properties properties) throws MetaMatrixComponentException;
	
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
    
	/**
	 * Adds a {@link StorageManager} to this BufferManager instance.  This 
	 * method may be called multiple times; it will be first called after the
	 * call to {@link #initialize}.
     * @param storageManager Storage manager to add
	 */
	void addStorageManager(StorageManager storageManager);

	/**
	 * Creates a tuple source based on a schema and properties describing
	 * hints about the source
     * @param elements Elements of the tuple source
     * @param groupName Tuple source group name
     * @param tupleSourceType Type of tuple source
     * @return Identifier for tuple source
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
	 */
	TupleSourceID createTupleSource(List elements, String[] types, String groupName, TupleSourceType tupleSourceType) 
    throws MetaMatrixComponentException;
	
	/**
	 * Removes a tuple source by ID 
     * @param tupleSourceID Tuple source identifier
     * @throws TupleSourceNotFoundException if tuple source could not be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
	 */
	void removeTupleSource(TupleSourceID tupleSourceID) 
    throws TupleSourceNotFoundException, MetaMatrixComponentException;

    /**
     * Removes all tuple sources by group name
     * @param groupName Tuple source group name
     * @throws TupleSourceNotFoundException if tuple source could not be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    void removeTupleSources(String groupName) 
    throws MetaMatrixComponentException;

	/**
	 * Gets a tuple source by ID
     * @param tupleSourceID Tuple source identifier
     * @return Tuple source to get tuples from the specified source
     * @throws TupleSourceNotFoundException if tuple source could not be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
	 */
	IndexedTupleSource getTupleSource(TupleSourceID tupleSourceID) 
    throws TupleSourceNotFoundException, MetaMatrixComponentException;

    /**
     * Gets a tuple batch by ID and indexes.  Pins this tuple batch in memory until 
     * it is unpinned or tuple source is removed.  If memory does not exist to pin the 
     * batch, a MemoryNotAvailableException is thrown.
     * 
     * @param tupleSourceID Tuple source identifier
     * @param beginRow First row index to return
     * @param maxEndRow Maximum last row index to return, may be less actually returned
     * @return Batch of rows starting from beginRow and not past maxEndRow
     * @throws TupleSourceNotFoundException if tuple source could not be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     * @throws MemoryNotAvailableException If memory was not available for the pin
     */
    TupleBatch pinTupleBatch(TupleSourceID tupleSourceID, int beginRow, int maxEndRow) 
    throws TupleSourceNotFoundException, MemoryNotAvailableException, MetaMatrixComponentException;

    /**
     * Unpins a range of rows from the given tuple source
     * @param tupleSourceID Tuple source identifier
     * @param firstRow First row to unpin
     * @param lastRow Last row to unpin (inclusive)
     * @throws TupleSourceNotFoundException if tuple source could not be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    void unpinTupleBatch(TupleSourceID tupleSourceID, int firstRow, int lastRow) 
    throws TupleSourceNotFoundException, MetaMatrixComponentException;

	/**
	 * Gets a tuple source schema by ID 
     * @param tupleSourceID Tuple source identifier
     * @return List of ElementSymbol describing elements of tuple source
     * @throws TupleSourceNotFoundException if tuple source could not be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
	 */
    List getTupleSchema(TupleSourceID tupleSourceID) 
    throws TupleSourceNotFoundException, MetaMatrixComponentException;


	/**
	 * Adds a batch of tuples for the specified tuple source
     * @param tupleSourceID Tuple source identifier
     * @param tupleBatch Batch of rows to add
     * @throws TupleSourceNotFoundException if tuple source could not be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
	 */
	void addTupleBatch(TupleSourceID tupleSourceID, TupleBatch tupleBatch) 
    throws TupleSourceNotFoundException, MetaMatrixComponentException;

    /**
     * Gets the current row count 
     * @param tupleSourceID Tuple source identifier
     * @return Current known number of rows in tuple source 
     * @throws TupleSourceNotFoundException if tuple source could not be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    int getRowCount(TupleSourceID tupleSourceID)
    throws TupleSourceNotFoundException, MetaMatrixComponentException;

	/**
	 * Sets the status of the tuple source
     * @param tupleSourceID Tuple source identifier
     * @param status New status
     * @throws TupleSourceNotFoundException if tuple source could not be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     * @see TupleSourceStatus#ACTIVE
     * @see TupleSourceStatus#FULL
	 */
	void setStatus(TupleSourceID tupleSourceID, TupleSourceStatus status) 
    throws TupleSourceNotFoundException, MetaMatrixComponentException;

	/**
	 * Gets the status of the tuple source
     * @param tupleSourceID Tuple source identifier
     * @return Status of tuple source
     * @throws TupleSourceNotFoundException if tuple source could not be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     * @see TupleSourceStatus#ACTIVE
     * @see TupleSourceStatus#FULL
	 */
	TupleSourceStatus getStatus(TupleSourceID tupleSourceID) 
    throws TupleSourceNotFoundException, MetaMatrixComponentException;

	/**
	 * Gets the final row count if tuple source is FULL, otherwise returns -1. 
	 * @param tupleSourceID Tuple source identifier
	 * @return Final row count if status == FULL, -1 otherwise
	 */
	int getFinalRowCount(TupleSourceID tupleSourceID) 
    throws TupleSourceNotFoundException, MetaMatrixComponentException;
    
    /**
     * Add a streamable object to the persistent store. The distinction is made between
     * regular tuple sources and streamable object beacuse, streamable objects are fairly
     * large in size can not be loaded totally into memory, as other tuple sources are done
     * this mechanism allows to stream these objects chunk by chunk. 
     * @param tupleSourceID
     * @param streamGlob part of the stream
     * @param stream
     * @throws TupleSourceNotFoundException
     * @throws MetaMatrixComponentException
     */
    void addStreamablePart(TupleSourceID tupleSourceID, LobChunk streamGlob, int beginRow)
    throws TupleSourceNotFoundException, MetaMatrixComponentException;
    
    /**
     * Returns the streamable batch object's part stored with specified identifier
     * @param tupleSourceID - identifier
     * @return LobChunk a part of the Streamable object stored.
     * @throws TupleSourceNotFoundException
     * @throws MetaMatrixComponentException
     */
    LobChunk getStreamablePart(TupleSourceID tupleSourceID, int beginRow)
    throws TupleSourceNotFoundException, MetaMatrixComponentException;
        
    void stop();
    
    /**
     * Release batches that have been pinned by this thread.
     * This method should be called when processing is terminated
     * to ensure that the memory can be freed. 
     */
    void releasePinnedBatches() throws MetaMatrixComponentException;
}
