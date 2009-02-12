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

import java.util.Properties;
import com.metamatrix.api.exception.MetaMatrixComponentException;

/**
 * Basic interface for a storage manager.  The storage manager deals in 
 * terms of TupleBatch objects.  The basic contract assumed here is that 
 * batches are requested ({@link #getBatch(TupleSourceID, int)} or removed 
 * ({@link #removeBatch(TupleSourceID, int)}) <b>only</b> with the same 
 * beginning row that they were added with.  For instance, if batches 
 * from row 1-100 and row 101-200 were added to the storage manager, it 
 * would be fine to get batch with begin row 1 and 101 but getting a batch
 * with beginning row 50 would fail.  This contract was created to minimize 
 * the amount of batch manipulation required by a storage manager.
 */
public interface StorageManager {

    /**
     * Constant for a StorageManager for in-memory storage
     */
    public static final int TYPE_MEMORY = 0;

    /**
     * Constant for a StorageManager for database storage
     */
    public static final int TYPE_DATABASE = 1;

    /**
     * Constant for a StorageManager for file storage
     */
    public static final int TYPE_FILE = 2;

    /**
     * Constant for a StorageManager for remote app storage
     */
    public static final int TYPE_REMOTE = 3;

    /**
     * Initialize the storage manager given the specified properties.  Properties
     * are typically specific to the particular storage manager.
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
	void initialize(Properties props) 
    throws MetaMatrixComponentException;

    /**
     * Get the type of storage as defined by constants.
     * @return Storage type
     * @see StorageManager#TYPE_MEMORY
     * @see StorageManager#TYPE_DATABASE
     * @see StorageManager#TYPE_FILE
     * @see StorageManager#TYPE_REMOTE
     */
    int getStorageType();

    /**
     * Add a batch to the storage manager.  
     * @param types a hint to the StorageManager about the types of data in the batch
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
	void addBatch(TupleSourceID sourceID, TupleBatch batch, String[] types) 
    throws MetaMatrixComponentException;

    /**
     * Returns a batch of tuples, starting at row beginRow.  The batch
     * will only be found if the batch that was added begins at beginRow.
     * @param types a hint to the StorageManager about the types of data in the batch
     * @return batch of tuples starting at row beginRow
     * @throws TupleSourceNotFoundException indicating the sourceID is unknown
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
	TupleBatch getBatch(TupleSourceID sourceID, int beginRow, String[] types) 
    throws TupleSourceNotFoundException, MetaMatrixComponentException;

    /**
     * Remove a batch from this storage as specified.  If the tuple source
     * is unknown or the batch is unknown, a TupleSourceNotFoundException is 
     * thrown.
     * @throws TupleSourceNotFoundException indicating the sourceID is unknown
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    void removeBatch(TupleSourceID sourceID, int beginRow) 
    throws TupleSourceNotFoundException, MetaMatrixComponentException;

    /**
     * Remove all batches for the specified tuple source.  If no batches exist,
     * no exception is thrown.
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
	void removeBatches(TupleSourceID sourceID) 
    throws MetaMatrixComponentException;

    /**
     * Shut down the Storage Manager.
     */
	void shutdown();
}
