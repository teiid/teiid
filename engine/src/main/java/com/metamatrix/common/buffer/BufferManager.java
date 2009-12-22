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
     * Removes all tuple sources by group name
     * @param groupName Tuple source group name
     * @throws TupleSourceNotFoundException if tuple source could not be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    void removeTupleBuffers(String groupName) 
    throws MetaMatrixComponentException;
    
}
