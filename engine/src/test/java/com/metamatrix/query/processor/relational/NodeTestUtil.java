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

package com.metamatrix.query.processor.relational;

import java.util.Properties;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.StorageManager;
import com.metamatrix.common.buffer.impl.BufferManagerImpl;
import com.metamatrix.common.buffer.impl.MemoryStorageManager;


/** 
 * @since 4.2
 */
public class NodeTestUtil {
    
    static BufferManager getTestBufferManager(long bytesAvailable, int procBatchSize, int connectorBatchSize) {
    	BufferManagerImpl bufferManager = new BufferManagerImpl();
    	bufferManager.setProcessorBatchSize(procBatchSize);
    	bufferManager.setConnectorBatchSize(connectorBatchSize);
        // Get the properties for BufferManager
        return createBufferManager(bufferManager);
    }
    
    static BufferManager getTestBufferManager(long bytesAvailable, int procBatchSize) {
    	BufferManagerImpl bufferManager = new BufferManagerImpl();
    	bufferManager.setProcessorBatchSize(procBatchSize);
        // Get the properties for BufferManager
        return createBufferManager(bufferManager);
    }
    
    static BufferManager createBufferManager(BufferManagerImpl bufferManager) {
        try {
			bufferManager.initialize();
		} catch (MetaMatrixComponentException e) {
			throw new RuntimeException(e);
		}

        // Add storage managers
        
        bufferManager.setStorageManager(createFakeDatabaseStorageManager());
        return bufferManager;
    }
    
    
    private static StorageManager createFakeDatabaseStorageManager() {
        return new MemoryStorageManager();        
    } 
    
}
