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
import java.util.Set;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.BufferManagerPropertyNames;
import com.metamatrix.common.buffer.MemoryNotAvailableException;
import com.metamatrix.common.buffer.StorageManager;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.common.buffer.TupleSourceNotFoundException;
import com.metamatrix.common.buffer.impl.BufferManagerImpl;
import com.metamatrix.common.buffer.storage.memory.MemoryStorageManager;
import com.metamatrix.core.MetaMatrixRuntimeException;


/** 
 * @since 4.2
 */
public class NodeTestUtil {
    static BufferManager getTestBufferManager(long bytesAvailable) {
        // Get the properties for BufferManager
        Properties bmProps = new Properties();                        
        bmProps.setProperty(BufferManagerPropertyNames.ID_CREATOR, "com.metamatrix.common.buffer.impl.LongIDCreator"); //$NON-NLS-1$
        bmProps.setProperty(BufferManagerPropertyNames.MEMORY_AVAILABLE, "" + bytesAvailable); //$NON-NLS-1$
        return createBufferManager(bmProps);
    }
    
    static BufferManager getTestBufferManager(long bytesAvailable, int procBatchSize, int connectorBatchSize) {

        // Get the properties for BufferManager
        Properties bmProps = new Properties();                        
        bmProps.setProperty(BufferManagerPropertyNames.ID_CREATOR, "com.metamatrix.common.buffer.impl.LongIDCreator"); //$NON-NLS-1$
        bmProps.setProperty(BufferManagerPropertyNames.MEMORY_AVAILABLE, "" + bytesAvailable); //$NON-NLS-1$
        bmProps.setProperty(BufferManagerPropertyNames.PROCESSOR_BATCH_SIZE, "" + procBatchSize); //$NON-NLS-1$
        bmProps.setProperty(BufferManagerPropertyNames.CONNECTOR_BATCH_SIZE, "" + connectorBatchSize); //$NON-NLS-1$
        return createBufferManager(bmProps);
    }
    
    static BufferManager getTestBufferManager(long bytesAvailable, int procBatchSize) {

        // Get the properties for BufferManager
        Properties bmProps = new Properties();                        
        bmProps.setProperty(BufferManagerPropertyNames.ID_CREATOR, "com.metamatrix.common.buffer.impl.LongIDCreator"); //$NON-NLS-1$
        bmProps.setProperty(BufferManagerPropertyNames.MEMORY_AVAILABLE, "" + bytesAvailable); //$NON-NLS-1$
        bmProps.setProperty(BufferManagerPropertyNames.PROCESSOR_BATCH_SIZE, "" + procBatchSize); //$NON-NLS-1$
        return createBufferManager(bmProps);
    }
    
    static BufferManager createBufferManager(Properties bmProps) {
        BufferManager bufferManager = new TestableBufferManagerImpl();
        bmProps.setProperty(BufferManagerPropertyNames.MANAGEMENT_INTERVAL, "0"); //$NON-NLS-1$
        try {
            bufferManager.initialize("local", bmProps); //$NON-NLS-1$
        } catch (MetaMatrixComponentException err) {
            throw new MetaMatrixRuntimeException(err);
        }

        // Add storage managers
        bufferManager.addStorageManager(createMemoryStorageManager());
        
        bufferManager.addStorageManager(createFakeDatabaseStorageManager());
        return bufferManager;
    }
    
    private static StorageManager createMemoryStorageManager() {
        return new MemoryStorageManager();
    }
    
    private static StorageManager createFakeDatabaseStorageManager() {
        return new MemoryStorageManager() {
            public int getStorageType() { 
                return StorageManager.TYPE_DATABASE;    
            }  
        };        
    } 
    
    public static class TestableBufferManagerImpl extends BufferManagerImpl {
        
        private Set blockOn;
        private int pinCount = 0;
        private boolean wasBlocked;
        
        /** 
         * @see com.metamatrix.common.buffer.impl.BufferManagerImpl#pinTupleBatch(com.metamatrix.common.buffer.TupleSourceID, int, int)
         */
        public TupleBatch pinTupleBatch(TupleSourceID tupleSourceID,
                                        int beginRow,
                                        int maxEndRow) throws TupleSourceNotFoundException,
                                                      MemoryNotAvailableException,
                                                      MetaMatrixComponentException {
            if (blockOn != null && blockOn.contains(new Integer(++pinCount))) {
                wasBlocked = true;
                throw new MemoryNotAvailableException();
            }
            
            wasBlocked = false;
            
            return super.pinTupleBatch(tupleSourceID, beginRow, maxEndRow);
        }

        
        /** 
         * @param blockOn The blockOn to set.
         */
        public void setBlockOn(Set blockOn) {
            this.blockOn = blockOn;
        }
        
        public boolean wasBlocked() {
            return wasBlocked;
        }
        
    }
    
}
