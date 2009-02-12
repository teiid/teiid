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

package com.metamatrix.common.buffer.storage.memory;

import java.util.*;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.buffer.*;
import com.metamatrix.common.buffer.impl.BatchMap;
import com.metamatrix.common.buffer.impl.BatchMapValueTranslator;
import com.metamatrix.query.execution.QueryExecPlugin;

/**
 */
public class MemoryStorageManager implements StorageManager {
    
    private static final BatchMapValueTranslator TRANSLATOR = new BatchMapValueTranslator() {
        public int getBeginRow(Object batchMapValue) {
            return ((TupleBatch)batchMapValue).getBeginRow();
        }
        public int getEndRow(Object batchMapValue) {
            return ((TupleBatch)batchMapValue).getEndRow();
        }
    };
        

    // TupleSourceID -> List<TupleBatch> (ordered by startRow)
    private Map<TupleSourceID, BatchMap> storage = Collections.synchronizedMap(new HashMap<TupleSourceID, BatchMap>());

    /**
     * @see StorageManager#initialize(Properties)
     */
    public void initialize(Properties props) throws MetaMatrixComponentException {
    }

    /**
     * @see StorageManager#getStorageType()
     */
    public int getStorageType() {
        return StorageManager.TYPE_MEMORY;
    }

    /**
     * @see StorageManager#addBatch(TupleSourceID, TupleBatch)
     */
    public void addBatch(TupleSourceID storageID, TupleBatch batch, String[] types)
        throws MetaMatrixComponentException {

        BatchMap batches = null;
        synchronized(this.storage) {
            batches = storage.get(storageID);
            if(batches == null) {
                batches = new BatchMap(TRANSLATOR);
                this.storage.put(storageID, batches);
            }
        }

        synchronized(batches) {
            batches.addBatch(batch.getBeginRow(), batch);
        }
    }

    /**
     * @see StorageManager#getBatch(TupleSourceID, int, int)
     */
    public TupleBatch getBatch(TupleSourceID storageID, int beginRow, String[] types)
        throws TupleSourceNotFoundException, MetaMatrixComponentException {

        BatchMap batches = storage.get(storageID);

        if(batches == null) {
           	throw new TupleSourceNotFoundException(QueryExecPlugin.Util.getString("BufferManagerImpl.tuple_source_not_found", storageID)); //$NON-NLS-1$
        }

        synchronized(batches) {
            Object batch = batches.getBatch(beginRow);
            if(batch != null) {
                return (TupleBatch)batch;
            }
            return new TupleBatch(beginRow, Collections.EMPTY_LIST);
        }
    }

    /**
     * @see com.metamatrix.common.buffer.StorageManager#removeBatch(TupleSourceID, int, int)
     */
    public void removeBatch(TupleSourceID sourceID, int beginRow)
        throws TupleSourceNotFoundException, MetaMatrixComponentException {

        BatchMap batches = storage.get(sourceID);

        if(batches == null) {
            return;
        }
        synchronized(batches) {
            batches.removeBatch(beginRow);

            if(batches.isEmpty()) {
                storage.remove(sourceID);
            }
        }
    }

    /**
     * @see StorageManager#removeStorageArea(TupleSourceID)
     */
    public void removeBatches(TupleSourceID storageID) throws MetaMatrixComponentException {
        storage.remove(storageID);
    }

    /**
     * @see StorageManager#shutdown()
     */
    public void shutdown() {
        this.storage.clear();
        this.storage = null;
    }


}
