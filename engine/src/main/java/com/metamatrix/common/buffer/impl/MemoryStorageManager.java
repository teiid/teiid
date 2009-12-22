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

package com.metamatrix.common.buffer.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.buffer.StorageManager;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.common.buffer.TupleSourceNotFoundException;
import com.metamatrix.query.execution.QueryExecPlugin;

public class MemoryStorageManager implements StorageManager {
    
    // TupleSourceID -> List<TupleBatch> (ordered by startRow)
    private Map<TupleSourceID, Map<Integer, TupleBatch>> storage = Collections.synchronizedMap(new HashMap<TupleSourceID, Map<Integer, TupleBatch>>());

    /**
     * @see StorageManager#initialize(Properties)
     */
    public void initialize(Properties props) throws MetaMatrixComponentException {
    }

    /**
     * @see StorageManager#addBatch(TupleSourceID, TupleBatch)
     */
    public void addBatch(TupleSourceID storageID, TupleBatch batch, String[] types)
        throws MetaMatrixComponentException {

    	Map<Integer, TupleBatch> batches = null;
        synchronized(this.storage) {
            batches = storage.get(storageID);
            if(batches == null) {
                batches = new HashMap<Integer, TupleBatch>();
                this.storage.put(storageID, batches);
            }
        }

        synchronized(batches) {
            batches.put(batch.getBeginRow(), batch);
        }
    }

    /**
     * @see StorageManager#getBatch(TupleSourceID, int, int)
     */
    public TupleBatch getBatch(TupleSourceID storageID, int beginRow, String[] types)
        throws TupleSourceNotFoundException, MetaMatrixComponentException {

    	Map<Integer, TupleBatch> batches = storage.get(storageID);

        if(batches == null) {
           	throw new TupleSourceNotFoundException(QueryExecPlugin.Util.getString("BufferManagerImpl.tuple_source_not_found", storageID)); //$NON-NLS-1$
        }

        synchronized(batches) {
            TupleBatch batch = batches.get(beginRow);
            if(batch == null) {
            	throw new MetaMatrixComponentException("unknown batch"); //$NON-NLS-1$
            }
            return batch;
        }
    }

    /**
     * @see StorageManager#removeStorageArea(TupleSourceID)
     */
    public void removeBatches(TupleSourceID storageID) {
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