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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.buffer.impl.BufferManagerImpl;
import com.metamatrix.query.execution.QueryExecPlugin;

/**
 * <p>Factory for BufferManager instances.  One method will get
 * a server buffer manager, as it should be instantiated in a running
 * MetaMatrix server.  That BufferManager is configured mostly by the
 * passed in properties.</p>
 *
 * <p>The other method returns a stand-alone, in-memory buffer manager.  This
 * is typically used for either in-memory testing or any time the
 * query processor component is not expected to run out of memory, such as
 * within the modeler.</p>
 */
public class BufferManagerFactory {
	
	public static class MemoryStorageManager implements StorageManager {
	    
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

	private static BufferManager INSTANCE;
	
    /**
     * Helper to get a buffer manager all set up for unmanaged standalone use.  This is
     * typically used for testing or when memory is not an issue.
     * @return BufferManager ready for use
     */
    public static BufferManager getStandaloneBufferManager() throws MetaMatrixComponentException {
    	if (INSTANCE == null) {
	        BufferManager bufferMgr = new BufferManagerImpl();
	        Properties props = new Properties();
	        props.setProperty(BufferManagerPropertyNames.MEMORY_AVAILABLE, String.valueOf(Long.MAX_VALUE));
	        props.setProperty(BufferManagerPropertyNames.SESSION_USE_PERCENTAGE, "100"); //$NON-NLS-1$
	        props.setProperty(BufferManagerPropertyNames.LOG_STATS_INTERVAL, "0"); //$NON-NLS-1$
	        props.setProperty(BufferManagerPropertyNames.MANAGEMENT_INTERVAL, "0"); //$NON-NLS-1$
	        bufferMgr.initialize("local", props); //$NON-NLS-1$
	
	        // Add unmanaged memory storage manager
	        bufferMgr.setStorageManager(new MemoryStorageManager());
	        INSTANCE = bufferMgr;
    	}

        return INSTANCE;
    }

}
