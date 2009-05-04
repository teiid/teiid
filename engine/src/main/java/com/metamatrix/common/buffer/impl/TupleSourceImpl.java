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

import java.lang.ref.WeakReference;
import java.util.List;

import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.BlockedOnMemoryException;
import com.metamatrix.common.buffer.IndexedTupleSource;
import com.metamatrix.common.buffer.MemoryNotAvailableException;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.common.buffer.TupleSourceNotFoundException;
import com.metamatrix.common.buffer.BufferManager.TupleSourceStatus;

class TupleSourceImpl implements IndexedTupleSource {
	private final BufferManagerImpl bufferManagerImpl;
	private TupleSourceID tupleSourceID;
    private List<?> schema;
    private int batchSize;
    private WeakReference<TupleBatch> currentBatch;
    private int currentRow = 1;
    private int mark = 1;
	private List<?> currentTuple;

    TupleSourceImpl(BufferManagerImpl bufferManagerImpl, TupleSourceID tupleSourceID, List schema, int batchSize){
        this.bufferManagerImpl = bufferManagerImpl;
		this.tupleSourceID = tupleSourceID;
        this.schema = schema;
        this.batchSize = batchSize;
    }
    
    @Override
    public int getCurrentIndex() {
    	return this.currentRow;
    }

    @Override
    public List getSchema(){
        return this.schema;
    }

    @Override
    public List<?> nextTuple()
    throws MetaMatrixComponentException{
    	List<?> result = null;
    	if (currentTuple != null){
			result = currentTuple;
			currentTuple = null;
    	} else {
    		result = getCurrentTuple();
    	} 
    	if (result != null) {
    		currentRow++;
    	}
        return result;
    }

	private List getCurrentTuple() throws MetaMatrixComponentException,
			BlockedException, ComponentNotFoundException {
		TupleBatch batch = getBatch();
        if(batch.getRowCount() == 0) {
            // Check if last
            try {
                TupleSourceStatus status = this.bufferManagerImpl.getStatus(this.tupleSourceID);
                if(status == TupleSourceStatus.FULL) {
                    unpinCurrentBatch();
                    return null;
                } 
                throw BlockedException.INSTANCE;
            } catch(TupleSourceNotFoundException e) {
                throw new ComponentNotFoundException(e, e.getMessage());
            }
        }

        return batch.getTuple(currentRow);
	}

    @Override
    public void closeSource()
    throws MetaMatrixComponentException{
        // Reset to same state as newly-instantiated TupleSourceImpl
        unpinCurrentBatch();
        mark = 1;
        reset();
        //TODO: this is not quite correct wrt the javadoc, close does
        //not need to ensure that we are back at the beginning
    }
    
    private TupleBatch getCurrentBatch() {
        if (currentBatch != null) {
            return currentBatch.get();
        }
        return null;
    }

    private void unpinCurrentBatch()
    throws MetaMatrixComponentException {
        TupleBatch batch = getCurrentBatch();
        if(batch != null ) {
            try {
                this.bufferManagerImpl.unpinTupleBatch(this.tupleSourceID, batch.getBeginRow(), batch.getEndRow());
            } catch (TupleSourceNotFoundException e) {
				throw new MetaMatrixComponentException(e);
			} finally {
                currentBatch = null;
            }
        }
    }
    
    // Retrieves the necessary batch based on the currentRow
    public TupleBatch getBatch()
    throws MetaMatrixComponentException{
        TupleBatch batch = getCurrentBatch();
        if (batch != null) {
            if (currentRow <= batch.getEndRow() && currentRow >= batch.getBeginRow()) {
                return batch;
            }
            unpinCurrentBatch();
        } 
        
        try{
            batch = this.bufferManagerImpl.pinTupleBatch(this.tupleSourceID, currentRow, (currentRow + batchSize -1));
            currentBatch = new WeakReference<TupleBatch>(batch);
        } catch (MemoryNotAvailableException e) {
            /* Defect 18499 - ProcessWorker doesn't know how to handle MemoryNotAvailableException properly,
             * and this should always be converted to a BlockedOnMemoryException during processing so that
             * the work can be requeued.
             */
            throw BlockedOnMemoryException.INSTANCE;
        } catch(MetaMatrixComponentException e) {
            throw e;
        } catch(TupleSourceNotFoundException e){
            throw new MetaMatrixComponentException(e);
        }
        return batch;
    }
    
    @Override
	public boolean hasNext() throws MetaMatrixComponentException {
        if (this.currentTuple != null) {
            return true;
        }
        
        this.currentTuple = getCurrentTuple();
		return this.currentTuple != null;
	}

	@Override
	public void reset() {
		this.setPosition(mark);
		this.mark = 1;
	}

    @Override
    public void mark() {
        this.mark = currentRow;
    }

    @Override
    public void setPosition(int position) {
        if (this.currentRow != position) {
	        this.currentRow = position;
	        this.currentTuple = null;
        }
    }
}