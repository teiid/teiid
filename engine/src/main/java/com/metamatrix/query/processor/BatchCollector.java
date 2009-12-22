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

package com.metamatrix.query.processor;

import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleBuffer;

public class BatchCollector {
	
	public interface BatchProducer {
	    /**
	     * Get a batch of results or possibly an Exception.
	     * @return Batch of results
	     * @throws BlockedException indicating next batch is not available yet
	     * @throws MetaMatrixComponentException for non-business rule exception
	     * @throws MetaMatrixProcessingException for business rule exception, related
	     * to user input or modeling
	     */
	    TupleBatch nextBatch() throws BlockedException, MetaMatrixComponentException, MetaMatrixProcessingException;
	    
	    /**
	     * Get list of resolved elements describing output columns for this plan.
	     * @return List of SingleElementSymbol
	     */
	    List getOutputElements();
	}
	
	public interface BatchHandler {
		void batchProduced(TupleBatch batch) throws MetaMatrixProcessingException, MetaMatrixComponentException;
	}

    private BatchProducer sourceNode;
    private BatchHandler batchHandler;

    private boolean done = false;
    private TupleBuffer buffer;
    private boolean collectedAny;
    
    public BatchCollector(BatchProducer sourceNode, TupleBuffer buffer) {
        this.sourceNode = sourceNode;
        this.buffer = buffer;
    }

    public TupleBuffer collectTuples() throws MetaMatrixComponentException, MetaMatrixProcessingException {
        TupleBatch batch = null;
    	while(!done) {
            batch = sourceNode.nextBatch();

            flushBatch(batch);

            // Check for termination condition
            if(batch.getTerminationFlag()) {
            	done = true;
            	buffer.close();
                break;
            }
        }
        return buffer;
    }
    
    public TupleBuffer getTupleBuffer() {
		return buffer;
	}
    
    /**
     * Flush the batch by giving it to the buffer manager.
     */
    private void flushBatch(TupleBatch batch) throws MetaMatrixComponentException, MetaMatrixProcessingException {
    	// Add batch
        if(batch.getRowCount() > 0) {
        	for (List tuple : batch.getAllTuples()) {
				buffer.addTuple(tuple);
			}
            collectedAny = true;
        }
		if (this.batchHandler != null && (batch.getRowCount() > 0 || batch.getTerminationFlag())) {
        	this.batchHandler.batchProduced(batch);
        }
    }
    
	public void setBatchHandler(BatchHandler batchHandler) {
		this.batchHandler = batchHandler;
	}
    
    public boolean collectedAny() {
		boolean result = collectedAny;
		collectedAny = false;
		return result;
	}
    
    public int getRowCount() {
        return buffer.getRowCount();
    }
    
    public boolean isDone() {
		return done;
	}

}
