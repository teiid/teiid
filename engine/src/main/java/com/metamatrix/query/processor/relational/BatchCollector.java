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

import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.common.buffer.TupleSourceNotFoundException;
import com.metamatrix.common.buffer.BufferManager.TupleSourceStatus;
import com.metamatrix.common.buffer.BufferManager.TupleSourceType;
import com.metamatrix.query.util.TypeRetrievalUtil;

public class BatchCollector {

    private RelationalNode sourceNode;

    private boolean done = false;
    private TupleSourceID tsID;
    private int rowCount = 0;
    private boolean collectedAny;
    
    public BatchCollector(RelationalNode sourceNode) throws MetaMatrixComponentException {
        this.sourceNode = sourceNode;
        List sourceElements = sourceNode.getElements();
        tsID = sourceNode.getBufferManager().createTupleSource(sourceElements, TypeRetrievalUtil.getTypeNames(sourceElements), sourceNode.getConnectionID(), TupleSourceType.PROCESSOR);
    }

    public TupleSourceID collectTuples() throws MetaMatrixComponentException, MetaMatrixProcessingException {
        return collectTuples(null);
    }
    
    public TupleSourceID collectTuples(TupleBatch batch) throws MetaMatrixComponentException, MetaMatrixProcessingException {
        while(!done) {
            if (batch == null) {
                batch = sourceNode.nextBatch();
            }

            // Add batch
            if(batch.getRowCount() > 0) {
                this.rowCount = batch.getEndRow();
                sourceNode.getBufferManager().addTupleBatch(tsID, batch);
                collectedAny = true;
            }

            // Check for termination condition - batch ending with null row
            if(batch.getTerminationFlag()) {
                break;
            }
            
            batch = null;
        }
        if (!done) {
            sourceNode.getBufferManager().setStatus(tsID, TupleSourceStatus.FULL);
            done = true;
        }
        return tsID;
    }
    
    public boolean collectedAny() {
		boolean result = collectedAny;
		collectedAny = false;
		return result;
	}
    
    public int getRowCount() {
        return rowCount;
    }
    
    public void close() throws MetaMatrixComponentException {
        try {
            this.sourceNode.getBufferManager().removeTupleSource(tsID);
        } catch (TupleSourceNotFoundException err) {
            //ignore
        }
    }
    
    public TupleSourceID getTupleSourceID() {
        return this.tsID;
    }
    
    public boolean isDone() {
		return done;
	}

}
