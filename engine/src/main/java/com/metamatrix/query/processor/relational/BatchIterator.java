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
import com.metamatrix.common.buffer.IndexedTupleSource;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;

final class BatchIterator implements
                                 IndexedTupleSource {

    private final RelationalNode source;

    BatchIterator(RelationalNode source) {
        this.source = source;
    }

    private boolean done;
    private int currentRow = 1;
    private TupleBatch currentBatch;
    private List currentTuple;

    public boolean hasNext() throws MetaMatrixComponentException,
                            MetaMatrixProcessingException {
        if (done) {
            return false;
        }
        while (currentTuple == null) {
            if (currentBatch == null) {
                currentBatch = this.source.nextBatch();
            }

            if (currentBatch.getEndRow() >= currentRow) {
                this.currentTuple = currentBatch.getTuple(currentRow++);
            } else {
                done = currentBatch.getTerminationFlag();
                currentBatch = null;
                if (done) {
                    return false;
                }
            }
        }
        return true;
    }
    
    @Override
    public void closeSource() throws MetaMatrixComponentException {
    	
    }
    
    @Override
    public List<SingleElementSymbol> getSchema() {
    	return source.getElements();
    }
    
    @Override
    public List<?> nextTuple() throws MetaMatrixComponentException,
    		MetaMatrixProcessingException {
        if (currentTuple == null && !hasNext()) {
            return null;
        }
        List result = currentTuple;
        currentTuple = null;
        return result;
    }

    public void reset() {
        throw new UnsupportedOperationException();
    }

    public void mark() {
        //does nothing
    }

    @Override
    public int getCurrentIndex() {
        return currentRow;
    }

    public void setPosition(int position) {
    	if (position == this.currentRow) {
    		return;
    	}
		if (position < this.currentRow && (this.currentBatch == null || position < this.currentBatch.getBeginRow())) {
			throw new UnsupportedOperationException("Backwards positioning is not allowed"); //$NON-NLS-1$
		}
        this.currentRow = position;
        this.currentTuple = null;
        if (currentBatch.getEndRow() < currentRow) {
        	this.currentBatch = null;
        }
    }
    
    @Override
    public TupleBatch getBatch() throws MetaMatrixComponentException {
		return currentBatch;
    }

}
