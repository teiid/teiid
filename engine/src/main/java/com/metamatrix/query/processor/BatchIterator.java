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
import com.metamatrix.common.buffer.IndexedTupleSource;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleBuffer;
import com.metamatrix.query.processor.BatchCollector.BatchProducer;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;

/**
 * A BatchIterator provides an iterator interface to a {@link BatchProducer}.
 * By setting {@link #setBuffer(TupleBuffer)}, 
 * the iterator can copy on read into a {@link TupleBuffer} for repeated reading.
 * 
 * Note that the saveOnMark buffering only lasts until the next mark is set.
 */
public class BatchIterator implements IndexedTupleSource {

    private final BatchProducer source;
    private boolean saveOnMark;
    private TupleBuffer buffer;
    private IndexedTupleSource bufferedTs;

    public BatchIterator(BatchProducer source) {
        this.source = source;
    }

    private boolean done;
    private int currentRow = 1;
    private TupleBatch currentBatch;
    private List currentTuple;
    private int bufferedIndex;
    private boolean mark;
    
    @Override
    public boolean hasNext() throws MetaMatrixComponentException,
                            MetaMatrixProcessingException {
    	
    	if (done && this.bufferedTs == null) {
            return false;
        }
        while (currentTuple == null) {
            if (currentBatch == null) {
            	if (this.bufferedTs != null) {
            		if (this.currentRow <= this.bufferedIndex) {
	            		this.bufferedTs.setPosition(currentRow++);
	            		this.currentTuple = this.bufferedTs.nextTuple();
	            		return true;
            		}
            		if (done) {
            			return false;
            		}
            	} 
                currentBatch = this.source.nextBatch();
                if (buffer != null && !saveOnMark) {
                	buffer.addTupleBatch(currentBatch, true);
                	bufferedIndex = currentBatch.getEndRow();
                }
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
    
    public void setBuffer(TupleBuffer buffer, boolean saveOnMark) {
		this.buffer = buffer;
		this.bufferedTs = this.buffer.createIndexedTupleSource();
		this.saveOnMark = saveOnMark;
	}
    
    @Override
    public void closeSource() throws MetaMatrixComponentException {
    	if (this.buffer != null) {
    		this.buffer.remove();
    		this.buffer = null;
    	}
    }
    
    @Override
    public List<SingleElementSymbol> getSchema() {
    	return source.getOutputElements();
    }
    
    @Override
    public List<?> nextTuple() throws MetaMatrixComponentException,
    		MetaMatrixProcessingException {
        if (currentTuple == null && !hasNext()) {
            return null;
        }
        List result = currentTuple;
        currentTuple = null;
        if (mark && saveOnMark && this.currentRow - 1 > this.buffer.getRowCount()) {
        	this.buffer.addTupleBatch(new TupleBatch(this.currentRow - 1, new List[] {result}), true);
        	this.bufferedIndex = this.currentRow - 1;
        }
        return result;
    }

    public void reset() {
    	if (this.bufferedTs != null) {
    		mark = false;
    		this.bufferedTs.reset();
    		this.currentRow = this.bufferedTs.getCurrentIndex();
    		return;
    	}
        throw new UnsupportedOperationException();
    }

    public void mark() {
    	if (this.bufferedTs != null) {
    		this.bufferedTs.mark();
    		if (saveOnMark && this.currentRow > this.bufferedIndex) {
    			this.buffer.purge();
    			this.bufferedIndex = 0;
    		}
    	}
    	mark = true;
    }

    @Override
    public int getCurrentIndex() {
        return currentRow;
    }

    public void setPosition(int position) {
    	if (this.bufferedTs != null) {
    		this.bufferedTs.setPosition(position);
    		this.currentRow = position;
    	}
    	if (this.currentBatch == null && position < this.currentRow) {
			throw new UnsupportedOperationException("Backwards positioning is not allowed"); //$NON-NLS-1$
    	}
    	this.currentRow = position;
        this.currentTuple = null;
    	if (this.currentBatch == null || !this.currentBatch.containsRow(position)) {
        	this.currentBatch = null;
    	}
    }
    
    @Override
    public int available() {
    	if (this.currentRow <= this.bufferedIndex) {
    		this.bufferedTs.setPosition(this.currentRow);
    		return this.bufferedTs.available();
    	}
    	if (currentBatch != null) {
    		return currentBatch.getEndRow() - currentRow + 1;
    	}
    	return 0;
    }
    
}
