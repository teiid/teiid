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
import java.util.NoSuchElementException;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.IndexedTupleSource;

/**
 * A ValueIterator implementation that iterates over the TupleSource
 */
class IndexedTupleSourceIterator implements TupleSourceIterator {

    private IndexedTupleSource tupleSource;
    private int currentRow = 1;
    private int mark = 1;
	private List currentTuple;
    
    IndexedTupleSourceIterator(IndexedTupleSource tupleSource){
        this.tupleSource = tupleSource;
	}
    
	/** 
     * @throws MetaMatrixProcessingException 
	 * @see com.metamatrix.query.processor.relational.TupleSourceIterator#hasNext()
     */
	public boolean hasNext() throws MetaMatrixComponentException, MetaMatrixProcessingException{
        if (this.currentTuple != null) {
            return true;
        }
        
	    this.tupleSource.setCurrentTupleIndex(currentRow);
        
        this.currentTuple = this.tupleSource.nextTuple();
        
		return this.currentTuple != null;
	}

	/** 
     * @throws MetaMatrixProcessingException 
	 * @see com.metamatrix.query.processor.relational.TupleSourceIterator#next()
     */
	public List next() throws MetaMatrixComponentException, MetaMatrixProcessingException{
        if (currentTuple == null && !hasNext()){
			throw new NoSuchElementException();
		}
        this.currentRow++;
		List result = currentTuple;
		currentTuple = null;
		return result;
	}
    
	/** 
     * @see com.metamatrix.query.processor.relational.TupleSourceIterator#reset()
     */
	public void reset() {
		this.currentRow = mark;
		this.mark = 1;
	}

    /** 
     * @see com.metamatrix.query.processor.relational.TupleSourceIterator#getCurrentIndex()
     */
    public int getCurrentIndex() {
        return currentRow;
    }

    /** 
     * @see com.metamatrix.query.processor.relational.TupleSourceIterator#mark()
     */
    public void mark() {
        this.mark = currentRow;
    }

    /** 
     * @see com.metamatrix.query.processor.relational.TupleSourceIterator#setPosition(int)
     */
    public void setPosition(int position) {
        this.currentRow = position;
    }
	
}
