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

package org.teiid.query.processor.relational;

import java.util.List;

import org.teiid.common.buffer.IndexedTupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.sql.util.ValueIterator;


/**
 * A ValueIterator implementation that iterates over the TupleSource
 * results of a subquery ProcessorPlan.  The plan will
 * always have only one result column.  Constant Object values will
 * be returned, not Expressions.
 * 
 * This implementation is resettable.
 */
class TupleSourceValueIterator implements ValueIterator{

    private IndexedTupleSource tupleSourceIterator;
    private int columnIndex;
    
    TupleSourceValueIterator(IndexedTupleSource tupleSource, int columnIndex){
        this.tupleSourceIterator = tupleSource;
        this.columnIndex = columnIndex;
	}
    
	/**
	 * @see java.util.Iterator#hasNext()
	 */
	public boolean hasNext() throws TeiidComponentException{
	    try {
            return tupleSourceIterator.hasNext();
        } catch (TeiidProcessingException e) {
             throw new TeiidComponentException(e);
        }
	}

	/**
	 * Returns constant Object values, not Expressions.
	 * @see java.util.Iterator#next()
	 */
	public Object next() throws TeiidComponentException{
	    return nextTuple().get(columnIndex);
	}

	protected List<?> nextTuple() throws TeiidComponentException {
		try {
            return tupleSourceIterator.nextTuple();
        } catch (TeiidProcessingException e) {
             throw new TeiidComponentException(e);
        }
	}
	
	public void close() {
		this.tupleSourceIterator.closeSource();
	}
    
	/**
	 * Flags a reset as being needed
	 * @see org.teiid.query.sql.util.ValueIterator#reset()
	 */
	public void reset() {
		this.tupleSourceIterator.reset();
	}
}
