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

import java.util.Collections;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.IndexedTupleSource;
import com.metamatrix.common.buffer.TupleBuffer;
import com.metamatrix.common.buffer.BufferManager.TupleSourceType;
import com.metamatrix.query.processor.BatchCollector;
import com.metamatrix.query.processor.BatchIterator;

class SourceState {

    private RelationalNode source;
    private BatchCollector collector;
    private TupleBuffer buffer;
    private List<Object> outerVals;
    private IndexedTupleSource iterator;
    private int[] expressionIndexes;
    private List currentTuple;
    private int maxProbeMatch = 1;
    private boolean distinct;
    
    private boolean canBuffer = true;
    
    public SourceState(RelationalNode source, List expressions) {
        this.source = source;
        List elements = source.getElements();
        this.outerVals = Collections.nCopies(elements.size(), null);
        this.expressionIndexes = getExpressionIndecies(expressions, elements);
    }
    
    private int[] getExpressionIndecies(List expressions,
                                        List elements) {
        if (expressions == null) {
            return new int[0];
        }
        int[] indecies = new int[expressions.size()];
        for (int i = 0; i < expressions.size(); i++) {
            indecies[i] = elements.indexOf(expressions.get(i));
        }
        return indecies;
    }
    
    TupleBuffer createSourceTupleBuffer() throws MetaMatrixComponentException {
    	return this.source.getBufferManager().createTupleBuffer(source.getElements(), source.getConnectionID(), TupleSourceType.PROCESSOR);
    }
    
    public List saveNext() throws MetaMatrixComponentException, MetaMatrixProcessingException {
        this.currentTuple = this.getIterator().nextTuple();
        return currentTuple;
    }
    
    public void reset() {
        this.getIterator().reset();
        this.getIterator().mark();
        this.currentTuple = null;
    }
    
    public void close() {
        closeTupleSource(); 
    }

    private void closeTupleSource() {
        if (this.buffer != null) {
            this.buffer.remove();
            this.buffer = null;
        }
        if (this.iterator != null) {
        	this.iterator = null;
        }
    }
    
    public int getRowCount() throws MetaMatrixComponentException, MetaMatrixProcessingException {
    	return this.getTupleBuffer().getRowCount();
    }

    void setTupleSource(TupleBuffer result) {
    	closeTupleSource();
        this.buffer = result;
    }
    
    IndexedTupleSource getIterator() {
        if (this.iterator == null) {
            if (this.buffer != null) {
                iterator = buffer.createIndexedTupleSource();
            } else {
            	canBuffer = false;
                // return a TupleBatch tuplesource iterator
                iterator = new BatchIterator(this.source);
            }
        }
        return this.iterator;
    }

    public List<Object> getOuterVals() {
        return this.outerVals;
    }

    public List getCurrentTuple() {
        return this.currentTuple;
    }

    public int[] getExpressionIndexes() {
        return this.expressionIndexes;
    }
    
    void setMaxProbeMatch(int maxProbeMatch) {
        this.maxProbeMatch = maxProbeMatch;
    }

    int getMaxProbeMatch() {
        return maxProbeMatch;
    }

    public TupleBuffer getTupleBuffer() throws MetaMatrixComponentException, MetaMatrixProcessingException {
        if (this.buffer == null) {
        	if (!canBuffer) {
        		throw new AssertionError("cannot buffer the source"); //$NON-NLS-1$
        	}
        	if (collector == null) {
                collector = new BatchCollector(source, createSourceTupleBuffer());
            }
            TupleBuffer result = collector.collectTuples();
            setTupleSource(result);
        }
        return this.buffer;
    }

    public boolean isDistinct() {
        return this.distinct;
    }

    public void markDistinct(boolean distinct) {
        this.distinct |= distinct;
    }

}
