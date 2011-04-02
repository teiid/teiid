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

import java.util.Collections;
import java.util.List;

import org.teiid.common.buffer.IndexedTupleSource;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.TupleSource;
import org.teiid.common.buffer.BufferManager.TupleSourceType;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.processor.BatchCollector;
import org.teiid.query.processor.BatchIterator;
import org.teiid.query.processor.relational.MergeJoinStrategy.SortOption;
import org.teiid.query.processor.relational.SortUtility.Mode;
import org.teiid.query.sql.lang.OrderBy;


class SourceState {

	enum ImplicitBuffer {
		NONE, FULL, ON_MARK
	}
	
    private RelationalNode source;
    private List expressions;
    private BatchCollector collector;
    private TupleBuffer buffer;
    private List<TupleBuffer> buffers;
    private List<Object> outerVals;
    private IndexedTupleSource iterator;
    private int[] expressionIndexes;
    private List currentTuple;
    private int maxProbeMatch = 1;
    private boolean distinct;
    private ImplicitBuffer implicitBuffer = ImplicitBuffer.FULL;
    boolean open;
    
    private SortUtility sortUtility;
    
    public SourceState(RelationalNode source, List expressions) {
        this.source = source;
        this.expressions = expressions;
        List elements = source.getElements();
        this.outerVals = Collections.nCopies(elements.size(), null);
        this.expressionIndexes = getExpressionIndecies(expressions, elements);
    }
    
    public RelationalNode getSource() {
		return source;
	}
    
    public void setImplicitBuffer(ImplicitBuffer implicitBuffer) {
		this.implicitBuffer = implicitBuffer;
	}
    
    static int[] getExpressionIndecies(List expressions,
                                        List elements) {
        if (expressions == null) {
            return new int[0];
        }
        int[] indecies = new int[expressions.size()];
        for (int i = 0; i < expressions.size(); i++) {
            indecies[i] = elements.indexOf(expressions.get(i));
            assert indecies[i] != -1;
        }
        return indecies;
    }
    
    TupleBuffer createSourceTupleBuffer() throws TeiidComponentException {
    	return this.source.getBufferManager().createTupleBuffer(source.getElements(), source.getConnectionID(), TupleSourceType.PROCESSOR);
    }
    
    public List saveNext() throws TeiidComponentException, TeiidProcessingException {
        this.currentTuple = this.getIterator().nextTuple();
        return currentTuple;
    }
    
    public void reset() throws TeiidComponentException {
        this.getIterator().reset();
        this.getIterator().mark();
        this.currentTuple = null;
    }
    
    public void close() {
    	while (nextBuffer()) {
    		//do nothing
    	}
        this.open = false;
    }

	private void closeBuffer() {
		if (this.buffer != null) {
            this.buffer.remove();
            this.buffer = null;
        }
        if (this.iterator != null) {
			this.iterator.closeSource();
        	this.iterator = null;
        }
        this.currentTuple = null;
	}

    public int getRowCount() throws TeiidComponentException, TeiidProcessingException {
    	return this.getTupleBuffer().getRowCount();
    }

    IndexedTupleSource getIterator() throws TeiidComponentException {
        if (this.iterator == null) {
            if (this.buffer != null) {
                iterator = buffer.createIndexedTupleSource();
            } else {
                // return a TupleBatch tuplesource iterator
                BatchIterator bi = new BatchIterator(this.source);
                if (implicitBuffer != ImplicitBuffer.NONE) {
                	bi.setBuffer(createSourceTupleBuffer(), implicitBuffer == ImplicitBuffer.ON_MARK);
                }
                this.iterator = bi;
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

    public TupleBuffer getTupleBuffer() throws TeiidComponentException, TeiidProcessingException {
        if (this.buffer == null) {
        	if (this.iterator instanceof BatchIterator) {
        		throw new AssertionError("cannot buffer the source"); //$NON-NLS-1$
        	}
        	if (collector == null) {
                collector = new BatchCollector(source, source.getBufferManager(), source.getContext(), false);
            }
            this.buffer = collector.collectTuples();
        }
        return this.buffer;
    }

    public boolean isDistinct() {
        return this.distinct;
    }

    public void markDistinct(boolean distinct) {
        this.distinct |= distinct;
    }
    
    public void sort(SortOption sortOption) throws TeiidComponentException, TeiidProcessingException {
    	if (sortOption == SortOption.ALREADY_SORTED) {
    		return;
    	}
    	if (this.sortUtility == null) {
    		TupleSource ts = null;
    		if (this.buffer != null) {
    			this.buffer.setForwardOnly(true);
    			ts = this.buffer.createIndexedTupleSource();
    		} else {
    			ts = new BatchIterator(this.source);
    		}
		    this.sortUtility = new SortUtility(ts, expressions, Collections.nCopies(expressions.size(), OrderBy.ASC), 
		    		sortOption == SortOption.SORT_DISTINCT?Mode.DUP_REMOVE_SORT:Mode.SORT, this.source.getBufferManager(), this.source.getConnectionID(), source.getElements());
		    this.markDistinct(sortOption == SortOption.SORT_DISTINCT && expressions.size() == this.getOuterVals().size());
		}
    	if (sortOption == SortOption.NOT_SORTED) {
    		this.buffers = sortUtility.onePassSort();
    		if (this.buffers.size() == 1) {
    			this.markDistinct(sortUtility.isDistinct());
    		}
    		nextBuffer();
    		return;
    	} 
		this.buffer = sortUtility.sort();
        this.markDistinct(sortUtility.isDistinct());
    }
    
    public boolean hasBuffer() {
    	return this.buffer != null;
    }
    
    public boolean nextBuffer() {
    	this.closeBuffer();
    	if (this.buffers == null || this.buffers.isEmpty()) {
    		return false;
    	}
    	this.buffer = this.buffers.remove(this.buffers.size() - 1);
    	this.buffer.setForwardOnly(false);
    	this.resetState();
    	return true;
    }

    /**
     * return the iterator to a fresh state
     */
	public void resetState() {
		if (this.iterator != null) {
			this.iterator.reset();
			this.iterator.setPosition(1);
		}
		this.currentTuple = null;
		this.maxProbeMatch = 1;
	}
    
}
