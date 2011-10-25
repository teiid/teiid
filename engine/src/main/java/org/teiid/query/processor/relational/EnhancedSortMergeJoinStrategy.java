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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.teiid.common.buffer.IndexedTupleSource;
import org.teiid.common.buffer.STree;
import org.teiid.common.buffer.TupleBrowser;
import org.teiid.common.buffer.TupleSource;
import org.teiid.common.buffer.BufferManager.BufferReserveMode;
import org.teiid.common.buffer.STree.InsertMode;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.optimizer.relational.rules.NewCalculateCostUtil;
import org.teiid.query.processor.CollectionTupleSource;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.SingleElementSymbol;


/**
 * Extends the basic fully sorted merge join to check for conditions necessary 
 * to not fully sort one of the sides.
 * 
 * Will be used for inner joins and only if both sorts are not required.
 * Degrades to a normal merge join if the tuples are balanced.
 * 
 * Refined in 7.4 to use a full index if it is small enough or a repeated merge, rather than a partitioning approach (which was really just a single level index)
 */
public class EnhancedSortMergeJoinStrategy extends MergeJoinStrategy {
	
	private boolean semiDep;
	
	private TupleSource currentSource;
	private SourceState sortedSource;
	private SourceState notSortedSource;
	private List<?> currentTuple;
	private TupleBrowser tb;
	private int reserved;
	private STree index;
	private int[] reverseIndexes;
	private List<?> sortedTuple;
	private boolean repeatedMerge;
	private boolean validSemiDep;
	
	/**
	 * Number of index batches we'll allow to marked as prefers memory regardless of buffer space
	 */
	private int preferMemCutoff = 8;

	public EnhancedSortMergeJoinStrategy(SortOption sortLeft, SortOption sortRight) {
		super(sortLeft, sortRight, false);
	}
	
	public void setPreferMemCutoff(int cutoff) {
		this.preferMemCutoff = cutoff;
	}
	
    @Override
    public void close() {
    	if (joinNode == null) {
    		return;
    	}
    	super.close();
    	if (this.index != null) {
    		this.index.remove();
    	}
    	releaseReserved();
    	this.index = null;
    	this.tb = null;
    	this.currentSource = null;
    	this.sortedSource = null;
    	this.notSortedSource = null;
    	this.sortedTuple = null;
    	this.reverseIndexes = null;
    }
    
    /**
     * Create an index of the smaller size
     *  
     * TODO: reuse existing temp table indexes
     */
    public void createIndex(SourceState state, boolean sorted) throws TeiidComponentException, TeiidProcessingException {
    	int keyLength = state.getExpressionIndexes().length;
    	List elements = state.getSource().getOutputElements();

    	//TODO: minimize reordering, or at least detect when it's not necessary
    	int[] reorderedSortIndex = Arrays.copyOf(state.getExpressionIndexes(), elements.size());
    	Set<Integer> used = new HashSet<Integer>();
    	for (int i : state.getExpressionIndexes()) {
			used.add(i);
    	}
    	int j = state.getExpressionIndexes().length;
    	for (int i = 0; i < elements.size(); i++) {
    		if (!used.contains(i)) {
    			reorderedSortIndex[j++] = i;
    		}
    	}
    	List<SingleElementSymbol> reordered = RelationalNode.projectTuple(reorderedSortIndex, elements);
    	if (!state.isDistinct()) {
    		//need to add a rowid, just in case
    		reordered = new ArrayList<SingleElementSymbol>(reordered);
    		ElementSymbol id = new ElementSymbol("rowId"); //$NON-NLS-1$
    		id.setType(DataTypeManager.DefaultDataClasses.INTEGER);
    		reordered.add(keyLength, id);
    		keyLength++;
    	}
    	index = this.joinNode.getBufferManager().createSTree(reordered, this.joinNode.getConnectionID(), keyLength);
    	index.setPreferMemory(true);
    	if (!state.isDistinct()) {
    		index.getComparator().setDistinctIndex(keyLength-2);
    	}
    	IndexedTupleSource its = state.getTupleBuffer().createIndexedTupleSource(!joinNode.isDependent());
    	int rowId = 0;
    	List<?> lastTuple = null;
    	boolean sortedDistinct = sorted && !state.isDistinct();
    	int sizeHint = index.getExpectedHeight(state.getTupleBuffer().getRowCount());
    	index.setBatchInsert(sorted);
    	outer: while (its.hasNext()) {
    		//detect if sorted and distinct
    		List<?> originalTuple = its.nextTuple();
    		//remove the tuple if it has null
    		for (int i : state.getExpressionIndexes()) {
    			if (originalTuple.get(i) == null) {
    				continue outer;
    			}
    		}
    		if (sortedDistinct && lastTuple != null && this.compare(lastTuple, originalTuple, state.getExpressionIndexes(), state.getExpressionIndexes()) == 0) {
    			sortedDistinct = false;
    		}
    		lastTuple = originalTuple;
    		List<Object> tuple = (List<Object>) RelationalNode.projectTuple(reorderedSortIndex, originalTuple);
    		if (!state.isDistinct()) {
    			tuple.add(keyLength - 1, rowId++);
    		}
    		index.insert(tuple, sorted?InsertMode.ORDERED:InsertMode.NEW, sizeHint);
    	}
    	if (!sorted) {
    		index.compact();
    	} else {
    		index.setBatchInsert(false);
    	}
    	its.closeSource();
    	this.reverseIndexes = new int[elements.size()];
    	for (int i = 0; i < reverseIndexes.length; i++) {
    		int oldIndex = reorderedSortIndex[i];
    		this.reverseIndexes[oldIndex] = i + (!state.isDistinct()&&i>=keyLength-1?1:0); 
    	}
    	if (!state.isDistinct() 
    			&& ((!sorted && index.getComparator().isDistinct()) || (sorted && sortedDistinct))) {
    		this.index.removeRowIdFromKey();
    		state.markDistinct(true);
    	}
    }
    
    @Override
    protected void loadLeft() throws TeiidComponentException,
    		TeiidProcessingException {
    	if (this.joinNode.isDependent()) {
        	this.leftSource.getTupleBuffer();
    	}
    }
    
    private boolean shouldIndexIfSmall(SourceState source) throws TeiidComponentException, TeiidProcessingException {
    	Number cardinality = source.getSource().getEstimateNodeCardinality();
    	return (source.hasBuffer() || (cardinality != null && cardinality.floatValue() != NewCalculateCostUtil.UNKNOWN_VALUE && cardinality.floatValue() <= this.joinNode.getBatchSize())) 
    	&& (source.getRowCount() <= this.joinNode.getBatchSize());
    }
    
    @Override
    protected void loadRight() throws TeiidComponentException,
    		TeiidProcessingException {
    	//the checks are done in a particular order to ensure we don't buffer if possible
    	if (processingSortRight == SortOption.SORT && shouldIndexIfSmall(this.leftSource)) {
    		this.processingSortRight = SortOption.NOT_SORTED; 
    	} else if (!this.leftSource.hasBuffer() && processingSortLeft == SortOption.SORT && shouldIndexIfSmall(this.rightSource)) {
    		this.processingSortLeft = SortOption.NOT_SORTED;
    	} else { 
    		this.leftSource.getTupleBuffer();
    		if (!this.rightSource.hasBuffer() && processingSortRight == SortOption.SORT && shouldIndexIfSmall(this.leftSource)) {
        		this.processingSortRight = SortOption.NOT_SORTED; 
        	} else if (processingSortRight == SortOption.SORT && shouldIndex(this.leftSource, this.rightSource)) {
    			this.processingSortRight = SortOption.NOT_SORTED;
	    	} else if (processingSortLeft == SortOption.SORT && shouldIndex(this.rightSource, this.leftSource)) {
	    		this.processingSortLeft = SortOption.NOT_SORTED;
	    	} 
    	}
    	if (this.processingSortLeft != SortOption.NOT_SORTED && this.processingSortRight != SortOption.NOT_SORTED) {
    		super.loadRight();
    		super.loadLeft();
    		return; //degrade to merge join
    	}
        if (this.processingSortLeft == SortOption.NOT_SORTED) {
        	this.sortedSource = this.rightSource;
        	this.notSortedSource = this.leftSource;

        	if (!repeatedMerge) {
        		createIndex(this.rightSource, this.processingSortRight == SortOption.ALREADY_SORTED);
        	} else {
        		super.loadRight(); //sort if needed
        		this.notSortedSource.sort(SortOption.NOT_SORTED); //do a single sort pass
        	}
        } else if (this.processingSortRight == SortOption.NOT_SORTED) {
        	this.sortedSource = this.leftSource;
        	this.notSortedSource = this.rightSource;

    		if (semiDep && this.leftSource.isDistinct()) {
    			this.rightSource.getTupleBuffer();
    			if (!this.joinNode.getDependentValueSource().isUnused()) {
    				//sort is not needed
    				this.processingSortRight = SortOption.NOT_SORTED;
    				this.validSemiDep = true;
    				//TODO: this requires full buffering and performs an unnecessary projection
    				return;
    			}
        	}

        	if (!repeatedMerge) {
        		createIndex(this.leftSource, this.processingSortLeft == SortOption.ALREADY_SORTED);
        	} else {
        		super.loadLeft(); //sort if needed
        		this.notSortedSource.sort(SortOption.NOT_SORTED); //do a single sort pass
        	}
        }
    }
    
    private boolean shouldIndex(SourceState possibleIndex, SourceState other) throws TeiidComponentException, TeiidProcessingException {
    	if (possibleIndex.getRowCount() * 4 > other.getRowCount()) {
    		return false; //index is too large
    	}
    	int schemaSize = this.joinNode.getBufferManager().getSchemaSize(other.getSource().getOutputElements());
    	int toReserve = this.joinNode.getBufferManager().getMaxProcessingSize();
    	//check if the other side can be sorted in memory
    	if (other.getRowCount() <= this.joinNode.getBatchSize() 
    			|| (possibleIndex.getRowCount() > this.joinNode.getBatchSize() && other.getRowCount()/this.joinNode.getBatchSize() < toReserve/schemaSize)) {
    		return false;
    	}
    	boolean useIndex = false;
    	int indexSchemaSize = this.joinNode.getBufferManager().getSchemaSize(possibleIndex.getSource().getOutputElements());
    	//approximate that 1/2 of the index will be memory resident 
    	toReserve = (int)(indexSchemaSize * possibleIndex.getTupleBuffer().getRowCount() / (possibleIndex.getTupleBuffer().getBatchSize() * .5)); 
    	if (toReserve < this.joinNode.getBufferManager().getMaxProcessingSize()) {
    		useIndex = true;
    	} else if (possibleIndex.getTupleBuffer().getRowCount() / this.joinNode.getBatchSize() < preferMemCutoff) {
    		useIndex = true;
    	} 
    	if (useIndex) {
    		reserved = this.joinNode.getBufferManager().reserveBuffers(toReserve, BufferReserveMode.FORCE);
    		return true;
    	} 
    	this.repeatedMerge = true;
    	return true;
    }
    
	private void releaseReserved() {
		this.joinNode.getBufferManager().releaseBuffers(this.reserved);
		this.reserved = 0;
	}
        
    @Override
    protected void process() throws TeiidComponentException,
    		TeiidProcessingException {
    	if (this.processingSortLeft != SortOption.NOT_SORTED && this.processingSortRight != SortOption.NOT_SORTED) {
    		super.process();
    		return;
    	}
    	if (this.sortedSource.getTupleBuffer().getRowCount() == 0) {
    		return;
    	}
    	if (repeatedMerge) {
    		while (this.notSortedSource.hasBuffer()) {
    			super.process();
    			resetMatchState();
    			this.sortedSource.resetState();
    			this.notSortedSource.nextBuffer();
    		}
    		return;
    	}
    	//else this is a single scan against the index
    	if (currentSource == null) {
    		currentSource = this.notSortedSource.getIterator();
    	}
    	while (true) {
	    	if (this.currentTuple == null) {
	    		currentTuple = this.currentSource.nextTuple();
	    		if (currentTuple == null) {
	    			return;
	    		}
	    		if (validSemiDep) {
	    			List<?> tuple = this.currentTuple;
	    			this.currentTuple = null;
	    			this.joinNode.addBatchRow(outputTuple(this.leftSource.getOuterVals(), tuple));
	    			continue;
	    		}
	        	List<?> key = RelationalNode.projectTuple(this.notSortedSource.getExpressionIndexes(), this.currentTuple);
	        	tb = new TupleBrowser(this.index, new CollectionTupleSource(Arrays.asList(key).iterator()), OrderBy.ASC);
	    	}
	    	if (sortedTuple == null) {
	    		sortedTuple = tb.nextTuple();
	    	
		    	if (sortedTuple == null) {
		    		currentTuple = null;
		    		continue;
		    	}
	    	}
	    	List<?> reorderedTuple = RelationalNode.projectTuple(reverseIndexes, sortedTuple);
			List outputTuple = outputTuple(this.processingSortLeft==SortOption.NOT_SORTED?currentTuple:reorderedTuple, 
					this.processingSortLeft==SortOption.NOT_SORTED?reorderedTuple:currentTuple);
			boolean matches = this.joinNode.matchesCriteria(outputTuple);
	        this.sortedTuple = null;
	        if (matches) {
	        	this.joinNode.addBatchRow(outputTuple);
	        }
    	}
    }
    
    @Override
    public EnhancedSortMergeJoinStrategy clone() {
    	EnhancedSortMergeJoinStrategy clone = new EnhancedSortMergeJoinStrategy(this.sortLeft, this.sortRight);
    	clone.semiDep = this.semiDep;
    	return clone;
    }
    
    @Override
    public String getName() {
    	return "ENHANCED SORT JOIN" + (semiDep?" [SEMI]":""); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
    }
    
    public void setSemiDep(boolean semiDep) {
		this.semiDep = semiDep;
	}
       	
}
