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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.api.exception.query.CriteriaEvaluationException;
import com.metamatrix.common.buffer.BlockedOnMemoryException;
import com.metamatrix.common.buffer.IndexedTupleSource;
import com.metamatrix.common.buffer.MemoryNotAvailableException;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.common.buffer.TupleSourceNotFoundException;

/**
 * Extends the basic fully sorted merge join to check for conditions necessary 
 * to not fully sort one of the sides
 * 
 * Will be used for inner joins and only if both sorts are not required.
 * Degrades to a normal merge join if the tuples are balanced. 
 */
public class PartitionedSortJoin extends MergeJoinStrategy {
	
	/**
	 * This is a compromise between the max size of the smaller side
	 * and effective partitioning assuming that we only want to hold
	 * two batches in memory during partitioning.
	 * 
	 * TODO: apply partitioning recursively and/or have a better mechanism
	 * for buffermanager reserve/release of memory 
	 * (would also help the sort utility)
	 */
	public static final int MAX_PARTITIONS = 8; 
	
	private List[] endTuples;
	private List<Boolean> overlap = new ArrayList<Boolean>();
	private List<Integer> endRows = new ArrayList<Integer>();
	private List<TupleSourceID> partitionIds = new ArrayList<TupleSourceID>();
	private List<TupleCollector> tupleCollectors = new ArrayList<TupleCollector>();
	private int currentPartition;
	private IndexedTupleSource currentSource;
	private SourceState sortedSource;
	private SourceState partitionedSource;
	private boolean partitioned;
	private List<?> partitionedTuple;
	private int matchBegin = -1;
	private int matchEnd = -1;

	public PartitionedSortJoin(SortOption sortLeft, SortOption sortRight) {
		super(sortLeft, sortRight, false);
	}
	
    @Override
    public void close() throws TupleSourceNotFoundException,
    		MetaMatrixComponentException {
    	super.close();
    	for (TupleSourceID tupleSourceID : this.partitionIds) {
    		try {
    			this.joinNode.getBufferManager().removeTupleSource(tupleSourceID);
    		} catch (TupleSourceNotFoundException e) {
    			
    		}
		}
    	this.endTuples = null;
    	this.overlap.clear();
    	this.endRows.clear();
    	this.partitionIds.clear();
    	this.tupleCollectors.clear();
    	this.currentSource = null;
    	this.sortedSource = null;
    	this.partitionedSource = null;
    	this.partitionedTuple = null;
    }
    
    @Override
    public void initialize(JoinNode joinNode)
    		throws MetaMatrixComponentException {
    	super.initialize(joinNode);
    	this.currentPartition = 0;
    	this.partitioned = false;
    	this.matchBegin = -1;
    	this.matchEnd = -1;
    }
	
    //TODO: save partial work
    public void computeBatchBounds(SourceState state) throws TupleSourceNotFoundException, MetaMatrixComponentException {
    	if (endTuples != null) {
    		return;
    	}
    	Comparator comp = new ListNestedSortComparator(state.getExpressionIndexes(), true);
    	ArrayList<List<?>> bounds = new ArrayList<List<?>>();
        int beginRow = 1;
        while (beginRow <= state.getRowCount()) {
        	TupleBatch batch = null;
        	try {
        		batch = this.joinNode.getBufferManager().pinTupleBatch(state.getTupleSourceID(), beginRow, beginRow + this.joinNode.getBatchSize() - 1);
        		if (batch.getRowCount() == 0) {
        			break;
        		}
        		beginRow = batch.getEndRow() + 1; 
        		this.joinNode.getBufferManager().unpinTupleBatch(state.getTupleSourceID(), batch.getBeginRow(), batch.getEndRow());
        		if (!bounds.isEmpty()) {
        			overlap.add(comp.compare(bounds.get(bounds.size() - 1), batch.getTuple(batch.getBeginRow())) == 0);
        		}
        		bounds.add(batch.getTuple(batch.getEndRow()));
        		endRows.add(batch.getEndRow());
        	} catch (MemoryNotAvailableException e) {
        		throw BlockedOnMemoryException.INSTANCE;
        	} 
        }
        this.endTuples = bounds.toArray(new List[bounds.size()]);
    }
    
    @Override
    protected void loadLeft() throws MetaMatrixComponentException,
    		MetaMatrixProcessingException {
    	//always buffer to determine row counts
    	this.leftSource.collectTuples();
    }
    
    @Override
    protected void loadRight() throws MetaMatrixComponentException,
    		MetaMatrixProcessingException {
    	super.loadRight();
    	int maxRows = this.joinNode.getBatchSize() * MAX_PARTITIONS;
    	if (processingSortRight == SortOption.SORT
    			&& this.leftSource.getRowCount() < maxRows
    			&& this.leftSource.getRowCount() * 4 < this.rightSource.getRowCount()) {
    		this.processingSortRight = SortOption.PARTITION;
    	} else if (processingSortLeft == SortOption.SORT
    			&& this.rightSource.getRowCount() < maxRows 
    			&& this.rightSource.getRowCount() * 4 < this.leftSource.getRowCount()) {
    		this.processingSortLeft = SortOption.PARTITION;
    	} 
        sortRight();
        if (this.processingSortLeft == SortOption.PARTITION) {
        	computeBatchBounds(this.rightSource);
        	this.sortedSource = this.rightSource;
        	this.partitionedSource = this.leftSource;
        }
    }
    
    @Override
    protected void postLoadLeft() throws MetaMatrixComponentException,
    		MetaMatrixProcessingException {
        super.postLoadLeft();

        if (this.processingSortRight == SortOption.PARTITION) {
        	computeBatchBounds(this.leftSource);
        	this.sortedSource = this.leftSource;
        	this.partitionedSource = this.rightSource;
        }
        
        if (this.processingSortLeft == SortOption.PARTITION) {
        	partitionSource(true);
        }        
    }
    
    @Override
    protected void postLoadRight() throws MetaMatrixComponentException,
    		MetaMatrixProcessingException {
    	if (this.processingSortRight == SortOption.PARTITION) {
    		partitionSource(false);
        	if (this.processingSortRight == SortOption.SORT) {
        		//degrade to a merge join
        		sortRight(); 
        	}
    	} 
    }

	private void partitionSource(boolean left) throws MetaMatrixComponentException,
			MetaMatrixProcessingException {
		if (partitioned) {
			return;
		}
		if (endTuples.length > MAX_PARTITIONS + 1) {
			if (left) {
				this.processingSortLeft = SortOption.SORT;
			} else {
				this.processingSortRight = SortOption.SORT;
			}
			return;
		}
		if (endTuples.length < 2) {
			partitionIds.add(this.partitionedSource.getTupleSourceID());
		} else {
			if (partitionIds.isEmpty()) {
				for (int i = 0; i < endTuples.length; i++) {
					TupleCollector tc = new TupleCollector(this.partitionedSource.createSourceTupleSource(), this.joinNode.getBufferManager());
					tc.setBatchSize(Math.max(1, this.joinNode.getBatchSize()/4));
					this.tupleCollectors.add(tc);
					this.partitionIds.add(tc.getTupleSourceID());
				}
			}
			while (this.partitionedSource.getIterator().hasNext()) {
				List<?> tuple = this.partitionedSource.getIterator().nextTuple();
				int index = binarySearch(tuple, this.endTuples, this.partitionedSource.getExpressionIndexes(), this.sortedSource.getExpressionIndexes());
				if (index < 0) {
					index = -index - 1;
				}
				if (index > this.tupleCollectors.size() -1) {
					continue;
				}
				while (index > 0 && this.overlap.get(index - 1) 
						&& compare(tuple, this.endTuples[index - 1], this.partitionedSource.getExpressionIndexes(), this.sortedSource.getExpressionIndexes()) == 0) {
					index--;
				}
				this.tupleCollectors.get(index).addTuple(tuple);
			}
			this.partitionedSource.getIterator().setPosition(1);
		}
		partitioned = true;
	}
        
    @Override
    protected List nextTuple() throws MetaMatrixComponentException,
    		CriteriaEvaluationException, MetaMatrixProcessingException {
    	if (this.processingSortLeft != SortOption.PARTITION && this.processingSortRight != SortOption.PARTITION) {
    		return super.nextTuple();
    	}
    	if (endRows.isEmpty()) {
    		return null; //no rows on the sorted side
    	}
    	while (currentPartition < partitionIds.size()) {
    		if (currentSource == null) {
    			if (!this.tupleCollectors.isEmpty()) {
    				this.tupleCollectors.get(currentPartition).close();
    			}
    			currentSource = this.joinNode.getBufferManager().getTupleSource(partitionIds.get(currentPartition));
    		}
    		
    		int beginIndex = currentPartition>0?endRows.get(currentPartition - 1)+1:1;
    		
    		this.sortedSource.getIterator().setPosition(beginIndex);
			List[] batch = this.sortedSource.getIterator().getBatch().getAllTuples();
						
    		while (partitionedTuple != null || currentSource.hasNext()) {
    			if (partitionedTuple == null) {
    				partitionedTuple = currentSource.nextTuple();
	    			int index = binarySearch(partitionedTuple, batch, this.partitionedSource.getExpressionIndexes(), this.sortedSource.getExpressionIndexes());
	    			if (index < 0) {
	            		partitionedTuple = null;
	    				continue;
	    			}
	    			matchBegin = index;
	    			matchEnd = index;
	    			if (!this.sortedSource.isDistinct()) {
		    			while (matchBegin > 0) {
		    				if (compare(partitionedTuple, batch[matchBegin - 1], this.partitionedSource.getExpressionIndexes(), this.sortedSource.getExpressionIndexes()) != 0) {
		    					break;
		    				}
		    				matchBegin--;
		    			}
		    			while (matchEnd < batch.length - 1) {
		    				if (compare(partitionedTuple, batch[matchEnd + 1], this.partitionedSource.getExpressionIndexes(), this.sortedSource.getExpressionIndexes()) != 0) {
		    					break;
		    				}
		    				matchEnd++;
		    			}
	    			}
	    			if (matchEnd == batch.length - 1 && currentPartition < overlap.size() && overlap.get(currentPartition)) {
	    				this.tupleCollectors.get(currentPartition + 1).addTuple(partitionedTuple);
	    			}
    			}
    			while (matchBegin <= matchEnd) {
    				List outputTuple = outputTuple(this.processingSortLeft==SortOption.PARTITION?partitionedTuple:batch[matchBegin], 
    						this.processingSortLeft==SortOption.PARTITION?batch[matchBegin]:partitionedTuple);
    				boolean matches = this.joinNode.matchesCriteria(outputTuple);
    				matchBegin++;
                    if (matches) {
                    	return outputTuple;
                    }
    			}
    			matchBegin = -1;
    			matchEnd = -1;
        		partitionedTuple = null;
    		}
    		currentSource = null;
    		currentPartition++;
    	}
    	return null;
    }
    
    public int binarySearch(List<?> tuple, List[] tuples, int[] leftIndexes, int[] rightIndexes) {
    	int begin = 0;
    	int end = tuples.length - 1;
    	while (begin <= end) {
	    	int mid = (begin + end)/2;
	    	int compare = compare(tuples[mid], tuple, rightIndexes, leftIndexes);
	    	if (compare == 0) {
	    		return mid;
	    	}
	    	if (compare < 0) {
	    		end = mid - 1;
	    	} else {
	    		begin = mid + 1;
	    	}
    	}
    	return -begin -1;
    }
    
    @Override
    public Object clone() {
    	return new PartitionedSortJoin(this.sortLeft, this.sortRight);
    }
    
    @Override
    public String getName() {
    	return "PARTITIONED SORT JOIN"; //$NON-NLS-1$
    }
       	
}
