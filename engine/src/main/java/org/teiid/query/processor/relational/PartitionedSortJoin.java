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
import java.util.Comparator;
import java.util.List;

import org.teiid.common.buffer.IndexedTupleSource;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.BufferManager.BufferReserveMode;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;


/**
 * Extends the basic fully sorted merge join to check for conditions necessary 
 * to not fully sort one of the sides
 * 
 * Will be used for inner joins and only if both sorts are not required.
 * Degrades to a normal merge join if the tuples are balanced. 
 */
public class PartitionedSortJoin extends MergeJoinStrategy {
	
	private List[] endTuples;
	private List<Boolean> overlap = new ArrayList<Boolean>();
	private List<Integer> endRows = new ArrayList<Integer>();
	private List<TupleBuffer> partitions = new ArrayList<TupleBuffer>();
	private int currentPartition;
	private IndexedTupleSource currentSource;
	private SourceState sortedSource;
	private SourceState partitionedSource;
	private boolean partitioned;
	private List<?> partitionedTuple;
	private int matchBegin = -1;
	private int matchEnd = -1;
	private int reserved;

	public PartitionedSortJoin(SortOption sortLeft, SortOption sortRight) {
		super(sortLeft, sortRight, false);
	}
	
    @Override
    public void close() {
    	if (joinNode == null) {
    		return;
    	}
    	super.close();
    	for (TupleBuffer tupleSourceID : this.partitions) {
			tupleSourceID.remove();
		}
    	releaseReserved();
    	this.endTuples = null;
    	this.overlap.clear();
    	this.endRows.clear();
    	this.partitions.clear();
    	this.currentSource = null;
    	this.sortedSource = null;
    	this.partitionedSource = null;
    	this.partitionedTuple = null;
    }
    
    @Override
    public void initialize(JoinNode joinNode) {
    	super.initialize(joinNode);
    	this.currentPartition = 0;
    	this.partitioned = false;
    	this.matchBegin = -1;
    	this.matchEnd = -1;
    }
	
    public void computeBatchBounds(SourceState state) throws TeiidComponentException, TeiidProcessingException {
    	if (endTuples != null) {
    		return;
    	}
    	Comparator comp = new ListNestedSortComparator(state.getExpressionIndexes(), true);
    	ArrayList<List<?>> bounds = new ArrayList<List<?>>();
        int beginRow = 1;
        while (beginRow <= state.getRowCount()) {
        	TupleBatch batch = state.getTupleBuffer().getBatch(beginRow);
    		if (batch.getRowCount() == 0) {
    			break;
    		}
    		beginRow = batch.getEndRow() + 1; 
    		if (!bounds.isEmpty()) {
    			overlap.add(comp.compare(bounds.get(bounds.size() - 1), batch.getTuple(batch.getBeginRow())) == 0);
    		}
    		bounds.add(batch.getTuple(batch.getEndRow()));
    		endRows.add(batch.getEndRow());
        }
        this.endTuples = bounds.toArray(new List[bounds.size()]);
    }
    
    @Override
    protected void loadLeft() throws TeiidComponentException,
    		TeiidProcessingException {
    	//always buffer to determine row counts
    	this.leftSource.getTupleBuffer();
    }
    
    @Override
    protected void loadRight() throws TeiidComponentException,
    		TeiidProcessingException {
    	this.rightSource.getTupleBuffer();
    	if (processingSortRight == SortOption.SORT
    			&& this.leftSource.getRowCount() * 4 < this.rightSource.getRowCount()
    			&& testAndSetPartitions(this.rightSource.getRowCount(), this.rightSource.getSource().getOutputElements())) {
    		this.processingSortRight = SortOption.PARTITION;
    	} else if (processingSortLeft == SortOption.SORT
    			&& this.rightSource.getRowCount() * 4 < this.leftSource.getRowCount()
    			&& testAndSetPartitions(this.leftSource.getRowCount(), this.leftSource.getSource().getOutputElements())) {
    		this.processingSortLeft = SortOption.PARTITION;
    	} 
        super.loadRight(); //sort right if needed
        if (this.processingSortLeft == SortOption.PARTITION) {
        	computeBatchBounds(this.rightSource);
        	this.sortedSource = this.rightSource;
        	this.partitionedSource = this.leftSource;
        }
        super.loadLeft(); //sort left if needed
        if (this.processingSortRight == SortOption.PARTITION) {
        	computeBatchBounds(this.leftSource);
        	this.sortedSource = this.leftSource;
        	this.partitionedSource = this.rightSource;
        }
        if (this.processingSortLeft == SortOption.PARTITION) {
        	partitionSource();
        } 
        if (this.processingSortRight == SortOption.PARTITION) {
    		partitionSource();
    	}
    }

    /**
     * Since the source to be partitioned is already loaded, then there's no
     * chance of a blocked exception during partitioning, so reserve some batches.
     * 
     * TODO: partition at the same time as the load to determine size
     * 
     * @return
     */
	private boolean testAndSetPartitions(int rowCount, List elements) {
		int partitionCount = (rowCount / this.joinNode.getBatchSize() + rowCount % this.joinNode.getBatchSize() == 0 ? 0:1) 
			* this.joinNode.getBufferManager().getSchemaSize(elements);
		if (partitionCount > this.joinNode.getBufferManager().getMaxProcessingKB() * 8) {
			return false; 
		}
		int toReserve = Math.max(1, (int)(partitionCount * .75));
		int excess = Math.max(0, toReserve - this.joinNode.getBufferManager().getMaxProcessingKB());
		reserved = this.joinNode.getBufferManager().reserveBuffers(toReserve - excess, BufferReserveMode.FORCE);
		if (excess > 0) {
			reserved += this.joinNode.getBufferManager().reserveBuffers(toReserve, BufferReserveMode.NO_WAIT);
		}
		if (reserved == toReserve) {
			return true;
		}
		releaseReserved();
		return false;
	}
    
	private void partitionSource() throws TeiidComponentException,
			TeiidProcessingException {
		if (partitioned) {
			return;
		}
		if (endTuples.length < 2) {
			partitions.add(this.partitionedSource.getTupleBuffer());
		} else {
			if (partitions.isEmpty()) {
				for (int i = 0; i < endTuples.length; i++) {
					TupleBuffer tc = this.partitionedSource.createSourceTupleBuffer();
					tc.setForwardOnly(true);
					this.partitions.add(tc);
				}
			}
			while (this.partitionedSource.getIterator().hasNext()) {
				List<?> tuple = this.partitionedSource.getIterator().nextTuple();
				int index = binarySearch(tuple, this.endTuples, this.partitionedSource.getExpressionIndexes(), this.sortedSource.getExpressionIndexes());
				if (index < 0) {
					index = -index - 1;
				}
				if (index > this.partitions.size() -1) {
					continue;
				}
				while (index > 0 && this.overlap.get(index - 1) 
						&& compare(tuple, this.endTuples[index - 1], this.partitionedSource.getExpressionIndexes(), this.sortedSource.getExpressionIndexes()) == 0) {
					index--;
				}
				this.partitions.get(index).addTuple(tuple);
			}
			for (TupleBuffer partition : this.partitions) {
				partition.close();
			}
			releaseReserved();
		}
		partitioned = true;
	}

	private void releaseReserved() {
		this.joinNode.getBufferManager().releaseBuffers(this.reserved);
		this.reserved = 0;
	}
        
    @Override
    protected void process() throws TeiidComponentException,
    		TeiidProcessingException {
    	if (this.processingSortLeft != SortOption.PARTITION && this.processingSortRight != SortOption.PARTITION) {
    		super.process();
    	}
    	if (endRows.isEmpty()) {
    		return; //no rows on the sorted side
    	}
    	while (currentPartition < partitions.size()) {
    		if (currentSource == null) {
    			currentSource = partitions.get(currentPartition).createIndexedTupleSource();
    		}
    		
    		int beginIndex = currentPartition>0?endRows.get(currentPartition - 1)+1:1;
    		
			List[] batch = this.sortedSource.getTupleBuffer().getBatch(beginIndex).getAllTuples();
						
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
	    				this.partitions.get(currentPartition + 1).addTuple(partitionedTuple);
	    			}
    			}
    			while (matchBegin <= matchEnd) {
    				List outputTuple = outputTuple(this.processingSortLeft==SortOption.PARTITION?partitionedTuple:batch[matchBegin], 
    						this.processingSortLeft==SortOption.PARTITION?batch[matchBegin]:partitionedTuple);
    				boolean matches = this.joinNode.matchesCriteria(outputTuple);
    				matchBegin++;
                    if (matches) {
                    	this.joinNode.addBatchRow(outputTuple);
                    }
    			}
    			matchBegin = -1;
    			matchEnd = -1;
        		partitionedTuple = null;
    		}
    		currentSource.closeSource();
    		currentSource = null;
    		currentPartition++;
    	}
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
    public PartitionedSortJoin clone() {
    	return new PartitionedSortJoin(this.sortLeft, this.sortRight);
    }
    
    @Override
    public String getName() {
    	return "PARTITIONED SORT JOIN"; //$NON-NLS-1$
    }
       	
}
