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
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BlockedOnMemoryException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.IndexedTupleSource;
import com.metamatrix.common.buffer.MemoryNotAvailableException;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.common.buffer.TupleSourceNotFoundException;
import com.metamatrix.common.buffer.BufferManager.TupleSourceStatus;
import com.metamatrix.common.buffer.BufferManager.TupleSourceType;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.query.sql.lang.OrderBy;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;
import com.metamatrix.query.util.TypeRetrievalUtil;

/**
 */
public class SortUtility {
	
	public enum Mode {
		SORT,
		DUP_REMOVE,
		DUP_REMOVE_SORT
	}

	//constructor state
    private TupleSourceID sourceID;
    protected List sortElements;
    protected List<Boolean> sortTypes;
    private Mode mode;
    protected BufferManager bufferManager;
    private String groupName;
    private boolean useAllColumns;
    
    //init state
    private int batchSize;
    protected List schema;
    private String[] schemaTypes;
    protected int[] sortCols;
	private ListNestedSortComparator comparator;

    private TupleSourceID outputID;
	private IndexedTupleSource outTs;
    private boolean doneReading;
    private int sortPhaseRow = 1;
    private int phase = INITIAL_SORT;
    protected List<TupleSourceID> activeTupleIDs = new ArrayList<TupleSourceID>();
    private List<TupleBatch> workingBatches;
    private int masterSortIndex;
	private TupleSourceID mergedID;
	private TupleSourceID tempOutId;

    // Phase constants for readability
    private static final int INITIAL_SORT = 1;
    private static final int MERGE = 2;
    private static final int DONE = 3;
    
    public SortUtility(TupleSourceID sourceID, List sortElements, List<Boolean> sortTypes, boolean removeDups, BufferManager bufferMgr,
            String groupName) {
    	this(sourceID, sortElements, sortTypes, removeDups?Mode.DUP_REMOVE_SORT:Mode.SORT, bufferMgr, groupName, false);
    }
    
    public SortUtility(TupleSourceID sourceID, List sortElements, List<Boolean> sortTypes, Mode mode, BufferManager bufferMgr,
                        String groupName, boolean useAllColumns) {
        this.sourceID = sourceID;
        this.sortElements = sortElements;
        this.sortTypes = sortTypes;
        this.mode = mode;
        this.bufferManager = bufferMgr;
        this.groupName = groupName;
        this.useAllColumns = useAllColumns;
    }
    
    public boolean isDone() {
    	return this.doneReading && this.phase == DONE;
    }
    
    public TupleSourceID sort()
        throws BlockedOnMemoryException, MetaMatrixComponentException {

        try {
            // One time setup
            if(this.schema == null) {
                initialize();
            }
            
            if(this.phase == INITIAL_SORT) {
                initialSort();
            }
            
            if(this.phase == MERGE) {
                try {
                    mergePhase();
                } finally {
                	if (this.mergedID != null) {
	                	this.bufferManager.removeTupleSource(mergedID);
	                	this.mergedID = null;
                	}
                	if (this.tempOutId != null) {
	                	this.bufferManager.removeTupleSource(tempOutId);
	                	this.tempOutId = null;
                	}
                    if (workingBatches != null) {
                        for (int i = 0; i < workingBatches.size(); i++) {
                            TupleBatch tupleBatch = workingBatches.get(i);
                            if (tupleBatch != null) {
                                unpinWorkingBatch(i, tupleBatch);
                            }
                        }
                    }
                    workingBatches = null;
                }
            }
            if (this.outputID != null) {
            	return this.outputID;
            }
            return this.activeTupleIDs.get(0);
        } catch(TupleSourceNotFoundException e) {
            throw new MetaMatrixComponentException(e, e.getMessage());
        }

    }

	private TupleSourceID createTupleSource() throws MetaMatrixComponentException {
		return bufferManager.createTupleSource(this.schema, this.schemaTypes, this.groupName, TupleSourceType.PROCESSOR);
	}

    protected void initialSort() throws BlockedOnMemoryException, TupleSourceNotFoundException, MetaMatrixComponentException {
    	while(!doneReading) {
        	ArrayList<int[]> pinned = new ArrayList<int[]>();     // of int[2] representing begin, end
            List<List<Object>> workingTuples = new ArrayList<List<Object>>();
	        // Load data until out of memory
	        while(!doneReading) {
	            try {
	                // Load and pin batch
	                TupleBatch batch = bufferManager.pinTupleBatch(sourceID, sortPhaseRow, sortPhaseRow + batchSize - 1);
	
	                if (batch.getRowCount() == 0) {
	                	if (bufferManager.getStatus(sourceID) == TupleSourceStatus.FULL) {
	                		doneReading = true;
		                }
	                	break;
	                }
                    // Remember pinned rows
                    pinned.add(new int[] { sortPhaseRow, batch.getEndRow() });

                    addTuples(workingTuples, batch);
                    
                    sortPhaseRow += batch.getRowCount();
	            } catch(MemoryNotAvailableException e) {
	                break;
	            }
	        }
	
	        if(workingTuples.isEmpty()) {
        		break;
	        }
		
	        TupleSourceID activeID = createTupleSource();
	        activeTupleIDs.add(activeID);
	        int sortedThisPass = workingTuples.size();
	        if (this.mode == Mode.SORT) {
	        	//perform a stable sort
	    		Collections.sort(workingTuples, comparator);
	        }
	        int writeBegin = 1;
	        while(writeBegin <= sortedThisPass) {
	            int writeEnd = Math.min(sortedThisPass, writeBegin + batchSize - 1);
	
	            TupleBatch writeBatch = new TupleBatch(writeBegin, workingTuples.subList(writeBegin-1, writeEnd));
	            bufferManager.addTupleBatch(activeID, writeBatch);
	            writeBegin += writeBatch.getRowCount();
	        }
	
	        // Clean up - unpin rows
	        for (int[] bounds : pinned) {
	            bufferManager.unpinTupleBatch(sourceID, bounds[0], bounds[1]);
	        }
        }

    	if (!doneReading && (mode != Mode.DUP_REMOVE || this.activeTupleIDs.isEmpty())) {
    		throw BlockedOnMemoryException.INSTANCE;
    	}
    	
    	if (this.activeTupleIDs.isEmpty()) {
            activeTupleIDs.add(createTupleSource());
        }  

        // Clean up
        this.phase = MERGE;
    }

	protected void addTuples(List workingTuples, TupleBatch batch) {
		if (this.mode == Mode.SORT) {
			workingTuples.addAll(Arrays.asList(batch.getAllTuples()));
			return;
		}
		for (List<Object> list : batch.getAllTuples()) {
			int index = Collections.binarySearch(workingTuples, list, comparator);
			if (index >= 0) {
				continue;
			}
			workingTuples.add(-index - 1, list);
		}
	}
	
    protected void mergePhase() throws BlockedOnMemoryException, MetaMatrixComponentException, TupleSourceNotFoundException {
        appendOutput();
        TupleCollector tempCollector = null;
    	while(this.activeTupleIDs.size() > 1) {    		
            // Load and pin batch from sorted sublists while memory available
            this.workingBatches = new ArrayList<TupleBatch>(activeTupleIDs.size());
            int sortedIndex = 0;
            for(; sortedIndex<activeTupleIDs.size(); sortedIndex++) {
                TupleSourceID activeID = activeTupleIDs.get(sortedIndex);
                try {
                    TupleBatch sortedBatch = bufferManager.pinTupleBatch(activeID, 1, this.batchSize);
                    workingBatches.add(sortedBatch);
                } catch(MemoryNotAvailableException e) {
                    break;
                }
            }
            
            //if we cannot make progress, just block for now
            if (workingBatches.size() < 2) {
                throw BlockedOnMemoryException.INSTANCE;
            }
            
            // Initialize pointers into working batches
            int[] workingPointers = new int[workingBatches.size()];
            Arrays.fill(workingPointers, 1);

            mergedID = createTupleSource();

            // Merge from working sorted batches
            TupleCollector collector = new TupleCollector(mergedID, this.bufferManager);
            while(true) {
                // Find least valued row among working batches
                List<?> currentRow = null;
                int chosenBatchIndex = -1;
                TupleBatch chosenBatch = null;
                for(int i=0; i<workingBatches.size(); i++) {
                    TupleBatch batch = workingBatches.get(i);
                    if(batch == null) {
                    	continue;
                    }
                    List<?> testRow = batch.getTuple(workingPointers[i]);
                    int compare = -1;
                    if (currentRow != null) {
                    	compare = comparator.compare(testRow, currentRow);
                    }
                    if(compare < 0) {
                        // Found lower row
                        currentRow = testRow;
                        chosenBatchIndex = i;
                        chosenBatch = batch;
                    } else if (compare == 0 && this.mode != Mode.SORT) {
                    	incrementWorkingBatch(i, workingPointers, batch);
                    }
                }

                // Check for termination condition - all batches must have been null
                if(currentRow == null) {
                    break;
                }
                // Output the row and update pointers
                collector.addTuple(currentRow);
                if (this.outputID != null && chosenBatchIndex != masterSortIndex && sortedIndex > masterSortIndex) {
                	if (tempCollector == null) {
                		tempOutId = createTupleSource();
                    	tempCollector = new TupleCollector(tempOutId, this.bufferManager);
                	}
                    tempCollector.addTuple(currentRow);
                }
                incrementWorkingBatch(chosenBatchIndex, workingPointers, chosenBatch);
            }

            // Save without closing
            collector.saveBatch();
            
            // Remove merged sublists
            for(int i=0; i<sortedIndex; i++) {
            	TupleSourceID id = activeTupleIDs.remove(0);
            	if (!id.equals(this.outputID)) {
            		bufferManager.removeTupleSource(id);
            	}
            }

            this.activeTupleIDs.add(mergedID);           
            this.mergedID = null;
            masterSortIndex = masterSortIndex - sortedIndex + 1;
            if (masterSortIndex < 0) {
            	masterSortIndex = this.activeTupleIDs.size() - 1;
            }
        } 
    	
    	if (tempCollector != null) {
	    	tempCollector.close();
	    	this.outTs = this.bufferManager.getTupleSource(tempOutId);
	        appendOutput();
    	}
        
        // Close sorted source (all others have been removed)
        if (doneReading) {
	        bufferManager.setStatus(activeTupleIDs.get(0), TupleSourceStatus.FULL);
	        if (this.outputID != null) {
	        	bufferManager.setStatus(outputID, TupleSourceStatus.FULL);
	        }
	        this.phase = DONE;
	        return;
        }
    	Assertion.assertTrue(mode == Mode.DUP_REMOVE);
    	if (this.outputID == null) {
    		this.outputID = activeTupleIDs.get(0);
    	}
    	this.phase = INITIAL_SORT;
    }

	private void appendOutput() throws TupleSourceNotFoundException,
			MetaMatrixComponentException {
		if (this.outTs != null) {
			//transfer the new dup removed tuples to the output id
        	TupleCollector tc = new TupleCollector(outputID, this.bufferManager);
        	try {
	        	try {
		        	while (outTs.hasNext()) {
		        		tc.addTuple(outTs.nextTuple());
		        	}
	        	} catch (MetaMatrixProcessingException e) {
	        		throw new MetaMatrixComponentException(e);
	        	}
			} finally {
				tc.saveBatch();
			}
        	outTs = null;
        }
	}

    /**
     * Increment the working batch at batchIndex.  The currentBatch is the currentBatch
     * for that batchIndex, which we already happen to have.  Return whether the batch
     * was changed or not.  True = changed.
     */
    private void incrementWorkingBatch(int batchIndex, int[] workingPointers, TupleBatch currentBatch) throws BlockedOnMemoryException, TupleSourceNotFoundException, MetaMatrixComponentException {
        workingPointers[batchIndex] += 1;
        if(workingPointers[batchIndex] > currentBatch.getEndRow()) {
            TupleSourceID tsID = unpinWorkingBatch(batchIndex, currentBatch);

            int beginRow = workingPointers[batchIndex];
            int endRow = beginRow + this.batchSize - 1;

            try {
                TupleBatch newBatch = bufferManager.pinTupleBatch(tsID, beginRow, endRow);
                if(newBatch.getRowCount() == 0) {
                    // Done with this working batch
                    workingBatches.set(batchIndex, null);
                } else {
                    workingBatches.set(batchIndex, newBatch);
                }
            } catch(MemoryNotAvailableException e) {
                throw BlockedOnMemoryException.INSTANCE;
            }
        }
    }
    
    private TupleSourceID unpinWorkingBatch(int batchIndex,
                                            TupleBatch currentBatch) throws TupleSourceNotFoundException,
                                                                    MetaMatrixComponentException {
        TupleSourceID tsID = activeTupleIDs.get(batchIndex);
        int lastBeginRow = currentBatch.getBeginRow();
        int lastEndRow = currentBatch.getEndRow();
        bufferManager.unpinTupleBatch(tsID, lastBeginRow, lastEndRow);
        return tsID;
    }

    private void initialize() throws TupleSourceNotFoundException, MetaMatrixComponentException {
        this.schema = this.bufferManager.getTupleSchema(this.sourceID);
        this.schemaTypes = TypeRetrievalUtil.getTypeNames(schema);
        this.batchSize = bufferManager.getProcessorBatchSize();
        int distinctIndex = sortElements != null? sortElements.size() - 1:0;
        if (useAllColumns && mode != Mode.SORT) {
	        if (this.sortElements != null) {
	        	this.sortElements = new ArrayList(this.sortElements);
	        	List toAdd = new ArrayList(schema);
	        	toAdd.removeAll(this.sortElements);
	        	this.sortElements.addAll(toAdd);
	        	this.sortTypes = new ArrayList<Boolean>(this.sortTypes);
	        	this.sortTypes.addAll(Collections.nCopies(this.sortElements.size() - this.sortTypes.size(), OrderBy.ASC));
        	} else {
	    		this.sortElements = this.schema;
	    		this.sortTypes = Collections.nCopies(this.sortElements.size(), OrderBy.ASC);
        	}
        }
        
        int[] cols = new int[sortElements.size()];

        Iterator iter = sortElements.iterator();
        
        for (int i = 0; i < cols.length; i++) {
            SingleElementSymbol elem = (SingleElementSymbol)iter.next();
            
            cols[i] = schema.indexOf(elem);
            Assertion.assertTrue(cols[i] != -1);
        }
        this.sortCols = cols;
        this.comparator = new ListNestedSortComparator(sortCols, sortTypes);
        this.comparator.setDistinctIndex(distinctIndex);
    }
    
    public boolean isDistinct() {
    	return this.comparator.isDistinct();
    }
    
}
