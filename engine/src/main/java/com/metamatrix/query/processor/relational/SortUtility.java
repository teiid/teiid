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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.BlockedOnMemoryException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.MemoryNotAvailableException;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.common.buffer.TupleSourceNotFoundException;
import com.metamatrix.common.buffer.BufferManager.TupleSourceStatus;
import com.metamatrix.common.buffer.BufferManager.TupleSourceType;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;
import com.metamatrix.query.util.TypeRetrievalUtil;

/**
 */
public class SortUtility {

    private TupleSourceID sourceID;
    private List sourceElements;
    private List sortElements;
    private List sortTypes;
    private boolean removeDuplicates;

    private BufferManager bufferManager;
    private String groupName;
    private int batchSize;
    private List schema;
    private String[] schemaTypes;

    // For collecting tuples to be sorted
    private int sortPhaseRow = 1;
    private int phase = SORT;
    private int rowCount;
    private int[] sortCols;
    private List activeTupleIDs = new ArrayList();
    private List workingBatches;
    private int[] workingPointers;

    // Phase constants for readability
    private static final int SORT = 1;
    private static final int MERGE = 2;
    private static final int DONE = 3;

    /**
     * Constructor for SortUtility.
     */
    public SortUtility(TupleSourceID sourceID, List sourceElements, List sortElements, List sortTypes, boolean removeDuplicates,
                        BufferManager bufferMgr, String groupName) {

        this.sourceID = sourceID;
        this.sourceElements = sourceElements;
        this.sortElements = sortElements;
        this.sortTypes = sortTypes;
        this.removeDuplicates = removeDuplicates;
        this.bufferManager = bufferMgr;
        this.groupName = groupName;
    }

    /**
     */
    public TupleSourceID sort()
        throws BlockedException, MetaMatrixComponentException {

        try {
            // One time setup
            if(this.schema == null) {
                initialize();
            }
            
            if (rowCount == 0) {
                TupleSourceID mergedID = bufferManager.createTupleSource(this.schema, this.schemaTypes, this.groupName, TupleSourceType.PROCESSOR);
                activeTupleIDs.add(mergedID);
                phase = DONE;
            }
            
            if(this.phase == SORT) {
                sortPhase();
            }

            if(this.phase == MERGE) {
                try {
                    mergePhase();
                } finally {
                    if (workingBatches != null) {
                        for (int i = 0; i < workingBatches.size(); i++) {
                            TupleBatch tupleBatch = (TupleBatch)workingBatches.get(i);
                            if (tupleBatch != null) {
                                unpinWorkingBatch(i, tupleBatch);
                            }
                        }
                    }
                    workingBatches = null;
                }
            }
            
            TupleSourceID result = (TupleSourceID) this.activeTupleIDs.get(0);
            this.bufferManager.setStatus(result, TupleSourceStatus.FULL);
            return result;
        } catch(TupleSourceNotFoundException e) {
            throw new MetaMatrixComponentException(e, e.getMessage());
        }

    }

    protected void sortPhase() throws BlockedException, TupleSourceNotFoundException, MetaMatrixComponentException {
        ArrayList pinned = new ArrayList();     // of int[2] representing begin, end

        // Loop until all data has been read and sorted
        while(sortPhaseRow <= this.rowCount) {
            List workingTuples = new ArrayList();

            // Load data until out of memory
            while(sortPhaseRow <= this.rowCount) {
                try {
                    // Load and pin batch
                    TupleBatch batch = bufferManager.pinTupleBatch(sourceID, sortPhaseRow, sortPhaseRow + batchSize - 1);

                    if(batch.getRowCount() > 0) {
                        // Remember pinned rows
                        pinned.add(new int[] { sortPhaseRow, batch.getEndRow() });

                        // Add to previous batches for sorting
                        workingTuples.addAll(Arrays.asList(batch.getAllTuples()));

                        // Adjust beginning of next batch
                        sortPhaseRow = batch.getEndRow() + 1;
                    }

                } catch(MemoryNotAvailableException e) {
                    break;
                }
            }

            // Check for no memory available and block
            if(workingTuples.isEmpty()) {
                /* Defect 19087: We couldn't load any batches in memory, and we need to re-enqueue the work,
                 * so this should be a BlockedOnMemoryException instead of a BlockedException
                 */
                throw BlockedOnMemoryException.INSTANCE;
            }

            // Sort whatever is in memory
            Comparator comp = new ListNestedSortComparator(sortCols, sortTypes);
            Collections.sort( workingTuples, comp );

            // Write to temporary tuple source
            TupleSourceID sortedID = bufferManager.createTupleSource(this.schema, this.schemaTypes, this.groupName, TupleSourceType.PROCESSOR);
            activeTupleIDs.add(sortedID);
            int sortedThisPass = workingTuples.size();
            int writeBegin = 1;
            while(writeBegin <= sortedThisPass) {
                int writeEnd = Math.min(sortedThisPass, writeBegin + batchSize - 1);

                TupleBatch writeBatch = new TupleBatch(writeBegin, workingTuples.subList(writeBegin-1, writeEnd));
                bufferManager.addTupleBatch(sortedID, writeBatch);
                writeBegin += writeBatch.getRowCount();
            }

            // Clean up - unpin rows
            Iterator iter = pinned.iterator();
            while(iter.hasNext()) {
                int[] bounds = (int[]) iter.next();
                bufferManager.unpinTupleBatch(sourceID, bounds[0], bounds[1]);
            }
            pinned.clear();
        }

        // Clean up
        this.phase = MERGE;
    }

    protected void mergePhase() throws BlockedException, TupleSourceNotFoundException, MetaMatrixComponentException {
        // In the case where there is a single activeTupleID but removeDuplicates == true,
        // we need to execute this loop exactly once.  We also need to execute any time
        // the number of active tuple IDs is > 1

        if(this.activeTupleIDs.size() > 1 || this.removeDuplicates) {
            do {
                // Load and pin batch from sorted sublists while memory available
                this.workingBatches = new ArrayList(activeTupleIDs.size());
                int sortedIndex = 0;
                for(; sortedIndex<activeTupleIDs.size(); sortedIndex++) {
                    TupleSourceID activeID = (TupleSourceID) activeTupleIDs.get(sortedIndex);
                    try {
                        TupleBatch sortedBatch = bufferManager.pinTupleBatch(activeID, 1, this.batchSize);
                        workingBatches.add(sortedBatch);
                    } catch(MemoryNotAvailableException e) {
                        break;
                    }
                }
                
                //if we cannot make progress, just block for now
                if (workingBatches.size() < 2 && !(workingBatches.size() == 1 && this.removeDuplicates && activeTupleIDs.size() == 1)) {
                    throw BlockedOnMemoryException.INSTANCE;
                }
                
                // Initialize pointers into working batches
                this.workingPointers = new int[workingBatches.size()];
                for(int i=0; i<workingBatches.size(); i++) {
                    this.workingPointers[i] = 1;
                }

                // Initialize merge output
                TupleSourceID mergedID = bufferManager.createTupleSource(this.schema, this.schemaTypes, this.groupName, TupleSourceType.PROCESSOR);
                int mergedRowBegin = 1;

                // Merge from working sorted batches
                List currentRows = new ArrayList(this.batchSize);
                ListNestedSortComparator comparator = new ListNestedSortComparator(sortCols, sortTypes);
                while(true) {
                    // Find least valued row among working batches
                    List currentRow = null;
                    int chosenBatchIndex = -1;
                    TupleBatch chosenBatch = null;
                    for(int i=0; i<workingBatches.size(); i++) {
                        TupleBatch batch = (TupleBatch) workingBatches.get(i);
                        if(batch != null) {
                            List testRow = batch.getTuple(workingPointers[i]);
                            if(currentRow == null || comparator.compare(testRow, currentRow) < 0) {
                                // Found lower row
                                currentRow = testRow;
                                chosenBatchIndex = i;
                                chosenBatch = batch;
                            }
                        }
                    }

                    // Check for termination condition - all batches must have been null
                    if(currentRow == null) {
                        break;
                    }
                    // Output the row and update pointers
                    currentRows.add(currentRow);
                    incrementWorkingBatch(chosenBatchIndex, chosenBatch);

                    // Move past this same row on all batches if dup removal is on
                    if(this.removeDuplicates) {
                        for(int i=0; i<workingBatches.size(); i++) {
                            TupleBatch batch = (TupleBatch) workingBatches.get(i);
                            while(batch != null) {
                                List testRow = batch.getTuple(workingPointers[i]);
                                if(comparator.compare(testRow, currentRow) == 0) {
                                    if(incrementWorkingBatch(i, batch)) {
                                        batch = (TupleBatch) workingBatches.get(i);
                                    }
                                } else {
                                    break;
                                }
                            }
                        }
                    }


                    // Check for full batch and store
                    if(currentRows.size() == this.batchSize) {
                        bufferManager.addTupleBatch(mergedID, new TupleBatch(mergedRowBegin, currentRows));
                        mergedRowBegin = mergedRowBegin + this.batchSize;
                        currentRows = new ArrayList(this.batchSize);
                    }
                }

                // Save any remaining partial batch
                if(currentRows.size() > 0) {
                    bufferManager.addTupleBatch(mergedID, new TupleBatch(mergedRowBegin, currentRows));
                }
                
                // Remove merged sublists
                for(int i=0; i<sortedIndex; i++) {
                    bufferManager.removeTupleSource((TupleSourceID)activeTupleIDs.remove(0));
                }

                this.activeTupleIDs.add(mergedID);
                
            } while(this.activeTupleIDs.size() > 1);
        }

        // Close sorted source (all others have been removed)
        bufferManager.setStatus((TupleSourceID) activeTupleIDs.get(0), TupleSourceStatus.FULL);
        this.phase = DONE;
    }

    /**
     * Increment the working batch at batchIndex.  The currentBatch is the currentBatch
     * for that batchIndex, which we already happen to have.  Return whether the batch
     * was changed or not.  True = changed.
     */
    private boolean incrementWorkingBatch(int batchIndex, TupleBatch currentBatch) throws BlockedOnMemoryException, TupleSourceNotFoundException, MetaMatrixComponentException {
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

                // Return true
                return true;

            } catch(MemoryNotAvailableException e) {
                throw BlockedOnMemoryException.INSTANCE;
            }

        }
        return false;
    }
    
    private TupleSourceID unpinWorkingBatch(int batchIndex,
                                            TupleBatch currentBatch) throws TupleSourceNotFoundException,
                                                                    MetaMatrixComponentException {
        TupleSourceID tsID = (TupleSourceID)activeTupleIDs.get(batchIndex);
        int lastBeginRow = currentBatch.getBeginRow();
        int lastEndRow = currentBatch.getEndRow();
        bufferManager.unpinTupleBatch(tsID, lastBeginRow, lastEndRow);
        return tsID;
    }

    private void initialize() throws TupleSourceNotFoundException, MetaMatrixComponentException {
        this.schema = this.bufferManager.getTupleSchema(this.sourceID);
        this.schemaTypes = TypeRetrievalUtil.getTypeNames(schema);
        this.batchSize = bufferManager.getProcessorBatchSize();
        this.rowCount = bufferManager.getRowCount(this.sourceID);
        
        int[] cols = new int[sortElements.size()];

        Iterator iter = sortElements.iterator();
        
        for (int i = 0; i < cols.length; i++) {
            SingleElementSymbol elem = (SingleElementSymbol) iter.next();
            
            cols[i] = sourceElements.indexOf(elem);
            Assertion.assertTrue(cols[i] != -1);
        }
        
        this.sortCols = cols;
    }
}
