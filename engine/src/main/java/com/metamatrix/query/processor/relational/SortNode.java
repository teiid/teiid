/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.BlockedOnMemoryException;
import com.metamatrix.common.buffer.MemoryNotAvailableException;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.common.buffer.TupleSourceNotFoundException;
import com.metamatrix.query.sql.lang.OrderBy;

public class SortNode extends RelationalNode {

	private List sortElements;
	private List sortTypes;
    private boolean removeDuplicates;

    private SortUtility sortUtility;
    private int phase = COLLECTION;
    private TupleSourceID outputID;
    private int rowCount;
    private int outputBeginRow = 1;
    private BatchCollector collector;

    private static final int COLLECTION = 1;
    private static final int SORT = 2;
    private static final int OUTPUT = 3;

	public SortNode(int nodeID) {
		super(nodeID);
	}

    public void reset() {
        super.reset();
        sortUtility = null;
        phase = COLLECTION;
        outputID = null;
        rowCount = 0;
        outputBeginRow = 1;
        this.collector = null;
    }

	public void setSortElements(List sortElements, List sortTypes) {
		this.sortElements = sortElements;
		this.sortTypes = sortTypes;
	}
	
	public List getSortElements() {
		return this.sortElements;
	}

    protected void setRemoveDuplicates(boolean removeDuplicates) {
        this.removeDuplicates = removeDuplicates;
    }

	public void open()
		throws MetaMatrixComponentException, MetaMatrixProcessingException {

		super.open();
		this.collector = new BatchCollector(this.getChildren()[0]);
	}

    /**
     * 1ST PHASE - COLLECTION
     *  Collect all batches from child node, save in collected tuple source
     *
     * 2ND PHASE - SORT INITIAL SUBLISTS
     *  Repeat until all batches from collection TS have been read
     *      Get and pin batches from collection TS until MemoryNotAvailableException
     *      Sort batches
     *      Write batches into new sorted TS
     *      Unpin all batches
     *  Remove collection TS
     *
     * 3RD PHASE - MERGE SORTED SUBLISTS
     *  Repeat until there is one sublist
     *      Repeat until all sorted sublists have been merged
     *          For each sorted sublist S
     *              Load and pin a batch until memory not available
     *          Merge from pinned batches
     *              As batch is done, unpin and load next
     *          Output merge into new sublist T
     *          Remove merged sublists
     *      Let sublists = set of T's
     *
     * 4TH PHASE - OUTPUT
     *  Return batches from single sublist from T
     */
	public TupleBatch nextBatchDirect()
		throws BlockedException, MetaMatrixComponentException, MetaMatrixProcessingException {

        try {

            if(this.phase == COLLECTION) {
                collectionPhase(null);
            }

            if(this.phase == SORT) {
                sortPhase();
            }

            if(this.phase == OUTPUT) {
                return outputPhase();
            }
        } catch(TupleSourceNotFoundException e) {
            throw new MetaMatrixComponentException(e, e.getMessage());
        }

        TupleBatch terminationBatch = new TupleBatch(1, Collections.EMPTY_LIST);
        terminationBatch.setTerminationFlag(true);
        return terminationBatch;
    }

    protected void collectionPhase(TupleBatch batch) throws BlockedException, TupleSourceNotFoundException, MetaMatrixComponentException, MetaMatrixProcessingException {
        RelationalNode sourceNode = this.getChildren()[0];
        TupleSourceID collectionID = collector.collectTuples(batch);
        this.rowCount = collector.getRowCount();
        if(this.rowCount == 0) {
            this.phase = OUTPUT;
        } else {
            List sourceElements = sourceNode.getElements();
            this.sortUtility = new SortUtility(collectionID, sourceElements,
                                                sortElements, sortTypes, this.removeDuplicates,
                                                getBufferManager(), getConnectionID());
            this.phase = SORT;
        }
    }

    private void sortPhase() throws BlockedException, TupleSourceNotFoundException, MetaMatrixComponentException {
        this.outputID = this.sortUtility.sort();
        this.rowCount = getBufferManager().getRowCount(outputID);
        this.phase = OUTPUT;

    }

    private TupleBatch outputPhase() throws BlockedException, TupleSourceNotFoundException, MetaMatrixComponentException {
        if(this.rowCount == 0 || this.outputBeginRow > this.rowCount) {
            TupleBatch terminationBatch = new TupleBatch(1, Collections.EMPTY_LIST);
            terminationBatch.setTerminationFlag(true);
            return terminationBatch;
        }
        int beginPinned = this.outputBeginRow;
        int endPinned = this.outputBeginRow+getBatchSize()-1;
        try {
            TupleBatch outputBatch = getBufferManager().pinTupleBatch(outputID, beginPinned, endPinned);

            this.outputBeginRow += outputBatch.getRowCount();

            if(outputBeginRow > rowCount) {
                outputBatch.setTerminationFlag(true);
            }

            return outputBatch;
        } catch(MemoryNotAvailableException e) {
            throw BlockedOnMemoryException.INSTANCE;
        } finally {
            getBufferManager().unpinTupleBatch(outputID, beginPinned, endPinned);
        }
    }

    public void close() throws MetaMatrixComponentException {
        if (!isClosed()) {
            super.close();
            try {
                if(this.collector != null) {
                    this.collector.close();
                    this.collector = null;
                }
                if(this.outputID != null) {
                    getBufferManager().removeTupleSource(outputID);
                    this.outputID = null;
                }
            } catch(TupleSourceNotFoundException e) {
                throw new MetaMatrixComponentException(e, e.getMessage());
            }
        }
    }

	protected void getNodeString(StringBuffer str) {
		super.getNodeString(str);
		str.append(sortElements);
	}

	protected void copy(SortNode source, SortNode target){
		super.copy(source, target);
		if(source.sortElements != null){
			target.sortElements = new ArrayList(source.sortElements);
		}
		if(sortTypes != null){
			target.sortTypes = new ArrayList(source.sortTypes);
		}
		target.removeDuplicates = source.removeDuplicates;
	}

	public Object clone(){
		SortNode clonedNode = new SortNode(super.getID());
		this.copy(this, clonedNode);

		return clonedNode;
	}
    
    public Map getDescriptionProperties() {
        // Default implementation - should be overridden
        Map props = super.getDescriptionProperties();
        props.put(PROP_TYPE, "Sort"); //$NON-NLS-1$
        
        if(this.sortElements != null) {
            Boolean ASC_B = Boolean.valueOf(OrderBy.ASC);
            List cols = new ArrayList(this.sortElements.size());
            for(int i=0; i<this.sortElements.size(); i++) {
                String elemName = this.sortElements.get(i).toString();
                if(this.sortTypes.get(i).equals(ASC_B)) {
                    cols.add(elemName + " ASC");  //$NON-NLS-1$
                } else {
                    cols.add(elemName + " DESC"); //$NON-NLS-1$
                }
            }
            props.put(PROP_SORT_COLS, cols);
        }
        
        props.put(PROP_REMOVE_DUPS, "" + this.removeDuplicates); //$NON-NLS-1$
        
        return props;
    }
    
}
