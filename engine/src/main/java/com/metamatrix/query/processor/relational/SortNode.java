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
import com.metamatrix.query.processor.relational.SortUtility.Mode;
import com.metamatrix.query.sql.lang.OrderBy;

public class SortNode extends RelationalNode {

	private List sortElements;
	private List<Boolean> sortTypes;
    private Mode mode = Mode.SORT;

    private SortUtility sortUtility;
    private int phase = COLLECTION;
    private TupleSourceID outputID;
    private int rowCount = -1;
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
        rowCount = -1;
        outputBeginRow = 1;
        this.collector = null;
    }

	public void setSortElements(List sortElements, List<Boolean> sortTypes) {
		this.sortElements = sortElements;
		this.sortTypes = sortTypes;
	}
	
	public List getSortElements() {
		return this.sortElements;
	}
	
	public Mode getMode() {
		return mode;
	}

	public void setMode(Mode mode) {
		this.mode = mode;
	}

	public void open()
		throws MetaMatrixComponentException, MetaMatrixProcessingException {

		super.open();
		this.collector = new BatchCollector(this.getChildren()[0]);
	}

	public TupleBatch nextBatchDirect()
		throws BlockedException, MetaMatrixComponentException, MetaMatrixProcessingException {

        try {

            if(this.phase == COLLECTION) {
                collectionPhase();
            }

            if(this.phase == SORT) {
                sortPhase();
            }

            return outputPhase();
        } catch(TupleSourceNotFoundException e) {
            throw new MetaMatrixComponentException(e, e.getMessage());
        }
    }

    private void collectionPhase() throws BlockedException, TupleSourceNotFoundException, MetaMatrixComponentException, MetaMatrixProcessingException {
		try {
			collector.collectTuples();
		} catch (BlockedOnMemoryException e) {
			throw e;
		} catch (BlockedException e) {
			if (mode != Mode.DUP_REMOVE || !collector.collectedAny()) {
				throw e;
			}
		}
		if (this.sortUtility == null) {
	        this.sortUtility = new SortUtility(collector.getTupleSourceID(), sortElements,
	                                            sortTypes, this.mode, getBufferManager(),
	                                            getConnectionID(), true);
		}
        this.phase = SORT;
    }

    private void sortPhase() throws BlockedException, MetaMatrixComponentException {
		this.outputID = this.sortUtility.sort();
        this.phase = OUTPUT;
    }

    private TupleBatch outputPhase() throws BlockedException, TupleSourceNotFoundException, MetaMatrixComponentException {
    	if (this.rowCount == -1) {
    		this.rowCount = getBufferManager().getFinalRowCount(outputID);
    		if (this.rowCount == -1) {
    			this.phase = this.collector.isDone()?SORT:COLLECTION;
    		}
    	}
        if(this.rowCount == 0 || (this.rowCount != -1 && this.outputBeginRow > this.rowCount)) {
            TupleBatch terminationBatch = new TupleBatch(1, Collections.EMPTY_LIST);
            terminationBatch.setTerminationFlag(true);
            return terminationBatch;
        }
        int beginPinned = this.outputBeginRow;
        int endPinned = this.outputBeginRow+getBatchSize()-1;
        try {
            TupleBatch outputBatch = getBufferManager().pinTupleBatch(outputID, beginPinned, endPinned);
            
            this.outputBeginRow += outputBatch.getRowCount();

            outputBatch = removeUnrelatedColumns(outputBatch);

            if(rowCount != -1 && outputBeginRow > rowCount) {
                outputBatch.setTerminationFlag(true);
            }

            return outputBatch;
        } catch(MemoryNotAvailableException e) {
            throw BlockedOnMemoryException.INSTANCE;
        } finally {
            getBufferManager().unpinTupleBatch(outputID, beginPinned, endPinned);
        }
    }

	private TupleBatch removeUnrelatedColumns(TupleBatch outputBatch) {
		int extraColumns = this.getChildren()[0].getElements().size() - this.getElements().size();
		
		if (extraColumns > 0) {
			for (List tuple : outputBatch.getAllTuples()) {
				addBatchRow(tuple.subList(0, this.getElements().size()));
			}
			outputBatch = pullBatch();
		}
		return outputBatch;
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
		str.append("[").append(mode).append("] "); //$NON-NLS-1$ //$NON-NLS-2$
		if (this.mode != Mode.DUP_REMOVE) {
			str.append(sortElements);
		}
	}

	protected void copy(SortNode source, SortNode target){
		super.copy(source, target);
		target.sortElements = source.sortElements;
		target.sortTypes = source.sortTypes;
		target.mode = source.mode;
	}

	public Object clone(){
		SortNode clonedNode = new SortNode(super.getID());
		this.copy(this, clonedNode);

		return clonedNode;
	}
    
    public Map getDescriptionProperties() {
        // Default implementation - should be overridden
        Map props = super.getDescriptionProperties();
        switch (mode) {
        case SORT:
            props.put(PROP_TYPE, "Sort"); //$NON-NLS-1$
        	break;
        case DUP_REMOVE:
        	props.put(PROP_TYPE, "Duplicate Removal"); //$NON-NLS-1$
        	break;
        case DUP_REMOVE_SORT:
        	props.put(PROP_TYPE, "Duplicate Removal And Sort"); //$NON-NLS-1$
        	break;
        }
        
        if(this.mode != Mode.DUP_REMOVE && this.sortElements != null) {
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
        
        props.put(PROP_REMOVE_DUPS, this.mode);
        
        return props;
    }
    
}
