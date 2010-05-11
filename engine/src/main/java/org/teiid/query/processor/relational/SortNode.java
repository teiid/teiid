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

import static org.teiid.query.analysis.AnalysisRecord.*;

import java.util.ArrayList;
import java.util.List;

import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.processor.BatchIterator;
import org.teiid.query.processor.relational.SortUtility.Mode;
import org.teiid.query.sql.lang.OrderBy;


public class SortNode extends RelationalNode {

	private List sortElements;
	private List<Boolean> sortTypes;
    private Mode mode = Mode.SORT;

    private SortUtility sortUtility;
    private int phase = SORT;
    private TupleBuffer output;
    private TupleSource outputTs;

    private static final int SORT = 2;
    private static final int OUTPUT = 3;

	public SortNode(int nodeID) {
		super(nodeID);
	}

    public void reset() {
        super.reset();
        sortUtility = null;
        phase = SORT;
        output = null;
        outputTs = null;
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

	public TupleBatch nextBatchDirect()
		throws BlockedException, TeiidComponentException, TeiidProcessingException {
        if(this.phase == SORT) {
            sortPhase();
        }

        return outputPhase();
    }

    private void sortPhase() throws BlockedException, TeiidComponentException, TeiidProcessingException {
    	if (this.sortUtility == null) {
	        this.sortUtility = new SortUtility(new BatchIterator(getChildren()[0]), sortElements,
	                                            sortTypes, this.mode, getBufferManager(),
	                                            getConnectionID());
		}
		this.output = this.sortUtility.sort();
		if (this.outputTs == null) {
			this.outputTs = this.output.createIndexedTupleSource();
		}
        this.phase = OUTPUT;
    }

    private TupleBatch outputPhase() throws BlockedException, TeiidComponentException, TeiidProcessingException {
		if (!this.output.isFinal()) {
			this.phase = SORT;
		} else {
			this.output.setForwardOnly(true);
		}
		List<?> tuple = null;
		try {
			while ((tuple = this.outputTs.nextTuple()) != null) {
				//resize to remove unrelated columns
				addBatchRow(tuple.subList(0, this.getElements().size()));
				if (this.isBatchFull()) {
					return pullBatch();
				}
			}
		} catch (BlockedException e) {
			if (this.hasPendingRows()) {
				return this.pullBatch();
			}
			throw e;
		}
		this.terminateBatches();
		return this.pullBatch();
    }

    public void closeDirect() {
        if(this.output != null) {
            this.output.remove();
            this.output = null;
            this.outputTs = null;
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
    
    public PlanNode getDescriptionProperties() {
    	PlanNode props = super.getDescriptionProperties();
        
        if(this.mode != Mode.DUP_REMOVE && this.sortElements != null) {
            Boolean ASC_B = Boolean.valueOf(OrderBy.ASC);
            List<String> cols = new ArrayList<String>(this.sortElements.size());
            for(int i=0; i<this.sortElements.size(); i++) {
                String elemName = this.sortElements.get(i).toString();
                if(this.sortTypes.get(i).equals(ASC_B)) {
                    cols.add(elemName + " ASC");  //$NON-NLS-1$
                } else {
                    cols.add(elemName + " DESC"); //$NON-NLS-1$
                }
            }
            props.addProperty(PROP_SORT_COLS, cols);
        }
        
        props.addProperty(PROP_SORT_MODE, this.mode.toString());
        
        return props;
    }
    
}
