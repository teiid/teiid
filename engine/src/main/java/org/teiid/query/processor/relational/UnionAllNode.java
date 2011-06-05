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

import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.BufferManager.BufferReserveMode;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.util.CommandContext;


public class UnionAllNode extends RelationalNode {

    private boolean[] sourceDone;
    
    private int outputRow = 1;
    private int reserved;
    private int schemaSize;
	
	public UnionAllNode(int nodeID) {
		super(nodeID);
	}
	    
    public void reset() {
        super.reset();
        
        sourceDone = null;
        outputRow = 1;   
    }    
    
    @Override
    public void initialize(CommandContext context, BufferManager bufferManager,
    		ProcessorDataManager dataMgr) {
    	super.initialize(context, bufferManager, dataMgr);
    	this.schemaSize = getBufferManager().getSchemaSize(getOutputElements());
    }
    
	public void open() 
		throws TeiidComponentException, TeiidProcessingException {

        // Initialize done flags
        sourceDone = new boolean[getChildren().length];
        if (reserved == 0) {
        	reserved = getBufferManager().reserveBuffers((getChildren().length - 1) * schemaSize, BufferReserveMode.FORCE);
        }
        // Open the children
        super.open();
	}

    public TupleBatch nextBatchDirect() 
        throws BlockedException, TeiidComponentException, TeiidProcessingException {

        // Walk through all children and for each one that isn't done, try to retrieve a batch
        // When all sources are done, set the termination flag on that batch
        
        RelationalNode[] children = getChildren();
        int activeSources = 0;
        TupleBatch batch = null;
        for(int i=0; i<children.length; i++) {
            if(children[i] != null && ! sourceDone[i]) {
                activeSources++;
                
                if(batch == null) {
                    try {
                        batch = children[i].nextBatch();
                        
                        // Got a batch
                        if(batch.getTerminationFlag() == true) {
                            // Mark source as being done and decrement the activeSources counter
                            sourceDone[i] = true;
                            activeSources--;
                            if (reserved > 0) {
                            	getBufferManager().releaseBuffers(schemaSize);
                            	reserved-=schemaSize;
                            }
                        }
                    } catch(BlockedException e) {
                        // no problem - try the next one
                    }
                } else {
                    // We already have a batch, so we know that 
                    // 1) we have a batch to return and 
                    // 2) this isn't the last active source, so we're not returning the last batch
                    
                    // This is sufficient to break the loop - we won't learn anything new after this                    
                    break;
                }                
            }
        }
        
        // Determine what to return
        TupleBatch outputBatch = null;
        if(batch != null) {
            // Rebuild the batch to reset the output row
            outputBatch = new TupleBatch(outputRow, batch.getTuples());
                        
            // This is the last unioned batch if:
            // 1) This batch is a termination batch from the child
            // 2) No other active sources exist
            outputBatch.setTerminationFlag(batch.getTerminationFlag() == true && activeSources == 0);
            
            // Update output row for next batch
            outputRow += outputBatch.getRowCount();
            
        } else if(activeSources > 0) {
            // Didn't get a batch but there are active sources so we are blocked
        	throw BlockedException.block(getContext().getRequestId(), "Blocking on union source."); //$NON-NLS-1$
        } else {
            // No batch and no active sources - return empty termination batch (should never happen but just in case)
            outputBatch = new TupleBatch(outputRow, Collections.EMPTY_LIST);
            outputBatch.setTerminationFlag(true);            
        }
        
        return outputBatch;
    }    
    
    @Override
    public void closeDirect() {
    	getBufferManager().releaseBuffers(reserved);
    	reserved = 0;
    }

	public Object clone(){
		UnionAllNode clonedNode = new UnionAllNode(super.getID());
		super.copy(this, clonedNode);
		return clonedNode;
	}
    
}
