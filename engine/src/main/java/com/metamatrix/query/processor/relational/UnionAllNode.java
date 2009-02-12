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

import java.util.Collections;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.BlockedOnMemoryException;
import com.metamatrix.common.buffer.TupleBatch;

public class UnionAllNode extends RelationalNode {

    private boolean[] sourceDone;
    
    private int outputRow = 1;
	
	public UnionAllNode(int nodeID) {
		super(nodeID);
	}
	    
    public void reset() {
        super.reset();
        
        sourceDone = null;
        outputRow = 1;   
    }    
    
	public void open() 
		throws MetaMatrixComponentException, MetaMatrixProcessingException {

        // Initialize done flags
        sourceDone = new boolean[getChildren().length];
        
        // Open the children
        super.open();
	}

    public TupleBatch nextBatchDirect() 
        throws BlockedException, MetaMatrixComponentException, MetaMatrixProcessingException {

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
                        }
                        
                    } catch (BlockedOnMemoryException e) {
                        throw e;
                    } catch(BlockedException e) {
                    	if(i<children.length-1 && hasDependentProcedureExecutionNode(children[0])){
                    		throw e;
                    	}
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
            outputBatch = new TupleBatch(outputRow, batch.getAllTuples());
                        
            // This is the last unioned batch if:
            // 1) This batch is a termination batch from the child
            // 2) No other active sources exist
            outputBatch.setTerminationFlag(batch.getTerminationFlag() == true && activeSources == 0);
            
            // Update output row for next batch
            outputRow += outputBatch.getRowCount();
            
        } else if(activeSources > 0) {
            // Didn't get a batch but there are active sources so we are blocked
            throw BlockedException.INSTANCE;
        } else {
            // No batch and no active sources - return empty termination batch (should never happen but just in case)
            outputBatch = new TupleBatch(outputRow, Collections.EMPTY_LIST);
            outputBatch.setTerminationFlag(true);            
        }
        
        return outputBatch;
    }    

	public Object clone(){
		UnionAllNode clonedNode = new UnionAllNode(super.getID());
		super.copy(this, clonedNode);
		return clonedNode;
	}

    public Map getDescriptionProperties() {
        // Default implementation - should be overridden
        Map props = super.getDescriptionProperties();
        props.put(PROP_TYPE, "Union All"); //$NON-NLS-1$
        
        return props;
    }
    
	private boolean hasDependentProcedureExecutionNode(RelationalNode node) {
		if(node == null){
			return false;
		}
		
		if(node instanceof DependentProcedureExecutionNode) {
			return true;
		}
		if(node.getChildren() != null ) { 
			for(int i=0; i<node.getChildren().length; i++){
				if(hasDependentProcedureExecutionNode(node.getChildren()[i])){
					return true;
				}
			}
		}	
		return false;
	}
    
}
