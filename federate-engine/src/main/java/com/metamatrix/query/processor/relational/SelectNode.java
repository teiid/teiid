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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.query.eval.Evaluator;
import com.metamatrix.query.sql.lang.Criteria;

public class SelectNode extends RelationalNode {

	private Criteria criteria;
    
    // Derived element lookup map
    private Map elementMap;    
	
    // State if blocked on evaluating a criteria
    private boolean blockedOnCriteria = false;
    private boolean blockedOnPrepare = false;
    private TupleBatch blockedBatch = null;
    private int blockedRow = 0;
    private Evaluator evaluator;
    
	public SelectNode(int nodeID) {
		super(nodeID);
	}
	
    public void reset() {
        super.reset();
        
        blockedOnCriteria = false;
        blockedOnPrepare = false;
        blockedBatch = null;
        blockedRow = 0;
    }

	public void setCriteria(Criteria criteria) { 
		this.criteria = criteria;
	}

	public Criteria getCriteria() { // made public to support change in ProcedurePlanner
		return this.criteria;
	}
	
	public void open() 
		throws MetaMatrixComponentException, MetaMatrixProcessingException {

		super.open();

        // Create element lookup map for evaluating project expressions
        if(this.elementMap == null) {
            this.elementMap = createLookupMap(this.getChildren()[0].getElements());
        }
        this.evaluator = new Evaluator(elementMap, getDataManager(), getContext());
	}
    
    /**
     * @see com.metamatrix.query.processor.relational.RelationalNode#nextBatchDirect()
     */
	public TupleBatch nextBatchDirect()
		throws BlockedException, MetaMatrixComponentException, MetaMatrixProcessingException {

        TupleBatch batch = blockedBatch; 		
        if(! blockedOnCriteria && ! blockedOnPrepare) {	
            batch = this.getChildren()[0].nextBatch();
        }
                
        if(batch.getRowCount() == 0) {
            return batch;    
            
        }   
        boolean doPrepareToProcessTuple = !blockedOnCriteria;             
        int row = blockedRow;
        if(! blockedOnCriteria && ! blockedOnPrepare) {
            row = batch.getBeginRow();               
        } else {
            // Reset blocked state
            blockedOnCriteria = false;
            blockedOnPrepare = false;
            blockedBatch = null;
            blockedRow = 0;
        }
        
        for(; row <= batch.getEndRow(); row++) {             
            List tuple = batch.getTuple(row);
        
            if (doPrepareToProcessTuple){
                try {
                    // Hook for subclasses
                    this.prepareToProcessTuple(this.elementMap, tuple);
                } catch(BlockedException e) {
                    // Save state and rethrow
                    blockedOnPrepare = true;
                    blockedBatch = batch;
                    blockedRow = row;
                    throw e;   
                }
            }
                        
            // Evaluate criteria with tuple
            try {
                if(evaluator.evaluate(this.criteria, tuple)) {
                    addBatchRow( projectTuple(elementMap, tuple, getElements()) );
                }
            } catch(BlockedException e) {
                // Save state and rethrow
                blockedOnCriteria = true;
                blockedBatch = batch;
                blockedRow = row;
                throw e;   
            }
        }   

        if(batch.getTerminationFlag()) { 
            terminateBatches();
        }

        return pullBatch();            
	}
    
    /**
     * This method is called by {@link #nextBatch} just after the current
     * tuple is pulled from the child processor node and just before any
     * processing is done (in this case, before the criteria is evaluated).
     * This gives subclasses a chance to do any custom processing - for example,
     * to examine the current tuple in order to execute correlated subqueries.
     * @param elementMap Map of ElementSymbol elements to Integer indices into
     * the currentTuple parameter
     * @param currentTuple the current tuple about to be processed by
     * this node
     * @throws MetaMatrixProcessingException for exception due to user input
     */
    protected void prepareToProcessTuple(Map elementMap, List currentTuple)
    throws BlockedException, MetaMatrixComponentException, MetaMatrixProcessingException {
        //Nothing done here        
    }
	
	protected void getNodeString(StringBuffer str) {
		super.getNodeString(str);
		str.append(criteria);
	}
	
	public Object clone(){
		SelectNode clonedNode = new SelectNode(super.getID());
		this.copy(this, clonedNode);
		return clonedNode;
	}
	
	protected void copy(SelectNode source, SelectNode target){
		super.copy(source, target);
		if(criteria != null){
			target.criteria = (Criteria)source.criteria.clone();
		}
		if(elementMap != null){
			target.elementMap = new HashMap(source.elementMap);
		}
	}
    
    /* 
     * @see com.metamatrix.query.processor.Describable#getDescriptionProperties()
     */
    public Map getDescriptionProperties() {   
        // Default implementation - should be overridden     
        Map props = super.getDescriptionProperties();
        props.put(PROP_TYPE, "Select"); //$NON-NLS-1$
        props.put(PROP_CRITERIA, this.criteria.toString());        
        return props;
    }
    
}
