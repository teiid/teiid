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

import java.util.List;

import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.logging.LogManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.sql.util.VariableContext;
import org.teiid.query.util.CommandContext;


//TODO: consolidate with QueryProcessor
public class PlanExecutionNode extends RelationalNode {

    // Initialization state
    private ProcessorPlan plan;
    private boolean isOpen;
    private boolean needsProcessing;

	protected PlanExecutionNode() {
		super();
	}
    
	public PlanExecutionNode(int nodeID) {
		super(nodeID);
	}
	
    public void reset() {
        super.reset();

        resetPlan();
    }

    private void resetPlan() {
        plan.reset();
        isOpen = false;
        needsProcessing = false;
    }

    public void setProcessorPlan(ProcessorPlan plan) {
        this.plan = plan;    
    }
    
	public void open() 
		throws TeiidComponentException, TeiidProcessingException {
		super.open();
        // Initialize plan for execution
        CommandContext subContext = getContext().clone();
        subContext.pushVariableContext(new VariableContext());
        plan.initialize(subContext, getDataManager(), this.getBufferManager());        
        
        if (openPlanImmediately() && prepareNextCommand()) {
            needsProcessing = true;
            plan.open();
            isOpen = true;
        }
	}
	
	protected boolean openPlanImmediately() {
		return true;
	}
	
	public TupleBatch nextBatchDirect()
		throws BlockedException, TeiidComponentException, TeiidProcessingException {
        
        if (!isOpen) {
            if (!needsProcessing) {
                while (true) {
                    if (prepareNextCommand()) {
                        needsProcessing = true;
                        break;
                    }
                    if (!hasNextCommand()) {
                        needsProcessing = false;
                        break;
                    }
                }
            }
            if (needsProcessing) {
                plan.open();
                isOpen = true;
            } 
        }
        
        if (!needsProcessing) {
            terminateBatches();
            return pullBatch();
        }
        
		TupleBatch batch = plan.nextBatch();
       
        for (List<?> tuple : batch.getTuples()) {
            addBatchRow(tuple);
		}
        
        if(batch.getTerminationFlag()) {
        	if (hasNextCommand()) {
        		resetPlan();
        	} else {
        		terminateBatches();
        	}
        }
        
        return pullBatch();
	}

    /**
	 * @throws BlockedException  
     * @throws TeiidComponentException 
     * @throws TeiidProcessingException 
	 */
    protected boolean prepareNextCommand() throws BlockedException,
                                          TeiidComponentException, TeiidProcessingException {
        return true;
    }
    
    protected boolean hasNextCommand() {
        return false;
    }
    
	public void closeDirect() {
        try {
        	plan.close();
		} catch (TeiidComponentException e1){
			LogManager.logDetail(org.teiid.logging.LogConstants.CTX_DQP, e1, "Error closing processor"); //$NON-NLS-1$
		}
	}
	
	protected void getNodeString(StringBuffer str) {
		super.getNodeString(str);
	}

    public ProcessorPlan getProcessorPlan(){
        return this.plan;
    }

	public Object clone(){
		PlanExecutionNode clonedNode = new PlanExecutionNode();
		copyTo(clonedNode);
        return clonedNode;
	}
    
    protected void copyTo(PlanExecutionNode target) {
        target.setProcessorPlan(plan.clone());
        super.copyTo(target);
    }

    public PlanNode getDescriptionProperties() {   
    	PlanNode props = super.getDescriptionProperties();
        props.addProperty(PROP_EXECUTION_PLAN, this.plan.getDescriptionProperties());                
        return props;
    }
    
    @Override
    public Boolean requiresTransaction(boolean transactionalReads) {
		if (Boolean.TRUE.equals(getProcessorPlan().requiresTransaction(transactionalReads))) {
			return true;
		}
		return null;
    }
    
}
