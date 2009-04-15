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
import java.util.List;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.sql.util.VariableContext;
import com.metamatrix.query.util.CommandContext;

public class PlanExecutionNode extends RelationalNode {

    // Initialization state
    private ProcessorPlan plan;
    private boolean isOpen;
    private boolean needsProcessing;

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
		throws MetaMatrixComponentException, MetaMatrixProcessingException {

        // Initialize plan for execution
        CommandContext subContext = (CommandContext) getContext().clone();
        subContext.pushVariableContext(new VariableContext());
        plan.initialize(subContext, getDataManager(), this.getBufferManager());        
        
        if (prepareNextCommand()) {
            needsProcessing = true;
            plan.open();
            isOpen = true;
        }
	}
	
	public TupleBatch nextBatchDirect()
		throws BlockedException, MetaMatrixComponentException, MetaMatrixProcessingException {
        
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
        boolean lastBatch = batch.getTerminationFlag();
       
        if(lastBatch) {
            if (!hasNextCommand()) {
                terminateBatches();
            } else {
                lastBatch = false;
                resetPlan();
            }
        }
        batch.setTerminationFlag(lastBatch);
        return batch;
	}

    protected boolean prepareNextCommand() throws BlockedException,
                                          MetaMatrixComponentException, MetaMatrixProcessingException {
        return true;
    }
    
    protected boolean hasNextCommand() {
        return false;
    }
    
	public void close() throws MetaMatrixComponentException {
        if (!isClosed()) {
            super.close();            
	        plan.close();
        }
	}
	
	protected void getNodeString(StringBuffer str) {
		super.getNodeString(str);
	}

    protected ProcessorPlan getProcessorPlan(){
        return this.plan;
    }

	public Object clone(){
		PlanExecutionNode clonedNode = new PlanExecutionNode(super.getID());
		copy(this, clonedNode);
        return clonedNode;
	}
    
    protected void copy(PlanExecutionNode source,
                        PlanExecutionNode target) {
        target.setProcessorPlan((ProcessorPlan)source.plan.clone());
        super.copy(source, target);
    }

    public Map getDescriptionProperties() {   
        // Default implementation - should be overridden     
        Map props = super.getDescriptionProperties();
        props.put(PROP_TYPE, "Plan Execution"); //$NON-NLS-1$
        props.put(PROP_EXECUTION_PLAN, this.plan.getDescriptionProperties());                
        return props;
    }
    
    /** 
     * @see com.metamatrix.query.processor.relational.RelationalNode#getSubPlans()
     * @since 4.2
     */
    public List getChildPlans() {
        List subPlans = new ArrayList(1);
        subPlans.add(this.plan);
        return subPlans;
    }
}
