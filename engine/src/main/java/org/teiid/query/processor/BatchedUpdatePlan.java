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

package org.teiid.query.processor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.util.VariableContext;
import org.teiid.query.util.CommandContext;



/** 
 * Plan for execution for a batched update command. The plan executes the child plans of the
 * individual commands in order.
 * 
 * If variableContexts are provided, then this is a bulk update where all plans are the same object.
 * 
 * @since 4.2
 */
public class BatchedUpdatePlan extends ProcessorPlan {
    
    /** Array that holds the child update plans */
    private ProcessorPlan[] updatePlans;
    /** */
    private boolean[] planOpened;
    /** Array that holds the update counts for each command in this batch */
    private List[] updateCounts;
    /** The position of the plan currently being executed */
    private int planIndex = 0;
    /** The position of the command for which the update count is being retrieved */
    private int commandIndex = 0;
    
    private List<VariableContext> contexts; //only set for bulk updates
    
    /**
     *  
     * @param childPlans the child update plans for this batch
     * @param commandsInBatch The total number of commands in this batch. This does not always equal the number of plans if some
     * commands have been batched together.
     * @since 4.2
     */
    public BatchedUpdatePlan(List childPlans, int commandsInBatch, List<VariableContext> contexts) {
        this.updatePlans = (ProcessorPlan[])childPlans.toArray(new ProcessorPlan[childPlans.size()]);
        this.planOpened = new boolean[updatePlans.length];
        this.updateCounts = new List[commandsInBatch];
        this.contexts = contexts;
    }

    /** 
     * @see java.lang.Object#clone()
     * @since 4.2
     */
    public BatchedUpdatePlan clone() {
        List<ProcessorPlan> clonedPlans = new ArrayList<ProcessorPlan>(updatePlans.length);
        
        clonedPlans.add(updatePlans[0].clone());
        for (int i = 1; i <updatePlans.length; i++) {
        	if (contexts == null) {
                clonedPlans.add(updatePlans[1].clone());        		
        	} else {
        		clonedPlans.add(clonedPlans.get(0));
        	}
        }
        return new BatchedUpdatePlan(clonedPlans, updateCounts.length, contexts);
    }

    /** 
     * @see org.teiid.query.processor.ProcessorPlan#initialize(org.teiid.query.util.CommandContext, org.teiid.query.processor.ProcessorDataManager, org.teiid.common.buffer.BufferManager)
     * @since 4.2
     */
    public void initialize(CommandContext context, ProcessorDataManager dataMgr, BufferManager bufferMgr) {
    	context = context.clone();
    	context.setVariableContext(new VariableContext()); //start a new root variable context
    	this.setContext(context);
        // Initialize all the child plans
        for (int i = 0; i < getPlanCount(); i++) {
            updatePlans[i].initialize(context, dataMgr, bufferMgr);
        }
    }
    
    /** 
     * @see org.teiid.query.processor.ProcessorPlan#getOutputElements()
     * @since 4.2
     */
    public List getOutputElements() {
        return Command.getUpdateCommandSymbol();
    }

    /** 
     * @see org.teiid.query.processor.ProcessorPlan#open()
     * @since 4.2
     */
    public void open() throws TeiidComponentException, TeiidProcessingException {
        // It's ok to open() the first plan, as it is not dependent on any prior commands.
        // See note for defect 16166 in the nextBatch() method.
       openPlan();
    }

    /** 
     * @see org.teiid.query.processor.ProcessorPlan#nextBatch()
     * @since 4.2
     */
    public TupleBatch nextBatch() throws BlockedException, TeiidComponentException, TeiidProcessingException {
        for (;planIndex < updatePlans.length; planIndex++) {
            if (!planOpened[planIndex]) { // Open the plan only once
                /* Defect 16166
                 * Some commands in a batch may depend on updates by previous commands in the same batch. A call
                 * to open() usually submits an atomic command, so calling open() on all the child plans at the same time
                 * will mean that the datasource may not be in the state expected by a later command within the batch. So,
                 * for a batch of commands, we only open() a later plan when we are finished with the previous plan to
                 * guarantee that the commands in the previous plan are completed before the commands in any subsequent
                 * plans are executed.
                 */
            	openPlan();
            }
            // Execute nextBatch() on each plan in sequence
            List<List<?>> currentBatch = updatePlans[planIndex].nextBatch().getTuples(); // Can throw BlockedException
            for (int i = 0; i < currentBatch.size(); i++, commandIndex++) {
                updateCounts[commandIndex] = currentBatch.get(i);
            }
            
            // since we are done with the plan explicitly close it.
            updatePlans[planIndex].close();
        }
        // Add tuples to current batch
        TupleBatch batch = new TupleBatch(1, updateCounts);
        batch.setTerminationFlag(true);
        return batch;
    }

	private void openPlan() throws TeiidComponentException,
			TeiidProcessingException {
		if (this.contexts != null && !this.contexts.isEmpty()) {
			CommandContext context = updatePlans[planIndex].getContext();
			context.getVariableContext().clear();
			VariableContext currentValues = this.contexts.get(planIndex);
			context.getVariableContext().putAll(currentValues); 
		}
		updatePlans[planIndex].reset();
		updatePlans[planIndex].open();
		planOpened[planIndex] = true;
	}

    /** 
     * @see org.teiid.query.processor.ProcessorPlan#close()
     * @since 4.2
     */
    public void close() throws TeiidComponentException {
        // if the plan opened but the atomic request got cancelled then close the last plan node.
        if (planIndex < updatePlans.length && planOpened[planIndex]) {
            updatePlans[planIndex].close();
        }
    }

    
    /** 
     * @see org.teiid.query.processor.ProcessorPlan#reset()
     * @since 4.2
     */
    public void reset() {
        super.reset();
        for (int i = 0; i < updatePlans.length; i++) {
            updatePlans[i].reset();
            planOpened[i] = false;
        }
        for (int i = 0; i < updateCounts.length; i++) {
            updateCounts[i] = null;
        }
        planIndex = 0;
        commandIndex = 0;
    }

    public PlanNode getDescriptionProperties() {
    	PlanNode props = super.getDescriptionProperties();
        for (int i = 0; i < getPlanCount(); i++) {
            props.addProperty("Batch Plan " + i, updatePlans[i].getDescriptionProperties()); //$NON-NLS-1$
        }
        return props;
    }
    
    public String toString() {
        StringBuffer val = new StringBuffer("BatchedUpdatePlan {\n"); //$NON-NLS-1$
        for (int i = 0; i < getPlanCount(); i++) {
            val.append(updatePlans[i])
               .append("\n"); //$NON-NLS-1$
        }
        val.append("}\n"); //$NON-NLS-1$
        return val.toString();
    }

	private int getPlanCount() {
		return (contexts != null?1:updatePlans.length);
	}

    /**
     * Returns the child plans for this batch. Used primarily for unit tests. 
     * @return
     * @since 4.2
     */
    public List getUpdatePlans() {
        return Arrays.asList(updatePlans);
    }
    
    @Override
    public boolean requiresTransaction(boolean transactionalReads) {
		if (updatePlans.length == 1) {
			return updatePlans[0].requiresTransaction(transactionalReads);
		}
		return true;
    }
    
    @Override
    public void getAccessedGroups(List<GroupSymbol> groups) {
        for (int i = 0; i < getPlanCount(); i++) {
        	updatePlans[i].getAccessedGroups(groups);
        }
    }

}
