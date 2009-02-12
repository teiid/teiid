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

package com.metamatrix.query.processor.batch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.query.processor.BaseProcessorPlan;
import com.metamatrix.query.processor.DescribableUtil;
import com.metamatrix.query.processor.ProcessorDataManager;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.util.CommandContext;


/** 
 * Plan for execution for a batched update command. The plan executes the child plans of the
 * individual commands in order.
 * @since 4.2
 */
public class BatchedUpdatePlan extends BaseProcessorPlan {
    
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
    
    /**
     *  
     * @param childPlans the child update plans for this batch
     * @param commandsInBatch The total number of commands in this batch. This does not always equal the number of plans if some
     * commands have been batched together.
     * @since 4.2
     */
    public BatchedUpdatePlan(List childPlans, int commandsInBatch) {
        this.updatePlans = (ProcessorPlan[])childPlans.toArray(new ProcessorPlan[childPlans.size()]);
        this.planOpened = new boolean[updatePlans.length];
        this.updateCounts = new List[commandsInBatch];
    }

    /** 
     * @see java.lang.Object#clone()
     * @since 4.2
     */
    public Object clone() {
        List clonedPlans = new ArrayList(updatePlans.length);
        for (int i = 0; i < updatePlans.length; i++) {
            clonedPlans.add(updatePlans[i].clone());
        }
        return new BatchedUpdatePlan(clonedPlans, updateCounts.length);
    }

    /** 
     * @see com.metamatrix.query.processor.ProcessorPlan#initialize(com.metamatrix.query.util.CommandContext, com.metamatrix.query.processor.ProcessorDataManager, com.metamatrix.common.buffer.BufferManager)
     * @since 4.2
     */
    public void initialize(CommandContext context, ProcessorDataManager dataMgr, BufferManager bufferMgr) {
        // Initialize all the child plans
        for (int i = 0; i < updatePlans.length; i++) {
            updatePlans[i].initialize(context, dataMgr, bufferMgr);
        }
    }
    
    /** 
     * @see com.metamatrix.query.processor.ProcessorPlan#getOutputElements()
     * @since 4.2
     */
    public List getOutputElements() {
        return Command.getUpdateCommandSymbol();
    }

    /** 
     * @see com.metamatrix.query.processor.ProcessorPlan#open()
     * @since 4.2
     */
    public void open() throws MetaMatrixComponentException, MetaMatrixProcessingException {
        // It's ok to open() the first plan, as it is not dependent on any prior commands.
        // See note for defect 16166 in the nextBatch() method.
        updatePlans[0].open();
        planOpened[0] = true;
    }

    /** 
     * @see com.metamatrix.query.processor.ProcessorPlan#nextBatch()
     * @since 4.2
     */
    public TupleBatch nextBatch() throws BlockedException, MetaMatrixComponentException, MetaMatrixProcessingException {
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
                updatePlans[planIndex].open();
                planOpened[planIndex] = true;
            }
            // Execute nextBatch() on each plan in sequence
            List[] currentBatch = updatePlans[planIndex].nextBatch().getAllTuples(); // Can throw BlockedException
            for (int i = 0; i < currentBatch.length; i++, commandIndex++) {
                updateCounts[commandIndex] = currentBatch[i];
            }
            
            // since we are done with the plan explicitly close it.
            updatePlans[planIndex].close();
        }
        // Add tuples to current batch
        TupleBatch batch = new TupleBatch(1, updateCounts);
        batch.setTerminationFlag(true);
        return batch;
    }

    /** 
     * @see com.metamatrix.query.processor.ProcessorPlan#close()
     * @since 4.2
     */
    public void close() throws MetaMatrixComponentException {
        // if the plan opened but the atomic request got cancelled then close the last plan node.
        if (planIndex < updatePlans.length && planOpened[planIndex]) {
            updatePlans[planIndex].close();
        }
    }

    
    /** 
     * @see com.metamatrix.query.processor.ProcessorPlan#reset()
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

    /** 
     * @see com.metamatrix.query.processor.Describable#getDescriptionProperties()
     * @since 4.2
     */
    public Map getDescriptionProperties() {
        Map props = new HashMap();
        props.put(PROP_TYPE, "Batched Update Plan"); //$NON-NLS-1$
        List children = new ArrayList();
        for (int i = 0; i < updatePlans.length; i++) {
            children.add(updatePlans[i].getDescriptionProperties());
        }
        props.put(PROP_CHILDREN, children);
        props.put(PROP_OUTPUT_COLS, DescribableUtil.getOutputColumnProperties(getOutputElements()));
        return props;
    }
    
    public String toString() {
        StringBuffer val = new StringBuffer("BatchedUpdatePlan {\n"); //$NON-NLS-1$
        for (int i = 0; i < updatePlans.length; i++) {
            val.append(updatePlans[i])
               .append("\n"); //$NON-NLS-1$
        }
        val.append("}\n"); //$NON-NLS-1$
        return val.toString();
    }

    /**
     * Returns the child plans for this batch. Used primarily for unit tests. 
     * @return
     * @since 4.2
     */
    public List getUpdatePlans() {
        return Arrays.asList(updatePlans);
    }

    /** 
     * @see com.metamatrix.query.processor.ProcessorPlan#getChildPlans()
     * @since 4.2
     */
    public Collection getChildPlans() {
        return Arrays.asList(updatePlans);
    }

}
