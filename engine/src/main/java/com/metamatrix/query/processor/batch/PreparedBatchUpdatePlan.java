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

import org.teiid.dqp.internal.process.PreparedStatementRequest;

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
 * Plan for execution for PreparedBatchUpdate.
 * @since 5.5.2
 */
public class PreparedBatchUpdatePlan extends BaseProcessorPlan {
    private ProcessorPlan[] updatePlans;
    private int[] updateCounts;
    private boolean isPlanOpened;
    private int planIndex = 0;
    private List parameterValuesList;
    private List parameterReferences;
    
    public PreparedBatchUpdatePlan(ProcessorPlan plan, List parameterValuesList, List parameterReferences) {
    	this.parameterValuesList = parameterValuesList;
    	this.parameterReferences = parameterReferences;
    	updatePlans = new ProcessorPlan[parameterValuesList.size()];
    	updatePlans[0] = plan;
    	for(int i=1; i<updatePlans.length; i++){
    		updatePlans[i] = (ProcessorPlan)plan.clone();
    	}
    	updateCounts = new int[updatePlans.length];
    }

    public Object clone() {
        return new PreparedBatchUpdatePlan((ProcessorPlan)updatePlans[0].clone(), parameterValuesList, parameterReferences);
    }

    /** 
     * @see com.metamatrix.query.processor.ProcessorPlan#initialize(com.metamatrix.query.util.CommandContext, com.metamatrix.query.processor.ProcessorDataManager, com.metamatrix.common.buffer.BufferManager)
     * @since 5.5.2
     */
    public void initialize(CommandContext context, ProcessorDataManager dataMgr, BufferManager bufferMgr) {
    	for (int i = 0; i < updatePlans.length; i++) {
    		updatePlans[i].initialize(context, dataMgr, bufferMgr);
    	}
    }
    
    /** 
     * @see com.metamatrix.query.processor.ProcessorPlan#getOutputElements()
     * @since 5.5.2
     */
    public List getOutputElements() {
        return Command.getUpdatesCommandSymbol();
    }

    /** 
     * @see com.metamatrix.query.processor.ProcessorPlan#open()
     * @since 5.5.2
     */
    public void open() throws MetaMatrixComponentException, MetaMatrixProcessingException{
    	if (!isPlanOpened) { // Open the plan only once
        	PreparedStatementRequest.resolveParameterValues(parameterReferences, (List)parameterValuesList.get(planIndex), this.getContext());          	
            updatePlans[planIndex].open();
            isPlanOpened = true;
        }
    }

    /** 
     * @see com.metamatrix.query.processor.ProcessorPlan#nextBatch()
     * @since 5.5.2
     */
    public TupleBatch nextBatch() throws BlockedException, MetaMatrixComponentException, MetaMatrixProcessingException {
        for (;planIndex < updatePlans.length; planIndex++) {
        	open();
            // Execute nextBatch() on each plan in sequence
            List[] currentBatch = updatePlans[planIndex].nextBatch().getAllTuples(); // Can throw BlockedException
            //updateCounts[planIndex] = currentBatch[0].get(0);
            updateCounts[planIndex] = ((Integer)currentBatch[0].get(0)).intValue();
            
            // since we are done with the plan explicitly close it.
            updatePlans[planIndex].close();
            isPlanOpened = false;
        }
        // Add tuples to current batch
        List rows = new ArrayList();
        List row = new ArrayList();
        row.add(updateCounts);
        rows.add(row);
        TupleBatch batch = new TupleBatch(1, rows);
        batch.setTerminationFlag(true);
        return batch;
    }

    /** 
     * @see com.metamatrix.query.processor.ProcessorPlan#close()
     * @since 5.5.2
     */
    public void close() throws MetaMatrixComponentException {
    }

    
    /** 
     * @see com.metamatrix.query.processor.ProcessorPlan#reset()
     * @since 5.5.2
     */
    public void reset() {
        super.reset();
        for (int i = 0; i < updatePlans.length; i++) {
            updatePlans[i].reset();
        }
    	Arrays.fill(updateCounts, -1);
        planIndex = 0;
        isPlanOpened = false;
    }

    /** 
     * @see com.metamatrix.query.processor.Describable#getDescriptionProperties()
     * @since 5.5.2
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
        StringBuffer val = new StringBuffer("PreparedBatchUpdatePlan\n"); //$NON-NLS-1$
        val.append(updatePlans[0]);
        val.append("\nValues:");//$NON-NLS-1$
        val.append(parameterValuesList);
        val.append("\n"); //$NON-NLS-1$
        return val.toString();
    }

    /** 
     * @see com.metamatrix.query.processor.ProcessorPlan#getChildPlans()
     * @since 5.5.2
     */
    public Collection getChildPlans() {
        return Arrays.asList(updatePlans);
    }

}
