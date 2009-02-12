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

package com.metamatrix.query.processor;

import java.util.*;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.buffer.*;
import com.metamatrix.query.util.CommandContext;

/**
 */
public class FakeProcessorPlan extends BaseProcessorPlan {

    private List outputElements;
    private List batches;
    int batchIndex = 0;
    private int nextBatchRow = 1;

    /**
     * Constructor for FakeProcessorPlan.
     * @param batches List of things to return in response to nextBatch() - typically 
     * this is TupleBatch, but it can also be BlockedException or a 
     * MetaMatrixComponentException.  
     */
    public FakeProcessorPlan(List outputElements, List batches) {
        this.outputElements = outputElements;
        this.batches = batches;
    }

    /**
     * @see java.lang.Object#clone()
     */
    public Object clone() {
        throw new UnsupportedOperationException();
    }

    /**
     * @see com.metamatrix.query.processor.ProcessorPlan#initialize(com.metamatrix.query.processor.ProcessorDataManager, java.lang.Object, com.metamatrix.common.buffer.BufferManager, java.lang.String, int)
     */
    public void initialize(
        CommandContext context,
        ProcessorDataManager dataMgr,
        BufferManager bufferMgr) {
            
        // nothing
    }

    /**
     * @see com.metamatrix.query.processor.ProcessorPlan#connectTupleSource(com.metamatrix.common.buffer.TupleSource, com.metamatrix.common.buffer.TupleSourceID)
     */
    public void connectTupleSource(TupleSource source, int dataRequestID) {
        // nothing
    }

    /**
     * @see com.metamatrix.query.processor.ProcessorPlan#getOutputElements()
     */
    public List getOutputElements() {
        return this.outputElements;
    }

    /**
     * @see com.metamatrix.query.processor.ProcessorPlan#open()
     */
    public void open() throws MetaMatrixComponentException {
        // nothing
    }

    /**
     * @see com.metamatrix.query.processor.ProcessorPlan#nextBatch()
     */
    public TupleBatch nextBatch() throws BlockedException, MetaMatrixComponentException {
        if(this.batches == null || this.batches.size() == 0 || batchIndex >= this.batches.size()) {
            // Return empty terminator batch
            TupleBatch batch = new TupleBatch(nextBatchRow, Collections.EMPTY_LIST);  
            batch.setTerminationFlag(true);
            return batch;  
        }
        Object nextReturn = this.batches.get(batchIndex);
        batchIndex++;

        if(nextReturn instanceof TupleBatch) { 
            TupleBatch batch = (TupleBatch) nextReturn;
            nextBatchRow = nextBatchRow + batch.getRowCount();
            return batch;
        }
        throw (MetaMatrixComponentException) nextReturn;
    }

    /**
     * @see com.metamatrix.query.processor.ProcessorPlan#close()
     */
    public void close() throws MetaMatrixComponentException {
        // nothing
    }

    /**
     * @see com.metamatrix.query.processor.ProcessorPlan#getSchema()
     */
    public List getSchema() {
        return this.outputElements;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.processor.ProcessorPlan#getUpdateCount()
     */
    public int getUpdateCount() {
        // TODO Auto-generated method stub
        return 0;
    }

    public Map getDescriptionProperties() {
        return new HashMap();
    }

    /** 
     * @see com.metamatrix.query.processor.ProcessorPlan#getChildPlans()
     * @since 4.2
     */
    public Collection getChildPlans() {
        return Collections.EMPTY_LIST;
    }

}
