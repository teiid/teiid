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

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.util.CommandContext;

/**
 */
public class FakeProcessorPlan extends ProcessorPlan {

    private List outputElements;
    private List batches;
    int batchIndex = 0;
    private int nextBatchRow = 1;
    private boolean opened = false;
    
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
    
    public FakeProcessorPlan(int counts) {
    	List[] rows = new List[counts];
    	for (int i = 0; i < counts; i++) {
            rows[i] = Arrays.asList(new Object[] {new Integer(1)});
        }
        TupleBatch batch = new TupleBatch(1, rows);
        batch.setTerminationFlag(true);
        this.batches = Arrays.asList(batch);
        this.outputElements = Command.getUpdateCommandSymbol();
    }
    
    public boolean isOpened() {
		return opened;
	}

    /**
     * @see java.lang.Object#clone()
     */
    public FakeProcessorPlan clone() {
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
     * @see com.metamatrix.query.processor.ProcessorPlan#getOutputElements()
     */
    public List getOutputElements() {
        return this.outputElements;
    }

    /**
     * @see com.metamatrix.query.processor.ProcessorPlan#open()
     */
    public void open() throws MetaMatrixComponentException {
    	assertFalse("ProcessorPlan.open() should not be called more than once", opened); //$NON-NLS-1$
        opened = true;
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

    public Map getDescriptionProperties() {
        return new HashMap();
    }

}
