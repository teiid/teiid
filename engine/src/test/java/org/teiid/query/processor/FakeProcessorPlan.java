/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.query.processor;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.sql.lang.Command;


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
     * @see org.teiid.query.processor.ProcessorPlan#getOutputElements()
     */
    public List getOutputElements() {
        return this.outputElements;
    }

    /**
     * @see org.teiid.query.processor.ProcessorPlan#open()
     */
    public void open() throws TeiidComponentException {
        assertFalse("ProcessorPlan.open() should not be called more than once", opened); //$NON-NLS-1$
        opened = true;
    }

    /**
     * @see org.teiid.query.processor.ProcessorPlan#nextBatch()
     */
    public TupleBatch nextBatch() throws BlockedException, TeiidComponentException {
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
        throw (TeiidComponentException) nextReturn;
    }

    /**
     * @see org.teiid.query.processor.ProcessorPlan#close()
     */
    public void close() throws TeiidComponentException {
        // nothing
    }

}
