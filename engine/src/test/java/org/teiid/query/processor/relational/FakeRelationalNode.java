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

package org.teiid.query.processor.relational;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.mockito.Mockito;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;


/**
 */
public class FakeRelationalNode extends RelationalNode {

    // For raw data mode
    private List[] data;

    // For tuple source mode
    private TupleSource source;
    private int batchSize;

    // State
    private int currentRow;

    private boolean useBuffer;

    /**
     * Constructor for FakeRelationalNode.
     * @param nodeID
     */
    public FakeRelationalNode(int nodeID, List[] data) {
        super(nodeID);
        this.data = data;
        this.currentRow = 0;
    }

    @Override
    public void reset() {
        super.reset();
        this.currentRow = 0;
    }

    public FakeRelationalNode(int nodeID, List[] data, int batchSize) {
        super(nodeID);
        this.data = data;
        this.currentRow = 0;
        this.batchSize = batchSize;
    }

    public FakeRelationalNode(int nodeID, TupleSource source, int batchSize) {
        super(nodeID);
        this.source = source;
        this.batchSize = batchSize;
    }

    public TupleBatch nextBatchDirect() throws BlockedException, TeiidComponentException, TeiidProcessingException {
        if(data != null) {
            if(currentRow < data.length) {
                int endRow = Math.min(data.length, currentRow+getBatchSize());
                List batchRows = new ArrayList();
                for(int i=currentRow; i<endRow; i++) {
                    batchRows.add(data[i]);
                }

                TupleBatch batch = new TupleBatch(currentRow+1, batchRows);
                currentRow += batch.getRowCount();

                if(currentRow >= data.length) {
                    batch.setTerminationFlag(true);
                }
                return batch;

            }
            TupleBatch batch = new TupleBatch(currentRow+1, Collections.EMPTY_LIST);
            batch.setTerminationFlag(true);
            return batch;
        }
        boolean last = false;
        List rows = new ArrayList(batchSize);
        for(int i=0; i<batchSize; i++) {
            List tuple = source.nextTuple();
            if(tuple == null) {
                last = true;
                break;
            }
            rows.add(tuple);
        }

        TupleBatch batch = new TupleBatch(currentRow+1, rows);
        if(last) {
            batch.setTerminationFlag(true);
        } else {
            currentRow += rows.size();
        }

        return batch;
    }


    @Override
    public boolean hasBuffer() {
        return useBuffer;
    }

    @Override
    protected TupleBuffer getBufferDirect(int maxRows) throws BlockedException,
            TeiidComponentException, TeiidProcessingException {
        TupleBuffer tb = Mockito.mock(TupleBuffer.class);
        Mockito.stub(tb.getRowCount()).toReturn((long)(maxRows != -1 ? Math.min(maxRows, data.length) : data.length));
        return tb;
    }

    /**
     * @see org.teiid.query.processor.relational.RelationalNode#getBatchSize()
     * @since 4.2
     */
    protected int getBatchSize() {
        if(this.batchSize != 0) {
            return this.batchSize;
        }
        return super.getBatchSize();
    }

    public Object clone(){
        throw new UnsupportedOperationException();
    }

    public void setUseBuffer(boolean useBuffer) {
        this.useBuffer = useBuffer;
    }
}
