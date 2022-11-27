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

import java.util.List;

import org.teiid.common.buffer.AbstractTupleSource;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.processor.BatchCollector.BatchProducer;


/**
 * A BatchIterator provides an iterator interface to a {@link BatchProducer}.
 * By setting {@link #setBuffer(TupleBuffer, boolean)},
 * the iterator can copy on read into a {@link TupleBuffer} for repeated reading.
 *
 * Note that the saveOnMark buffering only lasts until the next mark is set.
 */
public class BatchIterator extends AbstractTupleSource {

    private final BatchProducer source;
    private boolean saveOnMark;
    private TupleBuffer buffer;
    private boolean done;
    private boolean mark;

    public BatchIterator(BatchProducer source) {
        this.source = source;
    }

    @Override
    protected TupleBatch getBatch(long row) throws TeiidComponentException, TeiidProcessingException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected List<?> finalRow() throws TeiidComponentException, TeiidProcessingException {
        if (this.buffer != null && this.getCurrentIndex() <= this.buffer.getRowCount()) {
            batch = this.buffer.getBatch(this.getCurrentIndex());
        }
        while (available() < 1) {
            if (done) {
                return null;
            }
            batch = source.nextBatch();
            done = batch.getTerminationFlag();
            if (buffer != null && (!saveOnMark || mark) && !buffer.isForwardOnly()) {
                buffer.addTupleBatch(batch, true);
            }
            if (done && buffer != null) {
                this.buffer.close();
            }
        }
        return getCurrentTuple();
    }

    @Override
    protected List<?> getCurrentTuple() throws TeiidComponentException,
            BlockedException, TeiidProcessingException {
        List<?> tuple = super.getCurrentTuple();
        saveTuple(tuple);
        return tuple;
    }

    private void saveTuple(List<?> tuple) throws TeiidComponentException {
        if (tuple != null && mark && saveOnMark && this.getCurrentIndex() > this.buffer.getRowCount()) {
            this.buffer.setRowCount(this.getCurrentIndex() - 1);
            this.buffer.addTuple(tuple);
        }
    }

    public long available() {
        if (batch != null && batch.containsRow(getCurrentIndex())) {
            return batch.getEndRow() - getCurrentIndex() + 1;
        }
        return 0;
    }

    public void setBuffer(TupleBuffer buffer, boolean saveOnMark) {
        this.buffer = buffer;
        this.saveOnMark = saveOnMark;
    }

    @Override
    public void closeSource() {
        if (this.buffer != null) {
            this.buffer.remove();
            this.buffer = null;
        }
    }

    @Override
    public void reset() {
        super.reset();
        if (this.buffer != null) {
            mark = false;
            return;
        }
    }

    @Override
    public void mark() throws TeiidComponentException {
        super.mark();
        if (this.buffer != null && saveOnMark && this.getCurrentIndex() > this.buffer.getRowCount()) {
            this.buffer.purge();
        }
        mark = true;
        saveTuple(this.currentTuple);
    }

    @Override
    public void setPosition(long position) {
        if (this.buffer == null && position < getCurrentIndex() && position < (this.batch != null ? batch.getBeginRow() : Long.MAX_VALUE)) {
            throw new UnsupportedOperationException("Backwards positioning is not allowed"); //$NON-NLS-1$
        }
        super.setPosition(position);
    }

    /**
     * non-destructive method to set the mark
     * @return true if the mark was set
     */
    public boolean ensureSave() {
        if (!saveOnMark || mark) {
            return false;
        }
        mark = true;
        return true;
    }

    public void disableSave() {
        if (buffer != null) {
            this.saveOnMark = true;
            this.mark = false;
            if (batch != null && batch.getEndRow() <= this.buffer.getRowCount()) {
                this.batch = null;
            }
        }
    }

    public void readAhead(long limit) throws TeiidComponentException, TeiidProcessingException {
        if (buffer == null || done) {
            return;
        }
        if (this.buffer.getManagedRowCount() >= limit) {
            return;
        }
        if (this.batch != null && this.buffer.getRowCount() < this.batch.getEndRow() && !this.buffer.isForwardOnly()) {
            //haven't saved already
            this.buffer.addTupleBatch(this.batch, true);
        }
        TupleBatch tb = source.nextBatch();
        done = tb.getTerminationFlag();
        this.buffer.addTupleBatch(tb, true);
        if (done) {
            this.buffer.close();
        }
    }

    public TupleBuffer getBuffer() {
        return buffer;
    }

}
