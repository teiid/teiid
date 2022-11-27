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

import java.util.Collections;
import java.util.List;

import org.teiid.common.buffer.BufferManager.TupleSourceType;
import org.teiid.common.buffer.IndexedTupleSource;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.util.Assertion;
import org.teiid.query.processor.BatchCollector;
import org.teiid.query.processor.BatchIterator;
import org.teiid.query.processor.relational.MergeJoinStrategy.SortOption;
import org.teiid.query.processor.relational.SortUtility.Mode;
import org.teiid.query.sql.lang.OrderBy;


class SourceState {

    enum ImplicitBuffer {
        NONE, FULL, ON_MARK
    }

    private RelationalNode source;
    private List expressions;
    private BatchCollector collector;
    private TupleBuffer buffer;
    private List<TupleBuffer> buffers;
    private List<Object> outerVals;
    private IndexedTupleSource iterator;
    private int[] expressionIndexes;
    private List currentTuple;
    private long maxProbeMatch = 1;
    private boolean distinct;
    private ImplicitBuffer implicitBuffer = ImplicitBuffer.FULL;
    boolean open;

    private SortUtility sortUtility;
    private boolean limited;

    public SourceState(RelationalNode source, List expressions) {
        this.source = source;
        this.expressions = expressions;
        List elements = source.getElements();
        this.outerVals = Collections.nCopies(elements.size(), null);
        this.expressionIndexes = getExpressionIndecies(expressions, elements);
    }

    public RelationalNode getSource() {
        return source;
    }

    public void setImplicitBuffer(ImplicitBuffer implicitBuffer) {
        this.implicitBuffer = implicitBuffer;
    }

    public ImplicitBuffer getImplicitBuffer() {
        return implicitBuffer;
    }

    static int[] getExpressionIndecies(List expressions,
                                        List elements) {
        if (expressions == null) {
            return new int[0];
        }
        int[] indecies = new int[expressions.size()];
        for (int i = 0; i < expressions.size(); i++) {
            indecies[i] = elements.indexOf(expressions.get(i));
            assert indecies[i] != -1;
        }
        return indecies;
    }

    TupleBuffer createSourceTupleBuffer() throws TeiidComponentException {
        return this.source.getBufferManager().createTupleBuffer(source.getElements(), source.getConnectionID(), TupleSourceType.PROCESSOR);
    }

    public List saveNext() throws TeiidComponentException, TeiidProcessingException {
        this.currentTuple = this.getIterator().nextTuple();
        return currentTuple;
    }

    public void reset() throws TeiidComponentException, TeiidProcessingException {
        this.getIterator().reset();
        this.getIterator().mark();
        this.currentTuple = null;
    }

    public void close() {
        closeBuffer();
        if (buffers != null) {
            for (TupleBuffer tb : buffers) {
                tb.remove();
            }
        }
        this.buffers = null;
        this.open = false;
        if (this.sortUtility != null) {
            this.sortUtility.remove();
            this.sortUtility = null;
        }
    }

    private void closeBuffer() {
        if (this.buffer != null) {
            this.buffer.remove();
            this.buffer = null;
        }
        if (this.iterator != null) {
            this.iterator.closeSource();
            this.iterator = null;
        }
        this.currentTuple = null;
    }

    public long getRowCount() throws TeiidComponentException, TeiidProcessingException {
        return this.getTupleBuffer().getRowCount();
    }

    /**
     * Uses the prefetch logic to determine an incremental row count
     */
    public boolean rowCountLE(long count) throws TeiidComponentException, TeiidProcessingException {
        if (buffer != null) {
            return buffer.getRowCount() <= count;
        }
        if (iterator != null || this.sortUtility != null) {
            throw new IllegalStateException();
        }
        while (buffer == null) {
            if (getIncrementalRowCount(true) > count) {
                return false;
            }
            prefetch(count + 1);
        }
        return buffer.getRowCount() <= count;
    }

    IndexedTupleSource getIterator() throws TeiidComponentException, TeiidProcessingException {
        if (this.iterator == null) {
            if (this.buffer != null) {
                iterator = buffer.createIndexedTupleSource();
            } else {
                // return a TupleBatch tuplesource iterator
                BatchIterator bi = new BatchIterator(this.source);
                if (this.collector != null) {
                    bi.setBuffer(this.collector.getTupleBuffer(), implicitBuffer == ImplicitBuffer.ON_MARK);
                    if (implicitBuffer == ImplicitBuffer.NONE) {
                        bi.getBuffer().setForwardOnly(true);
                    }
                    this.collector = null;
                } else if (implicitBuffer != ImplicitBuffer.NONE) {
                    bi.setBuffer(createSourceTupleBuffer(), implicitBuffer == ImplicitBuffer.ON_MARK);
                }
                this.iterator = bi;
            }
        }
        return this.iterator;
    }

    /**
     * Pro-actively pull batches for later use.
     * There are unfortunately quite a few cases to cover here.
     */
    protected void prefetch(long limit) throws TeiidComponentException, TeiidProcessingException {
        if (!open) {
            return;
        }
        if (this.buffer == null) {
            if (this.sortUtility != null) {
                return;
            }
            if (this.iterator != null) {
                ((BatchIterator)this.iterator).readAhead(limit);
                return;
            }
            if (source.hasBuffer()) {
                this.buffer = source.getBuffer(-1);
                return;
            }
            if (collector == null) {
                collector = new BatchCollector(source, source.getBufferManager(), source.getContext(), false);
            }
            if (collector.getTupleBuffer() != null && collector.getTupleBuffer().getManagedRowCount() >= limit) {
                return;
            }
            this.buffer = collector.collectTuples(true);
        }
    }

    public List<Object> getOuterVals() {
        return this.outerVals;
    }

    public List getCurrentTuple() {
        return this.currentTuple;
    }

    public int[] getExpressionIndexes() {
        return this.expressionIndexes;
    }

    void setMaxProbeMatch(long maxProbeMatch) {
        this.maxProbeMatch = maxProbeMatch;
    }

    long getMaxProbeMatch() {
        return maxProbeMatch;
    }

    public TupleBuffer getTupleBuffer() throws TeiidComponentException, TeiidProcessingException {
        if (this.buffer == null) {
            if (this.iterator instanceof BatchIterator) {
                throw new AssertionError("cannot buffer the source"); //$NON-NLS-1$
            }
            if (source.hasBuffer()) {
                this.buffer = source.getBuffer(-1);
                Assertion.assertTrue(this.buffer.isFinal());
                return this.buffer;
            }
            if (collector == null) {
                collector = new BatchCollector(source, source.getBufferManager(), source.getContext(), false);
            }
            this.buffer = collector.collectTuples();
        }
        return this.buffer;
    }

    /**
     * @return true if the join expressions are a distinct set
     */
    public boolean isExpresssionDistinct() {
        return this.distinct;
    }

    public void markExpressionsDistinct(boolean distinct) {
        this.distinct |= distinct;
    }

    public void sort(SortOption sortOption) throws TeiidComponentException, TeiidProcessingException {
        if (sortOption == SortOption.ALREADY_SORTED) {
            return;
        }
        if (this.sortUtility == null) {
            TupleSource ts = null;
            if (source.hasBuffer()) {
                this.buffer = getTupleBuffer();
            } else if (this.buffer == null && this.collector != null) {
                if (sortOption == SortOption.NOT_SORTED) {
                    //pass the buffer and the source
                    this.buffer = this.collector.getTupleBuffer();
                    ts = new BatchCollector.BatchProducerTupleSource(this.source, this.buffer.getRowCount() + 1);
                } else {
                    //fully read
                    this.buffer = this.collector.collectTuples();
                }
            }
            if (this.buffer != null) {
                this.buffer.setForwardOnly(true);
            } else {
                ts = new BatchIterator(this.source);
            }
            this.sortUtility = new SortUtility(ts, expressions, Collections.nCopies(expressions.size(), OrderBy.ASC),
                    sortOption == SortOption.SORT_DISTINCT?Mode.DUP_REMOVE_SORT:Mode.SORT, this.source.getBufferManager(), this.source.getConnectionID(), source.getElements());
            this.markExpressionsDistinct(sortOption == SortOption.SORT_DISTINCT && expressions.size() == this.getOuterVals().size());
            if (this.buffer != null) {
                this.sortUtility.setWorkingBuffer(this.buffer);
            }
        }
        TupleBuffer sorted = null;
        if (sortOption == SortOption.NOT_SORTED) {
            if (this.buffers != null || sortUtility.isDoneReading()) {
                return;
            }
            this.buffers = sortUtility.onePassSort(limited);
            if (this.buffers.size() != 1 || !sortUtility.isDoneReading()) {
                nextBuffer();
                return;
            }
            sorted = this.buffers.get(0);
            this.buffers = null;
        } else {
            sorted = sortUtility.sort();
        }
        //only remove the buffer if this is the first time through
        if (this.buffer != null && this.buffer != sorted) {
            this.buffer.remove();
        }
        this.buffer = sorted;
        this.markExpressionsDistinct(sortUtility.isDistinct());
    }

    public boolean hasBuffer() {
        return this.buffer != null || this.source.hasBuffer();
    }

    public boolean nextBuffer() throws TeiidComponentException, TeiidProcessingException {
        this.closeBuffer();
        if (this.buffers == null || this.buffers.isEmpty()) {
            if (!sortUtility.isDoneReading()) {
                this.buffers = sortUtility.onePassSort(limited);
                return nextBuffer();
            }
            return false;
        }
        this.buffer = this.buffers.remove(this.buffers.size() - 1);
        this.buffer.setForwardOnly(false);
        this.resetState();
        return true;
    }

    /**
     * return the iterator to a fresh state
     */
    public void resetState() {
        if (this.iterator != null) {
            this.iterator.reset();
            this.iterator.setPosition(1);
        }
        this.currentTuple = null;
        this.maxProbeMatch = 1;
    }

    public void setMaxProbePosition() throws TeiidComponentException, TeiidProcessingException {
        this.getIterator().setPosition(this.getMaxProbeMatch());
        this.currentTuple = null;
    }

    public long getIncrementalRowCount(boolean low) {
        if (this.buffer != null) {
            return this.buffer.getRowCount();
        }
        if (this.collector != null) {
            return this.collector.getTupleBuffer().getRowCount();
        }
        if (sortUtility == null) {
            if (this.iterator instanceof BatchIterator) {
                TupleBuffer tb = ((BatchIterator)this.iterator).getBuffer();
                if (tb != null) {
                    return tb.getRowCount();
                }
                //TODO: should estimate the rows
            }
            //TODO: should estimate the rows based upon what is being fed into the sort
        }
        return low?0:Long.MAX_VALUE;
    }

    public SortUtility getSortUtility() {
        return sortUtility;
    }

    public void isLimited(boolean hasLimit) {
        this.limited = hasLimit;
    }

}
