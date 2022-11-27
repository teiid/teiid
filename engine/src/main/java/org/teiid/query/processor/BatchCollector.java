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

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.BufferManager.TupleSourceType;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.util.Assertion;
import org.teiid.query.util.CommandContext;


public class BatchCollector {

    public interface BatchProducer {
        /**
         * Get a batch of results or possibly an Exception.
         * @return Batch of results
         * @throws BlockedException indicating next batch is not available yet
         * @throws TeiidComponentException for non-business rule exception
         * @throws TeiidProcessingException for business rule exception, related
         * to user input or modeling
         */
        TupleBatch nextBatch() throws BlockedException, TeiidComponentException, TeiidProcessingException;

        /**
         * Get list of resolved elements describing output columns for this plan.
         * @return List of SingleElementSymbol
         */
        List getOutputElements();

        /**
         * return the final tuple buffer or null if not available
         * @param maxRows
         * @return
         * @throws TeiidProcessingException
         * @throws TeiidComponentException
         * @throws BlockedException
         */
        TupleBuffer getBuffer(int maxRows) throws BlockedException, TeiidComponentException, TeiidProcessingException;

        boolean hasBuffer();

        void close() throws TeiidComponentException;
    }

    public static class BatchProducerTupleSource implements TupleSource {
        private final BatchProducer sourceNode;
        private TupleBatch sourceBatch;           // Current batch loaded from the source, if blocked
        private long sourceRow = 1;

        public BatchProducerTupleSource(BatchProducer sourceNode) {
            this.sourceNode = sourceNode;
        }

        public BatchProducerTupleSource(BatchProducer sourceNode, long startRow) {
            this.sourceNode = sourceNode;
            this.sourceRow = startRow;
        }

        @Override
        public List<Object> nextTuple() throws TeiidComponentException,
                TeiidProcessingException {
            while (true) {
                if(sourceBatch == null) {
                    // Read next batch
                    sourceBatch = sourceNode.nextBatch();
                }

                if(sourceBatch.getRowCount() > 0 && sourceRow <= sourceBatch.getEndRow()) {
                    // Evaluate expressions needed for grouping
                    List tuple = sourceBatch.getTuple(sourceRow);
                    tuple = updateTuple(tuple);
                    sourceRow++;
                    return tuple;
                }

                // Check for termination condition
                if(sourceBatch.getTerminationFlag()) {
                    sourceBatch = null;
                    return null;
                }
                sourceBatch = null;
            }
        }

        @SuppressWarnings("unused")
        protected List<?> updateTuple(List<?> tuple) throws ExpressionEvaluationException, BlockedException, TeiidComponentException {
            return tuple;
        }

        @Override
        public void closeSource() {

        }
    }

    private BatchProducer sourceNode;

    private boolean done = false;
    private TupleBuffer buffer;
    private boolean forwardOnly;
    private int rowLimit = -1; //-1 means no_limit
    private boolean hasFinalBuffer;

    private boolean saveLastRow;

    public BatchCollector(BatchProducer sourceNode, BufferManager bm, CommandContext context, boolean forwardOnly) throws TeiidComponentException {
        this.sourceNode = sourceNode;
        this.forwardOnly = forwardOnly;
        this.hasFinalBuffer = this.sourceNode.hasBuffer();
        if (!this.hasFinalBuffer) {
            this.buffer = bm.createTupleBuffer(sourceNode.getOutputElements(), context.getConnectionId(), TupleSourceType.PROCESSOR);
            this.buffer.setForwardOnly(forwardOnly);
        }
    }

    public TupleBuffer collectTuples() throws TeiidComponentException, TeiidProcessingException {
        return collectTuples(false);
    }

    public TupleBuffer collectTuples(boolean singleBatch) throws TeiidComponentException, TeiidProcessingException {
        TupleBatch batch = null;
        while(!done) {
            if (this.hasFinalBuffer) {
                if (this.buffer == null) {
                    TupleBuffer finalBuffer = this.sourceNode.getBuffer(rowLimit);
                    Assertion.isNotNull(finalBuffer);
                    this.buffer = finalBuffer;
                }
                if (this.buffer.isFinal()) {
                    this.buffer.setForwardOnly(forwardOnly);
                    done = true;
                    break;
                }
            }
            batch = sourceNode.nextBatch();

            if (rowLimit > 0 && rowLimit <= batch.getEndRow()) {
                if (!done) {
                    this.sourceNode.close();
                }
                List<?> lastTuple = null;
                if (saveLastRow) {
                    if (batch.getTerminationFlag()) {
                        lastTuple = batch.getTuples().get(batch.getTuples().size() - 1);
                    } else if (rowLimit < batch.getBeginRow()) {
                        continue; //skip until end
                    }
                }
                boolean modified = false;
                if (rowLimit < batch.getEndRow()) {
                    //we know row limit must be smaller than max int, so an int cast is safe here
                    int firstRow = (int)Math.min(rowLimit + 1, batch.getBeginRow());
                    List<List<?>> tuples = batch.getTuples().subList(0, rowLimit - firstRow + 1);
                    batch = new TupleBatch(firstRow, tuples);
                    modified = true;
                }
                if (lastTuple != null) {
                    if (!modified) {
                        batch = new TupleBatch(batch.getBeginRow(), batch.getTuples());
                    }
                    batch.getTuples().add(lastTuple);
                }
                batch.setTerminationFlag(true);
            }

            flushBatch(batch);

            // Check for termination condition
            if(batch.getTerminationFlag()) {
                done = true;
                if (!this.sourceNode.hasBuffer()) {
                    buffer.close();
                }
                break;
            }

            if (singleBatch) {
                return null;
            }
        }
        return buffer;
    }

    public TupleBuffer getTupleBuffer() {
        return buffer;
    }

    /**
     * Flush the batch by giving it to the buffer manager.
     */
    private void flushBatch(TupleBatch batch) throws TeiidComponentException, TeiidProcessingException {
        if (batch.getRowCount() == 0 && batch.getTermination() == TupleBatch.NOT_TERMINATED) {
            return;
        }
        flushBatchDirect(batch, true);
    }

    @SuppressWarnings("unused")
    protected void flushBatchDirect(TupleBatch batch, boolean add) throws TeiidComponentException, TeiidProcessingException {
        if (!this.hasFinalBuffer) {
            buffer.addTupleBatch(batch, add);
        }
    }

    public long getRowCount() {
        if (buffer == null) {
            return 0;
        }
        return buffer.getRowCount();
    }

    public void setRowLimit(int rowLimit) {
        this.rowLimit = rowLimit;
    }

    public void setSaveLastRow(boolean saveLastRow) {
        this.saveLastRow = saveLastRow;
    }

    public boolean isSaveLastRow() {
        return saveLastRow;
    }

}
