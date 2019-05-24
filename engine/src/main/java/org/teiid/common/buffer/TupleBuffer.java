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

package org.teiid.common.buffer;

import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.TreeMap;

import org.teiid.client.ResizingArrayList;
import org.teiid.common.buffer.LobManager.ReferenceMode;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.Streamable;
import org.teiid.core.util.Assertion;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.query.QueryPlugin;
import org.teiid.query.sql.symbol.Expression;


public class TupleBuffer {

    public class TupleBufferTupleSource extends
            AbstractTupleSource {
        private final boolean singleUse;
        private boolean noBlocking;
        private boolean reverse;

        private TupleBufferTupleSource(boolean singleUse) {
            this.singleUse = singleUse;
        }

        @Override
        protected List<?> finalRow() throws TeiidComponentException, TeiidProcessingException {
            if(isFinal || noBlocking || reverse) {
                return null;
            }
            throw BlockedException.blockWithTrace("Blocking on non-final TupleBuffer", tupleSourceID, "size", getRowCount()); //$NON-NLS-1$ //$NON-NLS-2$
        }

        @Override
        protected long available() {
            if (!reverse) {
                return rowCount - getCurrentIndex() + 1;
            }
            return getCurrentIndex();
        }

        @Override
        protected TupleBatch getBatch(long row) throws TeiidComponentException {
            return TupleBuffer.this.getBatch(row);
        }

        @Override
        public void closeSource() {
            super.closeSource();
            if (singleUse) {
                remove();
            }
        }

        public void setNoBlocking(boolean noBlocking) {
            this.noBlocking = noBlocking;
        }

        public void setReverse(boolean reverse) {
            this.reverse = reverse;
        }

        @Override
        public long getCurrentIndex() {
            if (!reverse) {
                return super.getCurrentIndex();
            }
            return getRowCount() - super.getCurrentIndex() + 1;
        }

    }

    /**
     * Gets the data type names for each of the input expressions, in order.
     * @param expressions List of Expressions
     * @return
     * @since 4.2
     */
    public static String[] getTypeNames(List<? extends Expression> expressions) {
        if (expressions == null) {
            return null;
        }
        String[] types = new String[expressions.size()];
        for (ListIterator<? extends Expression> i = expressions.listIterator(); i.hasNext();) {
            Expression expr = i.next();
            types[i.previousIndex()] = DataTypeManager.getDataTypeName(expr.getType());
        }
        return types;
    }

    //construction state
    private BatchManager manager;
    private String tupleSourceID;
    private List<? extends Expression> schema;
    private int batchSize;

    private long rowCount;
    private boolean isFinal;
    private TreeMap<Long, Long> batches = new TreeMap<Long, Long>();
    private List<List<?>> batchBuffer;
    private boolean removed;
    private boolean forwardOnly;

    private LobManager lobManager;
    private String uuid;

    public TupleBuffer(BatchManager manager, String id, List<? extends Expression> schema, LobManager lobManager, int batchSize) {
        this.manager = manager;
        this.tupleSourceID = id;
        this.schema = schema;
        this.lobManager = lobManager;
        this.batchSize = batchSize;
    }

    public void setInlineLobs(boolean inline) {
        if (this.lobManager != null) {
            this.lobManager.setInlineLobs(inline);
        }
    }

    public void removeLobTracking() {
        if (this.lobManager != null) {
            this.lobManager.remove();
            this.lobManager = null;
        }
    }

    public String getId() {
        if (this.uuid == null) {
            this.uuid = java.util.UUID.randomUUID().toString();
        }
        return this.uuid;
    }

    public void setId(String uuid) {
        this.uuid = uuid;
    }

    public boolean isLobs() {
        return lobManager != null;
    }

    public void addTuple(List<?> tuple) throws TeiidComponentException {
        if (isLobs()) {
            lobManager.updateReferences(tuple, ReferenceMode.CREATE);
        }
        this.rowCount++;
        if (batchBuffer == null) {
            batchBuffer = new ResizingArrayList<List<?>>(batchSize/4);
        }
        batchBuffer.add(tuple);
        if (batchBuffer.size() == batchSize) {
            saveBatch(false);
        }
    }

    /**
     * Adds the given batch preserving row offsets.
     * @param batch
     * @throws TeiidComponentException
     */
    public void addTupleBatch(TupleBatch batch, boolean save) throws TeiidComponentException {
        setRowCount(batch.getBeginRow() - 1);
        List<List<?>> tuples = batch.getTuples();
        if (save) {
            for (int i = 0; i < batch.getRowCount(); i++) {
                addTuple(tuples.get(i));
            }
        } else {
            //add the lob references only, since they may still be referenced later
            if (isLobs()) {
                for (int i = 0; i < batch.getRowCount(); i++) {
                    lobManager.updateReferences(tuples.get(i), ReferenceMode.CREATE);
                }
            }
        }
    }

    public void setRowCount(long rowCount)
            throws TeiidComponentException {
        assert this.rowCount <= rowCount;
        if (this.rowCount != rowCount) {
            saveBatch(true);
            this.rowCount = rowCount;
        }
    }

    public void purge() {
        if (this.batchBuffer != null) {
            this.batchBuffer.clear();
        }
        for (Long batch : this.batches.values()) {
            this.manager.remove(batch);
        }
        if (this.lobManager != null) {
            this.lobManager.remove();
        }
        this.batches.clear();
    }

    public void persistLobs() throws TeiidComponentException {
        if (this.lobManager != null) {
            this.lobManager.persist();
        }
    }

    /**
     * Force the persistence of any rows held in memory.
     * @throws TeiidComponentException
     */
    public void saveBatch() throws TeiidComponentException {
        this.saveBatch(false);
    }

    void saveBatch(boolean force) throws TeiidComponentException {
        Assertion.assertTrue(!this.isRemoved());
        if (batchBuffer == null || batchBuffer.isEmpty() || (!force && batchBuffer.size() < Math.max(1, batchSize / 32))) {
            return;
        }
        Long mbatch = manager.createManagedBatch(batchBuffer, null, false);
        this.batches.put(rowCount - batchBuffer.size() + 1, mbatch);
        batchBuffer = null;
    }

    public void close() throws TeiidComponentException {
        saveBatch(false);
        this.isFinal = true;
    }

    /**
     * Get the batch containing the given row.
     * NOTE: the returned batch may be empty or may begin with a row other
     * than the one specified.
     * @param row
     * @return
     * @throws TeiidComponentException
     *
     * TODO: a method to get the raw batch
     */
    public TupleBatch getBatch(long row) throws TeiidComponentException {
        assert !removed;
        TupleBatch result = null;
        if (row > rowCount) {
            result = new TupleBatch(rowCount + 1, new List[] {});
        } else if (this.batchBuffer != null && row > rowCount - this.batchBuffer.size()) {
            result = new TupleBatch(rowCount - this.batchBuffer.size() + 1, batchBuffer);
            if (forwardOnly) {
                this.batchBuffer = null;
            }
        } else {
            if (this.batchBuffer != null && !this.batchBuffer.isEmpty()) {
                //this is just a sanity check to ensure we're not holding too many
                //hard references to batches.
                saveBatch(false);
            }
            Map.Entry<Long, Long> entry = batches.floorEntry(row);
            Assertion.isNotNull(entry);
            Long batch = entry.getValue();
            List<List<?>> rows = manager.getBatch(batch, !forwardOnly);
            result = new TupleBatch(entry.getKey(), rows);
            if (isFinal && result.getEndRow() == rowCount) {
                result.setTerminationFlag(true);
            }
            if (forwardOnly) {
                batches.remove(entry.getKey());
            }
        }
        if (isFinal && result.getEndRow() == rowCount) {
            result.setTerminationFlag(true);
        }
        return result;
    }

    public void remove() {
        if (!removed) {
            if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.DETAIL)) {
                LogManager.logDetail(LogConstants.CTX_BUFFER_MGR, "Removing TupleBuffer:", this.tupleSourceID); //$NON-NLS-1$
            }
            this.batchBuffer = null;
            purge();
            this.manager.remove();
            removed = true;
        }
    }

    /**
     * Returns the total number of rows contained in managed batches
     * @return
     */
    public long getManagedRowCount() {
        if (!this.batches.isEmpty()) {
            long start = this.batches.firstKey();
            return rowCount - start + 1;
        } else if (this.batchBuffer != null) {
            return this.batchBuffer.size();
        }
        return 0;
    }

    /**
     * Returns the last row number
     * @return
     */
    public long getRowCount() {
        return rowCount;
    }

    public boolean isFinal() {
        return isFinal;
    }

    public void setFinal(boolean isFinal) {
        this.isFinal = isFinal;
    }

    public List<? extends Expression> getSchema() {
        return schema;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public Streamable<?> getLobReference(String id) throws TeiidComponentException {
        if (lobManager == null) {
             throw new TeiidComponentException(QueryPlugin.Event.TEIID30032, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30032));
        }
        return lobManager.getLobReference(id);
    }

    public void setForwardOnly(boolean forwardOnly) {
        this.forwardOnly = forwardOnly;
    }

    public TupleBufferTupleSource createIndexedTupleSource() {
        return createIndexedTupleSource(false);
    }

    /**
     * Create a new iterator for this buffer
     * @return
     */
    public TupleBufferTupleSource createIndexedTupleSource(final boolean singleUse) {
        if (singleUse) {
            setForwardOnly(true);
        }
        return new TupleBufferTupleSource(singleUse);
    }

    @Override
    public String toString() {
        return this.tupleSourceID;
    }

    public boolean isRemoved() {
        return removed;
    }

    public boolean isForwardOnly() {
        return forwardOnly;
    }

    public void setPrefersMemory(boolean prefersMemory) {
        this.manager.setPrefersMemory(prefersMemory);
    }

    public String[] getTypes() {
        return manager.getTypes();
    }

    public int getLobCount() {
        if (this.lobManager == null) {
            return 0;
        }
        return this.lobManager.getLobCount();
    }

    public void truncateTo(int rowLimit) throws TeiidComponentException {
        if (rowCount <= rowLimit) {
            return;
        }
        //TODO this could be more efficient with handling the last batch
        if (this.batchBuffer != null) {
            for (int i = batchBuffer.size() - 1; i >= 0; i--) {
                if (this.rowCount == rowLimit) {
                    break;
                }
                this.rowCount--;
                List<?> tuple = this.batchBuffer.remove(i);
                if (this.lobManager != null) {
                    this.lobManager.updateReferences(tuple, ReferenceMode.REMOVE);
                }
            }
        }
        TupleBatch last = null;
        while (rowCount > rowLimit) {
            last = this.getBatch(rowCount);
            Long id = this.batches.remove(last.getBeginRow());
            if (id != null) {
                this.manager.remove(id);
            }
            if (this.lobManager != null) {
                for (List<?> tuple : last.getTuples()) {
                    this.lobManager.updateReferences(tuple, ReferenceMode.REMOVE);
                }
            }
            rowCount = last.getBeginRow() - 1;
        }
        if (rowCount < rowLimit) {
            List<List<?>> tuples = last.getTuples();
            int i = 0;
            while (rowCount < rowLimit) {
                addTuple(tuples.get(i++));

            }
        }
        saveBatch(false);
    }

    /**
     * Return a more accurate batch estimate or 0 if a new estimate is not available
     */
    public int getRowSizeEstimate() {
        return this.manager.getRowSizeEstimate();
    }

}
