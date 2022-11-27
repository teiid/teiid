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

package org.teiid.query.tempdata;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.IntBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.BufferManager.BufferReserveMode;
import org.teiid.common.buffer.BufferManager.TupleSourceType;
import org.teiid.common.buffer.STree;
import org.teiid.common.buffer.STree.InsertMode;
import org.teiid.common.buffer.TupleBrowser;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.TupleBuffer.TupleBufferTupleSource;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.query.QueryPlugin;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.processor.CollectionTupleSource;
import org.teiid.query.processor.relational.RelationalNode;
import org.teiid.query.processor.relational.SortUtility;
import org.teiid.query.processor.relational.SortUtility.Mode;
import org.teiid.query.sql.lang.CacheHint;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.SetClauseList;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.Array;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.ExpressionSymbol;
import org.teiid.query.util.CommandContext;
import org.teiid.query.util.GeneratedKeysImpl;

/**
 * A Teiid Temp Table
 * TODO: in this implementation blocked exceptions will not happen
 *       allowing for subquery evaluation though would cause pauses
 */
public class TempTable implements Cloneable, SearchableTable {

    private final class InsertUpdateProcessor extends UpdateProcessor {

        private boolean addRowId;
        private int[] indexes;
        private GeneratedKeysImpl keys;
        private boolean upsert;
        private TupleBuffer upsertUndoLog;

        private InsertUpdateProcessor(TupleSource ts, boolean addRowId, int[] indexes, boolean canUndo, boolean upsert)
                throws TeiidComponentException {
            super(null, ts, canUndo);
            if (upsert && addRowId) {
                throw new AssertionError("invalid state"); //$NON-NLS-1$
            }
            this.addRowId = addRowId;
            this.indexes = indexes;
            this.upsert = upsert;
            if (canUndo && upsert) {
                this.upsertUndoLog = bm.createTupleBuffer(columns, sessionID, TupleSourceType.PROCESSOR);
            }
        }

        @Override
        long process() throws ExpressionEvaluationException,
                TeiidComponentException, TeiidProcessingException {
            tree.setBatchInsert(addRowId);
            return super.process();
        }

        @Override
        protected void afterCompletion(boolean success) throws TeiidComponentException {
            if (!success && upsertUndoLog != null) {
                upsertUndoLog.setFinal(true);
                TupleBufferTupleSource undoTs = upsertUndoLog.createIndexedTupleSource();
                undoTs.setReverse(true);
                List<?> tuple = null;
                try {
                    while ((tuple = undoTs.nextTuple()) != null) {
                        try {
                            updateTuple(tuple);
                        } catch (TeiidException e) {
                            LogManager.logError(LogConstants.CTX_DQP, e, e.getMessage());
                        }
                    }
                } catch (TeiidProcessingException e) {
                    //shouldn't happen
                    throw new TeiidComponentException(e);
                }
            }
            tree.setBatchInsert(false);
        }

        @Override
        protected void tuplePassed(List tuple) throws BlockedException,
                TeiidComponentException, TeiidProcessingException {
            List<Object> generatedKey = null;
            if (indexes != null) {
                List<Object> newTuple = new ArrayList<Object>(columns.size());
                if (keys != null) {
                    generatedKey = new ArrayList<Object>(keys.getColumnNames().length);
                }
                if (addRowId) {
                    newTuple.add(rowId.getAndIncrement());
                }
                for (int i = 0; i < indexes.length; i++) {
                    if (indexes[i] == -1) {
                        AtomicInteger sequence = sequences.get(i + (addRowId?1:0));
                        if (sequence != null) {
                            int val = sequence.getAndIncrement();
                            if (generatedKey != null && i < tree.getKeyLength()) {
                                generatedKey.add(val);
                            }
                            newTuple.add(val);
                        } else {
                            newTuple.add(null);
                        }
                    } else {
                        newTuple.add(tuple.get(indexes[i]));
                    }
                }
                tuple = newTuple;
            } else if (addRowId) {
                tuple = new ArrayList<Object>(tuple);
                tuple.add(0, rowId.getAndIncrement());
            }
            currentTuple = tuple;

            validateNotNull(tuple);
            if (upsert) {
                //TODO: we're potentially wasting a sequence value here
                List<?> existing = tree.insert(tuple, indexes == null?InsertMode.UPDATE:InsertMode.NEW, -1);
                if (existing != null && indexes != null) {
                    for (int i = 0; i < indexes.length; i++) {
                        if (indexes[i] == -1) {
                            AtomicInteger sequence = sequences.get(i + (addRowId?1:0));
                            if (sequence == null) {
                                tuple.set(i, existing.get(i));
                            }
                        }
                    }
                    tree.insert(tuple, InsertMode.UPDATE, -1);
                }
                upsertUndoLog.addTuple(tuple);
                //don't add to main undo log
                currentTuple = null;
                return;
            }
            insertTuple(tuple, addRowId, true);
            if (generatedKey != null) {
                this.keys.addKey(generatedKey);
            }
        }

        @Override
        protected void undo(List<?> tuple) throws TeiidComponentException,
                TeiidProcessingException {
            deleteTuple(tuple);
        }

        public void setGeneratedKeys(GeneratedKeysImpl keys) {
            this.keys = keys;
        }

        @Override
        public void close() {
            super.close();
            if (this.upsertUndoLog != null) {
                this.upsertUndoLog.remove();
            }
        }
    }

    private final class QueryTupleSource implements TupleSource {
        private final Evaluator eval;
        private final Criteria condition;
        private final boolean project;
        private final int[] indexes;
        private int reserved;
        private TupleBrowser browser;

        private QueryTupleSource(TupleBrowser browser, Map map,
                List<? extends Expression> projectedCols, Criteria condition) {
            this.browser = browser;
            this.indexes = RelationalNode.getProjectionIndexes(map, projectedCols);
            this.eval = new Evaluator(map, null, null);
            this.condition = condition;
            this.project = shouldProject();
            this.reserved = reserveBuffers();
            if (updatable) {
                lock.readLock().lock();
            }
        }

        @Override
        public List<?> nextTuple() throws TeiidComponentException,
                TeiidProcessingException {
            for (;;) {
                List<?> next = browser.nextTuple();
                if (next == null) {
                    bm.releaseBuffers(reserved);
                    reserved = 0;
                    return null;
                }
                if (condition != null && !eval.evaluate(condition, next)) {
                    continue;
                }
                if (project) {
                    next = RelationalNode.projectTuple(indexes, next);
                }
                return next;
            }
        }

        @Override
        public void closeSource() {
            if (updatable) {
                lock.readLock().unlock();
            }
            bm.releaseBuffers(reserved);
            reserved = 0;
            browser.closeSource();
        }

        private boolean shouldProject() {
            if (indexes.length == getColumns().size()) {
                for (int i = 0; i < indexes.length; i++) {
                    if (indexes[i] != i) {
                        return true;
                    }
                }
                return false;
            }
            return true;
        }

    }

    private abstract class UpdateProcessor {
        private TupleSource ts;
        protected Evaluator eval;
        private Criteria crit;
        protected long updateCount = 0;
        protected List currentTuple;

        protected TupleBuffer undoLog;

        UpdateProcessor(Criteria crit, TupleSource ts, boolean canUndo) throws TeiidComponentException {
            this.ts = ts;
            this.eval = new Evaluator(columnMap, null, null);
            this.crit = crit;
            if (canUndo) {
                this.undoLog = bm.createTupleBuffer(columns, sessionID, TupleSourceType.PROCESSOR);
            }
        }

        long process() throws ExpressionEvaluationException, TeiidComponentException, TeiidProcessingException {
            int reserved = reserveBuffers();
            lock.writeLock().lock();
            boolean success = false;
            try {
                while (currentTuple != null || (currentTuple = ts.nextTuple()) != null) {
                    if (crit == null || eval.evaluate(crit, currentTuple)) {
                        tuplePassed(currentTuple);
                        updateCount++;
                        if (undoLog != null && currentTuple != null) {
                            undoLog.addTuple(currentTuple);
                        }
                    }
                    currentTuple = null;
                }
                bm.releaseBuffers(reserved);
                reserved = 0;
                success();
                success = true;
            } finally {
                try {
                    afterCompletion(success);
                    if (!success && undoLog != null) {
                        undoLog.setFinal(true);
                        TupleBufferTupleSource undoTs = undoLog.createIndexedTupleSource();
                        undoTs.setReverse(true);
                        List<?> tuple = null;
                        while ((tuple = undoTs.nextTuple()) != null) {
                            try {
                                undo(tuple);
                            } catch (TeiidException e) {
                                LogManager.logError(LogConstants.CTX_DQP, e, e.getMessage());
                                break;
                            }
                        }
                    }
                } finally {
                    bm.releaseBuffers(reserved);
                    lock.writeLock().unlock();
                    close();
                }
            }
            return updateCount;
        }

        /**
         *
         * @param success
         * @throws TeiidComponentException
         */
        protected void afterCompletion(boolean success) throws TeiidComponentException {

        }

        @SuppressWarnings("unused")
        void success() throws TeiidComponentException, ExpressionEvaluationException, TeiidProcessingException {}

        protected abstract void tuplePassed(List tuple) throws BlockedException, TeiidComponentException, TeiidProcessingException;

        protected abstract void undo(List<?> tuple) throws TeiidComponentException, TeiidProcessingException;

        public void close() {
            ts.closeSource();
            ts = null;
            if (undoLog != null) {
                undoLog.remove();
            }
        }

    }
    private static AtomicLong ID_GENERATOR = new AtomicLong();

    private Long id = ID_GENERATOR.getAndIncrement();
    private STree tree;
    private AtomicLong rowId;
    private List<ElementSymbol> columns;
    private BufferManager bm;
    private String sessionID;
    private TempMetadataID tid;
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private boolean updatable = true;
    private LinkedHashMap<List<ElementSymbol>, TempTable> indexTables;

    private int keyBatchSize;
    private int leafBatchSize;
    private Map<Expression, Integer> columnMap;

    private int[] notNull;
    private Map<Integer, AtomicInteger> sequences;
    private int uniqueColIndex;

    private AtomicInteger activeReaders = new AtomicInteger();

    private boolean allowImplicitIndexing;

    TempTable(TempMetadataID tid, BufferManager bm, List<ElementSymbol> columns, int primaryKeyLength, String sessionID) {
        this.tid = tid;
        this.bm = bm;
        int startIndex = 0;
        if (primaryKeyLength == 0) {
            startIndex = 1;
            ElementSymbol rid = new ElementSymbol("rowId"); //$NON-NLS-1$
            rid.setType(DataTypeManager.DefaultDataClasses.LONG);
            columns.add(0, rid);
            rowId = new AtomicLong();
            tree = bm.createSTree(columns, sessionID, 1);
        } else {
            this.uniqueColIndex = primaryKeyLength;
            tree = bm.createSTree(columns, sessionID, primaryKeyLength);
        }
        this.tree.setMinStorageSize(0);
        this.columnMap = RelationalNode.createLookupMap(columns);
        this.columns = columns;
        IntBuffer notNullList = IntBuffer.allocate(columns.size());
        if (!tid.getElements().isEmpty()) {
            //not relevant for indexes
            for (int i = startIndex; i < columns.size(); i++) {
                TempMetadataID col = (TempMetadataID) columns.get(i).getMetadataID();
                if (col == null) {
                    continue;
                }
                if (col.isAutoIncrement()) {
                    if (this.sequences == null) {
                        this.sequences = new HashMap<Integer, AtomicInteger>();
                    }
                    sequences.put(i, new AtomicInteger(1));
                }
                if (col.isNotNull()) {
                    notNullList.put(i);
                }
            }
        }
        this.notNull = Arrays.copyOf(notNullList.array(), notNullList.position());
        if (this.sequences == null) {
            this.sequences = Collections.emptyMap();
        }
        this.sessionID = sessionID;
        this.keyBatchSize = bm.getSchemaSize(columns.subList(0, primaryKeyLength));
        this.leafBatchSize = bm.getSchemaSize(columns);
        tid.setCardinality(0);
    }

    private void validateNotNull(List tuple)
            throws TeiidProcessingException {
        for (int i = 0; i < notNull.length; i++) {
            if (tuple.get(notNull[i]) == null) {
                 throw new TeiidProcessingException(QueryPlugin.Event.TEIID30236, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30236, columns.get(i)));
            }
        }
    }

    public TempTable clone() {
        lock.readLock().lock();
        try {
            TempTable clone = (TempTable) super.clone();
            clone.lock = new ReentrantReadWriteLock();
            if (clone.indexTables != null) {
                clone.indexTables = new LinkedHashMap<List<ElementSymbol>, TempTable>(clone.indexTables);
                for (Map.Entry<List<ElementSymbol>, TempTable> entry : clone.indexTables.entrySet()) {
                    TempTable indexClone = entry.getValue().clone();
                    indexClone.lock = clone.lock;
                    entry.setValue(indexClone);
                }
            }
            clone.tree = tree.clone();
            clone.activeReaders = new AtomicInteger();
            return clone;
        } catch (CloneNotSupportedException e) {
             throw new TeiidRuntimeException(e);
        } finally {
            lock.readLock().unlock();
        }
    }

    public AtomicInteger getActive() {
        return activeReaders;
    }

    void addIndex(List<ElementSymbol> indexColumns, boolean unique) throws TeiidComponentException, TeiidProcessingException {
        List<ElementSymbol> keyColumns = columns.subList(0, tree.getKeyLength());
        if (keyColumns.equals(indexColumns) || (indexTables != null && indexTables.containsKey(indexColumns))) {
            return;
        }
        TempTable indexTable = createIndexTable(indexColumns, unique);
        //TODO: ordered insert optimization
        TupleSource ts = createTupleSource(indexTable.getColumns(), null, null);
        indexTable.insert(ts, indexTable.getColumns(), false, false, null);
        indexTable.getTree().compact();
    }

    private TempTable createIndexTable(List<ElementSymbol> indexColumns,
            boolean unique) {
        List<ElementSymbol> allColumns = new ArrayList<ElementSymbol>(indexColumns);
        for (ElementSymbol elementSymbol : columns.subList(0, tree.getKeyLength())) {
            if (allColumns.indexOf(elementSymbol) < 0) {
                allColumns.add(elementSymbol);
            }
        }
        TempTable indexTable = new TempTable(new TempMetadataID("idx", Collections.EMPTY_LIST), this.bm, allColumns, allColumns.size(), this.sessionID); //$NON-NLS-1$
        indexTable.setPreferMemory(this.tree.isPreferMemory());
        indexTable.lock = this.lock;
        if (unique) {
            indexTable.uniqueColIndex = indexColumns.size();
        }
        if (indexTables == null) {
            indexTables = new LinkedHashMap<List<ElementSymbol>, TempTable>();
            indexTables.put(indexColumns, indexTable);
        }
        indexTable.setUpdatable(this.updatable);
        return indexTable;
    }

    private int reserveBuffers() {
        return bm.reserveBuffers(leafBatchSize + (tree.getHeight() - 1)*keyBatchSize, BufferReserveMode.FORCE);
    }

    public TupleSource createTupleSource(final List<? extends Expression> projectedCols, final Criteria condition, OrderBy orderBy) throws TeiidComponentException, TeiidProcessingException {
        //special handling for count(*)
        boolean agg = false;
        for (Expression singleElementSymbol : projectedCols) {
            if (singleElementSymbol instanceof ExpressionSymbol && ((ExpressionSymbol)singleElementSymbol).getExpression() instanceof AggregateSymbol) {
                agg = true;
                break;
            }
        }
        if (agg) {
            if (condition == null) {
                long count = this.getRowCount();
                return new CollectionTupleSource(Arrays.asList(Collections.nCopies(projectedCols.size(), (int)Math.min(Integer.MAX_VALUE, count))).iterator());
            }
            orderBy = null;
        }
        IndexInfo primary = new IndexInfo(this, projectedCols, condition, orderBy, true);
        IndexInfo ii = primary;
        if ((indexTables != null || (!this.updatable && allowImplicitIndexing && condition != null && this.getRowCount() > 2*this.getTree().getPageSize(true))) && (condition != null || orderBy != null) && ii.valueSet.size() != 1) {
            LogManager.logDetail(LogConstants.CTX_DQP, "Considering indexes on table", this, "for query", projectedCols, condition, orderBy); //$NON-NLS-1$ //$NON-NLS-2$
            long rowCost = this.tree.getRowCount();
            long bestCost = estimateCost(orderBy, ii, rowCost);
            if (this.indexTables != null) {
                for (TempTable table : this.indexTables.values()) {
                    IndexInfo secondary = new IndexInfo(table, projectedCols, condition, orderBy, false);
                    long cost = estimateCost(orderBy, secondary, rowCost);
                    if (cost < bestCost) {
                        ii = secondary;
                        bestCost = cost;
                    }
                }
            }
            if (ii == primary && allowImplicitIndexing) {
                //TODO: detect if it should be covering
                if (createImplicitIndexIfNeeded(condition)) {
                    IndexInfo secondary = new IndexInfo(this.indexTables.values().iterator().next(), projectedCols, condition, orderBy, false);
                    LogManager.logDetail(LogConstants.CTX_DQP, "Created an implicit index ", secondary.table); //$NON-NLS-1$
                    long cost = estimateCost(orderBy, secondary, rowCost);
                    if (cost < bestCost) {
                        ii = secondary;
                        bestCost = cost;
                    } else {
                        LogManager.logDetail(LogConstants.CTX_DQP, "Did not utilize the implicit index"); //$NON-NLS-1$ //$NON-NLS-2$
                    }
                }
            }
            LogManager.logDetail(LogConstants.CTX_DQP, "Choose index", ii.table, "covering:", ii.coveredCriteria,"ordering:", ii.ordering); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            if (ii.covering) {
                return ii.table.createTupleSource(projectedCols, condition, orderBy, ii, agg);
            }
            List<ElementSymbol> pkColumns = this.columns.subList(0, this.tree.getKeyLength());
            if (ii.ordering != null) {
                //use order and join
                primary.valueTs = ii.table.createTupleSource(pkColumns,
                        ii.coveredCriteria, orderBy, ii, agg);
                primary.ordering = null;
                return createTupleSource(projectedCols, ii.nonCoveredCriteria, null, primary, agg);
            }
            //order by pk to localize lookup costs, then join
            OrderBy pkOrderBy = new OrderBy();
            for (ElementSymbol elementSymbol : pkColumns) {
                pkOrderBy.addVariable(elementSymbol);
            }
            primary.valueTs = ii.table.createTupleSource(pkColumns,
                    ii.coveredCriteria, pkOrderBy, ii, agg);
            return createTupleSource(projectedCols, ii.nonCoveredCriteria, orderBy, primary, agg);
        }
        return createTupleSource(projectedCols, condition, orderBy, ii, agg);
    }

    private boolean createImplicitIndexIfNeeded(final Criteria condition) throws TeiidComponentException, TeiidProcessingException {
        int operator = CompareCriteria.EQ;
        LinkedHashSet<ElementSymbol> symbols = null;
        for (Criteria c : Criteria.separateCriteriaByAnd(condition)) {
            if (!(c instanceof CompareCriteria)) {
                continue;
            }
            CompareCriteria cc = (CompareCriteria)c;
            if (cc.getOperator() == CompareCriteria.NE) {
                continue;
            }
            if (symbols == null) {
                symbols = new LinkedHashSet<>();
            } else {
                if (!symbols.isEmpty()) {
                    if (operator != cc.getOperator()) {
                        break;
                    }
                }
                operator = cc.getOperator();
            }
            //TODO: an assumption is that only a single predicate will be bind eligible - if not we'll just take the first
            if (cc.getRightExpression() instanceof Constant && ((Constant)cc.getRightExpression()).isBindEligible()) {
                //the left can be a array or a column
                if (cc.getLeftExpression() instanceof ElementSymbol) {
                    symbols.add((ElementSymbol) cc.getLeftExpression());
                } else if (cc.getLeftExpression() instanceof Array) {
                    for (Expression ex : ((Array)cc.getLeftExpression()).getExpressions()) {
                        if (ex instanceof ElementSymbol) {
                            symbols.add((ElementSymbol)ex);
                        } else {
                            break;
                        }
                    }
                }
            }
        }
        if (symbols != null && !symbols.isEmpty()) {
            //TODO: order by ndv
            this.addIndex(new ArrayList<>(symbols), false);
            return true;
        }
        return false;
    }

    private TupleSource createTupleSource(
            final List<? extends Expression> projectedCols,
            final Criteria condition, OrderBy orderBy, IndexInfo ii, boolean agg)
            throws TeiidComponentException, TeiidProcessingException {
        TupleBrowser browser = ii.createTupleBrowser(bm.getOptions().getDefaultNullOrder(), true);
        TupleSource ts = new QueryTupleSource(browser, columnMap, agg?getColumns():projectedCols, condition);

        boolean usingQueryTupleSource = false;
        boolean success = false;
        TupleBuffer tb = null;
        try {
            if (ii.ordering == null && orderBy != null) {
                SortUtility sort = new SortUtility(ts, orderBy.getOrderByItems(), Mode.SORT, bm, sessionID, projectedCols);
                sort.setNonBlocking(true);
                tb = sort.sort();
            } else if (agg) {
                int count = 0;
                while (ts.nextTuple() != null) {
                    count++;
                }
                success = true;
                return new CollectionTupleSource(Arrays.asList(Collections.nCopies(projectedCols.size(), count)).iterator());
            } else if (updatable) {
                tb = bm.createTupleBuffer(projectedCols, sessionID, TupleSourceType.PROCESSOR);
                List<?> next = null;
                while ((next = ts.nextTuple()) != null) {
                    tb.addTuple(next);
                }
            } else {
                usingQueryTupleSource = true;
                success = true;
                return ts;
            }
            tb.close();
            success = true;
            return tb.createIndexedTupleSource(true);
        } finally {
            if (!success && tb != null) {
                tb.remove();
            }
            if (!usingQueryTupleSource) {
                //ensure the buffers get released
                ts.closeSource();
            }
        }
    }

    /**
     * TODO: this could easily use statistics - the tree level 1 would be an ideal place
     * to compute them, since it minimizes page loads, and is a random sample.
     * TODO: this should also factor in the block size
     */
    private long estimateCost(OrderBy orderBy, IndexInfo ii, long rowCost) {
        long initialCost = rowCost;
        long additionalCost = 0;
        if (ii.valueSet.size() != 0) {
            int length = ii.valueSet.get(0).size();
            rowCost = ii.valueSet.size();
            additionalCost = rowCost * (64 - Long.numberOfLeadingZeros(initialCost - 1));
            if (ii.table.uniqueColIndex != length) {
                rowCost *= 3*(ii.table.uniqueColIndex - length);
            }
            if (rowCost > initialCost) {
                additionalCost = rowCost - initialCost;
                rowCost = initialCost;
            }
        } else if (ii.upper != null) {
            additionalCost = (64 - Long.numberOfLeadingZeros(initialCost - 1));
            rowCost /= 3;
        } else if (ii.lower != null) {
            additionalCost = (64 - Long.numberOfLeadingZeros(initialCost - 1));
            rowCost /= 3;
        }
        if (rowCost > 1 && !ii.covering) {
            //primary lookup
            additionalCost += rowCost * (64 - Long.numberOfLeadingZeros(rowCost - 1));
        }
        if (rowCost > 1 && orderBy != null && ii.ordering != null) {
            //pk order or non-covered ordering
            //TODO: this should be based upon the filtered rowCost, but instead it is
            //written as a bonus
            additionalCost -= Math.min(additionalCost, rowCost * (64 - Long.numberOfLeadingZeros(rowCost - 1)));
        }
        return rowCost + additionalCost;
    }

    private TupleBrowser createTupleBrower(Criteria condition, boolean direction) throws TeiidComponentException {
        IndexInfo ii = new IndexInfo(this, null, condition, null, true);
        ii.ordering = direction;
        return ii.createTupleBrowser(bm.getOptions().getDefaultNullOrder(), false);
    }

    public long getRowCount() {
        return tree.getRowCount();
    }

    public long truncate(boolean force) {
        this.tid.getTableData().dataModified(tree.getRowCount());
        return tree.truncate(force);
    }

    public void remove() {
        lock.writeLock().lock();
        try {
            tid.getTableData().removed();
            tree.remove();
            if (this.indexTables != null) {
                for (TempTable indexTable : this.indexTables.values()) {
                    indexTable.remove();
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Object matchesPkColumn(int pkIndex, Expression ex) {
        if (rowId != null) {
            return false;
        }
        if (ex instanceof Array) {
            Array array = (Array)ex;
            List<Expression> exprs = array.getExpressions();
            int toIndex = Math.min(this.getPkLength(), exprs.size());
            int[] indexes = new int[toIndex];
            for (int i = pkIndex; i < toIndex; i++) {
                int index = exprs.indexOf(this.columns.get(i));
                indexes[i] = index;
                if (index == -1) {
                    if (i == pkIndex) {
                        return false;
                    }
                    break;
                }
            }
            return indexes;
        }
        return columns.get(pkIndex).equals(ex);
    }

    @Override
    public boolean supportsOrdering(int pkIndex, Expression ex) {
        //all indexes are currently ordered
        return true;
    }

    public List<ElementSymbol> getColumns() {
        if (rowId != null) {
            return columns.subList(1, columns.size());
        }
        return columns;
    }

    public TupleSource insert(TupleSource tuples, final List<ElementSymbol> variables, boolean canUndo, boolean upsert, CommandContext context) throws TeiidComponentException, ExpressionEvaluationException, TeiidProcessingException {
        List<ElementSymbol> cols = getColumns();
        final int[] indexes = new int[cols.size()];
        boolean shouldProject = false;
        for (int i = 0; i < cols.size(); i++) {
            indexes[i] = variables.indexOf(cols.get(i));
            shouldProject |= (indexes[i] != i);
        }
        InsertUpdateProcessor up = new InsertUpdateProcessor(tuples, rowId != null, shouldProject?indexes:null, canUndo, upsert);
        if (context != null && rowId == null) {
            List<String> colNames = null;
            List<Class<?>> colTypes = null;
            for (int i = 0; i < tree.getKeyLength(); i++) {
                TempMetadataID col = tid.getElements().get(i);
                if (col.isAutoIncrement() && indexes[i] == -1) {
                    if (colNames == null) {
                        colNames = new ArrayList<String>();
                        colTypes = new ArrayList<Class<?>>();
                    }
                    colNames.add(col.getName());
                    colTypes.add(col.getType());
                    break;
                }
            }
            if (colNames != null) {
                GeneratedKeysImpl keys = context.returnGeneratedKeys(colNames.toArray(new String[colNames.size()]), colTypes.toArray(new Class<?>[colTypes.size()]));
                up.setGeneratedKeys(keys);
            }
        }
        long updateCount = up.process();
        tid.setCardinality(tree.getRowCount());
        tid.getTableData().dataModified(updateCount);
        return CollectionTupleSource.createUpdateCountArrayTupleSource(updateCount);
    }

    public TupleSource update(Criteria crit, final SetClauseList update) throws TeiidComponentException, ExpressionEvaluationException, TeiidProcessingException {
        final boolean primaryKeyChangePossible = canChangePrimaryKey(update);
        final TupleBrowser browser = createTupleBrower(crit, OrderBy.ASC);
        UpdateProcessor up = new UpdateProcessor(crit, browser, true) {

            protected TupleBuffer changeSet;
            protected UpdateProcessor changeSetProcessor;

            @Override
            protected void tuplePassed(List tuple)
                    throws BlockedException, TeiidComponentException, TeiidProcessingException {
                List<Object> newTuple = new ArrayList<Object>(tuple);
                for (Map.Entry<ElementSymbol, Expression> entry : update.getClauseMap().entrySet()) {
                    newTuple.set(columnMap.get(entry.getKey()), eval.evaluate(entry.getValue(), tuple));
                }
                validateNotNull(newTuple);
                if (primaryKeyChangePossible) {
                    browser.removed();
                    deleteTuple(tuple);
                    if (changeSet == null) {
                        changeSet = bm.createTupleBuffer(columns, sessionID, TupleSourceType.PROCESSOR);
                    }
                    changeSet.addTuple(newTuple);
                } else {
                    browser.update(newTuple);
                }
            }

            @Override
            protected void undo(List<?> tuple) throws TeiidComponentException, TeiidProcessingException {
                if (primaryKeyChangePossible) {
                    insertTuple(tuple, false, true);
                } else {
                    updateTuple(tuple);
                }
            }

            @Override
            void success() throws TeiidComponentException, ExpressionEvaluationException, TeiidProcessingException {
                //existing tuples have been removed
                //changeSet contains possible updates
                if (primaryKeyChangePossible) {
                    changeSet.close();
                    if (changeSetProcessor == null) {
                        changeSetProcessor = new InsertUpdateProcessor(changeSet.createIndexedTupleSource(true), false, null, true, false);
                    }
                    changeSetProcessor.process(); //when this returns, we're up to date
                }
            }

            @Override
            public void close() {
                super.close();
                changeSetProcessor = null;
                if (changeSet != null) {
                    changeSet.remove();
                    changeSet = null;
                }
            }

        };
        long updateCount = up.process();
        tid.getTableData().dataModified(updateCount);
        return CollectionTupleSource.createUpdateCountTupleSource((int)Math.min(Integer.MAX_VALUE, updateCount));
    }

    private boolean canChangePrimaryKey(final SetClauseList update) {
        if (rowId == null) {
            Set<ElementSymbol> affectedColumns = new HashSet<ElementSymbol>(update.getClauseMap().keySet());
            affectedColumns.retainAll(columns.subList(0, tree.getKeyLength()));
            if (!affectedColumns.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    public TupleSource delete(Criteria crit) throws TeiidComponentException, ExpressionEvaluationException, TeiidProcessingException {
        final TupleBrowser browser = createTupleBrower(crit, OrderBy.ASC);
        UpdateProcessor up = new UpdateProcessor(crit, browser, true) {
            @Override
            protected void tuplePassed(List tuple)
                    throws ExpressionEvaluationException,
                    BlockedException, TeiidComponentException {
                browser.removed();
                deleteTuple(tuple);
            }

            @Override
            protected void undo(List<?> tuple) throws TeiidComponentException, TeiidProcessingException {
                insertTuple(tuple, false, true);
            }
        };
        long updateCount = up.process();
        tid.setCardinality(tree.getRowCount());
        tid.getTableData().dataModified(updateCount);
        return CollectionTupleSource.createUpdateCountTupleSource((int)Math.min(Integer.MAX_VALUE, updateCount));
    }

    boolean insertTuple(List<?> list, boolean ordered, boolean checkDuplidate) throws TeiidComponentException, TeiidProcessingException {
        if (tree.insert(list, ordered?InsertMode.ORDERED:InsertMode.NEW, -1) != null) {
            if (!checkDuplidate) {
                return false;
            }
            throw new TeiidProcessingException(QueryPlugin.Event.TEIID30238, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30238, this.tid.getID()));
        }
        return true;
    }

    private void deleteTuple(List<?> tuple) throws TeiidComponentException {
        if (tree.remove(tuple) == null) {
            throw new AssertionError("Delete failed"); //$NON-NLS-1$
        }
    }

    void writeTo(ObjectOutputStream oos) throws TeiidComponentException, IOException {
        this.lock.readLock().lock();
        try {
            this.tree.writeValuesTo(oos);
            if (this.indexTables == null) {
                oos.writeInt(0);
            } else {
                oos.writeInt(this.indexTables.size());
                for (Map.Entry<List<ElementSymbol>, TempTable> entry : this.indexTables.entrySet()) {
                    oos.writeBoolean(entry.getValue().uniqueColIndex > 0);
                    oos.writeInt(entry.getKey().size());
                    for (ElementSymbol es : entry.getKey()) {
                        oos.writeInt(this.columnMap.get(es));
                    }
                    entry.getValue().writeTo(oos);
                }
            }
        } finally {
            this.lock.readLock().unlock();
        }
    }

    void readFrom(ObjectInputStream ois) throws TeiidComponentException, IOException, ClassNotFoundException {
        this.tree.readValuesFrom(ois);
        int numIdx = ois.readInt();
        for (int i = 0; i < numIdx; i++) {
            boolean unique = ois.readBoolean();
            int numCols = ois.readInt();
            ArrayList<ElementSymbol> indexColumns = new ArrayList<ElementSymbol>(numCols);
            for (int j = 0; j < numCols; j++) {
                int colIndex = ois.readInt();
                indexColumns.add(this.columns.get(colIndex));
            }
            TempTable tt = this.createIndexTable(indexColumns, unique);
            tt.readFrom(ois);
        }
    }

    List<?> updateTuple(List<?> tuple, boolean remove) throws TeiidComponentException {
        try {
            lock.writeLock().lock();
            if (remove) {
                List<?> result = tree.remove(tuple);
                if (result == null) {
                    return null;
                }
                if (indexTables != null) {
                    for (TempTable index : this.indexTables.values()) {
                        tuple = RelationalNode.projectTuple(RelationalNode.getProjectionIndexes(index.getColumnMap(), index.columns), result);
                        index.tree.remove(tuple);
                    }
                }
                tid.getTableData().dataModified(1);
                return result;
            }
            List<?> result = tree.insert(tuple, InsertMode.UPDATE, -1);
            if (indexTables != null) {
                for (TempTable index : this.indexTables.values()) {
                    tuple = RelationalNode.projectTuple(RelationalNode.getProjectionIndexes(index.getColumnMap(), index.columns), tuple);
                    index.tree.insert(tuple, InsertMode.UPDATE, -1);
                }
            }
            tid.getTableData().dataModified(1);
            return result;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void updateTuple(List<?> tuple) throws TeiidComponentException {
        if (tree.insert(tuple, InsertMode.UPDATE, -1) == null) {
            throw new AssertionError("Update failed"); //$NON-NLS-1$
        }
    }

    void setPreferMemory(boolean preferMemory) {
        this.tree.setPreferMemory(preferMemory);
    }

    void setUpdatable(boolean updatable) {
        this.updatable = updatable;
        if (this.indexTables != null) {
            for (TempTable index : this.indexTables.values()) {
                index.setUpdatable(updatable);
            }
        }
    }

    CacheHint getCacheHint() {
        return this.tid.getCacheHint();
    }

    public int getPkLength() {
        if (rowId != null) {
            return 0;
        }
        return this.tree.getKeyLength();
    }

    public boolean isUpdatable() {
        return updatable;
    }

    @Override
    public String toString() {
        return tid.getID() + " (" + columns + ")\n"; //$NON-NLS-1$ //$NON-NLS-2$
    }

    public Map<Expression, Integer> getColumnMap() {
        return this.columnMap;
    }

    STree getTree() {
        return tree;
    }

    public TempMetadataID getMetadataId() {
        return tid;
    }

    public Long getId() {
        return id;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof TempTable)) {
            return false;
        }
        TempTable other = (TempTable)obj;
        return id.equals(other.id);
    }

    public void setAllowImplicitIndexing(boolean b) {
        this.allowImplicitIndexing = b;
    }

}