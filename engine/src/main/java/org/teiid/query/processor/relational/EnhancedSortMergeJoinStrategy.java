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

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.common.buffer.BufferManager.BufferReserveMode;
import org.teiid.common.buffer.IndexedTupleSource;
import org.teiid.common.buffer.STree;
import org.teiid.common.buffer.STree.InsertMode;
import org.teiid.common.buffer.TupleBrowser;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.query.QueryPlugin;
import org.teiid.query.processor.relational.SourceState.ImplicitBuffer;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;


/**
 * Extends the basic fully sorted merge join to check for conditions necessary
 * to not fully sort one of the sides.
 *
 * Will be used for inner joins and only if both sorts are not required.
 * Degrades to a normal merge join if the tuples are balanced.
 *
 * Refined in 7.4 to use a full index if it is small enough or a repeated merge, rather than a partitioning approach (which was really just a single level index)
 *
 * TODO: add a tree method for insert that reuses a place list
 */
public class EnhancedSortMergeJoinStrategy extends MergeJoinStrategy {

    private static final class SingleTupleSource extends AbstractList<Object> implements TupleSource {
        boolean returned;
        private int[] indexes;
        private List<?> keyTuple;

        @Override
        public List<?> nextTuple() throws TeiidComponentException,
                TeiidProcessingException {
            if (!returned) {
                returned = true;
                return this;
            }
            return null;
        }

        @Override
        public Object get(int index) {
            return keyTuple.get(indexes[index]);
        }

        @Override
        public int size() {
            return indexes.length;
        }

        public void setValues(List<?> values) {
            returned = false;
            this.keyTuple = values;
        }

        @Override
        public void closeSource() {
        }
    }

    private boolean semiDep;

    private TupleSource currentSource;
    private SourceState sortedSource;
    private SourceState notSortedSource;
    private List<?> currentTuple;
    private boolean matched;
    private TupleBrowser tb;
    private SingleTupleSource keyTs;
    private STree index;
    private int[] reverseIndexes;
    private List<?> sortedTuple;
    private boolean repeatedMerge;
    private boolean validSemiDep;

    /**
     * Number of index batches we'll allow to marked as prefers memory regardless of buffer space
     */
    private int preferMemCutoff = 8;

    public EnhancedSortMergeJoinStrategy(SortOption sortLeft, SortOption sortRight) {
        super(sortLeft, sortRight, false);
    }

    public void setPreferMemCutoff(int cutoff) {
        this.preferMemCutoff = cutoff;
    }

    @Override
    public void close() {
        if (joinNode == null) {
            return;
        }
        super.close();
        if (this.index != null) {
            this.index.remove();
        }
    }

    /**
     * Create an index of the smaller size
     */
    public void createIndex(SourceState state, SortOption sortOption) throws TeiidComponentException, TeiidProcessingException {
        //this is inefficient as it fully buffers, then builds the index.  if possible
        //we should build off of the streaming batches
        IndexedTupleSource its = state.getTupleBuffer().createIndexedTupleSource(!joinNode.isDependent());
        boolean sorted = sortOption == SortOption.ALREADY_SORTED;
        int[] expressionIndexes = state.getExpressionIndexes();
        int keyLength = expressionIndexes.length;
        List elements = state.getSource().getOutputElements();

        //TODO: minimize reordering, or at least detect when it's not necessary
        LinkedHashSet<Integer> used = new LinkedHashSet<Integer>();
        for (int i : expressionIndexes) {
            used.add(i);
        }
        int[] reorderedSortIndex = Arrays.copyOf(expressionIndexes, keyLength + elements.size() - used.size());
        int j = keyLength;
        for (int i = 0; i < elements.size(); i++) {
            if (!used.contains(i)) {
                reorderedSortIndex[j++] = i;
            }
        }
        List<Expression> reordered = RelationalNode.projectTuple(reorderedSortIndex, elements);
        if (sortOption == SortOption.SORT_DISTINCT) {
            keyLength = elements.size();
        } else if (!state.isExpresssionDistinct()) {
            //need to add a rowid, just in case
            reordered = new ArrayList<Expression>(reordered);
            ElementSymbol id = new ElementSymbol("rowId"); //$NON-NLS-1$
            id.setType(DataTypeManager.DefaultDataClasses.INTEGER);
            reordered.add(keyLength, id);
            keyLength++;
        }
        index = this.joinNode.getBufferManager().createSTree(reordered, this.joinNode.getConnectionID(), keyLength);
        index.setPreferMemory(true);
        if (sortOption == SortOption.SORT_DISTINCT) {
            index.getComparator().setDistinctIndex(expressionIndexes.length);
        } else if (!state.isExpresssionDistinct()) {
            index.getComparator().setDistinctIndex(keyLength-2);
        }
        int rowId = 0;
        List<?> lastTuple = null;
        boolean sortedDistinct = sorted && !state.isExpresssionDistinct();
        int sizeHint = index.getExpectedHeight(state.getRowCount());
        index.setBatchInsert(sorted);
        outer: while (its.hasNext()) {
            //detect if sorted and distinct
            List<?> originalTuple = its.nextTuple();
            //remove the tuple if it has null
            for (int i : expressionIndexes) {
                if (originalTuple.get(i) == null) {
                    continue outer;
                }
            }
            if (sortedDistinct && lastTuple != null && this.compare(lastTuple, originalTuple, expressionIndexes, expressionIndexes) == 0) {
                sortedDistinct = false;
            }
            lastTuple = originalTuple;
            List<Object> tuple = (List<Object>) RelationalNode.projectTuple(reorderedSortIndex, originalTuple);
            if (!state.isExpresssionDistinct() && sortOption != SortOption.SORT_DISTINCT) {
                tuple.add(keyLength - 1, rowId++);
            }
            index.insert(tuple, sorted?InsertMode.ORDERED:InsertMode.NEW, sizeHint);
        }
        if (!sorted) {
            index.compact();
        } else {
            index.setBatchInsert(false);
        }
        its.closeSource();
        this.reverseIndexes = new int[elements.size()];
        for (int i = 0; i < reorderedSortIndex.length; i++) {
            int oldIndex = reorderedSortIndex[i];
            this.reverseIndexes[oldIndex] = i + ((!state.isExpresssionDistinct()&&(i>=keyLength-1)&&sortOption!=SortOption.SORT_DISTINCT)?1:0);
        }
        //TODO: this logic doesn't catch distinct join expressions when using sort distinct very well
        if (!state.isExpresssionDistinct()
                && ((!sorted && index.getComparator().isDistinct()) || (sorted && sortedDistinct))) {
            if (sortOption!=SortOption.SORT_DISTINCT) {
                this.index.removeRowIdFromKey();
            }
            state.markExpressionsDistinct(true);
        }
        keyTs = new SingleTupleSource();
        keyTs.indexes = this.notSortedSource.getExpressionIndexes();
        tb = new TupleBrowser(this.index, keyTs, OrderBy.ASC);
    }

    @Override
    protected void loadLeft() throws TeiidComponentException,
            TeiidProcessingException {
        if (this.joinNode.isDependent()) {
            this.leftSource.getTupleBuffer();
        }
    }

    private boolean shouldIndexIfSmall(SourceState source) throws TeiidComponentException, TeiidProcessingException {
        return source.rowCountLE(source.getSource().getBatchSize() / 2);
    }

    @Override
    protected void loadRight() throws TeiidComponentException,
            TeiidProcessingException {
        //the checks are done in a particular order to ensure we don't buffer if possible
        if (processingSortRight == SortOption.SORT && this.joinNode.getJoinType() != JoinType.JOIN_LEFT_OUTER && shouldIndexIfSmall(this.leftSource)) {
            this.processingSortRight = SortOption.NOT_SORTED;
        } else if (!this.leftSource.hasBuffer() && processingSortLeft == SortOption.SORT && shouldIndexIfSmall(this.rightSource)) {
            this.processingSortLeft = SortOption.NOT_SORTED;
        } else {
            if (!this.rightSource.hasBuffer() && processingSortRight == SortOption.SORT && this.joinNode.getJoinType() != JoinType.JOIN_LEFT_OUTER && shouldIndexIfSmall(this.leftSource)) {
                this.processingSortRight = SortOption.NOT_SORTED;
            } else if (!semiDep && processingSortRight == SortOption.SORT && this.joinNode.getJoinType() != JoinType.JOIN_LEFT_OUTER && ((!this.rightSource.hasBuffer() && processingSortLeft != SortOption.SORT) || shouldIndex(this.leftSource, this.rightSource))) {
                this.processingSortRight = SortOption.NOT_SORTED;
            } else if (!semiDep && processingSortLeft == SortOption.SORT && shouldIndex(this.rightSource, this.leftSource)) {
                this.processingSortLeft = SortOption.NOT_SORTED;
            }
        }
        RelationalNode parent = this.joinNode.getParent();
        boolean hasLimit = false;
        while (parent != null && !hasLimit) {
            if (parent instanceof LimitNode) {
                LimitNode ln = (LimitNode)parent;
                if (ln.getLimit() != -1) {
                    hasLimit = true;
                }
                break;
            }
            if (parent instanceof WindowFunctionProjectNode) {
                break;
            }
            if (parent instanceof GroupingNode) {
                break;
            }
            if (parent instanceof JoinNode) {
                break;
            }
            if (parent instanceof SortNode) {
                break;
            }
            if (parent instanceof DupRemoveNode) {
                break;
            }
            parent = parent.getParent();
        }

        if (semiDep && this.leftSource.isExpresssionDistinct()) {
            this.rightSource.rowCountLE(1); //do a prefetch to see if any select nodes are triggered
            if (!this.joinNode.getDependentValueSource().isUnused()) {
                if (!this.joinNode.getDependentValueSource().inMemory()) {
                    //must be pushed - TODO detect that without buffering
                    this.rightSource.getTupleBuffer();
                    if (!this.joinNode.getDependentValueSource().isUnused()) {
                        this.validSemiDep = true;
                    }
                } else {
                    this.validSemiDep = true;
                }

                if (this.validSemiDep) {
                    this.sortedSource = this.leftSource;
                    this.notSortedSource = this.rightSource;
                    //sort is not needed
                    this.processingSortRight = SortOption.NOT_SORTED;
                    //TODO: this performs an unnecessary projection
                    return;
                }
            }
        }

        if (this.processingSortLeft != SortOption.NOT_SORTED && this.processingSortRight != SortOption.NOT_SORTED) {
            if (hasLimit && this.joinNode.getJoinType() != JoinType.JOIN_LEFT_OUTER) {
                //still use a more incremental join/sort approach
                if (this.processingSortLeft == SortOption.SORT &&
                        (!this.rightSource.rowCountLE(this.leftSource.getIncrementalRowCount(false)) || this.processingSortRight != SortOption.SORT)) {
                    this.processingSortRight = SortOption.NOT_SORTED;
                    repeatedMerge = true;
                } else if (this.processingSortRight == SortOption.SORT) {
                    this.processingSortLeft = SortOption.NOT_SORTED;
                    repeatedMerge = true;
                }
            }
            if (this.processingSortLeft != SortOption.NOT_SORTED && this.processingSortRight != SortOption.NOT_SORTED) {
                if (this.leftSource.rowCountLE(0)) {
                    return;
                }
                super.loadRight();
                super.loadLeft();
                if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
                    LogManager.logDetail(LogConstants.CTX_DQP, "degrading to merged join", this.joinNode.getID()); //$NON-NLS-1$
                }
                return; //degrade to merge join
            }
        }

        if (this.processingSortLeft == SortOption.NOT_SORTED) {
            this.sortedSource = this.rightSource;
            this.notSortedSource = this.leftSource;

            if (!repeatedMerge) {
                createIndex(this.rightSource, this.processingSortRight);
            } else {
                this.notSortedSource.isLimited(hasLimit);
                super.loadRight(); //sort if needed
                this.notSortedSource.sort(SortOption.NOT_SORTED); //do a single sort pass
                if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
                    LogManager.logDetail(LogConstants.CTX_DQP, "performing single pass sort right", this.joinNode.getID()); //$NON-NLS-1$
                }
            }
        } else if (this.processingSortRight == SortOption.NOT_SORTED) {
            this.sortedSource = this.leftSource;
            this.notSortedSource = this.rightSource;

            if (!repeatedMerge) {
                createIndex(this.leftSource, this.processingSortLeft);
            } else {
                this.notSortedSource.isLimited(hasLimit);
                super.loadLeft(); //sort if needed
                this.notSortedSource.sort(SortOption.NOT_SORTED); //do a single sort pass
                if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
                    LogManager.logDetail(LogConstants.CTX_DQP, "performing single pass sort left", this.joinNode.nodeToString()); //$NON-NLS-1$
                }
            }
        }
    }

    private boolean shouldIndex(SourceState possibleIndex, SourceState other) throws TeiidComponentException, TeiidProcessingException {
        long size = joinNode.getBatchSize();
        long indexSize = possibleIndex.hasBuffer()?possibleIndex.getRowCount():-1;
        long otherSize = other.hasBuffer()?other.getRowCount():-1;
        int schemaSize = this.joinNode.getBufferManager().getSchemaSize(other.getSource().getOutputElements());
        int toReserve = this.joinNode.getBufferManager().getMaxProcessingSize();
        int minSize = toReserve/schemaSize*this.joinNode.getBatchSize();
        if (otherSize != -1 && otherSize < this.joinNode.getBatchSize()) {
            //return false;
        }

        //determine sizes in an incremental fashion as to avoid a full buffer of the unsorted side
        while (size < Integer.MAX_VALUE && (indexSize == -1 || otherSize == -1)) {
            if (indexSize == -1 && (possibleIndex.rowCountLE((int)size) || possibleIndex.hasBuffer())) {
                indexSize = possibleIndex.getRowCount();
            }
            if (otherSize == -1 && (other.rowCountLE((int)size) || other.hasBuffer())) {
                otherSize = other.getRowCount();
            }
            if (indexSize == -1 && otherSize != -1 && size * 4 > otherSize) {
                return false;
            }
            if (indexSize != -1 && otherSize == -1 && indexSize * 4 <= size) {
                break;
            }
            size *=2;
        }
        if ((size > Integer.MAX_VALUE && (indexSize == -1 || otherSize == -1)) || (indexSize != -1 && otherSize != -1 && indexSize * 4 > otherSize)) {
            return false; //index is too large
        }
        //check if the other side can be sorted in memory
        if (other.hasBuffer() && ((other.getRowCount() <= this.joinNode.getBatchSize())
                || (possibleIndex.getRowCount() > this.joinNode.getBatchSize() && other.rowCountLE(minSize)))) {
            return false; //just use a merge join
        }
        boolean useIndex = false;
        int indexSchemaSize = this.joinNode.getBufferManager().getSchemaSize(possibleIndex.getSource().getOutputElements());
        //approximate that the tree depth will be memory resident
        int depth = 0;
        long rows = possibleIndex.getRowCount();
        while (rows > 0) {
            rows/=Math.max(2, possibleIndex.getSource().getBatchSize());
            depth+=1;
        }
        toReserve = indexSchemaSize * depth;
        if (toReserve < this.joinNode.getBufferManager().getMaxProcessingSize()) {
            useIndex = true;
        } else if (possibleIndex.getRowCount() / this.joinNode.getBatchSize() < preferMemCutoff) {
            useIndex = true;
        }
        if (useIndex) {
            reserved += this.joinNode.getBufferManager().reserveBuffers(toReserve, BufferReserveMode.FORCE);
            if (other.hasBuffer()) {
                other.getTupleBuffer().setForwardOnly(true);
            }
            return true;
        }
        if (joinNode.getJoinType() == JoinType.JOIN_LEFT_OUTER) {
            return false; //repeated is not supported as it could produce multiple outer matches
        }
        this.repeatedMerge = true;
        possibleIndex.setImplicitBuffer(ImplicitBuffer.FULL);
        return true;
    }

    @Override
    protected void process() throws TeiidComponentException,
            TeiidProcessingException {
        if (this.processingSortLeft != SortOption.NOT_SORTED && this.processingSortRight != SortOption.NOT_SORTED) {
            super.process();
            return;
        }
        if (this.sortedSource.getRowCount() == 0 && joinNode.getJoinType() != JoinType.JOIN_LEFT_OUTER) {
            return;
        }
        if (repeatedMerge) {
            while (!this.notSortedSource.getSortUtility().isDoneReading() || this.notSortedSource.hasBuffer()) {
                if (!this.notSortedSource.hasBuffer()) {
                    this.notSortedSource.nextBuffer();
                }
                super.process();
                resetMatchState();
                this.sortedSource.resetState();
                this.notSortedSource.nextBuffer();
            }
            return;
        }
        //else this is a single scan against the index
        if (currentSource == null) {
            this.notSortedSource.setImplicitBuffer(ImplicitBuffer.NONE);
            currentSource = this.notSortedSource.getIterator();
        }
        while (true) {
            if (this.currentTuple == null) {
                currentTuple = this.currentSource.nextTuple();
                matched = false;
                if (currentTuple == null) {
                    return;
                }
                //short-cut when a match is not possible
                if (this.sortedSource.getRowCount() == 0 && joinNode.getJoinType() == JoinType.JOIN_LEFT_OUTER) {
                    outerMatch();
                    continue;
                }
                if (validSemiDep) {
                    List<?> tuple = this.currentTuple;
                    this.currentTuple = null;
                    this.joinNode.addBatchRow(outputTuple(this.leftSource.getOuterVals(), tuple));
                    continue;
                }
                this.keyTs.setValues(this.currentTuple);
                tb.reset(keyTs);
            }
            if (sortedTuple == null) {
                sortedTuple = tb.nextTuple();

                if (sortedTuple == null) {
                    outerMatch();
                    continue;
                }
            }
            List<?> reorderedTuple = RelationalNode.projectTuple(reverseIndexes, sortedTuple);
            List outputTuple = outputTuple(this.processingSortLeft==SortOption.NOT_SORTED?currentTuple:reorderedTuple,
                    this.processingSortLeft==SortOption.NOT_SORTED?reorderedTuple:currentTuple);
            boolean matches = this.joinNode.matchesCriteria(outputTuple);
            this.sortedTuple = null;
            if (matches) {
                if (singleMatch && matched) {
                    throw new ExpressionEvaluationException(QueryPlugin.Event.TEIID31293, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31293));
                }
                matched = true;
                this.joinNode.addBatchRow(outputTuple);
            }
        }
    }

    private void outerMatch() {
        List<?> tuple = currentTuple;
        currentTuple = null;
        if (!matched && joinNode.getJoinType() == JoinType.JOIN_LEFT_OUTER) {
            this.joinNode.addBatchRow(outputTuple(tuple, this.rightSource.getOuterVals()));
        }
    }

    @Override
    public EnhancedSortMergeJoinStrategy clone() {
        EnhancedSortMergeJoinStrategy clone = new EnhancedSortMergeJoinStrategy(this.sortLeft, this.sortRight);
        clone.semiDep = this.semiDep;
        clone.preferMemCutoff = this.preferMemCutoff;
        clone.singleMatch = this.singleMatch;
        return clone;
    }

    @Override
    public String getName() {
        StringBuilder result = new StringBuilder("ENHANCED SORT JOIN"); //$NON-NLS-1$
        if (semiDep) {
            result.append(" [SEMI]"); //$NON-NLS-1$
        }
        if (this.processingSortLeft != null) {
            if (this.processingSortLeft != SortOption.NOT_SORTED && this.processingSortRight != SortOption.NOT_SORTED) {
                result.append(" RAN AS SORT MERGE"); //$NON-NLS-1$
            } else if (repeatedMerge) {
                result.append(" RAN AS REPEATED SORT MERGE"); //$NON-NLS-1$
            }
        }
        return result.toString();
    }

    public void setSemiDep(boolean semiDep) {
        this.semiDep = semiDep;
    }

}
