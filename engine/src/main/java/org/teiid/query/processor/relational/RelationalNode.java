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

import static org.teiid.query.analysis.AnalysisRecord.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.processor.BatchCollector.BatchProducer;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.util.CommandContext;


public abstract class RelationalNode implements Cloneable, BatchProducer {

    static class NodeData {
        int nodeID;
        List<? extends Expression> elements;
        Number estimateNodeCardinality;
        Number setSizeEstimate;
        Number depAccessEstimate;
        Number estimateDepJoinCost;
        Number estimateJoinCost;
    }

    static class ProcessingState {
        CommandContext context;
        BufferManager bufferManager;
        ProcessorDataManager dataMgr;
        int batchSize;
        RelationalNodeStatistics nodeStatistics;
        int beginBatch = 1;
        List batchRows;
        boolean lastBatch;
        boolean closed;

        void reset() {
            this.beginBatch = 1;
            this.batchRows = null;
            this.lastBatch = false;
            this.closed = false;
        }
    }

    private ProcessingState processingState;
    private NodeData data;
    /** The parent of this node, null if root. */
    private RelationalNode parent;

    /** Child nodes, usually just 1 or 2 */
    private RelationalNode[] children = new RelationalNode[2];
    protected int childCount;

    protected RelationalNode() {

    }

    public RelationalNode(int nodeID) {
        this.data = new NodeData();
        this.data.nodeID = nodeID;
    }

    public int getChildCount() {
        return childCount;
    }

    public boolean isLastBatch() {
        return getProcessingState().lastBatch;
    }

    public void setContext(CommandContext context) {
        this.getProcessingState().context = context;
    }

    public void initialize(CommandContext context, BufferManager bufferManager, ProcessorDataManager dataMgr) {
        this.getProcessingState().context = context;
        this.getProcessingState().bufferManager = bufferManager;
        this.getProcessingState().dataMgr = dataMgr;

        if(context.getCollectNodeStatistics()) {
            this.getProcessingState().nodeStatistics = new RelationalNodeStatistics();
        }

        if (getOutputElements() != null) {
            this.getProcessingState().batchSize = bufferManager.getProcessorBatchSize(getOutputElements());
        } else {
            this.getProcessingState().batchSize = bufferManager.getProcessorBatchSize();
        }
    }

    public CommandContext getContext() {
        return this.getProcessingState().context;
    }

    public int getID() {
        return this.data.nodeID;
    }

    public void setID(int nodeID) {
        NodeData newData = new NodeData();
        newData.nodeID = nodeID;
        newData.elements = this.data.elements;
        this.data = newData;
    }

    protected BufferManager getBufferManager() {
        return this.getProcessingState().bufferManager;
    }

    protected ProcessorDataManager getDataManager() {
        return this.getProcessingState().dataMgr;
    }

    protected String getConnectionID() {
        return this.getProcessingState().context.getConnectionId();
    }

    protected int getBatchSize() {
        return this.getProcessingState().batchSize;
    }

    public void reset() {
        for(int i=0; i<children.length; i++) {
            if(children[i] != null) {
                children[i].reset();
            } else {
                break;
            }
        }
        if (this.getProcessingState() != null) {
            this.getProcessingState().reset();
        }
    }

    public void setElements(List<? extends Expression> elements) {
        this.data.elements = elements;
    }

    @Override
    public List<? extends Expression> getOutputElements() {
        return getElements();
    }

    public List<? extends Expression> getElements() {
        return this.data.elements;
    }

    public RelationalNode getParent() {
        return parent;
    }

    public void setParent(RelationalNode parent) {
        this.parent = parent;
    }

    public RelationalNode[] getChildren() {
        return this.children;
    }

    public void addChild(RelationalNode child) {
        // Set parent of child to match
        child.setParent(this);

        if (this.children.length == this.childCount) {
            // No room to add - double size of the array and copy
            RelationalNode[] newChildren = new RelationalNode[children.length * 2];
            System.arraycopy(this.children, 0, newChildren, 0, this.children.length);
            this.children = newChildren;
        }
        this.children[childCount++] = child;
    }

    protected void addBatchRow(List<?> row) {
        if(this.getProcessingState().batchRows == null) {
            this.getProcessingState().batchRows = new ArrayList(this.getProcessingState().batchSize / 4);
        }
        this.getProcessingState().batchRows.add(row);
    }

    protected void terminateBatches() {
        this.getProcessingState().lastBatch = true;
    }

    protected boolean isBatchFull() {
        return (this.getProcessingState().batchRows != null) && (this.getProcessingState().batchRows.size() >= this.getProcessingState().batchSize);
    }

    protected boolean hasPendingRows() {
        return this.getProcessingState().batchRows != null;
    }

    protected TupleBatch pullBatch() {
        TupleBatch batch = null;
        if(this.getProcessingState().batchRows != null) {
            batch = new TupleBatch(this.getProcessingState().beginBatch, this.getProcessingState().batchRows);
            getProcessingState().beginBatch += this.getProcessingState().batchRows.size();
        } else {
            batch = new TupleBatch(this.getProcessingState().beginBatch, Collections.EMPTY_LIST);
        }

        batch.setTerminationFlag(this.getProcessingState().lastBatch);

        // Reset batch state
        this.getProcessingState().batchRows = null;
        this.getProcessingState().lastBatch = false;

        // Return batch
        return batch;
    }

    public void open()
        throws TeiidComponentException, TeiidProcessingException {

        for(int i=0; i<children.length; i++) {
            if(children[i] != null) {
                children[i].open();
            } else {
                break;
            }
        }
    }

    /**
     * Wrapper for nextBatchDirect that does performance timing - callers
     * should always call this rather than nextBatchDirect().
     * @return
     * @throws BlockedException
     * @throws TeiidComponentException
     * @since 4.2
     */
    public final TupleBatch nextBatch() throws BlockedException,  TeiidComponentException, TeiidProcessingException {
        CommandContext context = this.getContext();
        if (context != null && context.isCancelled()) {
            throw new TeiidProcessingException(QueryPlugin.Event.TEIID30160, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30160, getContext().getRequestId()));
        }
        boolean recordStats = context != null && context.getCollectNodeStatistics();
        try {
            while (true) {
                //start timer for this batch
                if(recordStats) {
                    this.getProcessingState().nodeStatistics.startBatchTimer();
                }
                TupleBatch batch = nextBatchDirect();
                if (recordStats) {
                    // stop timer for this batch (normal)
                    this.getProcessingState().nodeStatistics.stopBatchTimer();
                    this.getProcessingState().nodeStatistics.collectCumulativeNodeStats((long)batch.getRowCount(), RelationalNodeStatistics.BATCHCOMPLETE_STOP);
                    if (batch.getTerminationFlag()) {
                        this.getProcessingState().nodeStatistics.collectNodeStats(this.getChildren());
                        //this.nodeStatistics.dumpProperties(this.getClassName());
                    }
                    this.recordBatch(batch);
                    recordStats = false;
                }
                //24663: only return non-zero batches.
                //there have been several instances in the code that have not correctly accounted for non-terminal zero length batches
                //this processing style however against the spirit of batch processing (but was already utilized by Sort and Grouping nodes)
                if (batch.getRowCount() != 0 || batch.getTerminationFlag()) {
                    if (batch.getTerminationFlag()) {
                        close();
                    }
                    return batch;
                }
            }
        } catch (BlockedException e) {
            if(recordStats) {
                // stop timer for this batch (BlockedException)
                this.getProcessingState().nodeStatistics.stopBatchTimer();
                this.getProcessingState().nodeStatistics.collectCumulativeNodeStats(null, RelationalNodeStatistics.BLOCKEDEXCEPTION_STOP);
                recordStats = false;
            }
            throw e;
        } finally {
            if(recordStats) {
                this.getProcessingState().nodeStatistics.stopBatchTimer();
            }
        }
    }

    /**
     * Template method for subclasses to implement.
     * @return
     * @throws BlockedException
     * @throws TeiidComponentException
     * @throws TeiidProcessingException if exception related to user input occured
     * @since 4.2
     */
    protected abstract TupleBatch nextBatchDirect()
        throws BlockedException, TeiidComponentException, TeiidProcessingException;

    public final void close()
        throws TeiidComponentException {

        if (!this.getProcessingState().closed) {
            closeDirect();
            for(int i=0; i<children.length; i++) {
                if(children[i] != null) {
                    children[i].close();
                } else {
                    break;
                }
            }
            this.getProcessingState().closed = true;
        }
    }

    public void closeDirect() {

    }

    /**
     * Check if the node has been already closed
     * @return
     */
    public boolean isClosed() {
        return this.getProcessingState().closed;
    }

    /**
     * Helper method for all the node that will filter the elements needed for the next node.
     */
    public static int[] getProjectionIndexes(Map<? extends Expression, Integer> tupleElements, List<? extends Expression> projectElements) {
        int[] result = new int[projectElements.size()];

        int i = 0;
        for (Expression symbol : projectElements) {
            Integer index = tupleElements.get(symbol);
            if (index == null) {
                throw new TeiidRuntimeException("Planning error. Could not find symbol: " + symbol); //$NON-NLS-1$
            }
            result[i++] = index;
        }

        return result;
    }

    public static <T> List<T> projectTuple(int[] indexes, List<T> tupleValues) {
        return projectTuple(indexes, tupleValues, false);
    }

    public static <T> List<T> projectTuple(int[] indexes, List<T> tupleValues, boolean omitMissing) {
        List<T> projectedTuple = new ArrayList<T>(indexes.length);
        for (int index : indexes) {
            if (omitMissing && index == -1) {
                projectedTuple.add(null);
            } else {
                projectedTuple.add(tupleValues.get(index));
            }
        }

        return projectedTuple;
    }

    /**
     * Useful function to build an element lookup map from an element list.
     * @param elements List of elements
     * @return Map of element to Integer, which is the index
     */
    public static Map<Expression, Integer> createLookupMap(List<? extends Expression> elements) {
        Map<Expression, Integer> lookupMap = new HashMap<Expression, Integer>();
        for(int i=0; i<elements.size(); i++) {
            Expression element = elements.get(i);
            //in some set ops implemented as joins it's possible to have element conflicts
            //across branches if they are constants, we need to track the first as to
            //not reference outer/null values
            lookupMap.putIfAbsent(element, i);
            lookupMap.putIfAbsent(SymbolMap.getExpression(element), i);
        }
        return lookupMap;
    }

    /**
     * For debugging purposes - all intermediate batches should go through this
     * method so we can easily trace data flow through the plan.
     * @param batch Batch being sent
     */
    private void recordBatch(TupleBatch batch) {
        if (!LogManager.isMessageToBeRecorded(org.teiid.logging.LogConstants.CTX_DQP, MessageLevel.TRACE)) {
            return;
        }
        // Print summary
        StringBuffer str = new StringBuffer();
        str.append(getClassName());
        str.append("("); //$NON-NLS-1$
        str.append(getID());
        str.append(") sending "); //$NON-NLS-1$
        str.append(batch);
        str.append("\n"); //$NON-NLS-1$

        // Print batch contents
        for (long row = batch.getBeginRow(); row <= batch.getEndRow(); row++) {
            str.append("\t").append(row).append(": ").append(batch.getTuple(row)).append("\n"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        }
        LogManager.logTrace(org.teiid.logging.LogConstants.CTX_DQP, str.toString());
    }

    // =========================================================================
    //            O V E R R I D D E N    O B J E C T     M E T H O D S
    // =========================================================================

    /**
     * Print plantree structure starting at this node
     * @return String representing this node and all children under this node
     */
    public String toString() {
        StringBuffer str = new StringBuffer();
        getRecursiveString(str, 0);
        return str.toString();
    }

    /**
     * Just print single node to string instead of node+recursive plan.
     * @return String representing just this node
     */
    public String nodeToString() {
        StringBuffer str = new StringBuffer();
        getNodeString(str);
        return str.toString();
    }

    // Define a single tab
    private static final String TAB = "  "; //$NON-NLS-1$

    private void setTab(StringBuffer str, int tabStop) {
        for(int i=0; i<tabStop; i++) {
            str.append(TAB);
        }
    }

    private void getRecursiveString(StringBuffer str, int tabLevel) {
        setTab(str, tabLevel);
        getNodeString(str);
        str.append("\n"); //$NON-NLS-1$

        // Recursively add children at one greater tab level
        for(int i=0; i<children.length; i++) {
            if(children[i] != null) {
                children[i].getRecursiveString(str, tabLevel+1);
            } else {
                break;
            }
        }
    }

    protected void getNodeString(StringBuffer str) {
        str.append(getClassName());
        str.append("("); //$NON-NLS-1$
        str.append(getID());
        str.append(") output="); //$NON-NLS-1$
        str.append(getElements());
        str.append(" "); //$NON-NLS-1$
    }

    /**
     * Helper for the toString to get the class name from the full class name.
     * @return Just the last part which is the class name
     */
    protected String getClassName() {
        return this.getClass().getSimpleName();
    }

    /**
     * All the implementation of Cloneable interface need to implement clone() method.
     * The plan is only clonable in the pre-execution stage, not the execution state
     * (things like program state, result sets, etc). It's only safe to call that method in between query processings,
     * in other words, it's only safe to call clone() on a plan after nextTuple() returns null,
     * meaning the plan has finished processing.
     */
    public abstract Object clone();

    protected void copyTo(RelationalNode target){
        target.data = this.data;

        target.children = new RelationalNode[this.children.length];
        for(int i=0; i<this.children.length; i++) {
            if(this.children[i] != null) {
                target.children[i] = (RelationalNode)this.children[i].clone();
                target.children[i].setParent(target);
            } else {
                break;
            }
        }
        target.childCount = this.childCount;
    }

    public PlanNode getDescriptionProperties() {
        // Default implementation - should be overridden
        PlanNode result = new PlanNode(getClassName());
        result.addProperty(PROP_ID, String.valueOf(getID()));
        result.addProperty(PROP_OUTPUT_COLS, AnalysisRecord.getOutputColumnProperties(this.data.elements));
        if(this.getProcessingState().context != null && this.getProcessingState().context.getCollectNodeStatistics()) {
            result.addProperty(PROP_NODE_STATS_LIST, this.getProcessingState().nodeStatistics.getStatisticsList());
        }
        List<String> costEstimates = this.getCostEstimates();
        if(costEstimates != null) {
            result.addProperty(PROP_NODE_COST_ESTIMATES, costEstimates);
        }
        for(int i=0; i<children.length; i++) {
            if(children[i] != null) {
                result.addProperty("Child " + i, this.children[i].getDescriptionProperties()); //$NON-NLS-1$
            }
        }
        return result;
    }

    /**
     * @return Returns the nodeStatistics.
     * @since 4.2
     */
    public RelationalNodeStatistics getNodeStatistics() {
        return this.getProcessingState().nodeStatistics;
    }

    public void setEstimateNodeCardinality(Number estimateNodeCardinality) {
        this.data.estimateNodeCardinality = estimateNodeCardinality;
    }

    public void setEstimateNodeSetSize(Number setSizeEstimate) {
        this.data.setSizeEstimate = setSizeEstimate;
    }

    public void setEstimateDepAccessCardinality(Number depAccessEstimate) {
        this.data.depAccessEstimate = depAccessEstimate;
    }

    public void setEstimateDepJoinCost(Number estimateDepJoinCost){
        this.data.estimateDepJoinCost = estimateDepJoinCost;
    }

    public void setEstimateJoinCost(Number estimateJoinCost){
        this.data.estimateJoinCost = estimateJoinCost;
    }

    private List<String> getCostEstimates() {
        List<String> costEstimates = new ArrayList<String>();
        if(this.data.estimateNodeCardinality != null) {
            costEstimates.add("Estimated Node Cardinality: "+ this.data.estimateNodeCardinality); //$NON-NLS-1$
        }
        if(this.data.setSizeEstimate != null) {
            costEstimates.add("Estimated Independent Node Produced Set Size: "+ this.data.setSizeEstimate); //$NON-NLS-1$
        }
        if(this.data.depAccessEstimate != null) {
            costEstimates.add("Estimated Dependent Access Cardinality: "+ this.data.depAccessEstimate); //$NON-NLS-1$
        }
        if(this.data.estimateDepJoinCost != null) {
            costEstimates.add("Estimated Dependent Join Cost: "+ this.data.estimateDepJoinCost); //$NON-NLS-1$
        }
        if(this.data.estimateJoinCost != null) {
            costEstimates.add("Estimated Join Cost: "+ this.data.estimateJoinCost); //$NON-NLS-1$
        }
        if(costEstimates.size() <= 0) {
            return null;
        }
        return costEstimates;
    }

    /**
     * @return Returns the estimateNodeCardinality.
     */
    public Number getEstimateNodeCardinality() {
        return this.data.estimateNodeCardinality;
    }

    private final ProcessingState getProcessingState() {
        //construct lazily since not all tests call initialize
        if (this.processingState == null) {
            this.processingState = new ProcessingState();
        }
        return processingState;
    }

    /**
     * Return true if the node provides a final buffer via getBuffer
     */
    public boolean hasBuffer() {
        return false;
    }

    /**
     * return the final tuple buffer or null if not available
     * @return
     * @throws TeiidProcessingException
     * @throws TeiidComponentException
     * @throws BlockedException
     */
    public final TupleBuffer getBuffer(int maxRows) throws BlockedException, TeiidComponentException, TeiidProcessingException {
        CommandContext context = this.getContext();
        if (context != null && context.isCancelled()) {
            throw new TeiidProcessingException(QueryPlugin.Event.TEIID30160, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30160, getContext().getRequestId()));
        }
        boolean recordStats = context != null && context.getCollectNodeStatistics() && !isLastBatch();
        try {
            //start timer for this batch
            if(recordStats) {
                this.getProcessingState().nodeStatistics.startBatchTimer();
            }
            TupleBuffer buffer = getBufferDirect(maxRows);
            terminateBatches();
            if (recordStats) {
                // stop timer for this batch (normal)
                this.getProcessingState().nodeStatistics.stopBatchTimer();
                this.getProcessingState().nodeStatistics.collectCumulativeNodeStats(buffer.getRowCount(), RelationalNodeStatistics.BATCHCOMPLETE_STOP);
                this.getProcessingState().nodeStatistics.collectNodeStats(this.getChildren());
                if (LogManager.isMessageToBeRecorded(org.teiid.logging.LogConstants.CTX_DQP, MessageLevel.TRACE) && !buffer.isForwardOnly()) {
                    for (long i = 1; i <= buffer.getRowCount(); i+=buffer.getBatchSize()) {
                        TupleBatch tb = buffer.getBatch(i);
                        recordBatch(tb);
                    }
                }

                recordStats = false;
            }
            return buffer;
        } catch (BlockedException e) {
            if(recordStats) {
                // stop timer for this batch (BlockedException)
                this.getProcessingState().nodeStatistics.stopBatchTimer();
                this.getProcessingState().nodeStatistics.collectCumulativeNodeStats(null, RelationalNodeStatistics.BLOCKEDEXCEPTION_STOP);
                recordStats = false;
            }
            throw e;
        } finally {
            if(recordStats) {
                this.getProcessingState().nodeStatistics.stopBatchTimer();
            }
        }
    }

    /**
     * For subclasses to override if they wish to return a buffer rather than batches.
     * @param maxRows
     * @return
     * @throws BlockedException
     * @throws TeiidComponentException
     * @throws TeiidProcessingException
     */
    protected TupleBuffer getBufferDirect(int maxRows) throws BlockedException, TeiidComponentException, TeiidProcessingException {
        return null;
    }

    public static void unwrapException(TeiidRuntimeException e)
    throws TeiidComponentException, TeiidProcessingException {
        if (e == null) {
            return;
        }
        if (e.getCause() instanceof TeiidComponentException) {
            throw (TeiidComponentException)e.getCause();
        }
        if (e.getCause() instanceof TeiidProcessingException) {
            throw (TeiidProcessingException)e.getCause();
        }
        throw e;
    }

    /**
     * @param transactionalReads
     * @return true if required, false if not required, and null if a single source command is issued and a transaction may be needed.
     */
    public Boolean requiresTransaction(boolean transactionalReads) {
        return false;
    }

}