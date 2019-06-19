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
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.relational.SourceState.ImplicitBuffer;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.util.CommandContext;


/**
 * @since 4.2
 */
public class JoinNode extends SubqueryAwareRelationalNode {

    static class BatchAvailableException extends RuntimeException {}

    static BatchAvailableException BATCH_AVILABLE = new BatchAvailableException();

    public enum JoinStrategyType {
        MERGE,
        ENHANCED_SORT,
        NESTED_LOOP,
        NESTED_TABLE
    }

    private enum State { LOAD_LEFT, LOAD_RIGHT, EXECUTE }
    private State state = State.LOAD_LEFT;

    private JoinStrategy joinStrategy;
    private JoinType joinType;
    private String dependentValueSource;

    private List leftExpressions;
    private List rightExpressions;
    private boolean leftDistinct;
    private boolean rightDistinct;
    private Criteria joinCriteria;

    private Map combinedElementMap;
    private int[] projectionIndexes;

    private DependentValueSource dvs;

    public JoinNode(int nodeID) {
        super(nodeID);
    }

    public void setJoinType(JoinType type) {
        this.joinType = type;
    }

    public JoinStrategy getJoinStrategy() {
        return this.joinStrategy;
    }

    public void setJoinStrategy(JoinStrategy joinStrategy) {
        this.joinStrategy = joinStrategy;
    }

    public void setJoinExpressions(List leftExpressions, List rightExpressions) {
        this.leftExpressions = leftExpressions;
        this.rightExpressions = rightExpressions;
    }

    public boolean isLeftDistinct() {
        return leftDistinct;
    }

    public void setLeftDistinct(boolean leftDistinct) {
        this.leftDistinct = leftDistinct;
    }

    public boolean isRightDistinct() {
        return rightDistinct;
    }

    public void setRightDistinct(boolean rightDistinct) {
        this.rightDistinct = rightDistinct;
    }

    public void setJoinCriteria(Criteria joinCriteria) {
        this.joinCriteria = joinCriteria;
    }

    @Override
    public void initialize(CommandContext context, BufferManager bufferManager,
            ProcessorDataManager dataMgr) {
        super.initialize(context, bufferManager, dataMgr);

        if (this.combinedElementMap == null) {
            // Create element lookup map for evaluating project expressions
            List combinedElements = new ArrayList(getChildren()[0].getElements());
            combinedElements.addAll(getChildren()[1].getElements());
            this.combinedElementMap = createLookupMap(combinedElements);
            this.projectionIndexes = getProjectionIndexes(combinedElementMap, getElements());
        }
    }

    public void open()
            throws TeiidComponentException, TeiidProcessingException {
        CommandContext context = getContext();
        if (!isDependent()) {
            boolean old = context.setParallel(true);
            try {
                openInternal();
            } finally {
                context.setParallel(old);
            }
        } else {
            openInternal();
        }
    }

    public void openInternal()
        throws TeiidComponentException, TeiidProcessingException {
        // Set Up Join Strategy
        this.joinStrategy.initialize(this);

        if (isDependent() && (this.joinType == JoinType.JOIN_ANTI_SEMI || this.joinType == JoinType.JOIN_SEMI)) {
            joinStrategy.openRight();
        } else {
            joinStrategy.openLeft();
        }

        if(!isDependent()) {
            joinStrategy.openRight();
        }

        this.state = State.LOAD_LEFT;
    }

    /**
     * @see org.teiid.query.processor.relational.RelationalNode#clone()
     * @since 4.2
     */
    public Object clone() {
        JoinNode clonedNode = new JoinNode(super.getID());
        super.copyTo(clonedNode);

        clonedNode.joinType = this.joinType;
        clonedNode.joinStrategy = this.joinStrategy.clone();

        clonedNode.joinCriteria = this.joinCriteria;

        clonedNode.leftExpressions = leftExpressions;

        clonedNode.rightExpressions = rightExpressions;
        clonedNode.dependentValueSource = this.dependentValueSource;
        clonedNode.rightDistinct = rightDistinct;
        clonedNode.leftDistinct = leftDistinct;

        return clonedNode;
    }

    public TupleBatch nextBatchDirect()
            throws TeiidComponentException, TeiidProcessingException {
        CommandContext context = getContext();
        if (!isDependent()) {
            boolean old = context.setParallel(true);
            try {
                return nextBatchDirectInternal();
            } finally {
                context.setParallel(old);
            }
        }
        return nextBatchDirectInternal();
    }

    /**
     * @see org.teiid.query.processor.relational.RelationalNode#nextBatchDirect()
     * @since 4.2
     */
    protected TupleBatch nextBatchDirectInternal() throws BlockedException,
                                          TeiidComponentException,
                                          TeiidProcessingException {
        try {
            if (state == State.LOAD_LEFT) {
                boolean rightDep = false;
                //dependent semi joins are processed right first
                if (isDependent() && (this.joinType == JoinType.JOIN_ANTI_SEMI || this.joinType == JoinType.JOIN_SEMI)) {
                    rightDep = true;
                    this.joinStrategy.openRight();
                    this.joinStrategy.loadRight();
                    TupleBuffer buffer = this.joinStrategy.rightSource.getTupleBuffer();
                    //the tuplebuffer may be from a lower node, so pass in the schema
                    dvs = new DependentValueSource(buffer, this.joinStrategy.rightSource.getSource().getElements());
                    dvs.setDistinct(this.joinStrategy.rightSource.isExpresssionDistinct());
                    this.getContext().getVariableContext().setGlobalValue(this.dependentValueSource, dvs);
                }
                if (this.joinType != JoinType.JOIN_FULL_OUTER || this.getJoinCriteria() == null) {
                    this.joinStrategy.leftSource.setImplicitBuffer(ImplicitBuffer.NONE);
                }
                this.joinStrategy.openLeft();
                this.joinStrategy.loadLeft();
                if (isDependent() && !rightDep) {
                    TupleBuffer buffer = this.joinStrategy.leftSource.getTupleBuffer();
                    //the tuplebuffer may be from a lower node, so pass in the schema
                    dvs = new DependentValueSource(buffer, this.joinStrategy.leftSource.getSource().getElements());
                    dvs.setDistinct(this.joinStrategy.leftSource.isExpresssionDistinct());
                    this.getContext().getVariableContext().setGlobalValue(this.dependentValueSource, dvs);
                }
                state = State.LOAD_RIGHT;
            }
        } catch (BlockedException e) {
            if (!isDependent()) {
                if (getJoinType() != JoinType.JOIN_FULL_OUTER && this.joinStrategy.leftSource.getSortUtility() == null && this.joinStrategy.leftSource.rowCountLE(0)) {
                    this.terminateBatches();
                    return pullBatch();
                }
                this.joinStrategy.openRight();
                this.joinStrategy.loadRight();
                prefetch(this.joinStrategy.rightSource, this.joinStrategy.leftSource);
            }
            throw e;
        }
        try {
            if (state == State.LOAD_RIGHT) {
                if (getJoinType() != JoinType.JOIN_FULL_OUTER && this.joinStrategy.leftSource.getSortUtility() == null && this.joinStrategy.leftSource.rowCountLE(0)) {
                    this.terminateBatches();
                    return pullBatch();
                }
                this.joinStrategy.openRight();
                this.joinStrategy.loadRight();
                state = State.EXECUTE;
            }
            this.joinStrategy.process();
            this.terminateBatches();
        } catch (BatchAvailableException e) {
            //pull the batch
        } catch (BlockedException e) {
            //TODO: this leads to duplicate exceptions, we
            //could track which side is blocking
            try {
                prefetch(this.joinStrategy.leftSource, this.joinStrategy.rightSource);
            } catch (BlockedException e1) {

            }
            prefetch(this.joinStrategy.rightSource, this.joinStrategy.leftSource);
            throw e;
        }
        return pullBatch();
    }

    private void prefetch(SourceState toFetch, SourceState other) throws TeiidComponentException,
            TeiidProcessingException {
        toFetch.prefetch(Math.max(1L, other.getIncrementalRowCount(false)/other.getSource().getBatchSize())*toFetch.getSource().getBatchSize());
    }

    /**
     * @see org.teiid.query.processor.relational.RelationalNode#getDescriptionProperties()
     * @since 4.2
     */
    public PlanNode getDescriptionProperties() {
        // Default implementation - should be overridden
        PlanNode props = super.getDescriptionProperties();

        if(isDependent()) {
            props.addProperty(PROP_DEPENDENT, Boolean.TRUE.toString());
        }
        props.addProperty(PROP_JOIN_STRATEGY, this.joinStrategy.toString());
        props.addProperty(PROP_JOIN_TYPE, this.joinType.toString());
        List<String> critList = getCriteriaList();
        props.addProperty(PROP_JOIN_CRITERIA, critList);
        return props;
    }

    private List<String> getCriteriaList() {
        List<String> critList = new ArrayList<String>();
        if (leftExpressions != null) {
            for(int i=0; i < this.leftExpressions.size(); i++) {
                critList.add(this.leftExpressions.get(i).toString() + "=" + this.rightExpressions.get(i).toString());  //$NON-NLS-1$
            }
        }
        if (this.joinCriteria != null) {
            for (Criteria crit : Criteria.separateCriteriaByAnd(joinCriteria)) {
                critList.add(crit.toString());
            }
        }
        return critList;
    }

    /**
     * @see org.teiid.query.processor.relational.RelationalNode#getNodeString(java.lang.StringBuffer)
     * @since 4.2
     */
    protected void getNodeString(StringBuffer str) {
        str.append(getClassName());
        str.append("("); //$NON-NLS-1$
        str.append(getID());
        str.append(") [");//$NON-NLS-1$
        if(isDependent()) {
            str.append("Dependent] [");//$NON-NLS-1$
        }
        str.append(this.joinStrategy.toString());
        str.append("] [");//$NON-NLS-1$
        str.append(this.joinType.toString());
        str.append("]"); //$NON-NLS-1$
        if (getJoinType() != JoinType.JOIN_CROSS) {
            str.append(" criteria=").append(getCriteriaList()); //$NON-NLS-1$
        }
        str.append(" output="); //$NON-NLS-1$
        str.append(getElements());
    }

    /**
     * @return Returns the isDependent.
     */
    public boolean isDependent() {
        return this.dependentValueSource != null;
    }

    public void setDependentValueSource(String dependentValueSource) {
        this.dependentValueSource = dependentValueSource;
    }

    public String getDependentValueSourceName() {
        return dependentValueSource;
    }

    public void closeDirect() {
        super.closeDirect();
        joinStrategy.close();
        if (this.getContext() != null && this.dependentValueSource != null) {
            this.getContext().getVariableContext().setGlobalValue(this.dependentValueSource, null);
        }
        this.dvs = null;
    }

    @Override
    public void reset() {
        super.reset();
        this.joinStrategy = this.joinStrategy.clone();
        this.dvs = null;
    }

    public JoinType getJoinType() {
        return this.joinType;
    }

    Map getCombinedElementMap() {
        return this.combinedElementMap;
    }

    public Criteria getJoinCriteria() {
        return this.joinCriteria;
    }

    boolean matchesCriteria(List outputTuple) throws BlockedException, TeiidComponentException, ExpressionEvaluationException {
        return (this.joinCriteria == null || getEvaluator(this.combinedElementMap).evaluate(this.joinCriteria, outputTuple));
    }

    public List getLeftExpressions() {
        return this.leftExpressions;
    }

    public List getRightExpressions() {
        return this.rightExpressions;
    }

    @Override
    protected void addBatchRow(List row) {
        List projectTuple = projectTuple(this.projectionIndexes, row);
        super.addBatchRow(projectTuple);
        if (isBatchFull()) {
            throw BATCH_AVILABLE;
        }
    }

    public DependentValueSource getDependentValueSource() {
        return dvs;
    }

    @Override
    public Collection<? extends LanguageObject> getObjects() {
        List<LanguageObject> all = new ArrayList<LanguageObject>();
        if (leftExpressions != null) {
            all.addAll(leftExpressions);
            all.addAll(rightExpressions);
        }
        if (joinCriteria != null) {
            all.add(joinCriteria);
        }
        return all;
    }

}
