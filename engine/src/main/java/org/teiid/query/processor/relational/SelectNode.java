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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.util.CommandContext;


public class SelectNode extends SubqueryAwareRelationalNode {

    private Criteria criteria;
    private Criteria preEvalCriteria;
    private List<Expression> projectedExpressions;
    private boolean shouldEvaluate = false;

    // Derived element lookup map
    private Map<Expression, Integer> elementMap;
    private int[] projectionIndexes;

    private boolean noRows;

    // State if blocked on evaluating a criteria
    private TupleBatch currentBatch;
    private int currentRow = 1;

    protected SelectNode() {
        super();
    }

    public SelectNode(int nodeID) {
        super(nodeID);
    }

    public void reset() {
        super.reset();

        currentBatch = null;
        currentRow = 1;
        noRows = false;
        preEvalCriteria = null;
    }

    public void setCriteria(Criteria criteria) {
        this.criteria = criteria;
    }

    public Criteria getCriteria() { // made public to support change in ProcedurePlanner
        return this.criteria;
    }

    public void setProjectedExpressions(List<Expression> projectedExpressions) {
        this.projectedExpressions = projectedExpressions;
    }

    @Override
    public void initialize(CommandContext context, BufferManager bufferManager,
            ProcessorDataManager dataMgr) {
        super.initialize(context, bufferManager, dataMgr);
        // Create element lookup map for evaluating project expressions
        if(this.elementMap == null) {
            this.elementMap = createLookupMap(this.getChildren()[0].getElements());
            this.projectionIndexes = getProjectionIndexes(this.elementMap, projectedExpressions!=null?projectedExpressions:getElements());
        }
    }

    /**
     * @see org.teiid.query.processor.relational.RelationalNode#nextBatchDirect()
     */
    public TupleBatch nextBatchDirect()
        throws BlockedException, TeiidComponentException, TeiidProcessingException {

        if (noRows) {
            this.terminateBatches();
            return pullBatch();
        }

        if(currentBatch == null) {
            currentBatch = this.getChildren()[0].nextBatch();
        }

        while (currentRow <= currentBatch.getEndRow() && !isBatchFull()) {
            List<?> tuple = currentBatch.getTuple(currentRow);

            if(getEvaluator(this.elementMap).evaluate(this.preEvalCriteria!=null?preEvalCriteria:criteria, tuple)) {
                addBatchRow(projectTuple(this.projectionIndexes, tuple));
            }
            currentRow++;
        }

        if (currentRow > currentBatch.getEndRow()) {
            if(currentBatch.getTerminationFlag()) {
                terminateBatches();
            }
            currentBatch = null;
        }

        return pullBatch();
    }

    protected void getNodeString(StringBuffer str) {
        super.getNodeString(str);
        str.append(criteria);
    }

    public Object clone(){
        SelectNode clonedNode = new SelectNode();
        this.copyTo(clonedNode);
        return clonedNode;
    }

    protected void copyTo(SelectNode target){
        super.copyTo(target);
        target.criteria = criteria;
        target.elementMap = elementMap;
        target.projectionIndexes = projectionIndexes;
        target.projectedExpressions = projectedExpressions;
        target.shouldEvaluate = shouldEvaluate;
    }

    public PlanNode getDescriptionProperties() {
        PlanNode props = super.getDescriptionProperties();
        AnalysisRecord.addLanaguageObjects(props, PROP_CRITERIA, Arrays.asList(this.criteria));
        return props;
    }

    @Override
    public Collection<? extends LanguageObject> getObjects() {
        return Arrays.asList(this.criteria);
    }

    public void setShouldEvaluateExpressions(boolean shouldEvaluate) {
        this.shouldEvaluate = shouldEvaluate;
    }

    @Override
    public void open()
            throws TeiidComponentException, TeiidProcessingException {
        if (shouldEvaluate) {
            preEvalCriteria = QueryRewriter.evaluateAndRewrite((Criteria)criteria.clone(), getEvaluator(elementMap), getContext(), this.getContext().getMetadata());
            if (preEvalCriteria.equals(QueryRewriter.FALSE_CRITERIA) || preEvalCriteria.equals(QueryRewriter.UNKNOWN_CRITERIA)) {
                noRows = true;
                return;
            }
        }
        super.open();
    }

}
