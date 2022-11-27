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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.symbol.AliasSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.util.SymbolMap;


public class ProjectNode extends SubqueryAwareRelationalNode {

    private static final List<?>[] EMPTY_TUPLE = new List[]{Arrays.asList(new Object[] {})};

    private List<? extends Expression> selectSymbols;

    // Derived element lookup map
    private Map<Expression, Integer> elementMap;
    private boolean needsProject = true;
    private List<Expression> expressions;
    private int[] projectionIndexes;

    // Saved state when blocked on evaluating a row - must be reset
    private TupleBatch currentBatch;
    private int currentRow = 1;

    protected ProjectNode() {
        super();
    }

    public ProjectNode(int nodeID) {
        super(nodeID);
    }

    public void reset() {
        super.reset();

        currentBatch = null;
        currentRow = 1;
    }

    /**
     * return List of select symbols
     * @return List of select symbols
     */
    public List<? extends Expression> getSelectSymbols() {
        return this.selectSymbols;
    }

    public void setSelectSymbols(List<? extends Expression> symbols) {
        this.selectSymbols = symbols;
        elementMap = Collections.emptyMap();
        this.projectionIndexes = new int[this.selectSymbols.size()];
        Arrays.fill(this.projectionIndexes, -1);

        this.expressions = new ArrayList<Expression>(this.selectSymbols.size());
        for (Expression ses : this.selectSymbols) {
            this.expressions.add(SymbolMap.getExpression(ses));
        }
    }

    @Override
    public void addChild(RelationalNode child) {
        super.addChild(child);
        init();
    }

    void init() {
        List<? extends Expression> childElements = getChildren()[0].getElements();
        // Create element lookup map for evaluating project expressions
        this.elementMap = createLookupMap(childElements);

        // Check whether project needed at all - this occurs if:
        // 1. outputMap == null (see previous block)
        // 2. project elements are either elements or aggregate symbols (no processing required)
        // 3. order of input values == order of output values
        needsProject = childElements.size() != selectSymbols.size();
        for(int i=0; i<selectSymbols.size(); i++) {
            Expression symbol = selectSymbols.get(i);

            if(symbol instanceof AliasSymbol) {
                Integer index = elementMap.get(symbol);
                if(index != null && index.intValue() == i) {
                    projectionIndexes[i] = index;
                    continue;
                }
                symbol = ((AliasSymbol)symbol).getSymbol();
            }

            Integer index = elementMap.get(symbol);
            if(index == null) {
                needsProject = true;
            } else {
                if (index.intValue() != i) {
                    needsProject = true;
                }
                projectionIndexes[i] = index;
            }
        }
    }

    public TupleBatch nextBatchDirect()
        throws BlockedException, TeiidComponentException, TeiidProcessingException {

        if(currentBatch == null) {
            // There was no saved batch, so get a new one
            //in the case of select with no from, should return only
            //one batch with one row
            if(this.getChildren()[0] == null){
                currentBatch = new TupleBatch(1, EMPTY_TUPLE);
                currentBatch.setTerminationFlag(true);
            }else{
                currentBatch = this.getChildren()[0].nextBatch();
            }

            // Check for no project needed and pass through
            if(!needsProject) {
                TupleBatch result = currentBatch;
                currentBatch = null;
                return result;
            }
        }

        while (currentRow <= currentBatch.getEndRow() && !isBatchFull()) {
            List<?> tuple = currentBatch.getTuple(currentRow);

            List<Object> projectedTuple = new ArrayList<Object>(selectSymbols.size());

            // Walk through symbols
            for(int i=0; i<expressions.size(); i++) {
                Expression symbol = expressions.get(i);
                updateTuple(symbol, i, tuple, projectedTuple);
            }

            // Add to batch
            addBatchRow(projectedTuple);
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

    private void updateTuple(Expression symbol, int projectionIndex, List<?> values, List<Object> tuple)
        throws BlockedException, TeiidComponentException, ExpressionEvaluationException {

        int index = this.projectionIndexes[projectionIndex];
        if(index != -1) {
            tuple.add(values.get(index));
        } else {
            tuple.add(getEvaluator(this.elementMap).evaluate(symbol, values));
        }
    }

    protected void getNodeString(StringBuffer str) {
        super.getNodeString(str);
        str.append(selectSymbols);
    }

    public Object clone(){
        ProjectNode clonedNode = new ProjectNode();
        this.copyTo(clonedNode);
        return clonedNode;
    }

    protected void copyTo(ProjectNode target){
        super.copyTo(target);
        target.selectSymbols = this.selectSymbols;
        target.needsProject = needsProject;
        target.elementMap = elementMap;
        target.expressions = expressions;
        target.projectionIndexes = projectionIndexes;
    }

    public PlanNode getDescriptionProperties() {
        PlanNode props = super.getDescriptionProperties();
        AnalysisRecord.addLanaguageObjects(props, PROP_SELECT_COLS, this.selectSymbols);
        return props;
    }

    @Override
    public Collection<? extends LanguageObject> getObjects() {
        return this.selectSymbols;
    }

    @Override
    public boolean hasBuffer() {
        return !needsProject && this.getChildren()[0].hasBuffer();
    }

    @Override
    public TupleBuffer getBufferDirect(int maxRows) throws BlockedException,
            TeiidComponentException, TeiidProcessingException {
        return this.getChildren()[0].getBuffer(maxRows);
    }

}
