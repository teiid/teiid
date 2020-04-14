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

import java.util.List;
import java.util.Map;

import org.teiid.common.buffer.IndexedTupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.processor.relational.SourceState.ImplicitBuffer;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.util.SymbolMap;

/**
 * Variation of a nested loop join that handles nested tables
 */
public class NestedTableJoinStrategy extends JoinStrategy {

    private SymbolMap rightMap;
    private Evaluator eval;
    private boolean outerMatched;

    @Override
    public NestedTableJoinStrategy clone() {
        NestedTableJoinStrategy clone = new NestedTableJoinStrategy();
        clone.rightMap = rightMap;
        return clone;
    }

    @Override
    public void initialize(JoinNode joinNode) {
        super.initialize(joinNode);
        this.eval = new Evaluator(null, joinNode.getDataManager(), joinNode.getContext());
    }

    public void setRightMap(SymbolMap rightMap) {
        this.rightMap = rightMap;
    }

    @Override
    protected void openRight() throws TeiidComponentException,
            TeiidProcessingException {
        if (rightMap == null) {
            super.openRight();
            this.rightSource.setImplicitBuffer(ImplicitBuffer.FULL);
        }
    }

    @Override
    protected void process() throws TeiidComponentException,
            TeiidProcessingException {

        IndexedTupleSource its = leftSource.getIterator();

        while (its.hasNext() || leftSource.getCurrentTuple() != null) {

            List<?> leftTuple = leftSource.getCurrentTuple();
            if (leftTuple == null) {
                leftTuple = leftSource.saveNext();
            }
            updateContext(leftTuple, leftSource.getSource().getElements());

            if (rightMap != null && !rightSource.open) {
                for (Map.Entry<ElementSymbol, Expression> entry : rightMap.asMap().entrySet()) {
                    joinNode.getContext().getVariableContext().setValue(entry.getKey(), eval.evaluate(entry.getValue(), null));
                }
                rightSource.getSource().reset();
                super.openRight();
            }

            IndexedTupleSource right = rightSource.getIterator();

            while (right.hasNext() || rightSource.getCurrentTuple() != null) {

                List<?> rightTuple = rightSource.getCurrentTuple();
                if (rightTuple == null) {
                    rightTuple = rightSource.saveNext();
                }

                List<?> outputTuple = outputTuple(this.leftSource.getCurrentTuple(), this.rightSource.getCurrentTuple());

                boolean matches = this.joinNode.matchesCriteria(outputTuple);

                rightSource.saveNext();

                if (matches) {
                    outerMatched = true;
                    joinNode.addBatchRow(outputTuple);
                }
            }

            try {
                if (!outerMatched && this.joinNode.getJoinType() == JoinType.JOIN_LEFT_OUTER) {
                    joinNode.addBatchRow(outputTuple(this.leftSource.getCurrentTuple(), this.rightSource.getOuterVals()));
                }
            } finally {
                outerMatched = false;

                if (rightMap == null) {
                    rightSource.getIterator().setPosition(1);
                } else {
                    rightSource.close();
                    for (Map.Entry<ElementSymbol, Expression> entry : rightMap.asMap().entrySet()) {
                        joinNode.getContext().getVariableContext().remove(entry.getKey());
                    }
                }

                leftSource.saveNext();
                updateContext(null, leftSource.getSource().getElements());
            }
        }

    }

    private void updateContext(List<?> tuple,
            List<? extends Expression> elements) {
        for (int i = 0; i < elements.size(); i++) {
            Expression element = elements.get(i);
            if (element instanceof ElementSymbol) {
                if (tuple == null) {
                    joinNode.getContext().getVariableContext().remove(element);
                } else {
                    joinNode.getContext().getVariableContext().setValue(element, tuple.get(i));
                }
            }
        }
    }

    public String toString() {
        return "NESTED TABLE JOIN"; //$NON-NLS-1$
    }

}
