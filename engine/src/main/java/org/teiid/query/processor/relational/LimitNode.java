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
import java.util.List;

import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.validator.ValidationVisitor;



public class LimitNode extends RelationalNode {

    private final Expression limitExpr;
    private final Expression offsetExpr;
    private int limit;
    private int offset;
    private int rowCounter;
    private boolean offsetPhase = true;
    private boolean implicit;

    public LimitNode(int nodeID, Expression limitExpr, Expression offsetExpr) {
        super(nodeID);
        this.limitExpr = limitExpr;
        this.offsetExpr = offsetExpr;
    }

    public void setImplicit(boolean implicit) {
        this.implicit = implicit;
    }

    public boolean isImplicit() {
        return implicit;
    }

    protected TupleBatch nextBatchDirect() throws BlockedException,
                                          TeiidComponentException,
                                          TeiidProcessingException {
        TupleBatch batch = null; // Can throw BlockedException

        if (limit == 0) {
            this.terminateBatches();
            return pullBatch();
        }

        // If we haven't reached the offset, then skip rows/batches
        if (offsetPhase) {
            while (rowCounter <= offset) {
                batch = getChildren()[0].nextBatch(); // Can throw BlockedException
                rowCounter += batch.getRowCount();
                if (batch.getTerminationFlag()) {
                    break;
                }
            }

            List<List<?>> tuples = null;

            if (rowCounter > offset) {
                List<List<?>> originalTuples = batch.getTuples();
                int rowsToKeep = rowCounter - offset;
                tuples = new ArrayList<List<?>>(originalTuples.subList(batch.getRowCount() - rowsToKeep, batch.getRowCount()));
            } else {
                tuples = Collections.emptyList();
            }
            TupleBatch resultBatch = new TupleBatch(1, tuples);
            resultBatch.setTerminationFlag(batch.getTerminationFlag());
            batch = resultBatch;
            offsetPhase = false;
            rowCounter = 0;
        } else {
            batch = getChildren()[0].nextBatch(); // Can throw BlockedException
        }

        List<List<?>> tuples = null;

        if (limit < 0 || rowCounter + batch.getRowCount() <= limit) {
            // Passthrough
           tuples = batch.getTuples();
        } else {
            // Partial batch
            List<List<?>> originalTuples = batch.getTuples();
            tuples = new ArrayList<List<?>>(originalTuples.subList(0, limit - rowCounter));
        }

        TupleBatch resultBatch = new TupleBatch(rowCounter+1, tuples);

        rowCounter += resultBatch.getRowCount();
        if (rowCounter == limit || batch.getTerminationFlag()) {
            resultBatch.setTerminationFlag(true);
        }
        return resultBatch;
    }

    public void open() throws TeiidComponentException, TeiidProcessingException {
        limit = -1;
        if (limitExpr != null) {
            Integer limitVal = (Integer)new Evaluator(Collections.emptyMap(), getDataManager(), getContext()).evaluate(limitExpr, Collections.emptyList());
            ValidationVisitor.LIMIT_CONSTRAINT.validate(limitVal);
            limit = limitVal.intValue();
        }
        if (limit == 0) {
            return;
        }
        if (offsetExpr != null) {
            Integer offsetVal = (Integer)new Evaluator(Collections.emptyMap(), getDataManager(), getContext()).evaluate(offsetExpr, Collections.emptyList());
            ValidationVisitor.LIMIT_CONSTRAINT.validate(offsetVal);
            offset = offsetVal.intValue();
        } else {
            offset = 0;
        }
        offsetPhase = offset > 0;
        if (limit > -1 && this.getChildren()[0] instanceof SortNode) {
            ((SortNode)this.getChildren()[0]).setRowLimit((int) Math.min(Integer.MAX_VALUE, (long)limit + offset));
        }
        super.open();
    }

    public void reset() {
        super.reset();
        rowCounter = 0;
        offsetPhase = true;
    }

    protected void getNodeString(StringBuffer buf) {
        super.getNodeString(buf);
        if (limitExpr != null) {
            buf.append("limit "); //$NON-NLS-1$
            buf.append(limitExpr);
        }
        if (offsetExpr != null) {
            buf.append(" offset "); //$NON-NLS-1$
            buf.append(offsetExpr);
        }
    }

    public PlanNode getDescriptionProperties() {
        PlanNode props = super.getDescriptionProperties();
        props.addProperty(PROP_ROW_OFFSET, String.valueOf(offsetExpr));
        props.addProperty(PROP_ROW_LIMIT, String.valueOf(limitExpr));
        return props;
    }

    public Object clone() {
        LimitNode node = new LimitNode(getID(), limitExpr, offsetExpr);
        node.implicit = this.implicit;
        copyTo(node);
        node.rowCounter = this.rowCounter;
        return node;
    }

    public Expression getLimitExpr() {
        return limitExpr;
    }

    public Expression getOffsetExpr() {
        return offsetExpr;
    }

    public int getLimit() {
        return limit;
    }

    public int getOffset() {
        return offset;
    }

    @Override
    public boolean hasBuffer() {
        //TODO: support offset
        return offsetExpr == null && this.getChildren()[0].hasBuffer();
    }

    @Override
    public TupleBuffer getBufferDirect(int maxRows) throws BlockedException,
            TeiidComponentException, TeiidProcessingException {
        if (maxRows >= 0) {
            if (limit >= 0) {
                maxRows = Math.min(maxRows, limit);
            }
        } else {
            maxRows = limit;
        }
        return this.getChildren()[0].getBuffer(maxRows);
    }

}
