/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package org.teiid.query.processor.relational;

import static org.teiid.query.analysis.AnalysisRecord.*;

import java.util.Collections;
import java.util.List;

import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.sql.symbol.Expression;



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
            
            List[] tuples = null;
            
            if (rowCounter > offset) {
                List[] originalTuples = batch.getAllTuples();
                int rowsToKeep = rowCounter - offset;
                tuples = new List[rowsToKeep];
                System.arraycopy(originalTuples, batch.getRowCount() - rowsToKeep, tuples, 0, tuples.length);
            } else {
                tuples = new List[0]; //empty batch
            }
            TupleBatch resultBatch = new TupleBatch(1, tuples);
            resultBatch.setTerminationFlag(batch.getTerminationFlag());
            batch = resultBatch;
            offsetPhase = false;
            rowCounter = 0;
        } else {
            batch = getChildren()[0].nextBatch(); // Can throw BlockedException
        }
        
        List[] tuples = null;
        
        if (limit < 0 || rowCounter + batch.getRowCount() <= limit) {
            // Passthrough
           tuples = batch.getAllTuples();
        } else {
            // Partial batch
            List[] originalTuples = batch.getAllTuples();
            tuples = new List[limit - rowCounter];
            System.arraycopy(originalTuples, 0, tuples, 0, tuples.length);
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
            limit = limitVal.intValue();
    	}
        if (limit == 0) {
        	return;
        }
        if (offsetExpr != null) {
            Integer offsetVal = (Integer)new Evaluator(Collections.emptyMap(), getDataManager(), getContext()).evaluate(offsetExpr, Collections.emptyList());
            offset = offsetVal.intValue();
        } else {
            offset = 0;
        }
        offsetPhase = offset > 0;
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
        copy(this, node);
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

}
