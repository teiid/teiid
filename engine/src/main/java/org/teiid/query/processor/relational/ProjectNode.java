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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.sql.symbol.AliasSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.SingleElementSymbol;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.util.CommandContext;


public class ProjectNode extends SubqueryAwareRelationalNode {

	private List<? extends SingleElementSymbol> selectSymbols;

    // Derived element lookup map
    private Map elementMap;
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
    public List<? extends SingleElementSymbol> getSelectSymbols() {
        return this.selectSymbols;
    }

	public void setSelectSymbols(List<? extends SingleElementSymbol> symbols) {
		this.selectSymbols = symbols;
	}
	
	@Override
	public void initialize(CommandContext context, BufferManager bufferManager,
			ProcessorDataManager dataMgr) {
		super.initialize(context, bufferManager, dataMgr);

        // Do this lazily as the node may be reset and re-used and this info doesn't change
        if(elementMap != null) {
        	return;
        }
    	this.projectionIndexes = new int[this.selectSymbols.size()];
    	Arrays.fill(this.projectionIndexes, -1);
    	
    	this.expressions = new ArrayList<Expression>(this.selectSymbols.size());
    	for (SingleElementSymbol ses : this.selectSymbols) {
			this.expressions.add(SymbolMap.getExpression(ses));
		}
        //in the case of select with no from, there is no child node
        //simply return at this point
        if(this.getChildren()[0] == null){
            elementMap = new HashMap();
            return;
        }

        // Create element lookup map for evaluating project expressions
        List childElements = this.getChildren()[0].getElements();
        this.elementMap = createLookupMap(childElements);

        // Check whether project needed at all - this occurs if:
        // 1. outputMap == null (see previous block)
        // 2. project elements are either elements or aggregate symbols (no processing required)
        // 3. order of input values == order of output values
        needsProject = childElements.size() != getElements().size();
        for(int i=0; i<selectSymbols.size(); i++) {
            SingleElementSymbol symbol = selectSymbols.get(i);
            
            if(symbol instanceof AliasSymbol) {
                Integer index = (Integer) elementMap.get(symbol);
                if(index != null && index.intValue() == i) {
                	projectionIndexes[i] = index;
                    continue;
                }
                symbol = ((AliasSymbol)symbol).getSymbol();
            }

            Integer index = (Integer) elementMap.get(symbol);
            if(index == null || index.intValue() != i) {
                // input / output element order is not the same
                needsProject = true;
            } else {
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
            	currentBatch = new TupleBatch(1, new List[]{Arrays.asList(new Object[] {})});
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
        this.copy(this, clonedNode);
		return clonedNode;
	}

    protected void copy(ProjectNode source, ProjectNode target){
        super.copy(source, target);
        target.selectSymbols = this.selectSymbols;
    }

    public PlanNode getDescriptionProperties() {
    	PlanNode props = super.getDescriptionProperties();
    	AnalysisRecord.addLanaguageObjects(props, PROP_SELECT_COLS, this.selectSymbols);
        return props;
    }
    
}
