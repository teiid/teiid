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

package com.metamatrix.query.processor.relational;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.api.exception.query.ExpressionEvaluationException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.query.execution.QueryExecPlugin;
import com.metamatrix.query.processor.ProcessorDataManager;
import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.sql.symbol.AggregateSymbol;
import com.metamatrix.query.sql.symbol.AliasSymbol;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.ExpressionSymbol;
import com.metamatrix.query.sql.symbol.SelectSymbol;
import com.metamatrix.query.util.CommandContext;
import com.metamatrix.query.util.ErrorMessageKeys;

public class ProjectNode extends SubqueryAwareRelationalNode {

	private List selectSymbols;

    // Derived element lookup map
    private Map elementMap;
    private boolean needsProject = true;

    // Saved state when blocked on evaluating a row - must be reset
    private TupleBatch currentBatch;
    private int currentRow;

	public ProjectNode(int nodeID) {
		super(nodeID);
	}

    public void reset() {
        super.reset();

        currentBatch = null;
        currentRow = 0;
    }

    /**
     * return List of select symbols
     * @return List of select symbols
     */
    public List getSelectSymbols() {
        return this.selectSymbols;
    }

	public void setSelectSymbols(List symbols) {
		this.selectSymbols = symbols;
	}
	
	@Override
	public void initialize(CommandContext context, BufferManager bufferManager,
			ProcessorDataManager dataMgr) {
		super.initialize(context, bufferManager, dataMgr);

        // Do this lazily as the node may be reset and re-used and this info doesn't change
        if(elementMap == null) {
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
            if(childElements.size() > 0) {
                // Start by assuming project is not needed
                needsProject = false;
                
                if(childElements.size() != getElements().size()) {
                    needsProject = true;                    
                } else {
                    for(int i=0; i<selectSymbols.size(); i++) {
                        SelectSymbol symbol = (SelectSymbol) selectSymbols.get(i);
                        
                        if(symbol instanceof AliasSymbol) {
                            Integer index = (Integer) elementMap.get(symbol);
                            if(index != null && index.intValue() == i) {
                                continue;
                            }
                            symbol = ((AliasSymbol)symbol).getSymbol();
                        }
    
                        if(symbol instanceof ElementSymbol || symbol instanceof AggregateSymbol) {
                            Integer index = (Integer) elementMap.get(symbol);
                            if(index == null || index.intValue() != i) {
                                // input / output element order is not the same
                                needsProject = true;
                                break;
                            }
    
                        } else {
                            // project element is either a constant or a function
                            needsProject = true;
                            break;
                        }
                    }
                }
            }
        }
    }
	
	public TupleBatch nextBatchDirect()
		throws BlockedException, MetaMatrixComponentException, MetaMatrixProcessingException {

        // currentBatch and currentRow hold temporary state saved in the case
        // of a BlockedException while evaluating an expression.  If that has
        // not occurred, currentBatch will be null and currentRow will be < 0.
        // blockedOnPrepare indicates that the BlockedException happened 
        // during the call to prepareToProcessTuple

        TupleBatch batch = this.currentBatch;
        int beginRow = this.currentRow;

        if(batch == null) {
            // There was no saved batch, so get a new one
            //in the case of select with no from, should return only
            //one batch with one row
            if(this.getChildren()[0] == null){
                batch = new TupleBatch(0, new List[]{Arrays.asList(new Object[] {})});
                batch.setTerminationFlag(true);
            }else{
                batch = this.getChildren()[0].nextBatch();
            }

            // Check for no project needed and pass through
            if(batch.getRowCount() == 0 || !needsProject) {
                // Just pass the batch through without processing
                return batch;
            }

            // Set the beginRow based on beginning row of the batch
            beginRow = batch.getBeginRow();

        } else {
            // There was a saved batch, but we grabbed the state so it can now be removed
            this.currentBatch = null;
            this.currentRow = 0;
        }

        for(int row = beginRow; row <= batch.getEndRow(); row++) {
    		List tuple = batch.getTuple(row);

			List projectedTuple = new ArrayList(selectSymbols.size());

			// Walk through symbols
            try {
                for(int i=0; i<selectSymbols.size(); i++) {
    				SelectSymbol symbol = (SelectSymbol) selectSymbols.get(i);
    				updateTuple(symbol, tuple, projectedTuple);
    			}
            } catch(BlockedException e) {
                // Expression blocked, so save state and rethrow
                this.currentBatch = batch;
                this.currentRow = row;
                throw e;
            }

            // Add to batch
            addBatchRow(projectedTuple);
		}

        // Check for termination tuple
        if(batch.getTerminationFlag()) {
            terminateBatches();
        }

        return pullBatch();
	}

	private void updateTuple(SelectSymbol symbol, List values, List tuple)
		throws BlockedException, MetaMatrixComponentException, ExpressionEvaluationException {

        if (symbol instanceof AliasSymbol) {
            // First check AliasSymbol itself
            Integer index = (Integer) elementMap.get(symbol);
            if(index != null) {
                tuple.add(values.get(index.intValue()));
                return;
            }   
            // Didn't find it, so try aliased symbol below
            symbol = ((AliasSymbol)symbol).getSymbol();
        }
        
        Integer index = (Integer) elementMap.get(symbol);
        if(index != null) {
			tuple.add(values.get(index.intValue()));
        } else if(symbol instanceof ExpressionSymbol) {
            Expression expression = ((ExpressionSymbol)symbol).getExpression();
			tuple.add(getEvaluator(this.elementMap).evaluate(expression, values));
        } else {
            Assertion.failed(QueryExecPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0034, symbol.getClass().getName()));
		}
	}

	protected void getNodeString(StringBuffer str) {
		super.getNodeString(str);
		str.append(selectSymbols);
	}

	public Object clone(){
		ProjectNode clonedNode = new ProjectNode(super.getID());
        this.copy(this, clonedNode);
		return clonedNode;
	}

    protected void copy(ProjectNode source, ProjectNode target){
        super.copy(source, target);
        target.selectSymbols = this.selectSymbols;
    }

    /*
     * @see com.metamatrix.query.processor.Describable#getDescriptionProperties()
     */
    public Map getDescriptionProperties() {
        // Default implementation - should be overridden
        Map props = super.getDescriptionProperties();
        props.put(PROP_TYPE, "Project"); //$NON-NLS-1$
        List selectCols = new ArrayList(selectSymbols.size());
        for(int i=0; i<this.selectSymbols.size(); i++) {
            selectCols.add(this.selectSymbols.get(i).toString());
        }
        props.put(PROP_SELECT_COLS, selectCols);

        return props;
    }
    
    @Override
    public Collection<LanguageObject> getLanguageObjects() {
    	return this.getSelectSymbols();
    }
    
}
