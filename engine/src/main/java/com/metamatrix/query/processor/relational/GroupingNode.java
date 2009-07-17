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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.api.exception.query.ExpressionEvaluationException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.IndexedTupleSource;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.common.buffer.TupleSourceNotFoundException;
import com.metamatrix.common.buffer.BufferManager.TupleSourceStatus;
import com.metamatrix.common.buffer.BufferManager.TupleSourceType;
import com.metamatrix.query.eval.Evaluator;
import com.metamatrix.query.function.aggregate.AggregateFunction;
import com.metamatrix.query.function.aggregate.Avg;
import com.metamatrix.query.function.aggregate.ConstantFunction;
import com.metamatrix.query.function.aggregate.Count;
import com.metamatrix.query.function.aggregate.Max;
import com.metamatrix.query.function.aggregate.Min;
import com.metamatrix.query.function.aggregate.NullFilter;
import com.metamatrix.query.function.aggregate.Sum;
import com.metamatrix.query.processor.relational.SortUtility.Mode;
import com.metamatrix.query.sql.ReservedWords;
import com.metamatrix.query.sql.lang.OrderBy;
import com.metamatrix.query.sql.symbol.AggregateSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;
import com.metamatrix.query.util.TypeRetrievalUtil;

public class GroupingNode extends RelationalNode {

    // Grouping columns set by the planner 
	private List sortElements;
	private List sortTypes;
	private boolean removeDuplicates;
    
    // Collection phase
    private int phase = COLLECTION;
    private Map elementMap;                    // Map of incoming symbol to index in source elements
    private List collectedExpressions;         // Collected Expressions
    private TupleBatch sourceBatch;           // Current batch loaded from the source, if blocked
    private int sourceRow;                    // Current row index in batch, if blocked
    private List[] collectedRows;             // Rows evaluated and collected 
    private TupleSourceID collectionID;       // Tuple source collecting the endpoint of collection phase
    private int rowCount;                     // Row count of incoming rows
       
    // Sort phase
    private SortUtility sortUtility;
    private TupleSourceID sortedID;

    // Group phase
    private Map expressionMap;                 // Index map for all collected expressions (Expression -> index in collectedExpressions)
    private AggregateFunction[] functions;
    private IndexedTupleSource groupTupleSource;
    private int groupBegin = 1;
    private List lastRow;
	private List currentGroupTuple;

    private static final int COLLECTION = 1;
    private static final int SORT = 2;
    private static final int GROUP = 3;

	public GroupingNode(int nodeID) {
		super(nodeID);
	}

    public void reset() {
        super.reset();

        phase = COLLECTION;
        elementMap = null;
        collectedExpressions = null;
        sourceBatch = null;
        sourceRow = -1;
        collectedRows = null;
        collectionID = null;
        rowCount = 0;
                
        sortUtility = null;
        sortedID = null;
        
        expressionMap = null;
        functions = null;
        groupBegin = 1;
        groupTupleSource = null;
        lastRow = null;
        currentGroupTuple = null;
    }
    
    public void setRemoveDuplicates(boolean removeDuplicates) {
		this.removeDuplicates = removeDuplicates;
	}

    /**
     * Called by the planner to initialize the grouping node.  Set the list of grouping
     * expressions - these may be either elements or expressions and the list itself may
     * be null to indicate an implied grouping on all columns
     * @param groupingElements
     * @since 4.2
     */
    public void setGroupingElements(List groupingElements) {
        this.sortElements = groupingElements;
        if(groupingElements != null) {
            sortTypes = new ArrayList(groupingElements.size());
            for(int i=0; i<groupingElements.size(); i++) {
                sortTypes.add(Boolean.valueOf(OrderBy.ASC));
            }
        }
    }

	public void open()
		throws MetaMatrixComponentException, MetaMatrixProcessingException {

		super.open();

        // Incoming elements and lookup map for evaluating expressions
        List sourceElements = this.getChildren()[0].getElements();
        this.elementMap = createLookupMap(sourceElements);
        
        // Determine expressions to build (all grouping expressions + expressions used by aggregates)   
        collectExpressions();

        this.collectionID = getBufferManager().createTupleSource(collectedExpressions, TypeRetrievalUtil.getTypeNames(collectedExpressions), getConnectionID(), TupleSourceType.PROCESSOR);

        initializeFunctionAccumulators();
	}

    /** 
     * Collect a list of all expressions that must be evaluated during collection.  This
     * will include all the expressions being sorted on AND all expressions used within 
     * aggregate functions.
     * 
     * @since 4.2
     */
    private void collectExpressions() {
        // List should contain all grouping columns / expressions as we need those for sorting
        if(this.sortElements != null) {
            this.collectedExpressions = new ArrayList(this.sortElements.size() + getElements().size());
            this.collectedExpressions.addAll(sortElements);
        } else {
            this.collectedExpressions = new ArrayList(getElements().size());
        }
        
        // Also need to include all expressions used within aggregates so that we can evaluate 
        // them once up front during collection rather than repeatedly during aggregate evaluation
        Iterator outputIter = getElements().iterator();
        while(outputIter.hasNext()) {
            Object outputSymbol = outputIter.next();
            if(outputSymbol instanceof AggregateSymbol) {
                AggregateSymbol agg = (AggregateSymbol) outputSymbol;
                Expression expr = agg.getExpression();
                if(expr != null && ! this.collectedExpressions.contains(expr)) {
                    this.collectedExpressions.add(expr);
                }
            }
        }
        
        // Build lookup map for evaluating aggregates later
        this.expressionMap = createLookupMap(collectedExpressions);
    }

    private void initializeFunctionAccumulators() {
        // Construct aggregate function state accumulators
        functions = new AggregateFunction[getElements().size()];
        for(int i=0; i<getElements().size(); i++) {
            SingleElementSymbol symbol = (SingleElementSymbol)getElements().get(i);
            Class<?> outputType = symbol.getType();
            Class<?> inputType = symbol.getType();
            if(symbol instanceof AggregateSymbol) {
                AggregateSymbol aggSymbol = (AggregateSymbol) symbol;

                if(aggSymbol.getExpression() == null) {
                    functions[i] = new Count();
                } else {
                    String function = aggSymbol.getAggregateFunction();
                    if(function.equals(ReservedWords.COUNT)) {
                        functions[i] = new Count();
                    } else if(function.equals(ReservedWords.SUM)) {
                        functions[i] = new Sum();
                    } else if(function.equals(ReservedWords.AVG)) {
                        functions[i] = new Avg();
                    } else if(function.equals(ReservedWords.MIN)) {
                        functions[i] = new Min();
                    } else {
                        functions[i] = new Max();
                    }

                    if(aggSymbol.isDistinct()) {
                        functions[i] = new DuplicateFilter(functions[i], getBufferManager(), getConnectionID(), getBatchSize());
                    }
                    
                    functions[i] = new NullFilter(functions[i]);
                    outputType = aggSymbol.getType();
                }
            } else {
                functions[i] = new ConstantFunction();
            }
            functions[i].initialize(outputType, inputType);
        }
    }    

	public TupleBatch nextBatchDirect()
		throws BlockedException, MetaMatrixComponentException, MetaMatrixProcessingException {

        try {
            // Take inputs, evaluate expressions, and build initial tuple source
            if(this.phase == COLLECTION) {
                collectionPhase();
            }

            // If necessary, sort to determine groups (if no group cols, no need to sort)
            if(this.phase == SORT) {
                sortPhase();
            }

            // Walk through the sorted results and for each group, emit a row
            if(this.phase == GROUP) {
                return groupPhase();
            }
            
            TupleBatch terminationBatch = new TupleBatch(1, Collections.EMPTY_LIST);
            terminationBatch.setTerminationFlag(true);
            return terminationBatch;
        } catch(TupleSourceNotFoundException e) {
            throw new MetaMatrixComponentException(e, e.getMessage());
        }

    }

    private void collectionPhase() throws BlockedException, TupleSourceNotFoundException, MetaMatrixComponentException, MetaMatrixProcessingException {
        RelationalNode sourceNode = this.getChildren()[0];

        while(true) {
            if(this.sourceBatch == null) {
                // Read next batch
                this.sourceBatch = sourceNode.nextBatch();
            }
            
            if(this.sourceBatch.getRowCount() > 0) {
                this.rowCount = this.sourceBatch.getEndRow();
                this.sourceRow = this.sourceBatch.getBeginRow();
                this.collectedRows = new List[this.sourceBatch.getRowCount()];
                // Evaluate expressions needed for grouping
                for(int row=this.sourceRow; row<=this.sourceBatch.getEndRow(); row++) {
                    List tuple = this.sourceBatch.getTuple(row);
                    
                    int columns = this.collectedExpressions.size();
                    List exprTuple = new ArrayList(columns);
                    for(int col = 0; col<columns; col++) { 
                        // The following call may throw BlockedException, but all state to this point
                        // is saved in class variables so we can start over on building this tuple
                        Object value = new Evaluator(this.elementMap, getDataManager(), getContext()).evaluate((Expression) this.collectedExpressions.get(col), tuple);
                        exprTuple.add(value);
                    }
                    
                    collectedRows[row-this.sourceBatch.getBeginRow()] = exprTuple;
                }
                
                // Add collected batch
                TupleBatch exprBatch = new TupleBatch(this.sourceBatch.getBeginRow(), collectedRows);
                getBufferManager().addTupleBatch(collectionID, exprBatch);                    
            }
            // Clear and setup for next loop
            this.sourceRow = -1;
            this.collectedRows = null;

            // Check for termination condition
            if(this.sourceBatch.getTerminationFlag()) {
                this.sourceBatch = null;
                break;
            } 
            
            this.sourceBatch = null;
        }

    	this.getBufferManager().setStatus(this.collectionID, TupleSourceStatus.FULL);

        if(this.sortElements == null || this.rowCount == 0) {
            // No need to sort
            this.sortedID = this.collectionID;
            this.collectionID = null;
            this.phase = GROUP;
        } else {
            this.sortUtility = new SortUtility(collectionID, sortElements,
                                                sortTypes, removeDuplicates?Mode.DUP_REMOVE_SORT:Mode.SORT, getBufferManager(),
                                                getConnectionID(), removeDuplicates);
            this.phase = SORT;
        }
    }

    private void sortPhase() throws BlockedException, MetaMatrixComponentException, MetaMatrixProcessingException {
        this.sortedID = this.sortUtility.sort();
        this.rowCount = this.getBufferManager().getFinalRowCount(this.sortedID);
        this.phase = GROUP;
    }

    private TupleBatch groupPhase() throws BlockedException, TupleSourceNotFoundException, MetaMatrixComponentException, MetaMatrixProcessingException {
        if (this.groupTupleSource == null) {
            this.groupTupleSource = this.getBufferManager().getTupleSource(sortedID);
        }
        
        while(groupBegin <= rowCount) {

        	if (currentGroupTuple == null) {
        		currentGroupTuple = this.groupTupleSource.nextTuple();
        	}
        	
            if(lastRow == null) {
                // First row we've seen
                lastRow = currentGroupTuple;

            } else if(! sameGroup(currentGroupTuple, lastRow)) {
                // Close old group
                List row = new ArrayList(functions.length);
                for(int i=0; i<functions.length; i++) {
                    row.add( functions[i].getResult() );
                }

                // Start a new group
                for(int i=0; i<functions.length; i++) {
                    functions[i].reset();
                }

                // Reset last tuple
                lastRow = currentGroupTuple;
                
                // Save in output batch
                addBatchRow(row);
                if (this.isBatchFull()) {
                	return pullBatch();
                }
            }

            // Update function accumulators with new row - can throw blocked exception
            updateAggregates(currentGroupTuple);
            currentGroupTuple = null;
            groupBegin++;
        }
        
        if(rowCount != 0 || sortElements == null) {
            // Close last group
            List row = new ArrayList(functions.length);
            for(int i=0; i<functions.length; i++) {
                row.add( functions[i].getResult() );
            }
            
            addBatchRow(row);
        } 

        this.terminateBatches();
        return pullBatch();
    }

    private boolean sameGroup(List newTuple, List oldTuple) {
        // Check for no grouping columns
        if(sortElements == null) {
            return true;
        }

        // Walk backwards through sort cols as the last columns are most likely to be different
        for(int i=sortElements.size()-1; i>=0; i--) {
            Object oldValue = oldTuple.get(i);
            Object newValue = newTuple.get(i);

            if(oldValue == null) {
                if(newValue != null) {
                    return false;
                }
            } else {
                if(newValue == null) {
                    return false;
                }
                if(! oldValue.equals(newValue)) {
                    return false;
                }
            }
        }

        return true;
    }

    private void updateAggregates(List tuple)
    throws MetaMatrixComponentException, ExpressionEvaluationException {

        for(int i=0; i<functions.length; i++) {
            Expression expression = (SingleElementSymbol) getElements().get(i);
            if(expression instanceof AggregateSymbol) {
                expression = ((AggregateSymbol)expression).getExpression();
            }

            Object value = null;
            if(expression != null) {
                Integer exprIndex = (Integer)expressionMap.get(expression);
                value = tuple.get(exprIndex.intValue());
            }
            functions[i].addInput(value);
        }
    }

    public void close() throws MetaMatrixComponentException {
        if (!isClosed()) {
            super.close();
            try {
                if(this.collectionID != null) {
                    getBufferManager().removeTupleSource(collectionID);
                }
                if(this.sortedID != null) {
                    getBufferManager().removeTupleSource(sortedID);
                }
            } catch(TupleSourceNotFoundException e) {
                throw new MetaMatrixComponentException(e, e.getMessage());
            }
        }
    }

	protected void getNodeString(StringBuffer str) {
		super.getNodeString(str);
		str.append(sortElements);
	}

	public Object clone(){
		GroupingNode clonedNode = new GroupingNode(super.getID());
		super.copy(this, clonedNode);
		clonedNode.sortElements = sortElements;
		clonedNode.sortTypes = sortTypes;
		clonedNode.removeDuplicates = removeDuplicates;
		return clonedNode;
	}

    /*
     * @see com.metamatrix.query.processor.Describable#getDescriptionProperties()
     */
    public Map getDescriptionProperties() {
        // Default implementation - should be overridden
        Map props = super.getDescriptionProperties();
        props.put(PROP_TYPE, "Grouping"); //$NON-NLS-1$

        if(sortElements != null) {
            int elements = sortElements.size();
            List groupCols = new ArrayList(elements);
            for(int i=0; i<elements; i++) {
                groupCols.add(this.sortElements.get(i).toString());
            }
            props.put(PROP_GROUP_COLS, groupCols);
        }
        
        props.put(PROP_REMOVE_DUPS, this.removeDuplicates);

        return props;
    }

}