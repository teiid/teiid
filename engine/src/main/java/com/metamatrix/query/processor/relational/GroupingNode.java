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

import static com.metamatrix.query.analysis.AnalysisRecord.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.teiid.client.plan.PlanNode;
import org.teiid.connector.language.SQLReservedWords;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.api.exception.query.ExpressionEvaluationException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleBuffer;
import com.metamatrix.common.buffer.TupleSource;
import com.metamatrix.query.eval.Evaluator;
import com.metamatrix.query.function.aggregate.AggregateFunction;
import com.metamatrix.query.function.aggregate.Avg;
import com.metamatrix.query.function.aggregate.ConstantFunction;
import com.metamatrix.query.function.aggregate.Count;
import com.metamatrix.query.function.aggregate.Max;
import com.metamatrix.query.function.aggregate.Min;
import com.metamatrix.query.function.aggregate.NullFilter;
import com.metamatrix.query.function.aggregate.Sum;
import com.metamatrix.query.processor.ProcessorDataManager;
import com.metamatrix.query.processor.relational.SortUtility.Mode;
import com.metamatrix.query.sql.lang.OrderBy;
import com.metamatrix.query.sql.symbol.AggregateSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;
import com.metamatrix.query.util.CommandContext;

public class GroupingNode extends RelationalNode {

    // Grouping columns set by the planner 
	private List sortElements;
	private List sortTypes;
	private boolean removeDuplicates;
    
    // Collection phase
    private int phase = COLLECTION;
    private Map elementMap;                    // Map of incoming symbol to index in source elements
    private List collectedExpressions;         // Collected Expressions
       
    // Sort phase
    private SortUtility sortUtility;
    private TupleBuffer sortBuffer;
    private TupleSource groupTupleSource;
    
    // Group phase
    private AggregateFunction[] functions;
    private int[] aggProjectionIndexes;
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
                
        sortUtility = null;
        sortBuffer = null;
        
        lastRow = null;
        currentGroupTuple = null;
        
        if (this.functions != null) {
	    	for (AggregateFunction function : this.functions) {
				function.reset();
			}
        }
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
            sortTypes = Collections.nCopies(groupingElements.size(), Boolean.valueOf(OrderBy.ASC));
        }
    }

	@Override
	public void initialize(CommandContext context, BufferManager bufferManager,
			ProcessorDataManager dataMgr) {
		super.initialize(context, bufferManager, dataMgr);
		
		if (this.functions != null) {
			return;
		}
		
        // Incoming elements and lookup map for evaluating expressions
        List sourceElements = this.getChildren()[0].getElements();
        this.elementMap = createLookupMap(sourceElements);

    	// List should contain all grouping columns / expressions as we need those for sorting
        if(this.sortElements != null) {
            this.collectedExpressions = new ArrayList(this.sortElements.size() + getElements().size());
            this.collectedExpressions.addAll(sortElements);
        } else {
            this.collectedExpressions = new ArrayList(getElements().size());
        }
        
        // Construct aggregate function state accumulators
        functions = new AggregateFunction[getElements().size()];
        aggProjectionIndexes = new int[getElements().size()];
        Arrays.fill(aggProjectionIndexes, -1);
        for(int i=0; i<getElements().size(); i++) {
            SingleElementSymbol symbol = (SingleElementSymbol)getElements().get(i);
            Class<?> outputType = symbol.getType();
            Class<?> inputType = symbol.getType();
            if(symbol instanceof AggregateSymbol) {
                AggregateSymbol aggSymbol = (AggregateSymbol) symbol;

                if(aggSymbol.getExpression() == null) {
                    functions[i] = new Count();
                } else {
                	int index = this.collectedExpressions.indexOf(aggSymbol.getExpression());
                	if(index == -1) {
                        index = this.collectedExpressions.size();
                        this.collectedExpressions.add(aggSymbol.getExpression());
                    }
                	aggProjectionIndexes[i] = index;
                    String function = aggSymbol.getAggregateFunction();
                    if(function.equals(SQLReservedWords.COUNT)) {
                        functions[i] = new Count();
                    } else if(function.equals(SQLReservedWords.SUM)) {
                        functions[i] = new Sum();
                    } else if(function.equals(SQLReservedWords.AVG)) {
                        functions[i] = new Avg();
                    } else if(function.equals(SQLReservedWords.MIN)) {
                        functions[i] = new Min();
                    } else {
                        functions[i] = new Max();
                    }

                    if(aggSymbol.isDistinct() && !function.equals(SQLReservedWords.MIN) && !function.equals(SQLReservedWords.MAX)) {
                        functions[i] = new DuplicateFilter(functions[i], getBufferManager(), getConnectionID());
                    }
                    
                    functions[i] = new NullFilter(functions[i]);
                    inputType = aggSymbol.getExpression().getType();
                }
            } else {
                functions[i] = new ConstantFunction();
                aggProjectionIndexes[i] = this.collectedExpressions.indexOf(symbol);
            }
            functions[i].initialize(outputType, inputType);
        }
    } 
    
    AggregateFunction[] getFunctions() {
		return functions;
	}

	public TupleBatch nextBatchDirect()
		throws BlockedException, MetaMatrixComponentException, MetaMatrixProcessingException {

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
        
        this.terminateBatches();
        return pullBatch();
    }
	
	public TupleSource getCollectionTupleSource() {
		
		final RelationalNode sourceNode = this.getChildren()[0];
		
		return new TupleSource() {
		    private TupleBatch sourceBatch;           // Current batch loaded from the source, if blocked
		    private int sourceRow = 1;                   

			@Override
			public List<?> nextTuple() throws MetaMatrixComponentException,
					MetaMatrixProcessingException {
				while (true) {
					if(sourceBatch == null) {
			            // Read next batch
			            sourceBatch = sourceNode.nextBatch();
			        }
			        
			        if(sourceBatch.getRowCount() > 0 && sourceRow <= sourceBatch.getEndRow()) {
			            // Evaluate expressions needed for grouping
		                List tuple = sourceBatch.getTuple(sourceRow);
		                
		                int columns = collectedExpressions.size();
		                List exprTuple = new ArrayList(columns);
		                for(int col = 0; col<columns; col++) { 
		                    // The following call may throw BlockedException, but all state to this point
		                    // is saved in class variables so we can start over on building this tuple
		                    Object value = new Evaluator(elementMap, getDataManager(), getContext()).evaluate((Expression) collectedExpressions.get(col), tuple);
		                    exprTuple.add(value);
		                }
		                sourceRow++;
			            return exprTuple;
			        }
			        
			        // Check for termination condition
			        if(sourceBatch.getTerminationFlag()) {
			        	sourceBatch = null;			            
			            return null;
			        } 
			        sourceBatch = null;
				}
			}
			
			@Override
			public List<SingleElementSymbol> getSchema() {
				return collectedExpressions;
			}
			
			@Override
			public void closeSource() {
				
			}
			
			@Override
			public int available() {
				if (sourceBatch != null) {
		    		return sourceBatch.getEndRow() - sourceRow + 1;
		    	}
				return 0;
			}
		};
		
	}

    private void collectionPhase() {
        if(this.sortElements == null) {
            // No need to sort
            this.groupTupleSource = getCollectionTupleSource();
            this.phase = GROUP;
        } else {
            this.sortUtility = new SortUtility(getCollectionTupleSource(), sortElements,
                                                sortTypes, removeDuplicates?Mode.DUP_REMOVE_SORT:Mode.SORT, getBufferManager(),
                                                getConnectionID());
            this.phase = SORT;
        }
    }

    private void sortPhase() throws BlockedException, MetaMatrixComponentException, MetaMatrixProcessingException {
        this.sortBuffer = this.sortUtility.sort();
        this.sortBuffer.setForwardOnly(true);
        this.groupTupleSource = this.sortBuffer.createIndexedTupleSource();
        this.phase = GROUP;
    }

    private TupleBatch groupPhase() throws BlockedException, MetaMatrixComponentException, MetaMatrixProcessingException {
        while(true) {

        	if (currentGroupTuple == null) {
        		currentGroupTuple = this.groupTupleSource.nextTuple();
        		if (currentGroupTuple == null) {
        			break;
        		}
        	}
        	
            if(lastRow == null) {
                // First row we've seen
                lastRow = currentGroupTuple;

            } else if(! sameGroup(currentGroupTuple, lastRow)) {
                // Close old group
                List row = new ArrayList(functions.length);
                for(int i=0; i<functions.length; i++) {
                    row.add( functions[i].getResult() );
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
        }
        if(lastRow != null || sortElements == null) {
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
            Object value = null;
            if(aggProjectionIndexes[i] != -1) {
                value = tuple.get(aggProjectionIndexes[i]);
            }
            functions[i].addInput(value);
        }
    }

    public void closeDirect() {
    	if (this.sortBuffer != null) {
    		this.sortBuffer.remove();
    		this.sortBuffer = null;
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
    public PlanNode getDescriptionProperties() {
        // Default implementation - should be overridden
    	PlanNode props = super.getDescriptionProperties();

        if(sortElements != null) {
            int elements = sortElements.size();
            List<String> groupCols = new ArrayList<String>(elements);
            for(int i=0; i<elements; i++) {
                groupCols.add(this.sortElements.get(i).toString());
            }
            props.addProperty(PROP_GROUP_COLS, groupCols);
        }
        
        props.addProperty(PROP_SORT_MODE, String.valueOf(this.removeDuplicates));

        return props;
    }

}