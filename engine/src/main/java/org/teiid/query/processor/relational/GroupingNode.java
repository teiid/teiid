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
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.function.aggregate.AggregateFunction;
import org.teiid.query.function.aggregate.ArrayAgg;
import org.teiid.query.function.aggregate.Avg;
import org.teiid.query.function.aggregate.ConstantFunction;
import org.teiid.query.function.aggregate.Count;
import org.teiid.query.function.aggregate.Max;
import org.teiid.query.function.aggregate.Min;
import org.teiid.query.function.aggregate.StatsFunction;
import org.teiid.query.function.aggregate.Sum;
import org.teiid.query.function.aggregate.TextAgg;
import org.teiid.query.function.aggregate.XMLAgg;
import org.teiid.query.processor.BatchCollector;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.relational.SortUtility.Mode;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.OrderByItem;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.TextLine;
import org.teiid.query.sql.symbol.AggregateSymbol.Type;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.util.CommandContext;


public class GroupingNode extends RelationalNode {

    // Grouping columns set by the planner 
	private List<Expression> sortElements;
	private List<Boolean> sortTypes;
	private boolean removeDuplicates;
	private SymbolMap outputMapping;
    
    // Collection phase
    private int phase = COLLECTION;
    private Map elementMap;                    // Map of incoming symbol to index in source elements
    private List<Expression> collectedExpressions;         // Collected Expressions
       
    // Sort phase
    private SortUtility sortUtility;
    private TupleBuffer sortBuffer;
    private TupleSource groupTupleSource;
    
    // Group phase
    private AggregateFunction[] functions;
    private int[] conditions;
    private List lastRow;
	private List currentGroupTuple;
	private Evaluator eval;

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
    public void setGroupingElements(List<Expression> groupingElements) {
        this.sortElements = groupingElements;
        if(groupingElements != null) {
            sortTypes = Collections.nCopies(groupingElements.size(), Boolean.valueOf(OrderBy.ASC));
        }
    }
    
    public void setOutputMapping(SymbolMap outputMapping) {
		this.outputMapping = outputMapping;
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
            this.collectedExpressions = new ArrayList<Expression>(this.sortElements.size() + getElements().size());
            this.collectedExpressions.addAll(sortElements);
        } else {
            this.collectedExpressions = new ArrayList<Expression>(getElements().size());
        }
        
        // Construct aggregate function state accumulators
        functions = new AggregateFunction[getElements().size()];
        conditions = new int[getElements().size()];
        for(int i=0; i<getElements().size(); i++) {
            Expression symbol = (Expression) getElements().get(i);
            if (this.outputMapping != null) {
            	symbol = outputMapping.getMappedExpression((ElementSymbol)symbol);
            }
            Class<?> outputType = symbol.getType();
            Class<?> inputType = symbol.getType();
            conditions[i] = -1;
            if(symbol instanceof AggregateSymbol) {
                AggregateSymbol aggSymbol = (AggregateSymbol) symbol;
                if (aggSymbol.getCondition() != null) {
                	conditions[i] = collectExpression(aggSymbol.getCondition());
                }
                if(aggSymbol.getExpression() == null) {
                    functions[i] = new Count();
                } else {
                	Expression ex = aggSymbol.getExpression();
                	inputType = ex.getType();
                	int index = collectExpression(ex);
                	Type function = aggSymbol.getAggregateFunction();
                	switch (function) {
                	case COUNT:
                		functions[i] = new Count();
                		break;
                	case SUM:
                		functions[i] = new Sum();
                		break;
                	case AVG:
                		functions[i] = new Avg();
                		break;
                	case MIN:
                		functions[i] = new Min();
                		break;
                	case MAX:
                		functions[i] = new Max();
                		break;
                	case XMLAGG:
                		functions[i] = new XMLAgg(context);
                		break;
                	case ARRAY_AGG:
                		functions[i] = new ArrayAgg(context);
                		break;                		
                	case TEXTAGG:
               			functions[i] = new TextAgg(context, (TextLine)ex);
                		break;                		
                	default:
                		functions[i] = new StatsFunction(function);
                	}

                    if(aggSymbol.isDistinct() && !function.equals(NonReserved.MIN) && !function.equals(NonReserved.MAX)) {
                        SortingFilter filter = new SortingFilter(functions[i], getBufferManager(), getConnectionID(), true);
                        ElementSymbol element = new ElementSymbol("val"); //$NON-NLS-1$
                        element.setType(inputType);
                        filter.setElements(Arrays.asList(element));
                        functions[i] = filter;
                    } else if (aggSymbol.getOrderBy() != null) { //handle the xmlagg case
                		int[] orderIndecies = new int[aggSymbol.getOrderBy().getOrderByItems().size()];
                		List<OrderByItem> orderByItems = new ArrayList<OrderByItem>(orderIndecies.length);
                		List<ElementSymbol> schema = new ArrayList<ElementSymbol>(orderIndecies.length + 1);
                		ElementSymbol element = new ElementSymbol("val"); //$NON-NLS-1$
                        element.setType(inputType);
                        schema.add(element);
                		for (ListIterator<OrderByItem> iterator = aggSymbol.getOrderBy().getOrderByItems().listIterator(); iterator.hasNext();) {
                			OrderByItem item = iterator.next();
                			orderIndecies[iterator.previousIndex()] = collectExpression(item.getSymbol());
                			element = new ElementSymbol(String.valueOf(iterator.previousIndex()));
                            element.setType(item.getSymbol().getType());
                			schema.add(element);
                			OrderByItem newItem = item.clone();
                			newItem.setSymbol(element);
                			orderByItems.add(newItem);
						}
                		SortingFilter filter = new SortingFilter(functions[i], getBufferManager(), getConnectionID(), false);
                		filter.setIndecies(orderIndecies);
                		filter.setElements(schema);
                		filter.setSortItems(orderByItems);
                        functions[i] = filter;
                	}
                    functions[i].setExpressionIndex(index);
                }
            } else {
                functions[i] = new ConstantFunction();
                functions[i].setExpressionIndex(this.collectedExpressions.indexOf(symbol));
            }
            functions[i].initialize(outputType, inputType);
        }
    }

	private int collectExpression(Expression ex) {
		int index = this.collectedExpressions.indexOf(ex);
		if(index == -1) {
		    index = this.collectedExpressions.size();
		    this.collectedExpressions.add(ex);
		}
		return index;
	} 
    
    AggregateFunction[] getFunctions() {
		return functions;
	}

	public TupleBatch nextBatchDirect()
		throws BlockedException, TeiidComponentException, TeiidProcessingException {

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
		
		return new BatchCollector.BatchProducerTupleSource(sourceNode) {
			
			@Override
			protected List updateTuple(List tuple) throws ExpressionEvaluationException, BlockedException, TeiidComponentException {
				int columns = collectedExpressions.size();
	            List<Object> exprTuple = new ArrayList<Object>(columns);
	            for(int col = 0; col<columns; col++) { 
	                // The following call may throw BlockedException, but all state to this point
	                // is saved in class variables so we can start over on building this tuple
	                Object value = eval.evaluate(collectedExpressions.get(col), tuple);
	                exprTuple.add(value);
	            }
	            return exprTuple;
			}
		};
		
	}

    private void collectionPhase() {
    	eval = new Evaluator(elementMap, getDataManager(), getContext());
        if(this.sortElements == null) {
            // No need to sort
            this.groupTupleSource = getCollectionTupleSource();
            this.phase = GROUP;
        } else {
        	//create a temporary positional schema
        	List<ElementSymbol> schema = new ArrayList<ElementSymbol>();
        	for (int i = 0; i < collectedExpressions.size(); i++) {
        		schema.add(new ElementSymbol(String.valueOf(i)));
        	}
            this.sortUtility = new SortUtility(getCollectionTupleSource(), schema.subList(0, sortElements.size()),
                                                sortTypes, removeDuplicates?Mode.DUP_REMOVE_SORT:Mode.SORT, getBufferManager(),
                                                getConnectionID(), schema);
            this.phase = SORT;
        }
    }

    private void sortPhase() throws BlockedException, TeiidComponentException, TeiidProcessingException {
        this.sortBuffer = this.sortUtility.sort();
        this.sortBuffer.setForwardOnly(true);
        this.groupTupleSource = this.sortBuffer.createIndexedTupleSource();
        this.phase = GROUP;
    }

    private TupleBatch groupPhase() throws BlockedException, TeiidComponentException, TeiidProcessingException {
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
                List<Object> row = new ArrayList<Object>(functions.length);
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
            List<Object> row = new ArrayList<Object>(functions.length);
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

    private void updateAggregates(List<?> tuple)
    throws TeiidComponentException, TeiidProcessingException {

        for(int i=0; i<functions.length; i++) {
        	if (conditions[i] != -1 && !Boolean.TRUE.equals(tuple.get(conditions[i]))) {
    			continue;
        	}
            functions[i].addInput(tuple);
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
		if (outputMapping != null) {
			str.append(outputMapping);
		}
	}

	public Object clone(){
		GroupingNode clonedNode = new GroupingNode(super.getID());
		super.copy(this, clonedNode);
		clonedNode.sortElements = sortElements;
		clonedNode.sortTypes = sortTypes;
		clonedNode.removeDuplicates = removeDuplicates;
		clonedNode.outputMapping = outputMapping;
		return clonedNode;
	}

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