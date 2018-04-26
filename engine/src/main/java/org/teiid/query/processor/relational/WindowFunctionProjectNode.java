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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.BufferManager.TupleSourceType;
import org.teiid.common.buffer.IndexedTupleSource;
import org.teiid.common.buffer.STree;
import org.teiid.common.buffer.STree.InsertMode;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.ArrayImpl;
import org.teiid.core.types.DataTypeManager;
import org.teiid.language.SortSpecification.NullOrdering;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.function.aggregate.AggregateFunction;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.relational.GroupingNode.ProjectingTupleSource;
import org.teiid.query.processor.relational.SortUtility.Mode;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.OrderByItem;
import org.teiid.query.sql.symbol.AggregateSymbol.Type;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.WindowFunction;
import org.teiid.query.sql.symbol.WindowSpecification;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.util.CommandContext;


public class WindowFunctionProjectNode extends SubqueryAwareRelationalNode {
	
	private static final List<Integer> SINGLE_VALUE_ID = Arrays.asList(0);

	private enum Phase {
		COLLECT,
		PROCESS,
		OUTPUT
	}
	
	private static class WindowFunctionInfo {
		WindowFunction function;
		int outputIndex;
	}
	
	private static class WindowSpecificationInfo {
		List<Integer> groupIndexes = new ArrayList<Integer>();
		List<Integer> sortIndexes = new ArrayList<Integer>();
		List<NullOrdering> nullOrderings = new ArrayList<NullOrdering>();
		List<Boolean> orderType = new ArrayList<Boolean>();
		List<WindowFunctionInfo> functions = new ArrayList<WindowFunctionInfo>();
		List<WindowFunctionInfo> rowValuefunctions = new ArrayList<WindowFunctionInfo>();
	}
	
	private LinkedHashMap<WindowSpecification, WindowSpecificationInfo> windows = new LinkedHashMap<WindowSpecification, WindowSpecificationInfo>();
	private LinkedHashMap<Expression, Integer> expressionIndexes;
	private List<int[]> passThrough = new ArrayList<int[]>();
	
	private Map<Expression, Integer> elementMap;
	
	//processing state
	private Phase phase = Phase.COLLECT;
	private TupleBuffer tb;
	private TupleSource inputTs;
	private STree[] partitionMapping;
	private STree[] valueMapping;
	private STree[] rowValueMapping;
	private IndexedTupleSource outputTs;
	
	public WindowFunctionProjectNode(int nodeId) {
		super(nodeId);
	}
	
	protected WindowFunctionProjectNode() {
	}
	
	@Override
	public void reset() {
		super.reset();
		this.tb = null;
		this.inputTs = null;
		this.phase = Phase.COLLECT;
		this.partitionMapping = null;
		this.valueMapping = null;
		this.rowValueMapping = null;
		this.outputTs = null;
	}
	
	@Override
	public void closeDirect() {
		if (tb != null) {
			tb.remove();
			tb = null;
		}
		removeMappings(partitionMapping);
		partitionMapping = null;
		removeMappings(valueMapping);
		valueMapping = null;
		removeMappings(rowValueMapping);
		rowValueMapping = null;
	}

	private void removeMappings(STree[] mappings) {
		if (mappings != null) {
			for (STree tree : mappings) {
				if (tree != null) {
					tree.remove();
				}
			}
		}
	}
	
	public Object clone(){
		WindowFunctionProjectNode clonedNode = new WindowFunctionProjectNode();
        this.copyTo(clonedNode);
        clonedNode.windows = windows;
        clonedNode.expressionIndexes = expressionIndexes;
        clonedNode.passThrough = passThrough;
		return clonedNode;
	}
	
	/**
	 * This state can be determined prior to initialize and is the same for all nodes,
	 * so it is moved into it's own init routine
	 */
	public void init() {
		expressionIndexes = new LinkedHashMap<Expression, Integer>();
		for (int i = 0; i < getElements().size(); i++) {
			Expression ex = SymbolMap.getExpression(getElements().get(i));
			if (ex instanceof WindowFunction) {
				WindowFunction wf = (WindowFunction)ex;
				WindowSpecification ws = wf.getWindowSpecification();
				WindowSpecificationInfo wsi = windows.get(ws);
				if (wsi == null) {
					wsi = new WindowSpecificationInfo();
					windows.put(wf.getWindowSpecification(), wsi);
					if (ws.getPartition() != null) {
						for (Expression ex1 : ws.getPartition()) {
							Integer index = GroupingNode.getIndex(ex1, expressionIndexes);
							wsi.groupIndexes.add(index);
							wsi.orderType.add(OrderBy.ASC);
							wsi.nullOrderings.add(null);
						}
					}
					if (ws.getOrderBy() != null) {
						for (OrderByItem item : ws.getOrderBy().getOrderByItems()) {
							Expression ex1 = SymbolMap.getExpression(item.getSymbol());
							Integer index = GroupingNode.getIndex(ex1, expressionIndexes);
							wsi.sortIndexes.add(index);
							wsi.orderType.add(item.isAscending());
							wsi.nullOrderings.add(item.getNullOrdering());
						}
					}
				}
				WindowFunctionInfo wfi = new WindowFunctionInfo();
				wfi.function = wf;
				//collect the agg expressions
				for (Expression e : wf.getFunction().getArgs()) {
					GroupingNode.getIndex(e, expressionIndexes);
				}
				if (wf.getFunction().getOrderBy() != null) {
					for (OrderByItem item : wf.getFunction().getOrderBy().getOrderByItems()) {
						GroupingNode.getIndex(item.getSymbol(), expressionIndexes);
					}
				}
				if (wf.getFunction().getCondition() != null) {
					GroupingNode.getIndex(wf.getFunction().getCondition(), expressionIndexes);
				}
				wfi.outputIndex = i;
				if (wf.getFunction().isRowValueFunction()) {
					wsi.rowValuefunctions.add(wfi);
				} else {
					wsi.functions.add(wfi);
				}
			} else {
				int index = GroupingNode.getIndex(ex, expressionIndexes);
				passThrough.add(new int[] {i, index});
			}
		}
	}

	@Override
	protected TupleBatch nextBatchDirect() throws BlockedException,
			TeiidComponentException, TeiidProcessingException {
		
		if (phase == Phase.COLLECT) {
			saveInput();
			phase = Phase.PROCESS;
			partitionMapping = new STree[this.windows.size()];
			valueMapping = new STree[this.windows.size()];
			rowValueMapping = new STree[this.windows.size()];
		}
		
		if (phase == Phase.PROCESS) {
			buildResults();
			phase = Phase.OUTPUT;
		}
		
		if (phase == Phase.OUTPUT) {
			if (outputTs == null) {
				outputTs = tb.createIndexedTupleSource(true);
			}
			while (outputTs.hasNext()) {
				List<?> tuple = outputTs.nextTuple();
				Integer rowId = (Integer)tuple.get(tuple.size() - 1);
				int size = getElements().size();
				ArrayList<Object> outputRow = new ArrayList<Object>(size);
				for (int i = 0; i < size; i++) {
					outputRow.add(null);
				}
				for (int[] entry : passThrough) {
					outputRow.set(entry[0], tuple.get(entry[1]));
				}
				List<Map.Entry<WindowSpecification, WindowSpecificationInfo>> specs = new ArrayList<Map.Entry<WindowSpecification,WindowSpecificationInfo>>(windows.entrySet());
				for (int specIndex = 0; specIndex < specs.size(); specIndex++) {
					Map.Entry<WindowSpecification, WindowSpecificationInfo> entry = specs.get(specIndex);
					List<?> idRow = Arrays.asList(rowId);
					List<WindowFunctionInfo> functions = entry.getValue().rowValuefunctions;
					if (!functions.isEmpty()) {
						List<?> valueRow = rowValueMapping[specIndex].find(idRow);
						for (int i = 0; i < functions.size(); i++) {
							WindowFunctionInfo wfi = functions.get(i);
							Object value = valueRow.get(i+1);
                            outputRow.set(wfi.outputIndex, value);
						}
					}
					functions = entry.getValue().functions;
					if (!functions.isEmpty()) {
						if (partitionMapping[specIndex] != null) {
							idRow = partitionMapping[specIndex].find(idRow);
							idRow = idRow.subList(1, 2);
						} else {
							idRow = SINGLE_VALUE_ID;
						}
						List<?> valueRow = valueMapping[specIndex].find(idRow);
						for (int i = 0; i < functions.size(); i++) {
							WindowFunctionInfo wfi = functions.get(i);
							Object value = valueRow.get(i+1);
							//special handling for lead lag
                            //an array value encodes what we need to know about
                            //the offset, default, and partition
                            if (wfi.function.getFunction().getAggregateFunction() == Type.LEAD
                                    || wfi.function.getFunction().getAggregateFunction() == Type.LAG) {
                                ArrayImpl array = (ArrayImpl)value;
                                Object[] args = array.getValues();
                                int offset = 1;
                                Object defaultValue = null;
                                if (args.length > 2) {
                                    offset = (int) args[1];
                                    if (args.length > 3) {
                                        defaultValue = args[2];
                                    }
                                }
                                List<?> newIdRow = Arrays.asList((Integer)idRow.get(0)+(wfi.function.getFunction().getAggregateFunction() == Type.LAG?-offset:offset));
                                List<?> newValueRow = valueMapping[specIndex].find(newIdRow);
                                if (newValueRow == null) {
                                    value = defaultValue;
                                } else {
                                    Object[] newArgs = ((ArrayImpl)newValueRow.get(i+1)).getValues();
                                    //make sure it's the same partition
                                    if (args[args.length-1].equals(newArgs[newArgs.length-1])) {
                                        value = newArgs[0];
                                    } else {
                                        value = defaultValue;
                                    }
                                }
                            }
							outputRow.set(wfi.outputIndex, value);
						}
					}
				}
				this.addBatchRow(outputRow);
				if (this.isBatchFull()) {
					return pullBatch();
				}
			}
			terminateBatches();
		}
		return this.pullBatch();
	}

	/**
	 * Build the results by maintaining indexes that either map
	 * rowid->values
	 * or
	 * rowid->partitionid and partitionid->values
	 * 
	 * TODO use the size hint for tree balancing
	 */
	private void buildResults() throws TeiidComponentException,
			TeiidProcessingException, FunctionExecutionException,
			ExpressionEvaluationException {
		List<Map.Entry<WindowSpecification, WindowSpecificationInfo>> specs = new ArrayList<Map.Entry<WindowSpecification,WindowSpecificationInfo>>(windows.entrySet());
		for (int specIndex = 0; specIndex < specs.size(); specIndex++) {
			Map.Entry<WindowSpecification, WindowSpecificationInfo> entry = specs.get(specIndex);
			WindowSpecificationInfo info = entry.getValue();
			IndexedTupleSource specificationTs = tb.createIndexedTupleSource();
			boolean multiGroup = false;
			int[] partitionIndexes = null;
			int[] orderIndexes = null;

			//if there is partitioning or ordering, then sort
			if (!info.orderType.isEmpty()) {
				multiGroup = true;
				int[] sortKeys = new int[info.orderType.size()];
				int i = 0;
				if (!info.groupIndexes.isEmpty()) {
					for (Integer sortIndex : info.groupIndexes) {
						sortKeys[i++] = sortIndex;
					}
					partitionIndexes = Arrays.copyOf(sortKeys, info.groupIndexes.size());
				}
				if (!info.sortIndexes.isEmpty()) {
					for (Integer sortIndex : info.sortIndexes) {
						sortKeys[i++] = sortIndex;
					}
					orderIndexes = Arrays.copyOfRange(sortKeys, info.groupIndexes.size(), info.groupIndexes.size() + info.sortIndexes.size());
				}
				if (!info.functions.isEmpty()) {
					ElementSymbol key = new ElementSymbol("rowId"); //$NON-NLS-1$
					key.setType(DataTypeManager.DefaultDataClasses.INTEGER);
					ElementSymbol value = new ElementSymbol("partitionId"); //$NON-NLS-1$
					value.setType(DataTypeManager.DefaultDataClasses.INTEGER);
					List<ElementSymbol> elements = Arrays.asList(key, value);
					partitionMapping[specIndex] = this.getBufferManager().createSTree(elements, this.getConnectionID(), 1);
				}
				SortUtility su = new SortUtility(null, Mode.SORT, this.getBufferManager(), this.getConnectionID(), tb.getSchema(), info.orderType, info.nullOrderings, sortKeys);
				su.setWorkingBuffer(tb);
				su.setNonBlocking(true);
				TupleBuffer sorted = su.sort();
				specificationTs = sorted.createIndexedTupleSource(true);
			}
			List<AggregateFunction> aggs = initializeAccumulators(info.functions, specIndex, false);
			List<AggregateFunction> rowValueAggs = initializeAccumulators(info.rowValuefunctions, specIndex, true);

			int groupId = 0;
			List<?> lastRow = null;
			while (specificationTs.hasNext()) {
				List<?> tuple = specificationTs.nextTuple();
				if (multiGroup) {
				    if (lastRow != null) {
				    	boolean samePartition = GroupingNode.sameGroup(partitionIndexes, tuple, lastRow) == -1;
				    	if (!aggs.isEmpty() && (!samePartition || GroupingNode.sameGroup(orderIndexes, tuple, lastRow) != -1)) {
			        		saveValues(specIndex, aggs, groupId, samePartition, false);
		        			groupId++;
				    	}
		        		saveValues(specIndex, rowValueAggs, lastRow.get(lastRow.size() - 1), samePartition, true);
		        	}
				    if (!aggs.isEmpty()) {
			        	List<Object> partitionTuple = Arrays.asList(tuple.get(tuple.size() - 1), groupId);
						partitionMapping[specIndex].insert(partitionTuple, InsertMode.NEW, -1);
				    }
		        }
		        for (AggregateFunction function : aggs) {
		        	function.addInput(tuple, getContext());
		        }
		        for (AggregateFunction function : rowValueAggs) {
		        	function.addInput(tuple, getContext());
		        }
		        lastRow = tuple;
			}
		    if(lastRow != null) {
		    	saveValues(specIndex, aggs, groupId, true, false);
		    	saveValues(specIndex, rowValueAggs, lastRow.get(lastRow.size() - 1), true, true);
		    }
		}
	}

	private void saveValues(int specIndex,
			List<AggregateFunction> aggs, Object id,
			boolean samePartition, boolean rowValue) throws FunctionExecutionException,
			ExpressionEvaluationException, TeiidComponentException,
			TeiidProcessingException {
		if (aggs.isEmpty()) {
			return;
		}
		List<Object> row = new ArrayList<Object>(aggs.size() + 1);
		row.add(id);
		for (AggregateFunction function : aggs) {
			row.add(function.getResult(getContext()));
			if (!samePartition) {
				function.reset();
			}
		}
		if (rowValue) {
			rowValueMapping[specIndex].insert(row, InsertMode.NEW, -1);
		} else {
			valueMapping[specIndex].insert(row, InsertMode.ORDERED, -1);	
		}
	}

	/**
	 * @param functions
	 * @param specIndex
	 * @param rowValues
	 * @return
	 */
	private List<AggregateFunction> initializeAccumulators(List<WindowFunctionInfo> functions, int specIndex, boolean rowValues) {
		List<AggregateFunction> aggs = new ArrayList<AggregateFunction>(functions.size());
		if (functions.isEmpty()) {
			return aggs;
		}
		List<ElementSymbol> elements = new ArrayList<ElementSymbol>(functions.size());
		ElementSymbol key = new ElementSymbol("key"); //$NON-NLS-1$
	    key.setType(DataTypeManager.DefaultDataClasses.INTEGER);
	    elements.add(key);
		for (WindowFunctionInfo wfi : functions) {
			aggs.add(GroupingNode.initAccumulator(wfi.function.getFunction(), this, expressionIndexes));
			Class<?> outputType = wfi.function.getType();
			if (wfi.function.getFunction().getAggregateFunction() == Type.LEAD 
			        || wfi.function.getFunction().getAggregateFunction() == Type.LAG) {
			    outputType = DataTypeManager.getArrayType(DataTypeManager.DefaultDataClasses.OBJECT);
			}
		    ElementSymbol value = new ElementSymbol("val"); //$NON-NLS-1$
		    value.setType(outputType);
		    elements.add(value);
		}
		if (!rowValues) {
			valueMapping[specIndex] = this.getBufferManager().createSTree(elements, this.getConnectionID(), 1);
		} else {
			rowValueMapping[specIndex] = this.getBufferManager().createSTree(elements, this.getConnectionID(), 1);
		}
		return aggs;
	}

	/**
	 * Save the input generating any necessary expressions and adding a row id
	 * @param collectedExpressions
	 * @return
	 * @throws TeiidComponentException
	 * @throws TeiidProcessingException
	 */
	private void saveInput()
			throws TeiidComponentException, TeiidProcessingException {
		if (inputTs == null) {
			List<Expression> collectedExpressions = new ArrayList<Expression>(expressionIndexes.keySet());
			Evaluator eval = new Evaluator(elementMap, getDataManager(), getContext());
			final RelationalNode sourceNode = this.getChildren()[0];
			inputTs = new ProjectingTupleSource(sourceNode, eval, collectedExpressions, elementMap) {
				int index = 0;
				@Override
				public List<Object> nextTuple() throws TeiidComponentException,
						TeiidProcessingException {
					List<Object> tuple = super.nextTuple();
					if (tuple != null) {
						tuple.add(index++);
					}
					return tuple;
				}
			};
			List<ElementSymbol> schema = new ArrayList<ElementSymbol>(collectedExpressions.size() + 1);
			int index = 0;
			for (Expression ex : collectedExpressions) {
				ElementSymbol es = new ElementSymbol(String.valueOf(index++));
				es.setType(ex.getType());
				schema.add(es);
			}
			//add in the row id
			ElementSymbol es = new ElementSymbol(String.valueOf(index++));
			es.setType(DataTypeManager.DefaultDataClasses.INTEGER);
			schema.add(es);
			tb = this.getBufferManager().createTupleBuffer(schema, this.getConnectionID(), TupleSourceType.PROCESSOR);
		}
		
		List<?> tuple = null;
		while ((tuple = inputTs.nextTuple()) != null) {
			tb.addTuple(tuple);
		}
		tb.close();
		inputTs.closeSource();
		inputTs = null;
	}

	@Override
	public void initialize(CommandContext context, BufferManager bufferManager,
			ProcessorDataManager dataMgr) {
		super.initialize(context, bufferManager, dataMgr);
		if (this.elementMap == null) {
			List<? extends Expression> sourceElements = this.getChildren()[0].getElements();
			this.elementMap = createLookupMap(sourceElements);
		}
	}
	
	@Override
	public Collection<? extends LanguageObject> getObjects() {
		return getElements();
	}
	
	@Override
	public PlanNode getDescriptionProperties() {
    	PlanNode props = super.getDescriptionProperties();
    	AnalysisRecord.addLanaguageObjects(props, PROP_WINDOW_FUNCTIONS, this.windows.keySet());
        return props;
	}
    
}
