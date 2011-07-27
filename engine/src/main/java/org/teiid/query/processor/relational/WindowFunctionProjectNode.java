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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.IndexedTupleSource;
import org.teiid.common.buffer.STree;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.TupleSource;
import org.teiid.common.buffer.BufferManager.TupleSourceType;
import org.teiid.common.buffer.STree.InsertMode;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.language.SortSpecification.NullOrdering;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.function.aggregate.AggregateFunction;
import org.teiid.query.function.aggregate.ArrayAgg;
import org.teiid.query.function.aggregate.Avg;
import org.teiid.query.function.aggregate.Count;
import org.teiid.query.function.aggregate.Max;
import org.teiid.query.function.aggregate.Min;
import org.teiid.query.function.aggregate.RankingFunction;
import org.teiid.query.function.aggregate.StatsFunction;
import org.teiid.query.function.aggregate.Sum;
import org.teiid.query.function.aggregate.TextAgg;
import org.teiid.query.function.aggregate.XMLAgg;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.relational.GroupingNode.ProjectingTupleSource;
import org.teiid.query.processor.relational.SortUtility.Mode;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.OrderByItem;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.TextLine;
import org.teiid.query.sql.symbol.WindowFunction;
import org.teiid.query.sql.symbol.WindowSpecification;
import org.teiid.query.sql.symbol.AggregateSymbol.Type;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.util.CommandContext;


public class WindowFunctionProjectNode extends SubqueryAwareRelationalNode {
	
	private enum Phase {
		COLLECT,
		PROCESS,
		OUTPUT
	}
	
	private static class WindowFunctionInfo {
		WindowFunction function;
		int expressionIndex = -1;
		int conditionIndex = -1;
		int outputIndex;
	}
	
	private static class WindowSpecificationInfo {
		List<Integer> groupIndexes = new ArrayList<Integer>();
		List<Integer> sortIndexes = new ArrayList<Integer>();
		List<NullOrdering> nullOrderings = new ArrayList<NullOrdering>();
		List<Boolean> orderType = new ArrayList<Boolean>();
		List<WindowFunctionInfo> functions = new ArrayList<WindowFunctionInfo>();
	}
	
	private LinkedHashMap<WindowSpecification, WindowSpecificationInfo> windows = new LinkedHashMap<WindowSpecification, WindowSpecificationInfo>();
	private LinkedHashMap<Expression, Integer> expressionIndexes;
	private LinkedHashMap<Integer, Integer> passThrough = new LinkedHashMap<Integer, Integer>();
	
	private Map elementMap;
	
	//processing state
	private Phase phase = Phase.COLLECT;
	private TupleBuffer tb;
	private TupleSource inputTs;
	private STree[] partitionMapping;
	private STree[] valueMapping;
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
		this.outputTs = null;
	}
	
	@Override
	public void closeDirect() {
		if (tb != null) {
			tb.remove();
			tb = null;
		}
		if (partitionMapping != null) {
			for (STree tree : partitionMapping) {
				if (tree != null) {
					tree.remove();
				}
			}
			partitionMapping = null;
		}
		if (valueMapping != null) {
			for (STree tree : valueMapping) {
				tree.remove();
			}
			valueMapping = null;
		}
	}
	
	public Object clone(){
		WindowFunctionProjectNode clonedNode = new WindowFunctionProjectNode();
        this.copy(this, clonedNode);
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
			Expression ex = SymbolMap.getExpression((Expression) getElements().get(i));
			if (ex instanceof WindowFunction) {
				WindowFunction wf = (WindowFunction)ex;
				WindowSpecification ws = wf.getWindowSpecification();
				WindowSpecificationInfo wsi = windows.get(ws);
				if (wsi == null) {
					wsi = new WindowSpecificationInfo();
					windows.put(wf.getWindowSpecification(), wsi);
					if (ws.getPartition() != null) {
						for (Expression ex1 : ws.getPartition()) {
							Integer index = getIndex(ex1);
							wsi.groupIndexes.add(index);
							wsi.orderType.add(OrderBy.ASC);
							wsi.nullOrderings.add(null);
						}
					}
					if (ws.getOrderBy() != null) {
						for (OrderByItem item : ws.getOrderBy().getOrderByItems()) {
							Expression ex1 = SymbolMap.getExpression(item.getSymbol());
							Integer index = getIndex(ex1);
							wsi.sortIndexes.add(index);
							wsi.orderType.add(item.isAscending());
							wsi.nullOrderings.add(item.getNullOrdering());
						}
					}
				}
				WindowFunctionInfo wfi = new WindowFunctionInfo();
				wfi.function = wf;
				ex = wf.getFunction().getExpression();
				if (ex != null) {
					wfi.expressionIndex = getIndex(ex);
				}
				if (wf.getFunction().getCondition() != null) {
					ex = wf.getFunction().getCondition();
					wfi.conditionIndex = getIndex(ex);
				}
				wfi.outputIndex = i;
				wsi.functions.add(wfi);
			} else {
				int index = getIndex(ex);
				passThrough.put(i, index);
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
				int rowId = (Integer)tuple.get(tuple.size() - 1);
				int size = getElements().size();
				ArrayList<Object> outputRow = new ArrayList<Object>(size);
				for (int i = 0; i < size; i++) {
					outputRow.add(null);
				}
				for (Map.Entry<Integer, Integer> entry : passThrough.entrySet()) {
					outputRow.set(entry.getKey(), tuple.get(entry.getValue()));
				}
				List<Map.Entry<WindowSpecification, WindowSpecificationInfo>> specs = new ArrayList<Map.Entry<WindowSpecification,WindowSpecificationInfo>>(windows.entrySet());
				for (int specIndex = 0; specIndex < specs.size(); specIndex++) {
					Map.Entry<WindowSpecification, WindowSpecificationInfo> entry = specs.get(specIndex);
					List<?> idRow = Arrays.asList(rowId);
					if (partitionMapping[specIndex] != null) {
						idRow = partitionMapping[specIndex].find(idRow);
						idRow = idRow.subList(1, 2);
					}
					List<?> valueRow = valueMapping[specIndex].find(idRow);
					List<WindowFunctionInfo> functions = entry.getValue().functions;
					for (int i = 0; i < functions.size(); i++) {
						WindowFunctionInfo wfi = functions.get(i);
						outputRow.set(wfi.outputIndex, valueRow.get(i+1));
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
			int[] groupingIndexes = null;
			int[] orderIndexes = null;

			//if there is partitioning or ordering, then sort
			if (!info.orderType.isEmpty()) {
				int[] sortKeys = new int[info.orderType.size()];
				int i = 0;
				if (!info.groupIndexes.isEmpty()) {
					for (Integer sortIndex : info.groupIndexes) {
						sortKeys[i++] = sortIndex;
					}
					groupingIndexes = Arrays.copyOf(sortKeys, info.groupIndexes.size());
					ElementSymbol key = new ElementSymbol("rowid"); //$NON-NLS-1$
					key.setType(DataTypeManager.DefaultDataClasses.INTEGER);
					ElementSymbol value = new ElementSymbol("partitionid"); //$NON-NLS-1$
					key.setType(DataTypeManager.DefaultDataClasses.INTEGER);
					List<ElementSymbol> elements = Arrays.asList(key, value);
					partitionMapping[specIndex] = this.getBufferManager().createSTree(elements, this.getConnectionID(), 1);
				}
				if (!info.sortIndexes.isEmpty()) {
					for (Integer sortIndex : info.sortIndexes) {
						sortKeys[i++] = sortIndex;
					}
					orderIndexes = Arrays.copyOfRange(sortKeys, info.groupIndexes.size(), info.groupIndexes.size() + info.sortIndexes.size());
				}
				SortUtility su = new SortUtility(specificationTs, Mode.SORT, this.getBufferManager(), this.getConnectionID(), tb.getSchema(), info.orderType, info.nullOrderings, sortKeys);
				TupleBuffer sorted = su.sort();
				specificationTs = sorted.createIndexedTupleSource(true);
			}
			List<AggregateFunction> aggs = initializeAccumulators(info, specIndex, orderIndexes);

			int partitionId = 0;
			List<?> lastRow = null;
			while (specificationTs.hasNext()) {
				List<?> tuple = specificationTs.nextTuple();
				boolean sameGroup = true;
				if (lastRow != null) {
		        	sameGroup = GroupingNode.sameGroup(groupingIndexes, tuple, lastRow);
		        	if (!sameGroup || orderIndexes != null) {
		        		saveValues(specIndex, orderIndexes, aggs, partitionId, lastRow, sameGroup);
		        	}
				}
		        if (orderIndexes == null) {
		        	if (!sameGroup) {
		        		partitionId++;
		        	}
		        	List<Object> partitionTuple = Arrays.asList(tuple.get(tuple.size() - 1), partitionId);
					partitionMapping[specIndex].insert(partitionTuple, InsertMode.NEW, -1);
		        }
		        for (AggregateFunction function : aggs) {
		        	function.addInput(tuple);
		        }
		        lastRow = tuple;
			}
		    if(lastRow != null) {
		    	saveValues(specIndex, orderIndexes, aggs, partitionId, lastRow, true);
		    }
		}
	}

	private void saveValues(int specIndex, int[] orderIndexes,
			List<AggregateFunction> aggs, int partitionId, List<?> tuple,
			boolean sameGroup) throws FunctionExecutionException,
			ExpressionEvaluationException, TeiidComponentException,
			TeiidProcessingException {
		List<Object> row = new ArrayList<Object>(aggs.size() + 1);
		if (orderIndexes == null) {
			row.add(partitionId);
		} else {
			//use the rowid
			row.add(tuple.get(tuple.size() - 1));
		}
		for (AggregateFunction function : aggs) {
			row.add(function.getResult());
			if (!sameGroup) {
				function.reset();
			}
		}
		valueMapping[specIndex].insert(row, orderIndexes != null?InsertMode.NEW:InsertMode.ORDERED, -1);
	}

	private List<AggregateFunction> initializeAccumulators(WindowSpecificationInfo info, int specIndex,
			int[] orderIndexes) {
		List<AggregateFunction> aggs = new ArrayList<AggregateFunction>();
		//initialize the function accumulators
		List<ElementSymbol> elements = new ArrayList<ElementSymbol>(info.functions.size() + 1);
		ElementSymbol key = new ElementSymbol("id"); //$NON-NLS-1$
		key.setType(DataTypeManager.DefaultDataClasses.INTEGER);
		elements.add(key);

		CommandContext context = this.getContext();
		for (WindowFunctionInfo wfi : info.functions) {
			AggregateSymbol aggSymbol = wfi.function.getFunction();
		    Class<?> outputType = aggSymbol.getType();
		    ElementSymbol value = new ElementSymbol("val"); //$NON-NLS-1$
		    value.setType(outputType);
		    elements.add(value);
		    Class<?> inputType = aggSymbol.getType();
		    if (aggSymbol.getExpression() != null) {
		    	inputType = aggSymbol.getExpression().getType();
		    }
			Type function = aggSymbol.getAggregateFunction();
			AggregateFunction af = null;
			switch (function) {
			case RANK:
			case DENSE_RANK:
				af = new RankingFunction(function, orderIndexes);
				break;
			case ROW_NUMBER: //same as count(*)
			case COUNT:
				af = new Count();
				break;
			case SUM:
				af = new Sum();
				break;
			case AVG:
				af = new Avg();
				break;
			case MIN:
				af = new Min();
				break;
			case MAX:
				af = new Max();
				break;
			case XMLAGG:
				af = new XMLAgg(context);
				break;
			case ARRAY_AGG:
				af = new ArrayAgg(context);
				break;                		
			case TEXTAGG:
				af = new TextAgg(context, (TextLine)aggSymbol.getExpression());
				break;                		
			default:
				af = new StatsFunction(function);
			}

			af.setExpressionIndex(wfi.expressionIndex);
			af.setConditionIndex(wfi.conditionIndex);
			af.initialize(outputType, inputType);
			aggs.add(af);
		}
		
		valueMapping[specIndex] = this.getBufferManager().createSTree(elements, this.getConnectionID(), 1);

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
			inputTs = new ProjectingTupleSource(sourceNode, eval, collectedExpressions) {
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

	private Integer getIndex(Expression ex) {
		Integer index = expressionIndexes.get(ex);
		if (index == null) {
			index = expressionIndexes.size();
			expressionIndexes.put(ex, index);
		}
		return index;
	}
	
	@Override
	public void initialize(CommandContext context, BufferManager bufferManager,
			ProcessorDataManager dataMgr) {
		super.initialize(context, bufferManager, dataMgr);
		List sourceElements = this.getChildren()[0].getElements();
        this.elementMap = createLookupMap(sourceElements);
	}
    
}
