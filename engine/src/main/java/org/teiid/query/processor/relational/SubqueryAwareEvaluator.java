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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.LRUCache;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.query.QueryPlugin;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.function.FunctionDescriptor;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.optimizer.relational.rules.CapabilitiesUtil;
import org.teiid.query.processor.BatchCollector;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.QueryProcessor;
import org.teiid.query.processor.RegisterRequestParameter;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.lang.SubqueryContainer;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ContextReference;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.ExpressionSymbol;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.ScalarSubquery;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.util.ValueIterator;
import org.teiid.query.sql.util.VariableContext;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.util.CommandContext;


/**
 * <p>This utility handles the work of processing a subquery; certain types
 * of processor nodes will use an instance of this class to do that work.
 */
public class SubqueryAwareEvaluator extends Evaluator {

	private static final class SimpleProcessorPlan extends ProcessorPlan {
		private final Query command;
		private final FunctionDescriptor fd;
		private ProcessorDataManager dataMgr;
		TupleSource ts;
		private List<? extends Expression> output;

		private SimpleProcessorPlan(Query command, FunctionDescriptor fd, List<? extends Expression> output) {
			this.command = command;
			this.fd = fd;
			this.output = output;
		}
		
		@Override
		public void initialize(CommandContext context,
				ProcessorDataManager dataMgr, BufferManager bufferMgr) {
			super.initialize(context, dataMgr, bufferMgr);
			this.dataMgr = dataMgr;
		}

		@Override
		public void open() throws TeiidComponentException, TeiidProcessingException {
			RegisterRequestParameter parameterObject = new RegisterRequestParameter();
			ts = dataMgr.registerRequest(getContext(), command, fd.getSchema(), parameterObject);
		}

		@Override
		public TupleBatch nextBatch() throws BlockedException,
				TeiidComponentException, TeiidProcessingException {
			ArrayList<List<?>> result = new ArrayList<List<?>>(2);
			List<?> list = ts.nextTuple();
			if (list != null) {
				result.add(list);
				list = ts.nextTuple();
				if (list != null) {
					result.add(list);
				}
			}
			ts.closeSource();
			TupleBatch tb = new TupleBatch(1, result);
			tb.setTerminationFlag(true);
			return tb;
		}

		@Override
		public List getOutputElements() {
			return output;
		}

		@Override
		public void close() throws TeiidComponentException {
			if (ts != null) {
				ts.closeSource();
			}
			ts = null;
		}

		@Override
		public void reset() {
			ts = null;
		}

		@Override
		public ProcessorPlan clone() {
			return new SimpleProcessorPlan(command, fd, output);
		}
	}

	@SuppressWarnings("serial")
	private final class LRUBufferCache extends LRUCache<List<?>, TupleBuffer> {
		
		private LRUCache<List<?>, TupleBuffer> spillOver;
		
		private LRUBufferCache(int maxSize, LRUCache<List<?>, TupleBuffer> spillOver) {
			super(maxSize);
			this.spillOver = spillOver;
		}

		protected boolean removeEldestEntry(Map.Entry<java.util.List<?>,TupleBuffer> eldest) {
			if (super.removeEldestEntry(eldest)) {
				if (spillOver != null && eldest.getValue().getRowCount() <= 2) {
					spillOver.put(eldest.getKey(), eldest.getValue());
				} else {
					eldest.getValue().remove();
				}
				return true;
			}
			return false;
		}
		
		@Override
		public void clear() {
			if (!isEmpty()) {
				for (TupleBuffer buffer : values()) {
					buffer.remove();
				}
				super.clear();
			}
		}
	}

	public static class SubqueryState {
		QueryProcessor processor;
		BatchCollector collector;
		ProcessorPlan plan;
		List<Object> refValues;
		boolean comparable = true;
		
		void close(boolean removeBuffer) {
			if (processor == null) {
				return;
			}
			processor.closeProcessing();
			if (removeBuffer) {
				collector.getTupleBuffer().remove();
			}
			processor = null;
		}
	}
	
	//environment
	private BufferManager manager;
	
	//processing state
	private Map<String, SubqueryState> subqueries = new HashMap<String, SubqueryState>();
	private Map<Command, String> commands = new HashMap<Command, String>(); //TODO: could determine this ahead of time
	private LRUCache<List<?>, TupleBuffer> smallCache = new LRUBufferCache(1024, null);
	private LRUCache<List<?>, TupleBuffer> cache = new LRUBufferCache(512, smallCache);
	private int maxTuples = BufferManager.DEFAULT_PROCESSOR_BATCH_SIZE << 4;
	private int currentTuples = 0;
	
	private Map<Function, ScalarSubquery> functionState;
	
	public SubqueryAwareEvaluator(Map elements, ProcessorDataManager dataMgr,
			CommandContext context, BufferManager manager) {
		super(elements, dataMgr, context);
		this.manager = manager;
		//default to 16 batches
		if (this.manager != null) {
			this.maxTuples = this.manager.getProcessorBatchSize() << 4;
		}
		//TODO the number of cache entries and the max tuples should be based upon the reference count and types involved as well.
	}
	
	public void reset() {
		for (SubqueryState subQueryState : subqueries.values()) {
			subQueryState.plan.reset();
			subQueryState.close(true);
		}
		cache.clear();
		smallCache.clear();
		currentTuples = 0;
		if (this.functionState != null) {
			this.functionState.clear();
		}
	}
	
	public void close() {
		reset();
		commands.clear();
		subqueries.clear();
	}
	
	@Override
	protected ValueIterator evaluateSubquery(SubqueryContainer<?> container,
			List<?> tuple) throws TeiidProcessingException, BlockedException,
			TeiidComponentException {
		ContextReference ref = (ContextReference)container;
		String key = ref.getContextSymbol();
		SubqueryState state = this.subqueries.get(key);
		if (state == null) {
			String otherKey = commands.get(container.getCommand());
			if (otherKey != null) {
				state = this.subqueries.get(otherKey);
				if (state != null) {
					key = otherKey;
				}
			}
		}
		if (state == null) {	
			state = new SubqueryState();
			state.plan = container.getCommand().getProcessorPlan().clone();
	        if (container.getCommand().getCorrelatedReferences() != null) {
		        for (ElementSymbol es : container.getCommand().getCorrelatedReferences().getKeys()) {
		        	if (DataTypeManager.isNonComparable(DataTypeManager.getDataTypeName(es.getType()))) {
		        		state.comparable = false;
		        		break;
		        	}
		        }
	        }
			this.subqueries.put(key, state);
			this.commands.put(container.getCommand(), key);
		}
		SymbolMap correlatedRefs = container.getCommand().getCorrelatedReferences();
		VariableContext currentContext = null;
		boolean shouldClose = false;
		boolean deterministic = true;
		if (state.processor != null) {
			Determinism determinism = state.processor.getContext().getDeterminismLevel();
			deterministic = Determinism.COMMAND_DETERMINISTIC.compareTo(determinism) <= 0;
			if (!deterministic) {
				shouldClose = true;
			}
		}
		boolean removeBuffer = true;
		if (correlatedRefs != null) {
            currentContext = new VariableContext();
            for (Map.Entry<ElementSymbol, Expression> entry : container.getCommand().getCorrelatedReferences().asMap().entrySet()) {
				currentContext.setValue(entry.getKey(), evaluate(entry.getValue(), tuple));
			}
            List<Object> refValues = currentContext.getLocalValues();
            if (!refValues.equals(state.refValues)) {
            	if (state.comparable && deterministic) {
            		if (state.processor != null) {
	    				//cache the old value
            			TupleBuffer tb = state.collector.collectTuples();
            			//recheck determinism as the plan may not have been fully processed by the initial check 
            			Determinism determinism = state.processor.getContext().getDeterminismLevel();
            			deterministic = Determinism.COMMAND_DETERMINISTIC.compareTo(determinism) <= 0;
            			if (deterministic) {
	            			//allowed to track up to 4x the maximum results size
		    				maxTuples = Math.max(tb.getRowCount() << 2, maxTuples);
		    				ArrayList<Object> cacheKey = new ArrayList<Object>(state.refValues);
		    				cacheKey.add(key);
		    				tb.saveBatch(); //ensure that we aren't leaving large last batches in memory
		    				this.cache.put(cacheKey, tb);
		    				removeBuffer = false;
		    				this.currentTuples += tb.getRowCount();
		    				while (this.currentTuples > maxTuples && !cache.isEmpty()) {
		    					Iterator<TupleBuffer> i = this.cache.values().iterator();
		    					TupleBuffer buffer = i.next();
		    					if (buffer.getRowCount() <= 2) {
		    						this.smallCache.put(cacheKey, buffer);
		    					} else {
		    						buffer.remove();
		    					}
		    					this.currentTuples -= buffer.getRowCount();
		    					i.remove();
		    				}
            			}
            		}
    				//find if we have cached values
    				List<Object> cacheKey = new ArrayList<Object>(refValues);
    				cacheKey.add(key);
    				TupleBuffer cachedResult = cache.get(cacheKey);
    				if (cachedResult == null) {
    					cachedResult = smallCache.get(cacheKey);
    				}
    				if (cachedResult != null) {
    					state.close(false);
    					return new TupleSourceValueIterator(cachedResult.createIndexedTupleSource(), 0);
    				}
    			}
            	state.refValues = refValues;
            	shouldClose = true;
            }
		}
		if (shouldClose) {
			state.close(removeBuffer);
		}
		if (state.processor == null) {
			CommandContext subContext = context.clone();
			state.plan.reset();
	        state.processor = new QueryProcessor(state.plan, subContext, manager, this.dataMgr);
	        if (currentContext != null) {
	        	state.processor.getContext().pushVariableContext(currentContext);
	        }
	        state.collector = state.processor.createBatchCollector();
		}
		return new TupleSourceValueIterator(state.collector.collectTuples().createIndexedTupleSource(), 0);
	}
	
	/**
	 * Implements must pushdown function hanlding if supported by the source.
	 * 
	 * The basic strategy is to create a dummy subquery to represent the evaluation
	 */
	@Override
	protected Object evaluatePushdown(Function function, List<?> tuple,
			Object[] values) throws TeiidComponentException, TeiidProcessingException {
		final FunctionDescriptor fd = function.getFunctionDescriptor();
	    if (fd.getMethod() == null || !CapabilitiesUtil.supports(Capability.SELECT_WITHOUT_FROM, fd.getMethod().getParent(), context.getMetadata(), context.getQueryProcessorFactory().getCapabiltiesFinder())) {
	    	throw new FunctionExecutionException(QueryPlugin.Event.TEIID30341, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30341, function.getFunctionDescriptor().getFullName()));
	    }
		ScalarSubquery ss = null;
		if (functionState != null) {
			ss = functionState.get(function);
		}
		Expression[] functionArgs = new Expression[values.length];
	    for(int i=0; i < values.length; i++) {
	        functionArgs[i] = new Constant(values[i]);
	    }  
		if (ss == null) {
	    	final Query command = new Query();
	    	Select select = new Select();
	    	command.setSelect(select);
		    Function f = new Function(function.getName(), functionArgs);
		    f.setType(function.getType());
		    f.setFunctionDescriptor(function.getFunctionDescriptor());
	    	select.addSymbol(f);
	    	ss = new ScalarSubquery(command);
	    	SymbolMap correlatedReferences = new SymbolMap();
	    	Collection<ElementSymbol> elements = ElementCollectorVisitor.getElements(function, true);
	    	if (!elements.isEmpty()) {
				for (ElementSymbol es : elements) {
		    		correlatedReferences.addMapping(es, es);
		    	}
	    		command.setCorrelatedReferences(correlatedReferences);
	    	}
	    	command.setProcessorPlan(new SimpleProcessorPlan(command, fd, Arrays.asList(new Constant(null, fd.getReturnType()))));
		} else {
			((Function)((ExpressionSymbol)ss.getCommand().getProjectedSymbols().get(0)).getExpression()).setArgs(functionArgs);
		}
		if (functionState == null) {
			this.functionState = new HashMap<Function, ScalarSubquery>(2);
		}
		functionState.put(function, ss);
		return internalEvaluate(ss, tuple);
	}
	
}
