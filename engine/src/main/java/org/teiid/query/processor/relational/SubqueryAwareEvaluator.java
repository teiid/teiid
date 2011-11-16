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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.LRUCache;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.processor.BatchCollector;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.QueryProcessor;
import org.teiid.query.sql.lang.SubqueryContainer;
import org.teiid.query.sql.symbol.ContextReference;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.util.ValueIterator;
import org.teiid.query.sql.util.VariableContext;
import org.teiid.query.util.CommandContext;


/**
 * <p>This utility handles the work of processing a subquery; certain types
 * of processor nodes will use an instance of this class to do that work.
 */
public class SubqueryAwareEvaluator extends Evaluator {

	public class SubqueryState {
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
	private LRUCache<List<?>, TupleBuffer> cache = new LRUCache<List<?>, TupleBuffer>(1024);
	private int maxTuples = BufferManager.DEFAULT_PROCESSOR_BATCH_SIZE << 4;
	private int currentTuples = 0;
	
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
		}
	}
	
	public void close() {
		for (SubqueryState state : subqueries.values()) {
			state.close(true);
		}
		for (TupleBuffer buffer : cache.values()) {
			buffer.remove();
		}
		cache.clear();
	}
	
	@Override
	protected ValueIterator evaluateSubquery(SubqueryContainer<?> container,
			List<?> tuple) throws TeiidProcessingException, BlockedException,
			TeiidComponentException {
		ContextReference ref = (ContextReference)container;
		String key = ref.getContextSymbol();
		SubqueryState state = this.subqueries.get(key);
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
		    					//TODO: this should handle empty results better
		    					Iterator<TupleBuffer> i = this.cache.values().iterator();
		    					TupleBuffer buffer = i.next();
		    					buffer.remove();
		    					this.currentTuples -= buffer.getRowCount();
		    					i.remove();
		    				}
            			}
            		}
    				//find if we have cached values
    				List<Object> cacheKey = new ArrayList<Object>(refValues);
    				cacheKey.add(key);
    				TupleBuffer cachedResult = cache.get(cacheKey);
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
	
}
