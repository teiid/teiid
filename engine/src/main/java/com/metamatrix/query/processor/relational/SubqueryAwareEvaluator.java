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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.common.buffer.TupleSourceNotFoundException;
import com.metamatrix.query.eval.Evaluator;
import com.metamatrix.query.processor.ProcessorDataManager;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.processor.QueryProcessor;
import com.metamatrix.query.sql.lang.SubqueryContainer;
import com.metamatrix.query.sql.symbol.ContextReference;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.util.ValueIterator;
import com.metamatrix.query.sql.util.VariableContext;
import com.metamatrix.query.util.CommandContext;

/**
 * <p>This utility handles the work of processing a subquery; certain types
 * of processor nodes will use an instance of this class to do that work.
 */
public class SubqueryAwareEvaluator extends Evaluator {

	public class SubqueryState {
		QueryProcessor processor;
		boolean done;
		List<?> tuple;
		ProcessorPlan plan;
		
		void close() throws MetaMatrixComponentException {
			if (processor == null) {
				return;
			}
			try {
				processor.closeProcessing();
				TupleSourceID id = processor.getResultsID();
	            manager.removeTupleSource(id);
			} catch (TupleSourceNotFoundException e) {
				
			} 
			processor = null;
			this.done = false;
		}
	}
	
	//environment
	private BufferManager manager;
	
	//processing state
	private Map<String, SubqueryState> subqueries = new HashMap<String, SubqueryState>();
		
	public SubqueryAwareEvaluator(Map elements, ProcessorDataManager dataMgr,
			CommandContext context, BufferManager manager) {
		super(elements, dataMgr, context);
		this.manager = manager;
	}

	public void reset() {
		for (SubqueryState subQueryState : subqueries.values()) {
			subQueryState.plan.reset();
		}
	}
	
	public void close() throws MetaMatrixComponentException {
		for (SubqueryState state : subqueries.values()) {
			state.close();
		}
	}
	
	@Override
	protected ValueIterator evaluateSubquery(SubqueryContainer container,
			List tuple) throws MetaMatrixProcessingException, BlockedException,
			MetaMatrixComponentException {
		ContextReference ref = (ContextReference)container;
		String key = (ref).getContextSymbol();
		SubqueryState state = this.subqueries.get(key);
		if (state == null) {
			state = new SubqueryState();
			state.plan = (ProcessorPlan)container.getCommand().getProcessorPlan().clone();
			this.subqueries.put(key, state);
		}
		if ((tuple == null && state.tuple != null) || (tuple != null && !tuple.equals(state.tuple))) {
			if (container.getCommand().getCorrelatedReferences() != null) {
				state.close();
			}
			state.tuple = tuple;
		}
		if (!state.done) {
			if (state.processor == null) {
				CommandContext subContext = (CommandContext) context.clone();
		        state.processor = new QueryProcessor(state.plan, subContext, manager, this.dataMgr);
		        if (container.getCommand().getCorrelatedReferences() != null) { 
		            VariableContext currentContext = new VariableContext();
		            for (Map.Entry<ElementSymbol, Expression> entry : container.getCommand().getCorrelatedReferences().asMap().entrySet()) {
						currentContext.setValue(entry.getKey(), evaluate(entry.getValue(), tuple));
					}
					state.processor.getContext().pushVariableContext(currentContext);
				}
			}
			state.processor.process(Integer.MAX_VALUE);
			state.done = true;
			state.processor.getProcessorPlan().reset();
		}
		return new DependentValueSource(state.processor.getResultsID(), this.manager).getValueIterator(ref.getValueExpression());
	}
	
}
