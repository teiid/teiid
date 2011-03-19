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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.processor.BatchCollector;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.QueryProcessor;
import org.teiid.query.sql.lang.SubqueryContainer;
import org.teiid.query.sql.symbol.ContextReference;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.ScalarSubquery;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.util.ValueIterator;
import org.teiid.query.sql.util.VariableContext;
import org.teiid.query.sql.visitor.FunctionCollectorVisitor;
import org.teiid.query.util.CommandContext;


/**
 * <p>This utility handles the work of processing a subquery; certain types
 * of processor nodes will use an instance of this class to do that work.
 */
public class SubqueryAwareEvaluator extends Evaluator {

	public class SubqueryState {
		QueryProcessor processor;
		BatchCollector collector;
		boolean done;
		ProcessorPlan plan;
		boolean nonDeterministic;
		List<Object> refValues;
		boolean comparable = true;
		
		void close() {
			if (processor == null) {
				return;
			}
			processor.closeProcessing();
			collector.getTupleBuffer().remove();
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
	
	public void close() {
		for (SubqueryState state : subqueries.values()) {
			state.close();
		}
	}
	
	@Override
	protected ValueIterator evaluateSubquery(SubqueryContainer container,
			List tuple) throws TeiidProcessingException, BlockedException,
			TeiidComponentException {
		ContextReference ref = (ContextReference)container;
		String key = (ref).getContextSymbol();
		SubqueryState state = this.subqueries.get(key);
		if (state == null) {
			state = new SubqueryState();
			state.plan = container.getCommand().getProcessorPlan().clone();
	        if (container instanceof ScalarSubquery) {
				state.nonDeterministic = FunctionCollectorVisitor.isNonDeterministic(container.getCommand());
			}
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
		boolean shouldClose = state.done && state.nonDeterministic;
		if (correlatedRefs != null) {
            currentContext = new VariableContext();
            for (Map.Entry<ElementSymbol, Expression> entry : container.getCommand().getCorrelatedReferences().asMap().entrySet()) {
				currentContext.setValue(entry.getKey(), evaluate(entry.getValue(), tuple));
			}
            List<Object> refValues = currentContext.getLocalValues();
            if (!refValues.equals(state.refValues)) {
            	state.refValues = refValues;
            	shouldClose = true;
            }
		}
		if (shouldClose) {
			//if (state.done && state.comparable) {
				//cache
			//} else {
			state.close();
			//}
		}
		if (!state.done) {
			if (state.processor == null) {
				CommandContext subContext = context.clone();
				state.plan.reset();
		        state.processor = new QueryProcessor(state.plan, subContext, manager, this.dataMgr);
		        if (currentContext != null) {
		        	state.processor.getContext().pushVariableContext(currentContext);
		        }
		        state.collector = state.processor.createBatchCollector();
			}
			state.done = true;
		}
		return new DependentValueSource(state.collector.collectTuples()).getValueIterator(ref.getValueExpression());
	}
	
}
