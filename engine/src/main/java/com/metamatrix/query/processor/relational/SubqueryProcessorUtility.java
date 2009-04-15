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
import java.util.List;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.IndexedTupleSource;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.common.buffer.TupleSourceNotFoundException;
import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.query.eval.Evaluator;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.processor.QueryProcessor;
import com.metamatrix.query.sql.lang.SubqueryContainer;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.util.SymbolMap;
import com.metamatrix.query.sql.util.ValueIterator;
import com.metamatrix.query.sql.util.ValueIteratorProvider;
import com.metamatrix.query.sql.util.VariableContext;
import com.metamatrix.query.util.CommandContext;

/**
 * <p>This utility handles the work of processing a subquery; certain types
 * of processor nodes will use an instance of this class to do that work.
 * All the corresponding "child" or "sub" ProcessorPlans
 * (one for each ValueIteratorProvider) must be processed before the
 * client processor node can do it's work.</p>
 *
 * <p>For example, a DependentSelectNode is basically a select node where
 * part of the criteria is a subquery.  The criteria cannot be evaluated
 * until the values of the subquery result are available.  A
 * DependentProjectNode is a project node where one of the projected
 * symbols is a scalar subquery.  That project cannot be performed until
 * the scalar subquery is executed and it's value is available so that the
 * subquery can be evaluated as an expression.</p>
 *
 * <p>The ValueIteratorProvider interface abstracts a language object that
 * needs to have it's subquery processed and the resulting values given to
 * it.  This utility class does the processing of the subquery ProcessorPlan,
 * and provides the ValueIteratorProvider instance with a ValueIterator
 * instance, which iterates over the subquery results.</p>
 */
public class SubqueryProcessorUtility {
	
	/**
     * List of ProcessorPlans for each subquery.  Contents match
     * 1-for-1 with the contents of {@link #valueIteratorProviders} List
     * see {@link #setPlansAndValueProviders}
     */
	private List<ProcessorPlan> processorPlans = new ArrayList<ProcessorPlan>();
	private List<QueryProcessor> processors = new ArrayList<QueryProcessor>();

	/**
     * List of ValueIteratorProvider for each subquery.  Contents match
     * 1-for-1 with the contents of {@link #processorPlans} List
     * see {@link #setPlansAndValueProviders}
     */
	private List<ValueIteratorProvider> valueIteratorProviders;

    private SymbolMap correlatedReferences;

	// "Placeholder" state, for resuming processing after
	// a BlockedException - not cloned
	private int currentIndex = 0;
	private QueryProcessor currentProcessor;

    // List <TupleSourceID> - same index-matchup as other two Lists
	// Need to clean up on close()
	private List<TupleSourceID> tupleSources = new ArrayList<TupleSourceID>();
	
	private VariableContext currentContext;

	public SubqueryProcessorUtility(List<ValueIteratorProvider> valList, SymbolMap references) {
		for (ValueIteratorProvider val : valList) {
			SubqueryContainer sc = (SubqueryContainer)val;
			this.processorPlans.add((ProcessorPlan)sc.getCommand().getProcessorPlan().clone());
		}
		this.valueIteratorProviders = valList;
		if (references != null && !references.asMap().isEmpty()) {
			this.correlatedReferences = references;
		}
	}

	List<ProcessorPlan> getSubqueryPlans(){
		return this.processorPlans;
	}

	List getValueIteratorProviders(){
		return this.valueIteratorProviders;
	}

	void reset() {
		this.currentIndex = 0;
		currentProcessor = null;
        // Reset internal plans
        for(int i=0; i<processorPlans.size(); i++) {
            ProcessorPlan plan = processorPlans.get(i);
            plan.reset();
        }
	}

	/**
	 * initializes each subquery ProcessorPlan
	 * @throws MetaMatrixComponentException 
	 */
	void open(RelationalNode parent) throws MetaMatrixComponentException {
		// Open subquery processor plans
		for (ProcessorPlan plan : this.processorPlans) {
            CommandContext subContext = (CommandContext) parent.getContext().clone();
            QueryProcessor processor = new QueryProcessor(plan, subContext, parent.getBufferManager(), parent.getDataManager());
            this.processors.add(processor);
            this.tupleSources.add(processor.getResultsID());
		}
	}

	/**
	 * Removes the temporary tuple sources of the subquery results
	 */
	void close(BufferManager bufferManager)
		throws MetaMatrixComponentException {

		for (QueryProcessor processor : this.processors) {
			try {
				processor.closeProcessing();
			} catch (TupleSourceNotFoundException e) {
			}
		}	
		this.processors.clear();
		
		for (TupleSourceID tsID : this.tupleSources) {
			try {
                bufferManager.removeTupleSource(tsID);
			} catch (TupleSourceNotFoundException e) {
				//ignore
			}
		}

        this.tupleSources.clear();
	}

	/**
     * <p>Processes processor plans (each key), stores as TupleSource Iterators in 
     * each ValueIteratorProvider (each value).  Continues synchronously until
     * all processing is completed or until a BlockedException is thrown.  This
     * method can be called after a BlockedException and will resume processing
     * where it left off.</p>
     * 
     * <p>After this method completes (including after multiple method calls
     * that resulted in a BlockedException), if this method is called again,
     * it will start processing over ONLY IF this instance has any correlated subquery
     * references.  The assumption is that it will not be called again until
     * a new outer currentTuple is being passed in.</p>
     * 
     * @param elementMap Map of ElementSymbol elements to Integer indices into
     * the currentTuple parameter
     * @param currentTuple current tuple of the containing query
     * @param bufferManager BufferManager
     * @param groupName String group name of client processor node
     * @throws BlockedException potentially at any time during processing
     * @throws MetaMatrixComponentException for unexpected exception
     * @throws MetaMatrixProcessingException for exception due to user input or modeling
	 */
	void process(RelationalNode parent, Map elementMap, List currentTuple)
		throws BlockedException, MetaMatrixComponentException, MetaMatrixProcessingException {

        // This IF block is only intended to be run when the outer query tuple has changed,
        // so it should not be run when this instance is resuming processing after a BlockedException
        if (this.currentContext == null && this.currentProcessor == null && this.correlatedReferences != null){ 
            // Close old tuple sources
            this.close(parent.getBufferManager());
            this.open(parent);
            this.currentContext = new VariableContext();
            for (Map.Entry<ElementSymbol, Expression> entry : this.correlatedReferences.asMap().entrySet()) {
				this.currentContext.setValue(entry.getKey(), new Evaluator(elementMap, parent.getDataManager(), parent.getContext()).evaluate(entry.getValue(), currentTuple));
			}
        }
        
		while (this.currentProcessor != null || this.currentIndex < this.processorPlans.size()){
			//Initialize current ProcessorPlan tuple source, if necessary
			if (this.currentProcessor == null){
				this.currentProcessor = this.processors.get(currentIndex);
				if (this.currentContext != null) {
					this.currentProcessor.getContext().pushVariableContext(this.currentContext);
				}
			}

			// Process the results
			IndexedTupleSource subqueryResults = null;
			try {
				this.currentProcessor.process(Integer.MAX_VALUE);
				subqueryResults = parent.getBufferManager().getTupleSource(this.currentProcessor.getResultsID());
				this.currentProcessor.getProcessorPlan().reset();
			} catch (MetaMatrixProcessingException e) {
				throw e;
			} catch (MetaMatrixComponentException e) {
				throw e;
			} catch (MetaMatrixCoreException e) {
				throw new MetaMatrixComponentException(e);
			}

			// Set the results on the ValueIteratorProviders
            ValueIteratorProvider vip = this.valueIteratorProviders.get(this.currentIndex);
            ValueIterator iterator = new TupleSourceValueIterator(subqueryResults, 0);
			vip.setValueIterator(iterator);

            this.currentProcessor = null;
			this.currentIndex++;
		}
        
        if (this.correlatedReferences != null){
            // If correlated references are present, re-zero currentIndex - 
            // If we've made it this far, then the next time 
            // this method is called, it should be for a new outer tuple 
            currentIndex = 0;
        }   
        this.currentContext = null;
	}
	
}
