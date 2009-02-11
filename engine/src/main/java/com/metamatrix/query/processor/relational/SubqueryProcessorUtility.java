/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.IndexedTupleSource;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.common.buffer.TupleSourceNotFoundException;
import com.metamatrix.common.buffer.BufferManager.TupleSourceStatus;
import com.metamatrix.common.buffer.BufferManager.TupleSourceType;
import com.metamatrix.query.execution.QueryExecPlugin;
import com.metamatrix.query.processor.ProcessorDataManager;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.sql.symbol.Reference;
import com.metamatrix.query.sql.util.ValueIterator;
import com.metamatrix.query.sql.util.ValueIteratorProvider;
import com.metamatrix.query.util.CommandContext;
import com.metamatrix.query.util.ErrorMessageKeys;
import com.metamatrix.query.util.TypeRetrievalUtil;

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
class SubqueryProcessorUtility {

	/**
     * List of ProcessorPlans for each subquery.  Contents match
     * 1-for-1 with the contents of {@link #valueIteratorProviders} List
     * see {@link #setPlansAndValueProviders}
     */
	private List processorPlans;

	/**
     * List of ValueIteratorProvider for each subquery.  Contents match
     * 1-for-1 with the contents of {@link #processorPlans} List
     * see {@link #setPlansAndValueProviders}
     */
	private List valueIteratorProviders;

    /**
     * List<Reference> to set data on
     */
    private List correlatedReferences;

	// "Placeholder" state, for resuming processing after
	// a BlockedException - not cloned
	private int currentIndex = 0;
	private ProcessorPlan currentPlan;
	private TupleSourceID currentID;

    // List <TupleSourceID> - same index-matchup as other two Lists
	// Need to clean up on close()
	private List tupleSources = new ArrayList();

	/**
	 * Constructor
	 */
	SubqueryProcessorUtility() {
	}

	/**
	 * Set the two Lists that map subquery ProcessorPlans to ValueIteratorProviders
	 * which hold the Commands represented by the ProcessorPlans.  The objects at
	 * each index of both lists are essentially "mapped" to each other, one to one.
	 * At a given index, the ProcessorPlan in the one List will be processed and
	 * "fill" the ValueIteratorProvider (of the other List) so the ValueIteratorProvider
	 * can be evalutated later.
	 * @param subqueryProcessorPlans List of ProcessorPlans
	 * @param valueIteratorProviders List of ValueIteratorProviders
	 */
	void setPlansAndValueProviders(List subqueryProcessorPlans, List valueIteratorProviders) {
		this.processorPlans = subqueryProcessorPlans;
		this.valueIteratorProviders = valueIteratorProviders;
	}

    /**
     * Set List of References needing to be updated with each outer tuple
     * @param correlatedReferences List<Reference> correlated reference to outer query
     */
    void setCorrelatedReferences(List correlatedReferences){
    	if (correlatedReferences != null && correlatedReferences.size() > 0) {
    		this.correlatedReferences = correlatedReferences;
    	}
    }

	List getSubqueryPlans(){
		return this.processorPlans;
	}

	List getValueIteratorProviders(){
		return this.valueIteratorProviders;
	}

    List getCorrelatedReferences(){
        return this.correlatedReferences;
    }
    
	void reset() {
		this.currentIndex = 0;
		currentPlan = null;
		currentID = null;

        // Reset internal plans
        for(int i=0; i<processorPlans.size(); i++) {
            ProcessorPlan plan = (ProcessorPlan) processorPlans.get(i);
            plan.reset();
        }
	}

	/**
	 * initializes each subquery ProcessorPlan
	 */
	void open(CommandContext processorContext,
                    int batchSize,
                    ProcessorDataManager dataManager,
                    BufferManager bufferManager) {
		// Open subquery processor plans
		Iterator plans = this.processorPlans.iterator();
		while (plans.hasNext()) {
			ProcessorPlan plan = (ProcessorPlan) plans.next();
            CommandContext subContext = (CommandContext) processorContext.clone();
            subContext.setOutputBatchSize(batchSize);
			plan.initialize(subContext, dataManager, bufferManager);
		}
	}

	/**
	 * Removes the temporary tuple sources of the subquery results
	 */
	void close(BufferManager bufferManager)
		throws MetaMatrixComponentException {

        Iterator i = this.tupleSources.iterator();
		while (i.hasNext()){
			TupleSourceID tsID = (TupleSourceID)i.next();
			try {
                bufferManager.removeTupleSource(tsID);
			} catch (TupleSourceNotFoundException e) {
				//ignore
			}
		}

        //Clear this list, just in case this obj is ever closed and then re-opened
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
	void process(Map elementMap, List currentTuple, BufferManager bufferManager, String groupName)
		throws BlockedException, MetaMatrixComponentException, MetaMatrixProcessingException {

        // This IF block is only intended to be run when the outer query tuple has changed,
        // so it should not be run when this instance is resuming processing after a BlockedException
        if (this.currentPlan == null && this.correlatedReferences != null){ 
            // Close old tuple sources
            this.close(bufferManager);
            Iterator refs = this.correlatedReferences.iterator();
            while (refs.hasNext()) {
                Reference ref = (Reference)refs.next();
                ref.setData(elementMap, currentTuple);
            }
        }

		while (this.currentPlan != null || this.currentIndex < this.processorPlans.size()){
			//Initialize current ProcessorPlan tuple source, if necessary
			if (this.currentPlan == null){
				this.currentPlan = (ProcessorPlan)this.processorPlans.get(this.currentIndex);

				// Run query processor on command
				List schema = this.currentPlan.getOutputElements();
				this.currentID = bufferManager.createTupleSource(schema, TypeRetrievalUtil.getTypeNames(schema), groupName, TupleSourceType.PROCESSOR);
				this.tupleSources.add(this.currentID);

                // Open plan
                this.currentPlan.open();
			}

			// Process the results
			IndexedTupleSource subqueryResults = null;
			while(true) {

				TupleBatch batch = this.currentPlan.nextBatch(); // Might throw BlockedException.
				flushBatch(bufferManager, batch, this.currentID);

				// Check if this was the last batch
				if(batch.getTerminationFlag() == true) {
                    this.currentPlan.close();
                    this.currentPlan.reset();
					try {
                        bufferManager.setStatus(this.currentID, TupleSourceStatus.FULL);
                        subqueryResults = bufferManager.getTupleSource(this.currentID);
					} catch (TupleSourceNotFoundException e) {
                        throw new MetaMatrixComponentException(ErrorMessageKeys.PROCESSOR_0029, QueryExecPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0029, this.currentID));
					}

					break;
				}
			}

			// Set the results on the ValueIteratorProviders
            ValueIteratorProvider vip = (ValueIteratorProvider)this.valueIteratorProviders.get(this.currentIndex);
            ValueIterator iterator = new TupleSourceValueIterator(subqueryResults, 0);
			vip.setValueIterator(iterator);

            this.currentID = null;
            this.currentPlan = null;
			this.currentIndex++;
		}
        
        if (this.correlatedReferences != null){
            // If correlated references are present, re-zero currentIndex - 
            // If we've made it this far, then the next time 
            // this method is called, it should be for a new outer tuple 
            currentIndex = 0;
        }        
	}


	// ========================================================================
	// PRIVATE UTILITY
	// ========================================================================

    /**
     * Flush the batch by giving it to the buffer manager.
     */
    private static void flushBatch(BufferManager bufferManager, TupleBatch batch, TupleSourceID tsID)
    throws MetaMatrixComponentException{
        if(batch != null && batch.getRowCount() > 0) {
            try {
                bufferManager.addTupleBatch(tsID, batch);
            } catch (TupleSourceNotFoundException e) {
                throw new MetaMatrixComponentException(ErrorMessageKeys.PROCESSOR_0029, QueryExecPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0029, tsID));
            }
        }
    }

}
