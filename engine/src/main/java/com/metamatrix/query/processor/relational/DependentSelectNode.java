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
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.sql.visitor.ValueIteratorProviderCollectorVisitor;

/**
 * This node represents a Select node for the case where the criteria
 * is or is composed of one or more SubqueryContainer criteria objects (for
 * subqueries in the criteria).  In
 * that case, all the corresponding "child" or "sub" ProcessorPlans
 * (one for each SubqueryContainer criteria) must be processed before the
 * entire criteria can be evaluated.
 */
public class DependentSelectNode extends SelectNode {

    private SubqueryProcessorUtility subqueryProcessor;

	/**
	 * Constructor for DependentSelectNode.
	 * @param nodeID
	 */
	public DependentSelectNode(int nodeID) {
		super(nodeID);
        this.subqueryProcessor = new SubqueryProcessorUtility();
	}

    /** for unit testing */
    SubqueryProcessorUtility getSubqueryProcessorUtility(){
        return this.subqueryProcessor;
    }

	/**
	 * Set the two Lists that map subquery ProcessorPlans to SubqueryContainer criteria
	 * which hold the Commands represented by the ProcessorPlans.  The objects at
	 * each index of both lists are essentially "mapped" to each other, one to one.
	 * At a given index, the ProcessorPlan in the one List will be processed and
	 * "fill" the SubqueryContainer criteria (of the other List) so the SubqueryContainer criteria
	 * can be evalutated later.
	 * @param subqueryProcessorPlans List of ProcessorPlans
	 * @param subqueryContainerCriteria List of SubqueryContainer criteria
	 */
	public void setPlansAndCriteriaMapping(List subqueryProcessorPlans, List subqueryContainerCriteria) {
        subqueryProcessor.setPlansAndValueProviders(subqueryProcessorPlans, subqueryContainerCriteria);
	}
    
    /**
     * Set List of References needing to be updated with each outer tuple
     * @param correlatedReferences List<Reference> correlated reference to outer query
     */
    public void setCorrelatedReferences(List correlatedReferences){
        subqueryProcessor.setCorrelatedReferences(correlatedReferences);
    }

	public void reset() {
		super.reset();
        this.subqueryProcessor.reset();
	}

	/**
	 * Calls super.open(), then initializes subquery processor
	 */
	public void open() 
		throws MetaMatrixComponentException, MetaMatrixProcessingException {

		super.open();
        this.subqueryProcessor.open(this.getContext(), this.getBatchSize(), this.getDataManager(), this.getBufferManager());
	}

	/**
	 * Closes the subquery processor (which removes the temporary tuple 
     * sources of the subquery results)
	 * @see com.metamatrix.query.processor.relational.RelationalNode#close()
	 */
	public void close()
		throws MetaMatrixComponentException {
	    if (!isClosed()) {
    		super.close();
            this.subqueryProcessor.close(this.getBufferManager());
        }
	}
	
	/**
     * This subclass will execute any subqueries which the criteria is
     * dependent on; if any subqueries are correlated, this class will  
     * use the current tuple to execute correlated subqueries
     * @param elementMap Map of ElementSymbol elements to Integer indices into
     * the currentTuple parameter
     * @param currentTuple the current tuple about to be processed by
     * this node
	 * @see com.metamatrix.query.processor.relational.SelectNode#prepareToProcessTuple
	 */
	protected void prepareToProcessTuple(Map elementMap, List currentTuple)
	throws BlockedException, MetaMatrixComponentException, MetaMatrixProcessingException {
        
        this.subqueryProcessor.process(elementMap, currentTuple, this.getBufferManager(), this.getConnectionID());
	}
		
	/**
	 * Returns a deep clone
	 * @return deep clone of this object
	 * @see java.lang.Object#clone()
	 */
	public Object clone(){
		DependentSelectNode clonedNode = new DependentSelectNode(super.getID());
		super.copy(this, clonedNode);
        
        List processorPlans = this.subqueryProcessor.getSubqueryPlans();
		
		List clonedValueIteratorProviders = new ArrayList(processorPlans.size());
        ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(clonedNode.getCriteria(), clonedValueIteratorProviders);
		List clonedProcessorPlans = new ArrayList(clonedValueIteratorProviders.size());
		for (int i=0; i<clonedValueIteratorProviders.size(); i++){
			ProcessorPlan aPlan = (ProcessorPlan)processorPlans.get(i);
			clonedProcessorPlans.add(aPlan.clone());
		}
		clonedNode.setPlansAndCriteriaMapping(clonedProcessorPlans, clonedValueIteratorProviders);

        //Since Reference.clone() returns itself and not a true clone, this shallow clone is sufficient
        //to clone the Collection of References
        if (this.subqueryProcessor.getCorrelatedReferences() != null){
            List clonedReferences = new ArrayList(this.subqueryProcessor.getCorrelatedReferences());
            clonedNode.setCorrelatedReferences(clonedReferences);
        }

		return clonedNode;
	}	
    
    /* 
     * @see com.metamatrix.query.processor.Describable#getDescriptionProperties()
     */
    public Map getDescriptionProperties() {   
        // Default implementation - should be overridden     
        Map props = super.getDescriptionProperties();
        props.put(PROP_TYPE, "Dependent Select"); //$NON-NLS-1$
        props.put(PROP_CRITERIA, getCriteria().toString());        
        return props;
    }

    /** 
     * @see com.metamatrix.query.processor.relational.RelationalNode#getSubPlans()
     * @since 4.2
     */
    public List getChildPlans() {
        return this.subqueryProcessor.getSubqueryPlans();
    }

}
