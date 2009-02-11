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
import java.util.List;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.sql.visitor.ValueIteratorProviderCollectorVisitor;

/**
 * A project node containing one or more scalar subqueries.
 * These subqueries must be processed first before the 
 * ScalarSubquery expression can be evaluated and the 
 * project can proceed.
 */
public class DependentProjectNode extends ProjectNode {

    private SubqueryProcessorUtility subqueryProcessor;

    /**
     * @param nodeID
     */
    public DependentProjectNode(int nodeID) {
        super(nodeID);
        this.subqueryProcessor = new SubqueryProcessorUtility();
    }

    public void setPlansAndValueProviders(List subqueryProcessorPlans, List valueIteratorProviders) {
        subqueryProcessor.setPlansAndValueProviders(subqueryProcessorPlans, valueIteratorProviders);
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
     * This subclass will execute any subqueries which the projection is
     * dependent on; if any subqueries are correlated, this class will  
     * use the current tuple to execute correlated subqueries
     * @param elementMap Map of ElementSymbol elements to Integer indices into
     * the currentTuple parameter
     * @param currentTuple the current tuple about to be processed by
     * this node
     * @see com.metamatrix.query.processor.relational.ProjectNode#prepareToProcessTuple
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
        DependentProjectNode clonedNode = new DependentProjectNode(super.getID());
        super.copy(this, clonedNode);
        
        List processorPlans = this.subqueryProcessor.getSubqueryPlans();
        
        List clonedValueIteratorProviders = new ArrayList(processorPlans.size());
        ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(clonedNode.getSelectSymbols(), clonedValueIteratorProviders);
        List clonedProcessorPlans = new ArrayList(clonedValueIteratorProviders.size());
        for (int i=0; i<clonedValueIteratorProviders.size(); i++){
            ProcessorPlan aPlan = (ProcessorPlan)processorPlans.get(i);
            clonedProcessorPlans.add(aPlan.clone());
        }
        clonedNode.setPlansAndValueProviders(clonedProcessorPlans, clonedValueIteratorProviders);

        //Since Reference.clone() returns itself and not a true clone, this shallow clone is sufficient
        //to clone the Collection of References
        if (this.subqueryProcessor.getCorrelatedReferences() != null){
            List clonedReferences = new ArrayList(this.subqueryProcessor.getCorrelatedReferences());
            clonedNode.setCorrelatedReferences(clonedReferences);
        }
        
        return clonedNode;
    }   

    public Map getDescriptionProperties() {
        // Default implementation - should be overridden
        Map props = super.getDescriptionProperties();
        props.put(PROP_TYPE, "Dependent Project"); //$NON-NLS-1$
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
