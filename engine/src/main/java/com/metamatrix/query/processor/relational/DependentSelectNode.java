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

import java.util.List;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.util.SymbolMap;
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
    private SymbolMap correlatedReferences;

	/**
	 * Constructor for DependentSelectNode.
	 * @param nodeID
	 */
	public DependentSelectNode(int nodeID, SymbolMap correlatedReferences) {
		super(nodeID);
		this.correlatedReferences = correlatedReferences;
	}

    /** for unit testing */
    SubqueryProcessorUtility getSubqueryProcessorUtility(){
        return this.subqueryProcessor;
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
        this.subqueryProcessor.open(this);
	}
	
	@Override
	public void setCriteria(Criteria criteria) {
		super.setCriteria(criteria);
		List valueList = ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(this.getCriteria());
		this.subqueryProcessor = new SubqueryProcessorUtility(valueList, this.correlatedReferences);
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
        
        this.subqueryProcessor.process(this, elementMap, currentTuple);
	}
		
	/**
	 * Returns a deep clone
	 * @return deep clone of this object
	 * @see java.lang.Object#clone()
	 */
	public Object clone(){
		DependentSelectNode clonedNode = new DependentSelectNode(super.getID(), this.correlatedReferences);
		super.copy(this, clonedNode);
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
