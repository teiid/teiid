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
    public DependentProjectNode(int nodeID, SubqueryProcessorUtility spu) {
        super(nodeID);
        this.subqueryProcessor = spu;
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
        
        this.subqueryProcessor.process(this, elementMap, currentTuple);
    }

    /**
     * Returns a deep clone
     * @return deep clone of this object
     * @see java.lang.Object#clone()
     */
    public Object clone(){
        DependentProjectNode clonedNode = new DependentProjectNode(super.getID(), this.subqueryProcessor.clone());
        super.copy(this, clonedNode);
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
