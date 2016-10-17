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

import java.util.List;

import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.util.CommandContext;


public class DependentProcedureExecutionNode extends PlanExecutionNode {

    private Criteria inputCriteria;
    private List inputReferences;
    private List inputDefaults;

    // processing state
    private DependentProcedureCriteriaProcessor criteriaProcessor;

    public DependentProcedureExecutionNode(int nodeID,
                                           Criteria crit,
                                           List references,
                                           List defaults) {
        super(nodeID);

        this.inputCriteria = crit;
        this.inputDefaults = defaults;
        this.inputReferences = references;
    }

    /**
     * @see org.teiid.query.processor.relational.PlanExecutionNode#clone()
     */
    public Object clone() {
        DependentProcedureExecutionNode copy = new DependentProcedureExecutionNode(getID(), (Criteria)inputCriteria.clone(),
                                                                                   inputReferences,
                                                                                   inputDefaults);
        copyTo(copy);
        return copy;
    }
    
    public void reset() {
        super.reset();
        criteriaProcessor = null;
    }
    
    public void closeDirect() {
        super.closeDirect();

        if (criteriaProcessor != null) {
            criteriaProcessor.close();
        }
    }

    /**
     * @see org.teiid.query.processor.relational.PlanExecutionNode#prepareNextCommand()
     */
    protected boolean prepareNextCommand() throws BlockedException,
                                          TeiidComponentException, TeiidProcessingException {

        if (this.criteriaProcessor == null) {
            this.criteriaProcessor = new DependentProcedureCriteriaProcessor(this, (Criteria)inputCriteria.clone(), inputReferences, inputDefaults);
        }
        
        return criteriaProcessor.prepareNextCommand(this.getProcessorPlan().getContext().getVariableContext());
    }

    /**
     * @see org.teiid.query.processor.relational.PlanExecutionNode#hasNextCommand()
     */
    protected boolean hasNextCommand() {
        return criteriaProcessor.hasNextCommand();
    }

    /** 
     * @return Returns the inputCriteria.
     */
    public Criteria getInputCriteria() {
        return this.inputCriteria;
    }
    
    @Override
    public void open() throws TeiidComponentException,
    		TeiidProcessingException {
    	super.open();
    	shareVariableContext(this, this.getProcessorPlan().getContext());
    }

	public static void shareVariableContext(RelationalNode node, CommandContext context) {
		// we need to look up through our parents and share this context
    	RelationalNode parent = node.getParent();
    	int projectCount = 0;
    	while (parent != null && projectCount < 2) {
    		parent.setContext(context);
    		if (parent instanceof ProjectNode) {
    			projectCount++;
    		}
    		parent = parent.getParent();
    	}
	}
	
	@Override
	public Boolean requiresTransaction(boolean transactionalReads) {
	    Boolean requires = super.requiresTransaction(transactionalReads);
        if (requires == null || requires) {
            return true;
        }
        return false;
	}

}
