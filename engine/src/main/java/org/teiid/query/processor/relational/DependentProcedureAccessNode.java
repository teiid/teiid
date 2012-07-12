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

import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.util.VariableContext;
import org.teiid.query.util.CommandContext;


public class DependentProcedureAccessNode extends AccessNode {

    private Criteria inputCriteria;
    private List inputReferences;
    private List inputDefaults;

    // processing state
    private DependentProcedureCriteriaProcessor criteriaProcessor;

    public DependentProcedureAccessNode(int nodeID,
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
        DependentProcedureAccessNode copy = new DependentProcedureAccessNode(getID(), inputCriteria,
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
    
    @Override
    public void open() throws TeiidComponentException,
    		TeiidProcessingException {
    	CommandContext context  = getContext().clone();
    	context.pushVariableContext(new VariableContext());
    	this.setContext(context);
    	DependentProcedureExecutionNode.shareVariableContext(this, context);
    	super.open();
    }

    /** 
     * @see org.teiid.query.processor.relational.AccessNode#prepareNextCommand(org.teiid.query.sql.lang.Command)
     */
    protected boolean prepareNextCommand(Command atomicCommand) throws TeiidComponentException, TeiidProcessingException {
        
        if (this.criteriaProcessor == null) {
            this.criteriaProcessor = new DependentProcedureCriteriaProcessor(this, (Criteria)inputCriteria.clone(), inputReferences, inputDefaults);
        }
        
        if (criteriaProcessor.prepareNextCommand(this.getContext().getVariableContext())) {
        	return super.prepareNextCommand(atomicCommand);
        }
        
        return false;
    }
    
    @Override
    protected boolean processCommandsIndividually() {
    	return true;
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
    public Boolean requiresTransaction(boolean transactionalReads) {
    	return true; //TODO: check the underlying 
    }

}
