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
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.util.VariableContext;
import com.metamatrix.query.util.CommandContext;

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

    /*
     * @see com.metamatrix.query.processor.Describable#getDescriptionProperties()
     */
    public Map getDescriptionProperties() {
        Map props = super.getDescriptionProperties();
        props.put(PROP_TYPE, "Dependent Procedure Access"); //$NON-NLS-1$
        return props;
    }

    /**
     * @see com.metamatrix.query.processor.relational.PlanExecutionNode#clone()
     */
    public Object clone() {
        DependentProcedureAccessNode copy = new DependentProcedureAccessNode(getID(), inputCriteria,
                                                                                   inputReferences,
                                                                                   inputDefaults);
        copy(this, copy);
        return copy;
    }
    
    public void reset() {
        super.reset();
        criteriaProcessor = null;
    }
    
    public void close() throws MetaMatrixComponentException {
        if (isClosed()) {
            return;
        }
        super.close();

        if (criteriaProcessor != null) {
            criteriaProcessor.close();
        }
    }
    
    @Override
    public void open() throws MetaMatrixComponentException,
    		MetaMatrixProcessingException {
    	CommandContext context  = (CommandContext)getContext().clone();
    	context.pushVariableContext(new VariableContext());
    	this.setContext(context);
    	DependentProcedureExecutionNode.shareVariableContext(this, context);
    	super.open();
    }

    /** 
     * @see com.metamatrix.query.processor.relational.AccessNode#prepareNextCommand(com.metamatrix.query.sql.lang.Command)
     */
    protected boolean prepareNextCommand(Command atomicCommand) throws MetaMatrixComponentException, MetaMatrixProcessingException {
        
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
     * @see com.metamatrix.query.processor.relational.PlanExecutionNode#hasNextCommand()
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

}
