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

package com.metamatrix.query.processor.proc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.query.eval.ExpressionEvaluator;
import com.metamatrix.query.execution.QueryExecPlugin;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.processor.program.ProgramEnvironment;
import com.metamatrix.query.processor.xml.ProcessorInstruction;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.util.VariableContext;
import com.metamatrix.query.util.ErrorMessageKeys;

/**
 * <p> This instruction updates the current variable context with a value for the Variable
 * defined using a DeclareInstruction, the variable value is obtained by either processing
 * a expression or a command(stored as a processplan). The Processing of the command is
 * expected to result in 1 column, 1 row tuple.</p>
 */
public abstract class AbstractAssignmentInstruction extends CommandInstruction {

	// variable whose value is updated in the context
	private ElementSymbol variable;
	// expression to be processed
	private Expression expression;
	// processorPlan to be processed
	private ProcessorPlan processPlan;
   
    /**
	 * <p> Updates the current variable context with a value for the Variable
	 * defined using a DeclareInstruction, the variable value is obtained by either processing
	 * a expression or a command(stored as a processplan). The Processing of the command is
	 * expected to result in 1 column, 1 row tuple, if more than a row is retuned an exception
	 * is thrown. Also updates the program counter.</p>
     * @throws BlockedException
	 * @throws MetaMatrixComponentException if error processing command or expression on this instruction
     * @see ProcessorInstruction#process(ProcessorEnvironment)
     */
    public void process(ProgramEnvironment env) throws BlockedException,
                                               MetaMatrixComponentException, MetaMatrixProcessingException {

        ProcedureEnvironment procEnv = (ProcedureEnvironment)env;
                
        VariableContext varContext = procEnv.getCurrentVariableContext();
        Object value = null;
        if (this.getExpression() != null || this.getProcessorPlan() != null) {
            
            // get the current set of references and set their values
            setReferenceValues(varContext);
    
            if (this.expression != null) {
                //Evaluated the given expression - may throw BlockedException!
                value = ExpressionEvaluator.evaluate(this.expression,
                                                     Collections.EMPTY_MAP,
                                                     Collections.EMPTY_LIST,
                                                     procEnv.getDataManager(),
                                                     procEnv.getContext());
            } else if (processPlan != null) {
                String rsName = "ASSIGNMENT_INSTRUCTION"; //$NON-NLS-1$
                procEnv.executePlan(processPlan, rsName);
    
                procEnv.iterateCursor(rsName);
                List tuple = procEnv.getCurrentRow(rsName);
                // did not expect more than one tupe
                if (procEnv.iterateCursor(rsName)) {
                    throw new MetaMatrixProcessingException(ErrorMessageKeys.PROCESSOR_0019,
                                                           QueryExecPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0019, variable));
                }
                if (tuple != null) {
                    value = tuple.get(0);
                }
                procEnv.removeResults(rsName);
            }
        }    
        processValue(value, varContext);
    }
    
    protected abstract void processValue(Object value, VariableContext varContext) throws MetaMatrixComponentException, MetaMatrixProcessingException;

    protected ProcessorPlan getProcessorPlan(){
        return this.processPlan;
    }
    
    
    protected void cloneState(AbstractAssignmentInstruction clone) {
        clone.setVariable((ElementSymbol)getVariable().clone());
        if (expression != null) {
            clone.setExpression((Expression)getExpression().clone());
        }
        if (processPlan != null) {
            clone.setProcessPlan((ProcessorPlan)getProcessPlan().clone());
        }
        if (this.getReferences() != null) {
            List copyReferences = cloneReferences();
            clone.setReferences(copyReferences);
        }
    }

    public Map getDescriptionProperties() {
        Map props = new HashMap();
        
        if (this.expression != null) {                
            props.put(PROP_EXPRESSION, this.expression.toString());
        } else if (this.processPlan != null){
            props.put(PROP_PROGRAM, this.processPlan.getDescriptionProperties()); 
        }
        
        getDescriptionProperties(props);
        
        return props;
    }
    
    protected abstract void getDescriptionProperties(Map props);

    /** 
     * @see com.metamatrix.query.processor.program.ProgramInstruction#getChildPlans()
     * @since 4.2
     */
    public Collection getChildPlans() {
        if(this.processPlan != null) {
            List plans = new ArrayList(1);
            plans.add(this.processPlan);
            return plans;
        }
        
        return null;
    }

    /** 
     * @return Returns the expression.
     */
    public Expression getExpression() {
        return this.expression;
    }
    
    /** 
     * @param expression The expression to set.
     */
    public void setExpression(Expression expression) {
        this.expression = expression;
    }
    
    /** 
     * @return Returns the processPlan.
     */
    public ProcessorPlan getProcessPlan() {
        return this.processPlan;
    }
    
    /** 
     * @param processPlan The processPlan to set.
     */
    public void setProcessPlan(ProcessorPlan processPlan) {
        this.processPlan = processPlan;
    }
    
    /** 
     * @return Returns the variable.
     */
    public ElementSymbol getVariable() {
        return this.variable;
    }
    
    /** 
     * @param variable The variable to set.
     */
    public void setVariable(ElementSymbol variable) {
        this.variable = variable;
    }
       
}