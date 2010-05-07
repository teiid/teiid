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

package com.metamatrix.query.processor.proc;

import static com.metamatrix.query.analysis.AnalysisRecord.*;

import org.teiid.client.ProcedureErrorInstructionException;
import org.teiid.client.plan.PlanNode;
import org.teiid.logging.LogManager;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.query.processor.program.ProgramInstruction;
import com.metamatrix.query.sql.symbol.Expression;

/**
 * <p> This instruction updates the current variable context with the Variable defined using
 * the declare statement that is used in constructing this instruction.</p>
 */
public class ErrorInstruction extends ProgramInstruction {
	
    public static final String ERROR_PREFIX = "Procedure error:"; //$NON-NLS-1$
    
    private Expression expression;
    
	/**
	 * Constructor for DeclareInstruction.
	 */
	public ErrorInstruction() {
	}
	
	public void setExpression(Expression expression) {
		this.expression = expression;
	}
    
    /** 
     * @see com.metamatrix.query.processor.program.ProgramInstruction#clone()
     */
    public ErrorInstruction clone() {
        ErrorInstruction clone = new ErrorInstruction();
        clone.expression = expression;
        return clone;
    }
	    
    public String toString() {
        return "RAISE ERROR INSTRUCTION: " + expression; //$NON-NLS-1$
    }  
    
    public PlanNode getDescriptionProperties() {
    	PlanNode node = new PlanNode("RAISE ERROR"); //$NON-NLS-1$
    	node.addProperty(PROP_EXPRESSION, this.expression.toString());
    	return node;
    }
    
    @Override
    public void process(ProcedurePlan env) throws MetaMatrixComponentException,
    		MetaMatrixProcessingException {
    	Object value = env.evaluateExpression(expression);
            LogManager.logTrace(org.teiid.logging.LogConstants.CTX_DQP,
                            new Object[] {"Processing RaiseErrorInstruction with the value :", value}); //$NON-NLS-1$ 
        throw new ProcedureErrorInstructionException(ERROR_PREFIX + (value != null ? value.toString() : "")); //$NON-NLS-1$ 
    }
 
}