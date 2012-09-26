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

package org.teiid.query.processor.proc;

import static org.teiid.query.analysis.AnalysisRecord.*;

import org.teiid.client.plan.PlanNode;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.jdbc.TeiidSQLException;
import org.teiid.logging.LogManager;
import org.teiid.query.QueryPlugin;
import org.teiid.query.sql.symbol.Expression;


/**
 * <p> This instruction updates the current variable context with the Variable defined using
 * the declare statement that is used in constructing this instruction.</p>
 */
public class ErrorInstruction extends ProgramInstruction {
	
    private Expression expression;
    private boolean warning;
    
	/**
	 * Constructor for DeclareInstruction.
	 */
	public ErrorInstruction() {
	}
	
	public void setExpression(Expression expression) {
		this.expression = expression;
	}
	
	public void setWarning(boolean warning) {
		this.warning = warning;
	}
    
    /** 
     * @see org.teiid.query.processor.proc.ProgramInstruction#clone()
     */
    public ErrorInstruction clone() {
        ErrorInstruction clone = new ErrorInstruction();
        clone.expression = expression;
        clone.warning = warning;
        return clone;
    }
	    
    public String toString() {
        return "RAISE " + (warning?"WARNING":"ERROR") +" INSTRUCTION: " + expression; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }  
    
    public PlanNode getDescriptionProperties() {
    	PlanNode node = new PlanNode("RAISE " + (warning?"WARNING":"ERROR")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    	node.addProperty(PROP_EXPRESSION, this.expression.toString());
    	return node;
    }
    
    @Override
    public void process(ProcedurePlan env) throws TeiidComponentException,
    		TeiidProcessingException, TeiidSQLException {
    	Object value = env.evaluateExpression(expression);
        LogManager.logTrace(org.teiid.logging.LogConstants.CTX_DQP, "Processing RAISE with the value :", value); //$NON-NLS-1$
        if (warning) {
        	env.getContext().addWarning((Exception)value);
        	return;
        }
        if (value == null) {
        	throw new TeiidProcessingException(QueryPlugin.Event.TEIID31122, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31122));
        }
        throw TeiidSQLException.create((Exception)value);
    }
 
}