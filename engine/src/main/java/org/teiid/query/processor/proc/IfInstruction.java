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

import java.util.List;

import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.logging.LogManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.sql.lang.Criteria;


/**
 * <p>This instruction an holds an if block and an else block and a criteria that determines
 * which block will be executed. These blocks are {@link Program} objects that could contain
 * nested if-else block.  Therefore, this <code>ProgramInstruction</code>
 * implements an arbitrarily deep if-else if-....else block.</p>
 *
 * <p>During processing, the Criteria is evaluated and if it evaluates to true,
 * the "if" block is executed else the "else" block if there is one is executed. These
 * programs are placed on the {@link ProgramEnvironment#getProgramStack stack}.</p>
 */
public class IfInstruction extends ProgramInstruction {

    // the "if" block
    private Program ifProgram;

    // optional "else" block
    private Program elseProgram;

    // criteria on the "if" block
    private Criteria condition;

    /**
     * Constructor for IfInstruction.
     * @param condition The <code>Criteria</code> used to determine which block to execute
     * @param ifProgram The <code>Program</code> representing the "if" block
     * @param elseProgram The <code>Program</code> representing the "else" block
     */
    public IfInstruction(Criteria condition, Program ifProgram, Program elseProgram) {
        this.condition = condition;
        this.ifProgram = ifProgram;
        this.elseProgram = elseProgram;
    }

    /**
     * Constructor for IfInstruction.
     * @param condition The <code>Criteria</code> used to determine which block to execute
     * @param ifProgram The <code>Program</code> representing the "if" block
     */
    public IfInstruction(Criteria condition, Program ifProgram) {
		this(condition, ifProgram, null);
    }

    /**
     * This instruction will evaluate it's criteria, if it evaluates
     * to true, it will push the corresponding sub Program on to the top of the
     * program stack, and break from the loop.  Regardless if whether any criteria
     * evaluate to true, this instruction will increment the program counter of the
     * current program.
     * @throws TeiidProcessingException 
     * @see ProgramInstruction#process(ProcedurePlan)
     */
    public void process(ProcedurePlan procEnv)
        throws BlockedException, TeiidComponentException, TeiidProcessingException {

    	boolean evalValue = procEnv.evaluateCriteria(condition);

        if(evalValue) {
	        LogManager.logTrace(org.teiid.logging.LogConstants.CTX_DQP, new Object[]{"IFInstruction: "+ //$NON-NLS-1$
		        	" The criteria on the if block evaluated to true, processing the if block"}); //$NON-NLS-1$

            //push the "if" Program onto the stack
            procEnv.push(ifProgram);
        } else if(elseProgram != null) {
	        LogManager.logTrace(org.teiid.logging.LogConstants.CTX_DQP, new Object[]{"IFInstruction: "+ //$NON-NLS-1$
		        	" The criteria on the if block evaluated to false, processing the else block"}); //$NON-NLS-1$            
            //push the "else" Program onto the stack
            procEnv.push(elseProgram);
        }

    }

    public Program getIfProgram(){ //Defect 13291 - made public to support changes to ProcedurePlan
        return this.ifProgram;
    }

    public Program getElseProgram(){ //Defect 13291 - made public to support changes to ProcedurePlan
        return this.elseProgram;
    }
    
    /**
     * Returns a deep clone
     */
    public IfInstruction clone(){
    	Program cloneIf = (Program) this.ifProgram.clone();
    	Program cloneElse = null;
    	if(elseProgram != null) {
    		cloneElse = (Program) this.elseProgram.clone();
    	}
        IfInstruction clone = new IfInstruction(this.condition, cloneIf, cloneElse);
        return clone;
    }

    public String toString() {
        return "IF INSTRUCTION:"; //$NON-NLS-1$
    }

    public PlanNode getDescriptionProperties() {
    	PlanNode props = new PlanNode("IF"); //$NON-NLS-1$
        props.addProperty(PROP_CRITERIA, this.condition.toString());
        props.addProperty(PROP_THEN, this.ifProgram.getDescriptionProperties());
        if(elseProgram != null) {
        	props.addProperty(PROP_ELSE, this.elseProgram.getDescriptionProperties());
        }
        return props;
    }
    
    @Override
    public void getChildPlans(List<ProcessorPlan> plans) {
    	ifProgram.getChildPlans(plans);
    	if (elseProgram != null) {
    		elseProgram.getChildPlans(plans);
    	}
    }
    
}
