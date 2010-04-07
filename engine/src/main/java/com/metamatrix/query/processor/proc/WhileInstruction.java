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

/*
 */
package com.metamatrix.query.processor.proc;

import static com.metamatrix.query.analysis.AnalysisRecord.*;

import org.teiid.client.plan.PlanNode;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.query.processor.program.Program;
import com.metamatrix.query.processor.program.ProgramInstruction;
import com.metamatrix.query.sql.lang.Criteria;

/**
 */
public class WhileInstruction extends ProgramInstruction implements RepeatedInstruction {
    // while block
    private Program whileProgram;

    // criteria for the while block
    private Criteria condition;

    public WhileInstruction(Program program, Criteria condition){
        this.whileProgram = program;
        this.condition = condition;
    }

    public void process(ProcedurePlan env) throws MetaMatrixComponentException {
        //do nothing
    }
    
    public Program getWhileProgram() { //Defect 13291 - added method to support changes to ProcedurePlan
        return whileProgram;
    }

    /**
     * Returns a deep clone
     */
    public WhileInstruction clone(){
        return new WhileInstruction((Program)this.whileProgram.clone(), this.condition);
    }

    public String toString() {
        return "WHILE INSTRUCTION:"; //$NON-NLS-1$
    }
    
    public PlanNode getDescriptionProperties() {
        PlanNode props = new PlanNode("WHILE"); //$NON-NLS-1$
        props.addProperty(PROP_CRITERIA, this.condition.toString());
        props.addProperty(PROP_PROGRAM, this.whileProgram.getDescriptionProperties());
        return props;
    }
    
    /** 
     * @throws MetaMatrixProcessingException 
     * @see com.metamatrix.query.processor.proc.RepeatedInstruction#testCondition(com.metamatrix.query.processor.proc.ProcedureEnvironment)
     */
    public boolean testCondition(ProcedurePlan procEnv) throws MetaMatrixComponentException, MetaMatrixProcessingException {
        return procEnv.evaluateCriteria(condition);
    }

    /** 
     * @see com.metamatrix.query.processor.proc.RepeatedInstruction#getNestedProgram()
     */
    public Program getNestedProgram() {
        return whileProgram;
    }

    /** 
     * @see com.metamatrix.query.processor.proc.RepeatedInstruction#postInstruction(com.metamatrix.query.processor.proc.ProcedureEnvironment)
     */
    public void postInstruction(ProcedurePlan procEnv) throws MetaMatrixComponentException {
    }
    
}
