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

/*
 */
package com.metamatrix.query.processor.proc;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.query.processor.program.Program;
import com.metamatrix.query.processor.program.ProgramEnvironment;
import com.metamatrix.query.processor.program.ProgramInstruction;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.visitor.ReferenceCollectorVisitor;

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

    public void process(ProgramEnvironment env) throws MetaMatrixComponentException {
        //do nothing
    }
    
    public Program getWhileProgram() { //Defect 13291 - added method to support changes to ProcedurePlan
        return whileProgram;
    }

    /**
     * Returns a deep clone
     */
    public Object clone(){
        return new WhileInstruction((Program)this.whileProgram.clone(), (Criteria)this.condition.clone());
    }

    public String toString() {
        return "WHILE INSTRUCTION:"; //$NON-NLS-1$
    }
    
    public Map getDescriptionProperties() {
        Map props = new HashMap();
        props.put(PROP_TYPE, "WHILE"); //$NON-NLS-1$
        props.put(PROP_CRITERIA, this.condition.toString());
        props.put(PROP_PROGRAM, this.whileProgram.getDescriptionProperties());
        return props;
    }
    
    /** 
     * @see com.metamatrix.query.processor.program.ProgramInstruction#getChildPlans()
     * @since 4.2
     */
    public Collection getChildPlans() {
        return this.whileProgram.getChildPlans();
    }


    /** 
     * @see com.metamatrix.query.processor.proc.RepeatedInstruction#testCondition(com.metamatrix.query.processor.proc.ProcedureEnvironment)
     */
    public boolean testCondition(ProcedureEnvironment procEnv) throws MetaMatrixComponentException {
        // get the current variable context
        CommandInstruction.setReferenceValues(procEnv.getCurrentVariableContext(), ReferenceCollectorVisitor.getReferences(condition));
        boolean evalValue = IfInstruction.evaluateCriteria(condition, procEnv);

        return evalValue;
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
    public void postInstruction(ProcedureEnvironment procEnv) throws MetaMatrixComponentException {
    }
    
}
