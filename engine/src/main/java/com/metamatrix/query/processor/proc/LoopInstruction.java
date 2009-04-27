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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.processor.program.Program;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;
import com.metamatrix.query.sql.util.VariableContext;

/**
 */
public class LoopInstruction extends CreateCursorResultSetInstruction implements RepeatedInstruction {
    // the loop block
    private Program loopProgram;
    
    //list of ElementSymbols theat represents all the possible
    //cursor columns
    private List elements;
    
    public LoopInstruction(Program loopProgram, String rsName, ProcessorPlan plan) {
        super(rsName, plan);
        this.loopProgram = loopProgram;
    }

    public void process(ProcedurePlan procEnv) throws MetaMatrixComponentException {
        List currentRow = procEnv.getCurrentRow(rsName); 
        VariableContext varContext = procEnv.getCurrentVariableContext();
        //set results to the variable context(the cursor.element is treated as variable)
        if(this.elements == null){
            List schema = procEnv.getSchema(rsName);
            elements = new ArrayList(schema.size());
            for(int i=0; i< schema.size(); i++){
                // defect 13432 - schema may contain AliasSymbols. Cast to SingleElementSymbol instead of ElementSymbol
                SingleElementSymbol element = (SingleElementSymbol)schema.get(i);
                elements.add(new ElementSymbol(rsName + "." + element.getShortName()));              //$NON-NLS-1$
            }
        }
        for(int i=0; i< elements.size(); i++){
            varContext.setValue((ElementSymbol)elements.get(i), currentRow.get(i));               
        }
    }
    
    public Program getLoopProgram() { //Defect 13291 - added method to support changes to ProcedurePlan
        return loopProgram;
    }

    /**
     * Returns a deep clone
     */
    public Object clone(){
        ProcessorPlan clonedPlan = (ProcessorPlan) this.plan.clone();
        return new LoopInstruction((Program)this.loopProgram.clone(), this.rsName, clonedPlan);
    }
    
    public String toString() {
        return "LOOP INSTRUCTION:"; //$NON-NLS-1$
    }
    
    public Map getDescriptionProperties() {
        Map props = new HashMap();
        props.put(PROP_TYPE, "LOOP"); //$NON-NLS-1$
        props.put(PROP_RESULT_SET, this.rsName);
        props.put(PROP_PROGRAM, this.loopProgram.getDescriptionProperties());
        return props;
    }

    /** 
     * @see com.metamatrix.query.processor.program.ProgramInstruction#getChildPlans()
     * @since 4.2
     */
    public Collection getChildPlans() {
        Collection result = new ArrayList(super.getChildPlans());
        result.addAll(loopProgram.getChildPlans());
        return result;
    }

    /** 
     * @see com.metamatrix.query.processor.proc.RepeatedInstruction#testCondition(com.metamatrix.query.processor.proc.ProcedureEnvironment)
     */
    public boolean testCondition(ProcedurePlan procEnv) throws MetaMatrixComponentException, MetaMatrixProcessingException {
        if(!procEnv.resultSetExists(rsName)) {
            procEnv.executePlan(plan, rsName);            
        }
        
        return procEnv.iterateCursor(rsName);
    }

    /** 
     * @see com.metamatrix.query.processor.proc.RepeatedInstruction#getNestedProgram()
     */
    public Program getNestedProgram() {
        return loopProgram;
    }

    /** 
     * @see com.metamatrix.query.processor.proc.RepeatedInstruction#postInstruction(com.metamatrix.query.processor.proc.ProcedureEnvironment)
     */
    public void postInstruction(ProcedurePlan procEnv) throws MetaMatrixComponentException {
        procEnv.removeResults(rsName);
    }

}
