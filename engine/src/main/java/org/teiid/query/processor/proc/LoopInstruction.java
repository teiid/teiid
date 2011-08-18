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
package org.teiid.query.processor.proc;

import static org.teiid.query.analysis.AnalysisRecord.*;

import java.util.ArrayList;
import java.util.List;

import org.teiid.client.plan.PlanNode;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.SingleElementSymbol;
import org.teiid.query.sql.util.VariableContext;


/**
 */
public class LoopInstruction extends CreateCursorResultSetInstruction implements RepeatedInstruction {
    // the loop block
    private Program loopProgram;
    
    private List elements;
    private String label;
    
    public LoopInstruction(Program loopProgram, String rsName, ProcessorPlan plan, String label) {
        super(rsName, plan, false);
        this.loopProgram = loopProgram;
        this.label = label;
    }
    
    @Override
    public String getLabel() {
    	return label;
    }
    
    @Override
    public void setLabel(String label) {
    	this.label = label;
    }

    public void process(ProcedurePlan procEnv) throws TeiidComponentException {
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
    
    /**
     * Returns a deep clone
     */
    public LoopInstruction clone(){
        ProcessorPlan clonedPlan = this.plan.clone();
        return new LoopInstruction((Program)this.loopProgram.clone(), this.rsName, clonedPlan, label);
    }
    
    public String toString() {
        return "LOOP INSTRUCTION: " + this.rsName; //$NON-NLS-1$
    }
    
    public PlanNode getDescriptionProperties() {
        PlanNode props = new PlanNode("LOOP"); //$NON-NLS-1$
        props.addProperty(PROP_SQL, this.plan.getDescriptionProperties());
        props.addProperty(PROP_RESULT_SET, this.rsName);
        props.addProperty(PROP_PROGRAM, this.loopProgram.getDescriptionProperties());
        return props;
    }

    public boolean testCondition(ProcedurePlan procEnv) throws TeiidComponentException, TeiidProcessingException {
        if(!procEnv.resultSetExists(rsName)) {
            procEnv.executePlan(plan, rsName, null, false);            
        }
        
        return procEnv.iterateCursor(rsName);
    }

    /** 
     * @see org.teiid.query.processor.proc.RepeatedInstruction#getNestedProgram()
     */
    public Program getNestedProgram() {
        return loopProgram;
    }

    public void postInstruction(ProcedurePlan procEnv) throws TeiidComponentException {
        procEnv.removeResults(rsName);
    }
    
    @Override
    public void getChildPlans(List<ProcessorPlan> plans) {
    	super.getChildPlans(plans);
    	this.loopProgram.getChildPlans(plans);
    }

}
