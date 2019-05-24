/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.Symbol;
import org.teiid.query.sql.util.VariableContext;


/**
 */
public class LoopInstruction extends CreateCursorResultSetInstruction implements RepeatedInstruction {
    // the loop block
    private Program loopProgram;

    private List<ElementSymbol> elements;
    private String label;

    public LoopInstruction(Program loopProgram, String rsName, ProcessorPlan plan, String label) {
        super(rsName, plan, Mode.NOHOLD);
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
        List<?> currentRow = procEnv.getCurrentRow(rsName);
        VariableContext varContext = procEnv.getCurrentVariableContext();
        //set results to the variable context(the cursor.element is treated as variable)
        if(this.elements == null){
            List schema = procEnv.getSchema(rsName);
            elements = new ArrayList<ElementSymbol>(schema.size());
            for(int i=0; i< schema.size(); i++){
                Expression element = (Expression)schema.get(i);
                ElementSymbol e = new ElementSymbol(rsName + Symbol.SEPARATOR + Symbol.getShortName(element));
                e.setType(element.getType());
                elements.add(e);
            }
        }
        for(int i=0; i< elements.size(); i++){
            varContext.setValue(elements.get(i), currentRow.get(i));
        }
    }

    /**
     * Returns a deep clone
     */
    public LoopInstruction clone(){
        ProcessorPlan clonedPlan = this.plan.clone();
        return new LoopInstruction(this.loopProgram.clone(), this.rsName, clonedPlan, label);
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
            procEnv.executePlan(plan, rsName, null, Mode.NOHOLD, false);
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
    public Boolean requiresTransaction(boolean transactionalReads) {
        Boolean requires = super.requiresTransaction(transactionalReads);
        if (requires != null && requires) {
            return true;
        }
        Boolean loopRequires = loopProgram.requiresTransaction(transactionalReads);
        if (loopRequires == null) {
            if (requires == null) {
                return true;
            }
            return null;
        }
        if (loopRequires) {
            return true;
        }
        return requires;
    }

}
