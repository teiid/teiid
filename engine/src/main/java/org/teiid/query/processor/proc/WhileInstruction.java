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

import org.teiid.client.plan.PlanNode;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.processor.relational.SubqueryAwareRelationalNode;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.visitor.ValueIteratorProviderCollectorVisitor;


/**
 */
public class WhileInstruction extends ProgramInstruction implements RepeatedInstruction {
    // while block
    private Program whileProgram;

    // criteria for the while block
    private Criteria condition;
    private String label;

    public WhileInstruction(Program program, Criteria condition, String label){
        this.whileProgram = program;
        this.condition = condition;
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

    public void process(ProcedurePlan env) throws TeiidComponentException {
        //do nothing
    }

    public Program getWhileProgram() { //Defect 13291 - added method to support changes to ProcedurePlan
        return whileProgram;
    }

    /**
     * Returns a deep clone
     */
    public WhileInstruction clone(){
        return new WhileInstruction(this.whileProgram.clone(), this.condition, this.label);
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
     * @throws TeiidProcessingException
     */
    public boolean testCondition(ProcedurePlan procEnv) throws TeiidComponentException, TeiidProcessingException {
        return procEnv.evaluateCriteria(condition);
    }

    /**
     * @see org.teiid.query.processor.proc.RepeatedInstruction#getNestedProgram()
     */
    public Program getNestedProgram() {
        return whileProgram;
    }

    public void postInstruction(ProcedurePlan procEnv) throws TeiidComponentException {
    }

    @Override
    public Boolean requiresTransaction(boolean transactionalReads) {
        Boolean conditionRequires = SubqueryAwareRelationalNode.requiresTransaction(transactionalReads, ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(condition));
        if (conditionRequires == null || conditionRequires) {
            return true;
        }
        Boolean requires = whileProgram.requiresTransaction(transactionalReads);
        if (requires == null || requires) {
            return true;
        }
        return false;
    }

}
