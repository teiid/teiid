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

package org.teiid.query.processor.proc;

import static org.teiid.query.analysis.AnalysisRecord.*;

import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.logging.LogManager;
import org.teiid.query.processor.relational.SubqueryAwareRelationalNode;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.visitor.ValueIteratorProviderCollectorVisitor;


/**
 * <p>This instruction an holds an if block and an else block and a criteria that determines
 * which block will be executed. These blocks are {@link Program} objects that could contain
 * nested if-else block.  Therefore, this <code>ProgramInstruction</code>
 * implements an arbitrarily deep if-else if-....else block.
 *
 * <p>During processing, the Criteria is evaluated and if it evaluates to true,
 * the "if" block is executed else the "else" block if there is one is executed. These
 * programs are placed on the stack.
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
        Program cloneIf = this.ifProgram.clone();
        Program cloneElse = null;
        if(elseProgram != null) {
            cloneElse = this.elseProgram.clone();
        }
        IfInstruction clone = new IfInstruction(this.condition, cloneIf, cloneElse);
        return clone;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer("IF INSTRUCTION: "); //$NON-NLS-1$
        sb.append(condition);
        sb.append("\n").append(ifProgram); //$NON-NLS-1$
        if (elseProgram!=null) {
            sb.append("\nELSE\n"); //$NON-NLS-1$
            sb.append(elseProgram);
        }
        return sb.toString();
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
    public Boolean requiresTransaction(boolean transactionalReads) {
        Boolean conditionRequires = SubqueryAwareRelationalNode.requiresTransaction(transactionalReads, ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(condition));
        if (conditionRequires != null && conditionRequires) {
            return true;
        }
        Boolean requires = ifProgram.requiresTransaction(transactionalReads);
        if (requires != null && requires) {
            return true;
        }
        if (elseProgram != null) {
            Boolean requiresElse = elseProgram.requiresTransaction(transactionalReads);
            if (requiresElse != null && requiresElse) {
                return true;
            }
            if (requiresElse == null) {
                return conditionRequires==null?true:null;
            }
        }
        if (requires == null) {
            return conditionRequires==null?true:null;
        }
        return conditionRequires;
    }

}
