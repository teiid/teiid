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

import java.util.Arrays;

import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.processor.relational.SubqueryAwareRelationalNode;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.util.VariableContext;
import org.teiid.query.sql.visitor.ValueIteratorProviderCollectorVisitor;


/**
 * <p> This instruction updates the current variable context with a value for the Variable
 * defined using a DeclareInstruction, the variable value is obtained by either processing
 * a expression or a command(stored as a processplan). The Processing of the command is
 * expected to result in 1 column, 1 row tuple.
 */
public class AssignmentInstruction extends ProgramInstruction {

    // variable whose value is updated in the context
    private ElementSymbol variable;
    // expression to be processed
    private Expression expression;

    public AssignmentInstruction() {
    }

    /**
     * <p> Updates the current variable context with a value for the Variable
     * defined using a DeclareInstruction, the variable value is obtained by either processing
     * a expression or a command(stored as a processplan). The Processing of the command is
     * expected to result in 1 column, 1 row tuple, if more than a row is returned an exception
     * is thrown. Also updates the program counter.
     * @throws BlockedException
     * @throws TeiidComponentException if error processing command or expression on this instruction
     */
    public void process(ProcedurePlan procEnv) throws BlockedException,
                                               TeiidComponentException, TeiidProcessingException {

        VariableContext varContext = procEnv.getCurrentVariableContext();
        Object value = null;
        if (this.expression != null) {
            value = procEnv.evaluateExpression(this.expression);
        }
        varContext.setValue(getVariable(), value);
        LogManager.logTrace(LogConstants.CTX_DQP,
                            new Object[] {this.toString() + " The variable " //$NON-NLS-1$
                                          + getVariable() + " in the variablecontext is updated with the value :", value}); //$NON-NLS-1$
    }

    public PlanNode getDescriptionProperties() {
        PlanNode props = new PlanNode("ASSIGNMENT"); //$NON-NLS-1$
        props.addProperty(PROP_VARIABLE, this.variable.toString());
        if (this.expression != null) {
            AnalysisRecord.addLanaguageObjects(props, PROP_EXPRESSION, Arrays.asList(this.expression));
        }
        return props;
    }

    /**
     * @return Returns the expression.
     */
    public Expression getExpression() {
        return this.expression;
    }

    /**
     * @param expression The expression to set.
     */
    public void setExpression(Expression expression) {
        this.expression = expression;
    }

    /**
     * @return Returns the variable.
     */
    public ElementSymbol getVariable() {
        return this.variable;
    }

    /**
     * @param variable The variable to set.
     */
    public void setVariable(ElementSymbol variable) {
        this.variable = variable;
    }

    public String toString() {
        return "ASSIGNMENT INSTRUCTION: " + variable; //$NON-NLS-1$
    }

    /**
     * @see org.teiid.query.processor.proc.ProgramInstruction#clone()
     */
    public AssignmentInstruction clone() {
        AssignmentInstruction clone = new AssignmentInstruction();
        clone.setVariable(this.variable);
        clone.setExpression(this.expression);
        return clone;
    }

    @Override
    public Boolean requiresTransaction(boolean transactionalReads) {
        if (expression == null) {
            return false;
        }
        return SubqueryAwareRelationalNode.requiresTransaction(transactionalReads, ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(expression));
    }

}