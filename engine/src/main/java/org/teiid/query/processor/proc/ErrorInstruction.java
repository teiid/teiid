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
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.jdbc.TeiidSQLException;
import org.teiid.logging.LogManager;
import org.teiid.query.QueryPlugin;
import org.teiid.query.processor.relational.SubqueryAwareRelationalNode;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.visitor.ValueIteratorProviderCollectorVisitor;


/**
 * <p> This instruction updates the current variable context with the Variable defined using
 * the declare statement that is used in constructing this instruction.
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

    @Override
    public Boolean requiresTransaction(boolean transactionalReads) {
        return SubqueryAwareRelationalNode.requiresTransaction(transactionalReads, ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(expression));
    }

}