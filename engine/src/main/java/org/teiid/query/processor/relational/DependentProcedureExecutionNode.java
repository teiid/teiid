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

package org.teiid.query.processor.relational;

import java.util.Collections;
import java.util.List;

import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.util.CommandContext;


public class DependentProcedureExecutionNode extends PlanExecutionNode {

    private Criteria inputCriteria;
    private List inputReferences;
    private List inputDefaults;

    // processing state
    private DependentProcedureCriteriaProcessor criteriaProcessor;

    public DependentProcedureExecutionNode(int nodeID,
                                           Criteria crit,
                                           List references,
                                           List defaults) {
        super(nodeID);

        this.inputCriteria = crit;
        this.inputDefaults = defaults;
        this.inputReferences = references;
    }

    /**
     * @see org.teiid.query.processor.relational.PlanExecutionNode#clone()
     */
    public Object clone() {
        DependentProcedureExecutionNode copy = new DependentProcedureExecutionNode(getID(), (Criteria)inputCriteria.clone(),
                                                                                   inputReferences,
                                                                                   inputDefaults);
        copyTo(copy);
        return copy;
    }

    public void reset() {
        super.reset();
        criteriaProcessor = null;
    }

    public void closeDirect() {
        super.closeDirect();

        if (criteriaProcessor != null) {
            criteriaProcessor.close();
        }
    }

    /**
     * @see org.teiid.query.processor.relational.PlanExecutionNode#prepareNextCommand()
     */
    protected boolean prepareNextCommand() throws BlockedException,
                                          TeiidComponentException, TeiidProcessingException {

        if (this.criteriaProcessor == null) {
            Criteria crit = (Criteria)inputCriteria.clone();
            crit = QueryRewriter.evaluateAndRewrite(crit, getEvaluator(Collections.emptyMap()), this.getContext(), this.getContext().getMetadata());
            this.criteriaProcessor = new DependentProcedureCriteriaProcessor(this, crit, inputReferences, inputDefaults);
        }

        return criteriaProcessor.prepareNextCommand(this.getProcessorPlan().getContext().getVariableContext());
    }

    /**
     * @see org.teiid.query.processor.relational.PlanExecutionNode#hasNextCommand()
     */
    protected boolean hasNextCommand() {
        return criteriaProcessor.hasNextCommand();
    }

    /**
     * @return Returns the inputCriteria.
     */
    public Criteria getInputCriteria() {
        return this.inputCriteria;
    }

    @Override
    public void open() throws TeiidComponentException,
            TeiidProcessingException {
        super.open();
        shareVariableContext(this, this.getProcessorPlan().getContext());
    }

    public static void shareVariableContext(RelationalNode node, CommandContext context) {
        // we need to look up through our parents and share this context
        RelationalNode parent = node.getParent();
        int projectCount = 0;
        while (parent != null && projectCount < 2) {
            parent.setContext(context);
            if (parent instanceof ProjectNode) {
                projectCount++;
            }
            parent = parent.getParent();
        }
    }

    @Override
    public Boolean requiresTransaction(boolean transactionalReads) {
        Boolean requires = super.requiresTransaction(transactionalReads);
        if (requires == null || requires) {
            return true;
        }
        return false;
    }

}
