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

import java.util.List;

import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.util.VariableContext;
import org.teiid.query.util.CommandContext;


public class DependentProcedureAccessNode extends AccessNode {

    private Criteria inputCriteria;
    private List inputReferences;
    private List inputDefaults;

    // processing state
    private DependentProcedureCriteriaProcessor criteriaProcessor;

    public DependentProcedureAccessNode(int nodeID,
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
        DependentProcedureAccessNode copy = new DependentProcedureAccessNode(getID(), inputCriteria,
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

    @Override
    public void open() throws TeiidComponentException,
            TeiidProcessingException {
        CommandContext context  = getContext().clone();
        context.pushVariableContext(new VariableContext());
        this.setContext(context);
        DependentProcedureExecutionNode.shareVariableContext(this, context);
        super.open();
    }

    /**
     * @see org.teiid.query.processor.relational.AccessNode#prepareNextCommand(org.teiid.query.sql.lang.Command)
     */
    protected boolean prepareNextCommand(Command atomicCommand) throws TeiidComponentException, TeiidProcessingException {

        if (this.criteriaProcessor == null) {
            this.criteriaProcessor = new DependentProcedureCriteriaProcessor(this, (Criteria)inputCriteria.clone(), inputReferences, inputDefaults);
        }

        if (criteriaProcessor.prepareNextCommand(this.getContext().getVariableContext())) {
            return super.prepareNextCommand(atomicCommand);
        }

        return false;
    }

    @Override
    protected boolean processCommandsIndividually() {
        return true;
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
    public Boolean requiresTransaction(boolean transactionalReads) {
        Boolean requires = super.requiresTransaction(transactionalReads);
        if (requires == null || requires) {
            return true;
        }
        return false;
    }

}
