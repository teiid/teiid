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

import static org.teiid.query.analysis.AnalysisRecord.*;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.logging.LogManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.util.VariableContext;
import org.teiid.query.util.CommandContext;


//TODO: consolidate with QueryProcessor
public class PlanExecutionNode extends SubqueryAwareRelationalNode {

    // Initialization state
    private ProcessorPlan plan;
    private boolean isOpen;
    private boolean needsProcessing;

    protected PlanExecutionNode() {
        super();
    }

    public PlanExecutionNode(int nodeID) {
        super(nodeID);
    }

    public void reset() {
        super.reset();

        resetPlan();
    }

    private void resetPlan() {
        plan.reset();
        isOpen = false;
        needsProcessing = false;
    }

    public void setProcessorPlan(ProcessorPlan plan) {
        this.plan = plan;
    }

    public void open()
        throws TeiidComponentException, TeiidProcessingException {
        super.open();
        // Initialize plan for execution
        CommandContext subContext = getContext().clone();
        subContext.pushVariableContext(new VariableContext());
        plan.initialize(subContext, getDataManager(), this.getBufferManager());

        if (openPlanImmediately() && prepareNextCommand()) {
            needsProcessing = true;
            plan.open();
            isOpen = true;
        }
    }

    protected boolean openPlanImmediately() {
        return true;
    }

    public TupleBatch nextBatchDirect()
        throws BlockedException, TeiidComponentException, TeiidProcessingException {

        if (!isOpen) {
            if (!needsProcessing) {
                while (true) {
                    if (prepareNextCommand()) {
                        needsProcessing = true;
                        break;
                    }
                    if (!hasNextCommand()) {
                        needsProcessing = false;
                        break;
                    }
                }
            }
            if (needsProcessing) {
                plan.open();
                isOpen = true;
            }
        }

        if (!needsProcessing) {
            terminateBatches();
            return pullBatch();
        }

        TupleBatch batch = plan.nextBatch();

        for (List<?> tuple : batch.getTuples()) {
            addBatchRow(tuple);
        }

        if(batch.getTerminationFlag()) {
            if (hasNextCommand()) {
                resetPlan();
            } else {
                terminateBatches();
            }
        }

        return pullBatch();
    }

    /**
     * @throws BlockedException
     * @throws TeiidComponentException
     * @throws TeiidProcessingException
     */
    protected boolean prepareNextCommand() throws BlockedException,
                                          TeiidComponentException, TeiidProcessingException {
        return true;
    }

    protected boolean hasNextCommand() {
        return false;
    }

    public void closeDirect() {
        try {
            plan.close();
        } catch (TeiidComponentException e1){
            LogManager.logDetail(org.teiid.logging.LogConstants.CTX_DQP, e1, "Error closing processor"); //$NON-NLS-1$
        }
    }

    protected void getNodeString(StringBuffer str) {
        super.getNodeString(str);
    }

    public ProcessorPlan getProcessorPlan(){
        return this.plan;
    }

    public Object clone(){
        PlanExecutionNode clonedNode = new PlanExecutionNode();
        copyTo(clonedNode);
        return clonedNode;
    }

    protected void copyTo(PlanExecutionNode target) {
        target.setProcessorPlan(plan.clone());
        super.copyTo(target);
    }

    public PlanNode getDescriptionProperties() {
        PlanNode props = super.getDescriptionProperties();
        props.addProperty(PROP_EXECUTION_PLAN, this.plan.getDescriptionProperties());
        return props;
    }

    @Override
    public Boolean requiresTransaction(boolean transactionalReads) {
        return getProcessorPlan().requiresTransaction(transactionalReads);
    }

    @Override
    public Collection<? extends LanguageObject> getObjects() {
        return Collections.emptyList();
    }

}
