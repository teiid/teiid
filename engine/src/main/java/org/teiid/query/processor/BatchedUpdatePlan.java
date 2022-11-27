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

package org.teiid.query.processor;

import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.client.plan.PlanNode;
import org.teiid.client.xa.XATransactionException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.dqp.service.TransactionContext;
import org.teiid.dqp.service.TransactionContext.Scope;
import org.teiid.dqp.service.TransactionService;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.query.QueryPlugin;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.util.VariableContext;
import org.teiid.query.util.CommandContext;
import org.teiid.translator.TranslatorBatchException;



/**
 * Plan for execution for a batched update command. The plan executes the child plans of the
 * individual commands in order.
 *
 * If variableContexts are provided, then this is a bulk update where all plans are the same object.
 *
 * @since 4.2
 */
public class BatchedUpdatePlan extends ProcessorPlan {

    /** Array that holds the child update plans */
    private ProcessorPlan[] updatePlans;
    /** */
    private boolean[] planOpened;
    private boolean[] startTxn;
    private TransactionContext[] planContexts;
    /** Array that holds the update counts for each command in this batch */
    private List[] updateCounts;
    /** The position of the plan currently being executed */
    private int planIndex = 0;
    /** The position of the command for which the update count is being retrieved */
    private int commandIndex = 0;

    private List<VariableContext> contexts; //only set for bulk updates

    private boolean singleResult;

    /**
     *
     * @param childPlans the child update plans for this batch
     * @param commandsInBatch The total number of commands in this batch. This does not always equal the number of plans if some
     * commands have been batched together.
     * @param singleResult indicates only a single update count is expected - non-single result plans are required to be top level
     * @since 4.2
     */
    public BatchedUpdatePlan(List<? extends ProcessorPlan> childPlans, int commandsInBatch, List<VariableContext> contexts, boolean singleResult) {
        this.updatePlans = childPlans.toArray(new ProcessorPlan[childPlans.size()]);
        this.planOpened = new boolean[updatePlans.length];
        this.startTxn = new boolean[updatePlans.length];
        this.planContexts = new TransactionContext[updatePlans.length];
        this.updateCounts = new List[commandsInBatch];
        this.contexts = contexts;
        this.singleResult = singleResult;
    }

    /**
     * @see java.lang.Object#clone()
     * @since 4.2
     */
    public BatchedUpdatePlan clone() {
        List<ProcessorPlan> clonedPlans = new ArrayList<ProcessorPlan>(updatePlans.length);

        clonedPlans.add(updatePlans[0].clone());
        for (int i = 1; i <updatePlans.length; i++) {
            if (contexts == null) {
                clonedPlans.add(updatePlans[1].clone());
            } else {
                clonedPlans.add(clonedPlans.get(0));
            }
        }
        BatchedUpdatePlan clone = new BatchedUpdatePlan(clonedPlans, updateCounts.length, contexts, singleResult);
        return clone;
    }

    /**
     * @see org.teiid.query.processor.ProcessorPlan#initialize(org.teiid.query.util.CommandContext, org.teiid.query.processor.ProcessorDataManager, org.teiid.common.buffer.BufferManager)
     * @since 4.2
     */
    public void initialize(CommandContext context, ProcessorDataManager dataMgr, BufferManager bufferMgr) {
        context = context.clone();
        context.setVariableContext(new VariableContext()); //start a new root variable context
        this.setContext(context);
        // Initialize all the child plans
        for (int i = 0; i < getPlanCount(); i++) {
            updatePlans[i].initialize(context, dataMgr, bufferMgr);
        }
    }

    /**
     * @see org.teiid.query.processor.ProcessorPlan#getOutputElements()
     * @since 4.2
     */
    public List getOutputElements() {
        return Command.getUpdateCommandSymbol();
    }

    /**
     * @see org.teiid.query.processor.ProcessorPlan#open()
     * @since 4.2
     */
    public void open() throws TeiidComponentException, TeiidProcessingException {
        try {
            // It's ok to open() the first plan, as it is not dependent on any prior commands.
            // See note for defect 16166 in the nextBatch() method.
            openPlan();
        } catch (BlockedException e){
            //should not happen
            throw e;
        } catch (TeiidComponentException | TeiidProcessingException e) {
            if (singleResult) {
                throw e;
            }
            Throwable cause = e;
            if (e.getCause() instanceof TranslatorBatchException) {
                TranslatorBatchException tbe = (TranslatorBatchException)e.getCause();
                for (int i = 0; i < tbe.getUpdateCounts().length; i++) {
                    updateCounts[commandIndex++] = Arrays.asList(updateCounts[i]);
                }
            }
            updateCounts = Arrays.copyOf(updateCounts, commandIndex);
            getContext().setBatchUpdateException(cause);
        }
    }

    /**
     * @see org.teiid.query.processor.ProcessorPlan#nextBatch()
     * @since 4.2
     */
    public TupleBatch nextBatch() throws BlockedException, TeiidComponentException, TeiidProcessingException {
        for (;planIndex < updatePlans.length && (getContext() == null || getContext().getBatchUpdateException() == null);) {
            try {
                if (!planOpened[planIndex]) { // Open the plan only once
                    /* Defect 16166
                     * Some commands in a batch may depend on updates by previous commands in the same batch. A call
                     * to open() usually submits an atomic command, so calling open() on all the child plans at the same time
                     * will mean that the datasource may not be in the state expected by a later command within the batch. So,
                     * for a batch of commands, we only open() a later plan when we are finished with the previous plan to
                     * guarantee that the commands in the previous plan are completed before the commands in any subsequent
                     * plans are executed.
                     */
                    openPlan();
                } else if (this.planContexts[planIndex] != null) {
                    this.getContext().getTransactionServer().resume(this.planContexts[planIndex]);
                }
                // Execute nextBatch() on each plan in sequence

                TupleBatch nextBatch = null;
                do {
                    nextBatch = updatePlans[planIndex].nextBatch(); // Can throw BlockedException
                    List<List<?>> currentBatch = nextBatch.getTuples();
                    for (int i = 0; i < currentBatch.size(); i++, commandIndex++) {
                        updateCounts[commandIndex] = currentBatch.get(i);
                    }
                } while (!nextBatch.getTerminationFlag());

                // since we are done with the plan explicitly close it.
                updatePlans[planIndex].close();
                if (this.planContexts[planIndex] != null) {
                    TransactionService ts = this.getContext().getTransactionServer();
                    ts.commit(this.planContexts[planIndex]);
                    this.planContexts[planIndex] = null;
                }
                planIndex++;
            } catch (BlockedException e){
                throw e;
            } catch (TeiidComponentException | TeiidProcessingException e) {
                if (singleResult) {
                    throw e;
                }
                Throwable cause = e;
                if (e.getCause() instanceof TranslatorBatchException) {
                    TranslatorBatchException tbe = (TranslatorBatchException)e.getCause();
                    for (int i = 0; i < tbe.getUpdateCounts().length; i++) {
                        updateCounts[commandIndex++] = Arrays.asList(tbe.getUpdateCounts()[i]);
                    }
                }
                updateCounts = Arrays.copyOf(updateCounts, commandIndex);
                getContext().setBatchUpdateException(cause);
            } finally {
                if (planIndex < updatePlans.length && this.planContexts[planIndex] != null) {
                    this.getContext().getTransactionServer().suspend(this.planContexts[planIndex]);
                }
            }
        }
        if (singleResult) {
            long result = 0;
            for (int i = 0; i < updateCounts.length; i++) {
                int value = (Integer)updateCounts[i].get(0);
                if (value == Statement.EXECUTE_FAILED) {
                    //this should not happen, but is possible should a translator return
                    //the batch results rather than throwing an exception
                    throw new TeiidProcessingException(QueryPlugin.Event.TEIID31199, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31198));
                }
                if (value > 0) {
                    result += value;
                }
            }
            TupleBatch batch = new TupleBatch(1, new List<?>[] {Arrays.asList((int)Math.min(Integer.MAX_VALUE, result)) });
            batch.setTerminationFlag(true);
            return batch;
        }
        // Add tuples to current batch
        TupleBatch batch = new TupleBatch(1, updateCounts);
        batch.setTerminationFlag(true);
        return batch;
    }

    private void openPlan() throws TeiidComponentException,
            TeiidProcessingException {
        //reset prior to updating the context
        updatePlans[planIndex].reset();
        if (this.contexts != null && !this.contexts.isEmpty()) {
            CommandContext context = updatePlans[planIndex].getContext();
            VariableContext vc = context.getVariableContext();
            //ensure that we're dealing with the global context
            //this is just a safe guard against the plan not correctly resetting the context
            while (vc.getParentContext() != null) {
                vc = vc.getParentContext();
            }
            vc.clear();
            VariableContext currentValues = this.contexts.get(planIndex);
            vc.putAll(currentValues);
        }
        TransactionContext tc = this.getContext().getTransactionContext();
        if (startTxn[planIndex] && tc != null && tc.getTransactionType() == Scope.NONE) {
            this.getContext().getTransactionServer().begin(tc);
            this.planContexts[planIndex] = tc;
        }
        updatePlans[planIndex].open();
        planOpened[planIndex] = true;
    }

    /**
     * @see org.teiid.query.processor.ProcessorPlan#close()
     * @since 4.2
     */
    public void close() throws TeiidComponentException {
        // if the plan opened but the atomic request got cancelled then close the last plan node.
        TransactionService ts = this.getContext().getTransactionServer();
        if (planIndex < updatePlans.length && planOpened[planIndex]) {
            try {
                updatePlans[planIndex].close();
            } catch (TeiidComponentException e) {
                LogManager.logWarning(LogConstants.CTX_DQP, e, e.getMessage());
            }
            if (this.planContexts[planIndex] != null) {
                try {
                    ts.resume(this.planContexts[planIndex]);
                    ts.rollback(this.planContexts[planIndex]);
                } catch (XATransactionException e) {
                    LogManager.logWarning(LogConstants.CTX_DQP, e, e.getMessage());
                }
                this.planContexts[planIndex] = null;
            }
        }
    }


    /**
     * @see org.teiid.query.processor.ProcessorPlan#reset()
     * @since 4.2
     */
    public void reset() {
        super.reset();
        for (int i = 0; i < updatePlans.length; i++) {
            updatePlans[i].reset();
            planOpened[i] = false;
            updateCounts[i] = null;
            this.planContexts[i] = null;
        }
        planIndex = 0;
        commandIndex = 0;
    }

    public PlanNode getDescriptionProperties() {
        PlanNode props = super.getDescriptionProperties();
        for (int i = 0; i < getPlanCount(); i++) {
            props.addProperty("Batch Plan " + i, updatePlans[i].getDescriptionProperties()); //$NON-NLS-1$
        }
        return props;
    }

    public String toString() {
        StringBuffer val = new StringBuffer("BatchedUpdatePlan {\n"); //$NON-NLS-1$
        for (int i = 0; i < getPlanCount(); i++) {
            val.append(updatePlans[i])
               .append("\n"); //$NON-NLS-1$
        }
        val.append("}\n"); //$NON-NLS-1$
        return val.toString();
    }

    private int getPlanCount() {
        return (contexts != null?1:updatePlans.length);
    }

    /**
     * Returns the child plans for this batch. Used primarily for unit tests.
     * @return
     * @since 4.2
     */
    public List getUpdatePlans() {
        return Arrays.asList(updatePlans);
    }

    @Override
    public Boolean requiresTransaction(boolean transactionalReads) {
        if (!singleResult) {
            //the transaction boundary should be around each command
            for (int i = 0; i < updatePlans.length; i++) {
                Boolean requires = updatePlans[i].requiresTransaction(transactionalReads);
                if (requires != null && requires) {
                    startTxn[i] = true;
                }
            }
            return false;
        }
        boolean possible = false;
        for (int i = 0; i < updatePlans.length; i++) {
            Boolean requires = updatePlans[i].requiresTransaction(transactionalReads);
            if (requires != null) {
                if (requires) {
                    return true;
                }
            } else {
                if (possible) {
                    return true;
                }
                possible = true;
            }
        }
        if (possible) {
            return null;
        }
        return false;
    }

    public void setSingleResult(boolean singleResult) {
        this.singleResult = singleResult;
    }

}
