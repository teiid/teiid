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

import static org.teiid.query.analysis.AnalysisRecord.*;

import java.util.List;

import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.processor.BatchCollector.BatchProducer;
import org.teiid.query.util.CommandContext;


/**
 * <p>This class represents a processor plan.  It is generic in that it
 * abstracts the interface to the plan by the processor, meaning that the
 * actual implementation of the plan or the types of processing done by the
 * plan is not important to the processor.
 * <p>All the implementations of this interface need to implement {@link #clone}
 * method. The plan is only clonable in the pre or post-processing stage, not
 * during the processing state (things like program state, result sets, etc).
 * It's only safe to clone in between query processings.  In other words, it's
 * only safe to call {@link #clone} before the call to {@link #open} or after
 * the call to {@link #close}.
 *
 */
public abstract class ProcessorPlan implements Cloneable, BatchProducer {

    private CommandContext context;

    /**
     * Initialize the plan with some required pieces of data for making
     * queries.  The data manager is used to make queries and the processorID
     * must be passed with the request so the data manager can find the
     * processor again.
     *
     * @param context Process execution context
     * @param dataMgr Data manager reference
     * @param bufferMgr Buffer manager reference
     */
    public void initialize(CommandContext context, ProcessorDataManager dataMgr, BufferManager bufferMgr) {
        this.context = context;
    }

    public void addWarning(TeiidException warning) {
        if (context != null) {
            context.addWarning(warning);
        }
    }

    /**
     * Reset a plan so that it can be processed again.
     */
    public void reset() {
    }

    /**
     * Get list of resolved elements describing output columns for this plan.
     * @return List of SingleElementSymbol
     */
    public abstract List getOutputElements();

    /**
     * Get the processor context, which can be modified.
     * @return context object
     */
    public CommandContext getContext() {
        return context;
    }

    public void setContext(CommandContext context) {
        this.context = context;
    }

    /**
     * Open the plan for processing.
     * @throws TeiidComponentException
     */
    public abstract void open() throws TeiidComponentException, TeiidProcessingException;

    /**
     * Get a batch of results or possibly an Exception.
     * @return Batch of results
     * @throws BlockedException indicating next batch is not available yet
     * @throws TeiidComponentException for non-business rule exception
     * @throws TeiidProcessingException for business rule exception, related
     * to user input or modeling
     */
    public abstract TupleBatch nextBatch() throws BlockedException, TeiidComponentException, TeiidProcessingException;

    /**
     * Close the plan after processing.
     * @throws TeiidComponentException
     */
    public abstract void close() throws TeiidComponentException;

    /**
     * Return a safe clone of the ProcessorPlan.  A ProcessorPlan may only be
     * safely cloned in between processings.  That is, it is only safe to clone
     * a plan before it is {@link #open opened} or after it is {@link #close
     * closed}.
     * @return safe clone of this ProcessorPlan, as long as it is not open for
     * processing
     */
    public abstract ProcessorPlan clone();

    public Boolean requiresTransaction(boolean transactionalReads) {
        return transactionalReads;
    }

    public PlanNode getDescriptionProperties() {
        PlanNode props = new PlanNode(this.getClass().getSimpleName());
        props.addProperty(PROP_OUTPUT_COLS, AnalysisRecord.getOutputColumnProperties(getOutputElements()));
        return props;
    }

    /**
     * return the final tuple buffer or null if not available
     * @return
     * @throws TeiidProcessingException
     * @throws TeiidComponentException
     * @throws BlockedException
     */
    public TupleBuffer getBuffer(int maxRows) throws BlockedException, TeiidComponentException, TeiidProcessingException {
        return null;
    }

    /**
     * Return true if the plan provides a final buffer via getBuffer
     */
    public boolean hasBuffer() {
        return false;
    }

}
