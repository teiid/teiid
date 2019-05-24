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

import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.BufferManager.BufferReserveMode;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.impl.BufferManagerImpl;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.util.CommandContext;


public class UnionAllNode extends RelationalNode {

    private static final int SMALL_LIMIT = 10;
    private boolean[] sourceDone;
    private boolean[] sourceOpen;

    private int outputRow = 1;
    private int reserved;
    private int schemaSize;

    public UnionAllNode(int nodeID) {
        super(nodeID);
    }

    public void reset() {
        super.reset();

        sourceDone = null;
        sourceOpen = null;
        outputRow = 1;
    }

    @Override
    public void initialize(CommandContext context, BufferManager bufferManager,
            ProcessorDataManager dataMgr) {
        super.initialize(context, bufferManager, dataMgr);
        this.schemaSize = getBufferManager().getSchemaSize(getOutputElements());
    }

    public void open()
            throws TeiidComponentException, TeiidProcessingException {
        CommandContext context = getContext();
        boolean old = context.setParallel(true);
        try {
            openInternal();
        } finally {
            context.setParallel(old);
        }
    }

    public void openInternal()
        throws TeiidComponentException, TeiidProcessingException {

        // Initialize done flags
        sourceDone = new boolean[getChildren().length];

        // Detect if we should be more conservative than the default strategy of opening all children
        RelationalNode parent = this.getParent();
        int rowLimit = -1;
        while (parent != null) {
            if (parent instanceof LimitNode) {
                LimitNode limit = (LimitNode)parent;
                rowLimit = limit.getLimit();
                if (rowLimit != -1 && limit.getOffset() > 0) {
                    rowLimit += limit.getOffset();
                }
                break;
            } else if (parent instanceof SortNode) {
                break;
            } else if (!(parent instanceof SelectNode) || !(parent instanceof ProjectNode) || !(parent instanceof UnionAllNode)) {
                break;
            }
            parent = parent.getParent();
        }
        if (rowLimit != -1 && rowLimit < Integer.MAX_VALUE) {
            int toOpen = 2;
            if (rowLimit > SMALL_LIMIT) {
                if (rowLimit < BufferManagerImpl.DEFAULT_PROCESSOR_BATCH_SIZE) {
                    toOpen = Math.max(Math.min(this.getChildCount(), this.getContext().getUserRequestSourceConcurrency())/3, 2);
                } else {
                    Math.max(2, this.getChildCount()/2 + this.getChildCount()%2);
                }
            }

            //we use the 2x multiple here because the default strategy can proactively execute unneeded results
            if (toOpen < this.getContext().getUserRequestSourceConcurrency()*2) {
                if (reserved == 0) {
                    reserved = getBufferManager().reserveBuffers((toOpen) * schemaSize, BufferReserveMode.FORCE);
                }
                sourceOpen = new boolean[this.getChildCount()];
                //we want to be selective about the number of children we open
                //ideally we would
                RelationalNode[] children = this.getChildren();
                for(int i=0; i<toOpen; i++) {
                    children[i].open();
                    sourceOpen[i] = true;
                }
                return;
            }
        }

        if (reserved == 0) {
            reserved = getBufferManager().reserveBuffers((getChildCount()) * schemaSize, BufferReserveMode.FORCE);
        }

        // Open the children
        super.open();
    }

    public TupleBatch nextBatchDirect()
            throws BlockedException, TeiidComponentException, TeiidProcessingException {
        CommandContext context = getContext();
        boolean old = context.setParallel(true);
        try {
            return nextBatchDirectInternal();
        } finally {
            context.setParallel(old);
        }
    }

    public TupleBatch nextBatchDirectInternal()
        throws BlockedException, TeiidComponentException, TeiidProcessingException {

        // Walk through all children and for each one that isn't done, try to retrieve a batch
        // When all sources are done, set the termination flag on that batch

        RelationalNode[] children = getChildren();
        int childCount = getChildCount();
        int activeSources = 0;
        TupleBatch batch = null;
        boolean additionalSources = false;
        for(int i=0; i<childCount; i++) {
            if(children[i] != null && ! sourceDone[i]) {
                if (sourceOpen != null && !sourceOpen[i]) {
                    additionalSources = true;
                    continue;
                }
                activeSources++;

                if(batch == null) {
                    try {
                        batch = children[i].nextBatch();

                        // Got a batch
                        if(batch.getTerminationFlag() == true) {
                            // Mark source as being done and decrement the activeSources counter
                            sourceDone[i] = true;
                            activeSources--;
                            if (reserved > 0) {
                                getBufferManager().releaseBuffers(schemaSize);
                                reserved-=schemaSize;
                            }
                        }
                    } catch(BlockedException e) {
                        // no problem - try the next one
                    }
                } else {
                    // We already have a batch, so we know that
                    // 1) we have a batch to return and
                    // 2) this isn't the last active source, so we're not returning the last batch

                    // This is sufficient to break the loop - we won't learn anything new after this
                    break;
                }
            }
        }

        // Determine what to return
        TupleBatch outputBatch = null;
        if(batch != null) {
            // Rebuild the batch to reset the output row
            outputBatch = new TupleBatch(outputRow, batch.getTuples());

            // This is the last unioned batch if:
            // 1) This batch is a termination batch from the child
            // 2) No other active sources exist
            outputBatch.setTerminationFlag(batch.getTerminationFlag() && activeSources == 0 && !additionalSources);

            // Update output row for next batch
            outputRow += outputBatch.getRowCount();

        } else if(activeSources > 0) {
            // Didn't get a batch but there are active sources so we are blocked
            throw BlockedException.block(getContext().getRequestId(), "Blocking on union source.", getID()); //$NON-NLS-1$
        } else {
            boolean openedAny = false;
            int toOpen = 0;
            if (sourceOpen != null) {
                for(int i=0; i<childCount; i++) {
                    if(sourceOpen[i] && sourceDone[i]) {
                        toOpen++;
                    }
                }
                for(int i=0; i<childCount && toOpen > 0; i++) {
                    if(!sourceOpen[i]) {
                        getBufferManager().reserveBuffers(schemaSize, BufferReserveMode.FORCE);
                        reserved+=schemaSize;
                        children[i].open();
                        sourceOpen[i] = true;
                        openedAny = true;
                        toOpen--;
                    }
                }
            }
            if (openedAny) {
                return nextBatchDirect();
            }
            // No batch and no active sources - return empty termination batch (should never happen but just in case)
            outputBatch = new TupleBatch(outputRow, Collections.EMPTY_LIST);
            outputBatch.setTerminationFlag(true);
        }

        return outputBatch;
    }

    @Override
    public void closeDirect() {
        if (reserved > 0) {
            getBufferManager().releaseBuffers(reserved);
            reserved = 0;
        }
    }

    public Object clone(){
        UnionAllNode clonedNode = new UnionAllNode(super.getID());
        super.copyTo(clonedNode);
        return clonedNode;
    }

}
