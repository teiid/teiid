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

import java.util.Arrays;
import java.util.List;

import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.dqp.internal.process.PreparedStatementRequest;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.sql.symbol.Reference;


public class InsertPlanExecutionNode extends PlanExecutionNode {

    private List<Reference> references;

    private int batchRow = 1;
    private int insertCount = 0;
    private TupleBatch currentBatch;
    private QueryMetadataInterface metadata;

    public InsertPlanExecutionNode(int nodeID, QueryMetadataInterface metadata) {
        super(nodeID);
        this.metadata = metadata;
    }

    public void setReferences(List<Reference> references) {
        this.references = references;
    }

    @Override
    protected void addBatchRow(List row) {
        this.insertCount += ((Integer)row.get(0)).intValue();
    }

    @Override
    protected TupleBatch pullBatch() {
        if (isLastBatch()) {
            super.addBatchRow(Arrays.asList(insertCount));
        }
        return super.pullBatch();
    }

    @Override
    protected boolean hasNextCommand() {
        return !this.currentBatch.getTerminationFlag() || batchRow <= this.currentBatch.getEndRow();
    }

    @Override
    protected boolean openPlanImmediately() {
        return false;
    }

    @Override
    protected boolean prepareNextCommand() throws BlockedException,
            TeiidComponentException, TeiidProcessingException {
        if (this.currentBatch == null) {
            this.currentBatch = this.getChildren()[0].nextBatch();
        }
        if (!hasNextCommand()) {
            return false;
        }
        //assign the reference values.
        PreparedStatementRequest.resolveParameterValues(this.references, this.currentBatch.getTuple(this.batchRow), getProcessorPlan().getContext(), this.metadata);
        this.batchRow++;
        return true;
    }

    public Object clone(){
        InsertPlanExecutionNode clonedNode = new InsertPlanExecutionNode(super.getID(), this.metadata);
        copyTo(clonedNode);
        return clonedNode;
    }

    protected void copyTo(InsertPlanExecutionNode target) {
        target.references = references;
        super.copyTo(target);
    }

    @Override
    public void closeDirect() {
        super.closeDirect();
        this.currentBatch = null;
    }

    @Override
    public void reset() {
        super.reset();
        this.currentBatch = null;
        this.batchRow = 1;
        this.insertCount = 0;
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
