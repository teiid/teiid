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

/*
 */
package org.teiid.query.processor.relational;

import static org.teiid.query.analysis.AnalysisRecord.*;

import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.api.exception.query.QueryProcessingException;
import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager.TupleSourceType;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.QueryPlugin;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.processor.RegisterRequestParameter;
import org.teiid.query.sql.lang.BatchedUpdateCommand;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.SourceHint;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.translator.ExecutionFactory.TransactionSupport;


public class ProjectIntoNode extends RelationalNode {

    public enum Mode {
        BATCH, ITERATOR, SINGLE
    }

    private static int REQUEST_CREATION = 1;
    private static int RESPONSE_PROCESSING = 2;

    // Initialization state
    private GroupSymbol intoGroup;
    private List intoElements;
    private String modelName;
    private Mode mode;
    private boolean upsert;

    // Processing state
    private long batchRow = 1;
    private long insertCount = 0;
    private int phase = REQUEST_CREATION;
    private int requestsRegistered = 0;
    private int tupleSourcesProcessed = 0;
    private boolean sourceDone;

    private TupleBuffer buffer;
    private TupleBuffer last;
    private TupleBatch currentBatch;

    private TupleSource tupleSource;

    private Criteria constraint;
    private Evaluator eval;

    private TransactionSupport transactionSupport;
    private SourceHint sourceHint;

    protected ProjectIntoNode() {
        super();
    }

    public ProjectIntoNode(int nodeID) {
        super(nodeID);
    }

    public void reset() {
        super.reset();
        this.phase = REQUEST_CREATION;
        this.batchRow = 1;
        this.insertCount = 0;
        this.tupleSourcesProcessed = 0;
        this.requestsRegistered = 0;
        this.currentBatch=null;
        this.sourceDone=false;
    }

    public void setIntoGroup(GroupSymbol group) {
        this.intoGroup = group;
    }

    public void setIntoElements(List intoElements) {
        this.intoElements = intoElements;
    }

    public void setModelName(String modelName) {
        this.modelName = modelName;
    }

    /**
     * Get batch from child node
     * Walk through each row of child batch
     *    Bind values to insertCommand
     *    Execute insertCommand
     *    Update insertCount
     * When no more data is available, output batch with single row containing insertCount
     */
    public TupleBatch nextBatchDirect()
        throws BlockedException, TeiidComponentException, TeiidProcessingException {

        while(phase == REQUEST_CREATION) {

            /* If we don't have a batch to work, get the next
             */
            if (currentBatch == null) {
                if (sourceDone) {
                    phase = RESPONSE_PROCESSING;
                    break;
                }
                currentBatch = getChildren()[0].nextBatch(); // can throw BlockedException
                sourceDone = currentBatch.getTerminationFlag();
                this.batchRow = currentBatch.getBeginRow();

                //normally we would want to skip a 0 sized batch, but it typically represents the terminal batch
                //and for implicit temp tables we need to issue an empty insert
                if(currentBatch.getRowCount() == 0
                        && (!currentBatch.getTerminationFlag() || mode != Mode.ITERATOR)) {
                    currentBatch = null;
                    continue;
                }
                if (this.constraint != null) {
                    //row based security check
                    if (eval == null) {
                        eval = new Evaluator(createLookupMap(this.intoElements), this.getDataManager(), getContext());
                    }
                    List<List<?>> tuples = this.currentBatch.getTuples();
                    for (int i = 0; i < tuples.size(); i++) {
                        if (!eval.evaluate(constraint, tuples.get(i))) {
                            throw new QueryProcessingException(QueryPlugin.Event.TEIID31130, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31130, new Insert(intoGroup, this.intoElements, convertValuesToConstants(tuples.get(i), intoElements))));
                        }
                    }
                }
            }

            if (mode != Mode.ITERATOR) { //delay the check in the iterator case to accumulate batches
                checkExitConditions();
            }

            int batchSize = currentBatch.getRowCount();
            int requests = 1;
            switch (mode) {
            case ITERATOR:
                if (buffer == null) {
                    buffer = getBufferManager().createTupleBuffer(intoElements, getConnectionID(), TupleSourceType.PROCESSOR);
                }

                if (sourceDone) {
                    //if there is a pending request we can't process the last until it is done
                    checkExitConditions();
                }

                for (List<?> tuple : currentBatch.getTuples()) {
                    buffer.addTuple(tuple);
                }

                try {
                    checkExitConditions();
                } catch (BlockedException e) {
                    //move to the next batch
                    this.batchRow += batchSize;
                    currentBatch = null;
                    continue;
                }

                if (currentBatch.getTerminationFlag() && (buffer.getRowCount() != 0 || intoGroup.isImplicitTempGroupSymbol())) {
                    registerIteratorRequest();
                } else if (buffer.getRowCount() >= buffer.getBatchSize() * 4) {
                    registerIteratorRequest();
                } else {
                    requests = 0;
                }
                break;
            case BATCH:
                // Register batched update command against source
                long endRow = currentBatch.getEndRow();
                List<Command> rows = new ArrayList<Command>((int)(endRow-batchRow));
                for(long rowNum = batchRow; rowNum <= endRow; rowNum++) {

                    Insert insert = new Insert( intoGroup,
                                                 intoElements,
                                                 convertValuesToConstants(currentBatch.getTuple(rowNum), intoElements));
                    insert.setSourceHint(sourceHint);
                    insert.setUpsert(upsert);
                    rows.add( insert );
                }
                registerRequest(new BatchedUpdateCommand( rows ));
                break;
            case SINGLE:
                batchSize = 1;
                // Register insert command against source
                // Defect 16036 - submit a new INSERT command to the DataManager.
                Insert insert = new Insert(intoGroup, intoElements, convertValuesToConstants(currentBatch.getTuple(batchRow), intoElements));
                insert.setSourceHint(sourceHint);
                insert.setUpsert(upsert);
                registerRequest(insert);
            }

            this.batchRow += batchSize;
            if (batchRow > currentBatch.getEndRow()) {
                currentBatch = null;
            }
            this.requestsRegistered+=requests;
        }

        checkExitConditions();

        if (this.buffer != null) {
            this.buffer.remove();
            this.buffer = null;
        }

        // End this node's work
        //report only a max int
        int count = (int)Math.min(Integer.MAX_VALUE, insertCount);
        addBatchRow(Arrays.asList(count));
        terminateBatches();
        return pullBatch();
    }

    private void registerIteratorRequest() throws TeiidComponentException,
            TeiidProcessingException {
        Insert insert = new Insert(intoGroup, intoElements, null);
        insert.setSourceHint(sourceHint);
        insert.setUpsert(upsert);
        buffer.close();
        insert.setTupleSource(buffer.createIndexedTupleSource(true));
        // Register insert command against source
        registerRequest(insert);
        //remove the old buffer when the insert is complete
        last = buffer;
        buffer = null;
    }

    private void checkExitConditions()  throws TeiidComponentException, BlockedException, TeiidProcessingException {
        if (tupleSource != null) {
            if (mode == Mode.BATCH || mode == Mode.ITERATOR) {
                List<?> tuple = null;
                while ((tuple = tupleSource.nextTuple()) != null) {
                    Integer count = (Integer)tuple.get(0);
                    if (count > 0 ||  count == Statement.SUCCESS_NO_INFO) {
                        insertCount++;
                    }
                }
            } else {
                Integer count = (Integer)tupleSource.nextTuple().get(0);
                insertCount += count.intValue();
            }
            closeRequest();
            // Mark as processed
            tupleSourcesProcessed++; // This should set tupleSourcesProcessed to be the same as requestsRegistered
        }
        // RESPONSE_PROCESSING: process tuple sources
        if (tupleSourcesProcessed < requestsRegistered) {
            throw BlockedException.block(getContext().getRequestId(), "Blocking on insert update count"); //$NON-NLS-1$
        }

    }

    private void registerRequest(Command command) throws TeiidComponentException, TeiidProcessingException {
        tupleSource = getDataManager().registerRequest(getContext(), command, this.modelName, new RegisterRequestParameter(null, getID(), -1));
    }

    private void closeRequest() {
        if (this.last != null) {
            this.last.remove();
            this.last = null;
        }
        if (this.tupleSource != null) {
            tupleSource.closeSource();
            this.tupleSource = null;
        }
    }

    protected void getNodeString(StringBuffer str) {
        super.getNodeString(str);
        str.append(intoGroup);
    }

    public Object clone(){
        ProjectIntoNode clonedNode = new ProjectIntoNode();
        super.copyTo(clonedNode);

        clonedNode.intoGroup = intoGroup;
        clonedNode.intoElements = intoElements;
        clonedNode.modelName = this.modelName;
        clonedNode.mode = this.mode;
        clonedNode.constraint = this.constraint;
        clonedNode.sourceHint = this.sourceHint;
        clonedNode.upsert = this.upsert;
        return clonedNode;
    }

    public PlanNode getDescriptionProperties() {
        PlanNode props = super.getDescriptionProperties();
        props.addProperty(PROP_INTO_GROUP, intoGroup.toString());
        if (upsert) {
            props.addProperty(PROP_UPSERT, "true"); //$NON-NLS-1$
        }
        List<String> selectCols = new ArrayList<String>(intoElements.size());
        for(int i=0; i<this.intoElements.size(); i++) {
            selectCols.add(this.intoElements.get(i).toString());
        }
        props.addProperty(PROP_SELECT_COLS, selectCols);

        return props;
    }

    private List<Constant> convertValuesToConstants(List<?> values, List<ElementSymbol> elements) {
        ArrayList<Constant> constants = new ArrayList<Constant>(values.size());
        for(int i=0; i<elements.size(); i++) {
            ElementSymbol es = elements.get(i);
            Class<?> type = es.getType();
            constants.add(new Constant(values.get(i),type));
        }
        return constants;
    }

    public Mode getMode() {
        return mode;
    }

    public void setMode(Mode mode) {
        this.mode = mode;
    }

    public boolean isTempGroupInsert() {
        return intoGroup.isTempGroupSymbol();
    }

    public void closeDirect() {
        if (this.buffer != null) {
            this.buffer.remove();
            this.buffer = null;
        }
        closeRequest();
    }

    public String getModelName() {
        return modelName;
    }

    @Override
    public Boolean requiresTransaction(boolean transactionalReads) {
        Boolean requires = this.getChildren()[0].requiresTransaction(transactionalReads);
        if (requires != null && requires) {
            return true;
        }
        if (transactionSupport == TransactionSupport.NONE) {
            return requires;
        }
        return true;
    }

    public void setConstraint(Criteria constraint) {
        this.constraint = constraint;
    }

    public void setTransactionSupport(TransactionSupport transactionSupport) {
        this.transactionSupport = transactionSupport;
    }

    public void setSourceHint(SourceHint property) {
        this.sourceHint = property;
    }

    public void setUpsert(boolean upsert) {
        this.upsert = upsert;
    }

}
