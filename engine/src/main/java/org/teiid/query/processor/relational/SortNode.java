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

import java.util.ArrayList;
import java.util.List;

import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.processor.BatchIterator;
import org.teiid.query.processor.relational.SortUtility.Mode;
import org.teiid.query.sql.lang.OrderByItem;


public class SortNode extends RelationalNode {

    private List<OrderByItem> items;
    private Mode mode = Mode.SORT;

    private SortUtility sortUtility;
    private int phase = SORT;
    private TupleBuffer output;
    private TupleSource outputTs;
    private boolean usingOutput;

    private int rowLimit = -1;

    private static final int SORT = 2;
    private static final int OUTPUT = 3;

    public SortNode(int nodeID) {
        super(nodeID);
    }

    public void reset() {
        super.reset();
        sortUtility = null;
        phase = SORT;
        output = null;
        outputTs = null;
        usingOutput = false;
        rowLimit = -1;
    }

    public void setSortElements(List<OrderByItem> items) {
        this.items = items;
    }

    public List<OrderByItem> getSortElements() {
        return this.items;
    }

    public Mode getMode() {
        return mode;
    }

    public void setMode(Mode mode) {
        this.mode = mode;
    }

    public TupleBatch nextBatchDirect()
        throws BlockedException, TeiidComponentException, TeiidProcessingException {
        if(this.phase == SORT) {
            sortPhase();
        }

        return outputPhase();
    }

    private void sortPhase() throws BlockedException, TeiidComponentException, TeiidProcessingException {
        if (this.sortUtility == null) {
            TupleSource ts = null;
            TupleBuffer working = null;
            if (!getChildren()[0].hasBuffer()) {
                ts = new BatchIterator(getChildren()[0]);
            } else {
                working = getChildren()[0].getBuffer(-1);
            }
            this.sortUtility = new SortUtility(ts, items, this.mode, getBufferManager(),
                    getConnectionID(), getChildren()[0].getElements());
            if (ts == null) {
                this.sortUtility.setWorkingBuffer(working);
            }
        }
        this.output = this.sortUtility.sort(rowLimit);
        if (this.outputTs == null) {
            this.outputTs = this.output.createIndexedTupleSource();
        }
        this.phase = OUTPUT;
    }

    private TupleBatch outputPhase() throws BlockedException, TeiidComponentException, TeiidProcessingException {
        if (!this.output.isFinal()) {
            this.phase = SORT;
        } else if (!usingOutput) {
            this.output.setForwardOnly(true);
        }
        List<?> tuple = null;
        try {
            while ((tuple = this.outputTs.nextTuple()) != null) {
                //resize to remove unrelated columns
                if (this.getElements().size() < tuple.size()) {
                    tuple = new ArrayList<Object>(tuple.subList(0, this.getElements().size()));
                }
                addBatchRow(tuple);
                if (this.isBatchFull()) {
                    return pullBatch();
                }
            }
        } catch (BlockedException e) {
            if (this.hasPendingRows()) {
                return this.pullBatch();
            }
            throw e;
        }
        this.terminateBatches();
        return this.pullBatch();
    }

    public void closeDirect() {
        if(this.output != null) {
            if (!usingOutput) {
                this.output.remove();
            }
            this.output = null;
        }
        if (this.sortUtility != null) {
            this.sortUtility.remove();
            this.sortUtility = null;
        }
        this.outputTs = null;
    }

    protected void getNodeString(StringBuffer str) {
        super.getNodeString(str);
        str.append("[").append(mode).append("] "); //$NON-NLS-1$ //$NON-NLS-2$
        str.append(this.items);
    }

    protected void copyTo(SortNode target){
        super.copyTo(target);
        target.items = items;
        target.mode = mode;
    }

    public Object clone(){
        SortNode clonedNode = new SortNode(super.getID());
        this.copyTo(clonedNode);

        return clonedNode;
    }

    public PlanNode getDescriptionProperties() {
        PlanNode props = super.getDescriptionProperties();

        if(this.items != null) {
            props.addProperty(PROP_SORT_COLS, this.items.toString());
        }

        props.addProperty(PROP_SORT_MODE, this.mode.toString());

        return props;
    }

    @Override
    public TupleBuffer getBufferDirect(int maxRows) throws BlockedException, TeiidComponentException, TeiidProcessingException {
        if (this.isClosed()) {
            throw new AssertionError("called after close"); //$NON-NLS-1$
        }
        this.rowLimit = maxRows;
        if (this.output == null) {
            sortPhase();
        }
        usingOutput = true;
        TupleBuffer result = this.output;
        if (this.output.isFinal()) {
            this.output = null;
            close();
        }
        return result;
    }

    @Override
    public boolean hasBuffer() {
        if (this.getElements().size() == this.getChildren()[0].getElements().size()) {
            return true;
        }
        return false;
    }

    public void setRowLimit(int rowLimit) {
        this.rowLimit = rowLimit;
    }

}
