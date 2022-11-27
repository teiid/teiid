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

import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.STree;
import org.teiid.common.buffer.STree.InsertMode;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;

public class DupRemoveNode extends RelationalNode {

    private STree stree = null;
    private TupleBatch batch;
    private int counter;

    public DupRemoveNode(int nodeID) {
        super(nodeID);
    }

    public void reset() {
        super.reset();
        stree = null;
        counter = 0;
        batch = null;
    }

    @Override
    public void open() throws TeiidComponentException, TeiidProcessingException {
        super.open();

        stree = getBufferManager().createSTree(this.getElements(), this.getConnectionID(), this.getElements().size());
    }

    public TupleBatch nextBatchDirect()
        throws BlockedException, TeiidComponentException, TeiidProcessingException {
        while (true) {
            if (batch == null) {
                batch = this.getChildren()[0].nextBatch();
            }

            List<List<?>> tuples = batch.getTuples();
            for (;counter < tuples.size(); counter++) {
                List<?> tuple = tuples.get(counter);
                List<?> existing = stree.insert(tuple, InsertMode.NEW, -1);
                if (existing != null) {
                    continue;
                }
                this.addBatchRow(tuple);
                if (this.isBatchFull()) {
                    return pullBatch();
                }
            }
            if (batch.getTerminationFlag()) {
                terminateBatches();
                return pullBatch();
            }
            batch = null;
            counter = 0;
        }
    }

    public void closeDirect() {
        if (stree != null) {
            stree.remove();
        }
    }

    public Object clone(){
        DupRemoveNode clonedNode = new DupRemoveNode(super.getID());
        copyTo(clonedNode);
        return clonedNode;
    }

}
