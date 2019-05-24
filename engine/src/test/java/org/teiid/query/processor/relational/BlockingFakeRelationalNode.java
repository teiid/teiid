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

import org.teiid.common.buffer.*;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;


public class BlockingFakeRelationalNode extends FakeRelationalNode {

    private int count = 1;

    private int returnPeriod = 2;

    /**
     * @param nodeID
     * @param data
     */
    public BlockingFakeRelationalNode(int nodeID, List[] data) {
        super(nodeID, data);
    }

    public BlockingFakeRelationalNode(int nodeID, List[] data, int batchSize) {
        super(nodeID, data, batchSize);
    }

    /**
     * @param nodeID
     * @param source
     * @param batchSize
     */
    public BlockingFakeRelationalNode(int nodeID, TupleSource source, int batchSize) {
        super(nodeID, source, batchSize);
    }

    public void setReturnPeriod(int returnPeriod) {
        this.returnPeriod = returnPeriod;
    }

    public TupleBatch nextBatchDirect() throws BlockedException, TeiidComponentException, TeiidProcessingException {
        if (count++%returnPeriod != 0) {
            throw BlockedException.INSTANCE;
        }
        return super.nextBatchDirect();
    }

}
