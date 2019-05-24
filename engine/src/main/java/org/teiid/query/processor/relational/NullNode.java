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
import java.util.List;

import org.teiid.common.buffer.TupleBatch;
import org.teiid.core.TeiidComponentException;


public class NullNode extends RelationalNode {

    public NullNode(int nodeID) {
        super(nodeID);
    }

    public TupleBatch nextBatchDirect()
        throws TeiidComponentException {

        this.terminateBatches();
        return pullBatch();
    }

    @Override
    public List getOutputElements() {
        return Collections.emptyList();
    }

    public Object clone(){
        NullNode clonedNode = new NullNode(super.getID());
        super.copyTo(clonedNode);
        return clonedNode;
    }

}
