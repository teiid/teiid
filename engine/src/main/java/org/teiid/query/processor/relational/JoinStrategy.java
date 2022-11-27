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

import java.util.ArrayList;
import java.util.List;

import org.teiid.common.buffer.BufferManager.BufferReserveMode;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;


public abstract class JoinStrategy {

    protected JoinNode joinNode;
    protected SourceState leftSource;
    protected SourceState rightSource;
    protected int reserved;

    public void close() {
        if (joinNode == null) {
            return;
        }
        joinNode.getBufferManager().releaseBuffers(reserved);
        reserved = 0;
        try {
            if (leftSource != null) {
                leftSource.close();
            }
        } finally {
            try {
                if (rightSource != null) {
                    rightSource.close();
                }
            } finally {
                leftSource = null;
                rightSource = null;
            }
        }
    }

    public void initialize(JoinNode joinNode) {
        this.joinNode = joinNode;
        this.leftSource = new SourceState(joinNode.getChildren()[0], joinNode.getLeftExpressions());
        this.leftSource.markExpressionsDistinct(this.joinNode.isLeftDistinct());
        this.rightSource = new SourceState(joinNode.getChildren()[1], joinNode.getRightExpressions());
        this.rightSource.markExpressionsDistinct(this.joinNode.isRightDistinct());
    }

    protected void loadLeft() throws TeiidComponentException, TeiidProcessingException {
    }

    protected void loadRight() throws TeiidComponentException, TeiidProcessingException {
    }

    /**
     * Output a combined, projected tuple based on tuple parts from the left and right.
     * @param leftTuple Left tuple part
     * @param rightTuple Right tuple part
     */
    protected List outputTuple(List leftTuple, List rightTuple) {
        List combinedRow = new ArrayList(this.joinNode.getCombinedElementMap().size());
        combinedRow.addAll(leftTuple);
        combinedRow.addAll(rightTuple);
        return combinedRow;
    }

    protected abstract void process() throws TeiidComponentException, TeiidProcessingException;

    public abstract JoinStrategy clone();

    protected void openLeft() throws TeiidComponentException, TeiidProcessingException {
        if (!this.leftSource.open) {
            leftSource.getSource().open();
            this.leftSource.open = true;
        }
    }

    protected void openRight() throws TeiidComponentException, TeiidProcessingException {
        if (!this.rightSource.open) {
            if (reserved == 0) {
                reserved = joinNode.getBufferManager().reserveBuffers(joinNode.getBufferManager().getSchemaSize(joinNode.getOutputElements()), BufferReserveMode.FORCE);
            }
            rightSource.getSource().open();
            this.rightSource.open = true;
        }
    }

}
