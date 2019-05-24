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
package org.teiid.common.buffer;

import java.util.List;

import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;

public abstract class AbstractTupleSource implements IndexedTupleSource {
    private long currentRow = 1;
    private long mark = 1;
    protected List<?> currentTuple;
    protected TupleBatch batch;

    @Override
    public long getCurrentIndex() {
        return this.currentRow;
    }

    @Override
    public List<?> nextTuple()
    throws TeiidComponentException, TeiidProcessingException{
        List<?> result = null;
        if (currentTuple != null){
            result = currentTuple;
            currentTuple = null;
        } else {
            result = getCurrentTuple();
        }
        if (result != null) {
            currentRow++;
        }
        return result;
    }

    protected List<?> getCurrentTuple() throws TeiidComponentException,
            BlockedException, TeiidProcessingException {
        if (available() > 0) {
            //if (forwardOnly) {
                long row = getCurrentIndex();
                if (batch == null || !batch.containsRow(row)) {
                    batch = getBatch(row);
                }
                return batch.getTuple(row);
            //}
            //TODO: determine if we should directly hold a soft reference here
            //return getRow(currentRow);
        }
        batch = null;
        return finalRow();
    }

    protected abstract List<?> finalRow() throws BlockedException, TeiidComponentException, TeiidProcessingException;

    protected abstract TupleBatch getBatch(long row) throws TeiidComponentException, TeiidProcessingException;

    protected abstract long available();

    @Override
    public void closeSource() {
        batch = null;
        mark = 1;
        reset();
    }

    @Override
    public boolean hasNext() throws TeiidComponentException, TeiidProcessingException {
        if (this.currentTuple != null) {
            return true;
        }

        this.currentTuple = getCurrentTuple();
        return this.currentTuple != null;
    }

    @Override
    public void reset() {
        this.setPosition(mark);
        this.mark = 1;
    }

    @Override
    public void mark() throws TeiidComponentException {
        this.mark = currentRow;
    }

    @Override
    public void setPosition(long position) {
        if (this.currentRow != position) {
            this.currentRow = position;
            this.currentTuple = null;
        }
    }

}