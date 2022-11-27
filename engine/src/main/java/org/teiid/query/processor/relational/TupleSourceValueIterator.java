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

import org.teiid.common.buffer.IndexedTupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.sql.util.ValueIterator;


/**
 * A ValueIterator implementation that iterates over the TupleSource
 * results of a subquery ProcessorPlan.  The plan will
 * always have only one result column.  Constant Object values will
 * be returned, not Expressions.
 *
 * This implementation is resettable.
 */
class TupleSourceValueIterator implements ValueIterator{

    private IndexedTupleSource tupleSourceIterator;
    private int columnIndex;

    TupleSourceValueIterator(IndexedTupleSource tupleSource, int columnIndex){
        this.tupleSourceIterator = tupleSource;
        this.columnIndex = columnIndex;
    }

    /**
     * @see java.util.Iterator#hasNext()
     */
    public boolean hasNext() throws TeiidComponentException{
        try {
            return tupleSourceIterator.hasNext();
        } catch (TeiidProcessingException e) {
             throw new TeiidComponentException(e);
        }
    }

    /**
     * Returns constant Object values, not Expressions.
     * @see java.util.Iterator#next()
     */
    public Object next() throws TeiidComponentException{
        return nextTuple().get(columnIndex);
    }

    protected List<?> nextTuple() throws TeiidComponentException {
        try {
            return tupleSourceIterator.nextTuple();
        } catch (TeiidProcessingException e) {
             throw new TeiidComponentException(e);
        }
    }

    public void close() {
        this.tupleSourceIterator.closeSource();
    }

    /**
     * Flags a reset as being needed
     * @see org.teiid.query.sql.util.ValueIterator#reset()
     */
    public void reset() {
        this.tupleSourceIterator.reset();
    }
}
