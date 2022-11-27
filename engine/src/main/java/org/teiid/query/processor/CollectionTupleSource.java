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

package org.teiid.query.processor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;


public class CollectionTupleSource implements TupleSource {

    public static final List<Integer> UPDATE_ROW = Arrays.asList(1);
    private Iterator<? extends List<?>> tuples;

    public static CollectionTupleSource createUpdateCountTupleSource(int count) {
        return new CollectionTupleSource(Arrays.asList(Arrays.asList(count)).iterator());
    }

    public static TupleSource createUpdateCountArrayTupleSource(final long count) {
        return new TupleSource() {
            long index = 0;

            @Override
            public List<?> nextTuple() throws TeiidComponentException,
                    TeiidProcessingException {
                if (index++ < count) {
                    return UPDATE_ROW;
                }
                return null;
            }

            @Override
            public void closeSource() {

            }
        };
    }

    public static CollectionTupleSource createNullTupleSource() {
        return new CollectionTupleSource(new ArrayList<List<Object>>(0).iterator());
    }

    public CollectionTupleSource(Iterator<? extends List<?>> tuples) {
        this.tuples = tuples;
    }

    @Override
    public List<?> nextTuple() {
        if (tuples.hasNext()) {
            return tuples.next();
        }
        return null;
    }

    @Override
    public void closeSource() {

    }

}