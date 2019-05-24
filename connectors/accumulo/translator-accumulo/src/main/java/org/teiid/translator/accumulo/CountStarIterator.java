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
package org.teiid.translator.accumulo;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
/**
 * Implements aggregate function Count(*) over Accumulo
 */
public class CountStarIterator extends WrappingIterator {
    public static final String ALIAS = "alias"; //$NON-NLS-1$
    private Key topKey;
    private Value topValue;
    private String alias;

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source,
            Map<String, String> options, IteratorEnvironment env)
            throws IOException {
        super.init(source, options, env);
        this.alias = options.get(ALIAS);
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        CountStarIterator newInstance;
        try {
            newInstance = this.getClass().newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        newInstance.setSource(getSource().deepCopy(env));
        newInstance.alias = alias;
        newInstance.topKey = topKey;
        newInstance.topValue = topValue;
        return newInstance;
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies,
            boolean inclusive) throws IOException {
        super.seek(range, columnFamilies, inclusive);

        if (getSource().hasTop()) {
            int count = 0;
            ByteSequence prevRowId  = null;
            while (getSource().hasTop()) {
                Key key = getSource().getTopKey();
                ByteSequence rowId = key.getRowData();
                if (prevRowId == null || !prevRowId.equals(rowId)) {
                    count++;
                    prevRowId = rowId;
                }
                getSource().next();
            }
            this.topKey = new Key("1", this.alias, this.alias);//$NON-NLS-1$
            this.topValue = new Value(AccumuloDataTypeManager.serialize(count));
        }
    }

    @Override
    public Value getTopValue() {
        return topValue;
    }

    @Override
    public Key getTopKey() {
        return topKey;
    }

    @Override
    public boolean hasTop() {
        return topKey != null;
    }

    @Override
    public void next() throws IOException {
        this.topKey = null;
        this.topValue = null;
    }
}