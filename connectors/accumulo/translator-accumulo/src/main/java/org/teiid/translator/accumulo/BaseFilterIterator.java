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
import java.util.Arrays;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.conf.ColumnSet;
import org.apache.accumulo.core.iterators.user.RowFilter;

/**
 * Base class for Filters
 */
public abstract class BaseFilterIterator extends RowFilter {
    public static final String NEGATE = "NEGATE"; //$NON-NLS-1$
    protected AccumuloMetadataProcessor.ValueIn valueIn;
    protected ColumnSet columnFilter;
    private boolean negate = false;

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source,
            Map<String, String> options, IteratorEnvironment env)
            throws IOException {
        super.init(source, options, env);

        String cf = options.get(AccumuloMetadataProcessor.CF);
        String cq = options.get(AccumuloMetadataProcessor.CQ);

        if (options.get(AccumuloMetadataProcessor.VALUE_IN) != null) {
            String value = options.get(AccumuloMetadataProcessor.VALUE_IN);
            this.valueIn = AccumuloMetadataProcessor.ValueIn.valueOf(value.substring(1, value.length()-1));
        } else {
            this.valueIn = AccumuloMetadataProcessor.ValueIn.VALUE;
        }

        if (cq != null) {
            this.columnFilter = new ColumnSet(Arrays.asList(cf + ":" + cq)); //$NON-NLS-1$
        } else {
            this.columnFilter = new ColumnSet(Arrays.asList(cf));
        }

        this.negate = false;
        if (options.get(NEGATE) != null) {
          this.negate = Boolean.parseBoolean(options.get(NEGATE));
        }
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        BaseFilterIterator instance = (BaseFilterIterator) super.deepCopy(env);
        instance.valueIn = this.valueIn;
        instance.columnFilter = this.columnFilter;
        return instance;
    }

    @Override
    public boolean acceptRow(SortedKeyValueIterator<Key,Value> rowItem) throws IOException {
        if (this.negate) {
            return !accept(rowItem);
        }
        return accept(rowItem);
    }

    private boolean accept(SortedKeyValueIterator<Key,Value> rowItem) {
        while(rowItem.hasTop()) {
            Key key = rowItem.getTopKey();
            if (this.columnFilter.contains(key)) {
                byte[] value;
                if (this.valueIn.equals(AccumuloMetadataProcessor.ValueIn.VALUE)) {
                    value = rowItem.getTopValue().get();
                }
                else {
                    value = key.getColumnQualifier().getBytes();
                }
                return accept(value);
            }
        }
        return true;
    }

    @SuppressWarnings("unused")
    public boolean accept(byte[] value) {
        return true;
    }
}
