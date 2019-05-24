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

package org.teiid.dqp.internal.process;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeSet;

import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.metadata.CompositeMetadataStore;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.processor.relational.RelationalNode;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.tempdata.BaseIndexInfo;
import org.teiid.query.tempdata.SearchableTable;
import org.teiid.query.util.CommandContext;

abstract class RecordTable<T extends AbstractMetadataRecord> implements SearchableTable {

    public interface SimpleIterator<T> {
        public T next() throws TeiidProcessingException, TeiidComponentException;
    }

    private static final SimpleIterator<?> empty = new SimpleIterator<Object>() {

        @Override
        public Object next() throws TeiidProcessingException, TeiidComponentException {
            return null;
        }
    };

    @SuppressWarnings("unchecked")
    public static <T> SimpleIterator<T> emptyIterator() {
        return (SimpleIterator<T>) empty;
    }

    public static class SimpleIteratorWrapper<T> implements SimpleIterator<T> {
        private Iterator<? extends T> iter;

        public SimpleIteratorWrapper(Iterator<? extends T> iter) {
            this.iter = iter;
        }

        public void setIterator(Iterator<? extends T> iter) {
            this.iter = iter;
        }

        @Override
        public T next() throws TeiidProcessingException,
                TeiidComponentException {
            while (iter.hasNext()) {
                T result = iter.next();
                if (result != null && isValid(result)) {
                    return result;
                }
            }
            return null;
        }

        protected boolean isValid(T result) {
            return true;
        }

    }

    public static abstract class ExpandingSimpleIterator<P, T> implements SimpleIterator<T> {
        private SimpleIterator<T> childIter;
        private SimpleIterator<P> parentIter;
        private P currentParent;

        public ExpandingSimpleIterator(SimpleIterator<P> parentIter) {
            this.parentIter = parentIter;
        }

        @Override
        public T next() throws TeiidProcessingException,
                TeiidComponentException {
            while (true) {
                if (childIter == null) {
                    currentParent = parentIter.next();
                    if (currentParent == null) {
                        return null;
                    }
                    childIter = getChildIterator(currentParent);
                }
                T t = childIter.next();
                if (t != null) {
                    return t;
                }
                childIter = null;
            }
        }

        public P getCurrentParent() {
            return currentParent;
        }

        protected abstract SimpleIterator<T> getChildIterator(P parent);
    }

    private Map<Expression, Integer> columnMap;
    private Expression[] pkColumns;
    protected Evaluator eval;

    public RecordTable(int[] pkColumnIndexs, List<ElementSymbol> columns) {
        this.columnMap = RelationalNode.createLookupMap(columns);
        this.pkColumns = new ElementSymbol[pkColumnIndexs.length];
        for (int i = 0; i < pkColumnIndexs.length; i++) {
            pkColumns[i] = columns.get(pkColumnIndexs[i]);
        }
        eval = new Evaluator(columnMap, null, null);
    }

    @Override
    public int getPkLength() {
        return pkColumns.length;
    }

    @Override
    public Map<Expression, Integer> getColumnMap() {
        return columnMap;
    }

    @Override
    public Boolean matchesPkColumn(int pkIndex, Expression ex) {
        if (ex instanceof Function) {
            Function f = (Function)ex;
            ex = f.getArg(0);
        }
        return (pkColumns[pkIndex].equals(ex));
    }

    @Override
    public boolean supportsOrdering(int pkIndex, Expression ex) {
        return !(ex instanceof ElementSymbol);
    }

    public abstract SimpleIterator<T> processQuery(final VDBMetaData vdb, CompositeMetadataStore metadataStore, BaseIndexInfo<?> ii, TransformationMetadata metadata, CommandContext commandContext);

    public SimpleIterator<T> processQuery(final VDBMetaData vdb, NavigableMap<String, ?> map, BaseIndexInfo<?> ii, final CommandContext commandContext) {
        final Criteria crit = ii.getCoveredCriteria();
        final ArrayList<Object> rowBuffer = new ArrayList<Object>(1);
        if (!ii.getValueSet().isEmpty()) {
            final List<List<Object>> vals = ii.getValueSet();
            final SortedMap<String, ?> fMap = map;
            return new SimpleIterator<T>() {
                int i = 0;
                TreeSet<String> seen = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
                @Override
                public T next() throws TeiidProcessingException, TeiidComponentException {
                    while (i < vals.size()) {
                        String key = (String)vals.get(i++).get(0);
                        if (!seen.add(key)) {
                            //filter to only a single match
                            continue;
                        }
                        T s = extractRecord(fMap.get(key));
                        if (isValid(s, vdb, rowBuffer, crit, commandContext)) {
                            return s;
                        }
                    }
                    return null;
                }
            };
        }
        try {
            if (ii.getLower() != null) {
                if (ii.getUpper() != null) {
                    map = map.subMap((String) ii.getLower().get(0), true, (String) ii.getUpper().get(0), true);
                } else {
                    map = map.tailMap((String) ii.getLower().get(0), true);
                }
            } else if (ii.getUpper() != null) {
                map = map.headMap((String) ii.getUpper().get(0), true);
            }
            final Iterator<?> iter = map.values().iterator();
            return new SimpleIterator<T>() {
                @Override
                public T next() throws TeiidProcessingException, TeiidComponentException {
                    while (iter.hasNext()) {
                        T s = extractRecord(iter.next());
                        if (isValid(s, vdb, rowBuffer, crit, commandContext)) {
                            return s;
                        }
                    }
                    return null;
                }
            };
        } catch (IllegalArgumentException e) {
            //this is a map bound issue or lower is greater than upper
            return emptyIterator();
        }
    }

    protected T extractRecord(Object val) {
        return (T) val;
    }

    public BaseIndexInfo<RecordTable<?>> planQuery(Query query, Criteria condition, CommandContext context) {
        BaseIndexInfo<RecordTable<?>> info = new BaseIndexInfo<RecordTable<?>>(this, Collections.EMPTY_LIST, condition, null, false);
        if (!info.getValueSet().isEmpty()) {
            info.sortValueSet(OrderBy.ASC, context.getBufferManager().getOptions().getDefaultNullOrder());
        }
        return info;
    }

    /**
     *
     * @param s
     * @param vdb
     * @param rowBuffer
     * @param condition
     * @param commandContext
     * @return
     * @throws TeiidProcessingException
     * @throws TeiidComponentException
     */
    protected boolean isValid(T s, VDBMetaData vdb, List<Object> rowBuffer, Criteria condition, CommandContext commandContext) throws TeiidProcessingException, TeiidComponentException {
        if (s == null) {
            return false;
        }
        if (!commandContext.getDQPWorkContext().isAdmin() && !commandContext.getAuthorizationValidator().isAccessible(s, commandContext)) {
            return false;
        }
        if (condition != null) {
            rowBuffer.clear();
            fillRow(s, rowBuffer);
            return eval.evaluate(condition, rowBuffer);
        }
        return true;
    }

    protected void fillRow(T s, List<Object> rowBuffer) {
        rowBuffer.add(s.getName());
    }

}