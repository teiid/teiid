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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.hadoop.io.Text;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidComponentException;
import org.teiid.metadata.Column;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.function.FunctionTree;
import org.teiid.query.function.SystemFunctionManager;
import org.teiid.query.metadata.CompositeMetadataStore;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.resolver.util.ResolverVisitor;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.util.CommandContext;
import org.teiid.translator.accumulo.AccumuloMetadataProcessor.ValueIn;

/**
 * This iterator makes uses of Teiid engine for criteria evaluation. For this to work, the teiid libraries
 * need to be copied over to the accumulo classpath.
 *
 * RowFilter based implemention fails with "java.lang.RuntimeException: Setting interrupt
 * flag after calling deep copy not supported", this is copy of WholeRowIterator
 */
public class EvaluatorIterator extends WrappingIterator {
    private static final SystemFunctionManager SFM = SystemMetadata.getInstance().getSystemFunctionManager();
    public static final String QUERYSTRING = "QUERYSTRING"; //$NON-NLS-1$
    public static final String TABLE = "TABLE";//$NON-NLS-1$
    public static final String DDL = "DDL";//$NON-NLS-1$

    public static final String IMPLICIT_MODEL_NAME = "model";//$NON-NLS-1$

    static class KeyValuePair{
        Key key;
        Value value;
    }

    private Criteria criteria;
    private Evaluator evaluator;
    private Collection<ElementSymbol> elementsInExpression;
    private EvaluatorUtil evaluatorUtil;
    private ArrayList<KeyValuePair> currentValues;

    private Iterator<KeyValuePair> rowIterator;
    private Key topKey;
    private Value topValue;

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source,
            Map<String, String> options, IteratorEnvironment env)
            throws IOException {
        super.init(source, options, env);

        try {
            GroupSymbol gs = null;
            String query = options.get(QUERYSTRING);
            TransformationMetadata tm = createTransformationMetadata(options.get(DDL));
            this.criteria = QueryParser.getQueryParser().parseCriteria(query);
            this.elementsInExpression = ElementCollectorVisitor.getElements(this.criteria, false);
            for (ElementSymbol es : this.elementsInExpression) {
                gs = es.getGroupSymbol();
                ResolverUtil.resolveGroup(gs, tm);
            }
            ResolverVisitor.resolveLanguageObject(this.criteria, tm);
            this.evaluatorUtil = new EvaluatorUtil(gs);
        } catch (QueryParserException e) {
            throw new IOException(e);
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        } catch (QueryResolverException e) {
            throw new IOException(e);
        } catch (TeiidComponentException e) {
            throw new IOException(e);
        }
        CommandContext cc = new CommandContext();
        this.evaluator = new Evaluator(this.evaluatorUtil.getElementMap(), null, cc);
    }

    public static TransformationMetadata createTransformationMetadata(String ddl) {
        MetadataStore mds = new MetadataStore();
        MetadataFactory mf = new MetadataFactory("vdb", 1, IMPLICIT_MODEL_NAME,
                SystemMetadata.getInstance().getRuntimeTypeMap(),
                new Properties(), null);
        QueryParser.getQueryParser().parseDDL(mf, ddl);
        mf.mergeInto(mds);
        CompositeMetadataStore store = new CompositeMetadataStore(mds);

        VDBMetaData vdbMetaData = new VDBMetaData();
        vdbMetaData.setName("vdb"); //$NON-NLS-1$
        vdbMetaData.setVersion(1);
        List<FunctionTree> udfs = new ArrayList<FunctionTree>();
        for (Schema schema : store.getSchemas().values()) {
            vdbMetaData.addModel(createModel(schema.getName(), schema.isPhysical()));
        }
        TransformationMetadata metadata = new TransformationMetadata(vdbMetaData, store, null, SFM.getSystemFunctions(), udfs);
        vdbMetaData.addAttachment(TransformationMetadata.class, metadata);
        vdbMetaData.addAttachment(QueryMetadataInterface.class, metadata);
        return metadata;
    }

    public static ModelMetaData createModel(String name, boolean source) {
        ModelMetaData model = new ModelMetaData();
        model.setName(name);
        if (source) {
            model.setModelType(Model.Type.PHYSICAL);
        }
        else {
            model.setModelType(Model.Type.VIRTUAL);
        }
        model.setVisible(true);
        model.setSupportsMultiSourceBindings(false);
        model.addSourceMapping(name, name, null);

        return model;
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        EvaluatorIterator newInstance;
        try {
            newInstance = this.getClass().newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        newInstance.setSource(getSource().deepCopy(env));
        newInstance.criteria = this.criteria;
        newInstance.currentValues = this.currentValues;
        newInstance.elementsInExpression = this.elementsInExpression;
        newInstance.evaluator = this.evaluator;
        newInstance.evaluatorUtil = this.evaluatorUtil;
        newInstance.topKey = this.topKey;
        newInstance.topValue = this.topValue;
        newInstance.rowIterator = this.rowIterator;
        return newInstance;
    }

      @Override
      public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {

        Key sk = range.getStartKey();

        if (sk != null && sk.getColumnFamilyData().length() == 0 && sk.getColumnQualifierData().length() == 0 && sk.getColumnVisibilityData().length() == 0
            && sk.getTimestamp() == Long.MAX_VALUE && !range.isStartKeyInclusive()) {
          // assuming that we are seeking using a key previously returned by this iterator
          // therefore go to the next row
          Key followingRowKey = sk.followingKey(PartialKey.ROW);
          if (range.getEndKey() != null && followingRowKey.compareTo(range.getEndKey()) > 0)
            return;

          range = new Range(sk.followingKey(PartialKey.ROW), true, range.getEndKey(), range.isEndKeyInclusive());
        }

        getSource().seek(range, columnFamilies, inclusive);
        prepKeys();
      }

    private void prepKeys() throws IOException {
        this.currentValues = new ArrayList<EvaluatorIterator.KeyValuePair>();
        Text currentRow;
        do {
            this.currentValues.clear();
            this.rowIterator = null;
            if (getSource().hasTop() == false) {
                this.currentValues = null;
                return;
            }
            currentRow = new Text(getSource().getTopKey().getRow());
            while (getSource().hasTop() && getSource().getTopKey().getRow().equals(currentRow)) {
                KeyValuePair kv = new KeyValuePair();
                kv.key = getSource().getTopKey();
                kv.value = new Value(getSource().getTopValue());
                this.currentValues.add(kv);
                getSource().next();
            }
        } while (!filter(this.currentValues));
    }

    protected boolean filter(ArrayList<KeyValuePair> values) throws IOException {
        if (acceptRow(values)) {
            this.rowIterator = values.iterator();
            advanceRow();
            return true;
        }
        return false;
    }

    @Override
    public Key getTopKey() {
        return this.topKey;
    }

    @Override
    public Value getTopValue() {
        return this.topValue;
    }

    @Override
    public boolean hasTop() {
        return this.topKey != null;
    }

    @Override
    public void next() throws IOException {
        if (!advanceRow()) {
            prepKeys();
        }
    }

    private boolean advanceRow() {
        if (this.rowIterator != null && this.rowIterator.hasNext()) {
            KeyValuePair kv = this.rowIterator.next();
            this.topKey = kv.key;
            this.topValue = kv.value;
            return true;
        }
        this.topKey = null;
        this.topValue = null;
        this.rowIterator = null;
        return false;
    }

    private boolean acceptRow(ArrayList<KeyValuePair> values) throws IOException {
        try {
            return this.evaluator.evaluate(this.criteria, this.evaluatorUtil.buildTuple(values));
        } catch (ExpressionEvaluationException e) {
            throw new IOException(e);
        } catch (BlockedException e) {
            throw new IOException(e);
        } catch (TeiidComponentException e) {
            throw new IOException(e);
        }
    }

    private static class ColumnInfo {
        ElementSymbol es;
        int pos;
        AccumuloMetadataProcessor.ValueIn in;
    }

    private static class ColumnSet extends org.apache.accumulo.core.iterators.conf.ColumnSet {
        private Text colf;
        private Text colq;
        public ColumnSet(Text colf, Text colq) {
            super.add(colf, colq);
            this.colf = colf;
            this.colq = colq;
        }
        public ColumnSet(Text colf) {
            super.add(colf);
            this.colf = colf;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((colf == null) ? 0 : colf.hashCode());
            result = prime * result + ((colq == null) ? 0 : colq.hashCode());
            return result;
        }
        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            ColumnSet other = (ColumnSet) obj;
            if (colf == null) {
                if (other.colf != null)
                    return false;
            } else if (!colf.equals(other.colf))
                return false;
            if (colq == null) {
                if (other.colq != null)
                    return false;
            } else if (!colq.equals(other.colq))
                return false;
            return true;
        }

    }

    private static class EvaluatorUtil {
        private Map<ColumnSet, ColumnInfo> columnMap =  new HashMap<ColumnSet, ColumnInfo>();
        private Map<ElementSymbol, Integer> elementMap = new HashMap<ElementSymbol, Integer>();

        public EvaluatorUtil(GroupSymbol group) throws ClassNotFoundException {

            List<Column> columns = ((Table)(group.getMetadataID())).getColumns();
            for (int i = 0; i < columns.size(); i++) {
                Column column = columns.get(i);
                ElementSymbol element = new ElementSymbol(column.getName(), group, Class.forName(column.getDatatype().getJavaClassName()));
                this.elementMap.put(element, i);

                String valueIn = column.getProperty(AccumuloMetadataProcessor.VALUE_IN, false);
                String cf = column.getProperty(AccumuloMetadataProcessor.CF, false);
                String cq = column.getProperty(AccumuloMetadataProcessor.CQ, false);

                AccumuloMetadataProcessor.ValueIn valueInEnum = AccumuloMetadataProcessor.ValueIn.VALUE;
                if (valueIn != null) {
                    valueInEnum = AccumuloMetadataProcessor.ValueIn.valueOf(valueIn.substring(1, valueIn.length()-1));
                }

                ColumnInfo col = new ColumnInfo();
                col.es = element;
                col.in = valueInEnum;
                col.pos = i;

                ColumnSet cs = null;
                if (cf != null && cq != null) {
                    cs = new ColumnSet(new Text(cf), new Text(cq));
                }
                else {
                    if (cf == null) {
                        cf = AccumuloMetadataProcessor.ROWID;
                    }
                    cs = new ColumnSet(new Text(cf));
                }
                this.columnMap.put(cs, col);
            }
        }

        public List<?> buildTuple (ArrayList<KeyValuePair> values) {
            Object[] tuple = new Object[this.elementMap.size()];
            for (KeyValuePair kv:values) {
                ColumnInfo info = findColumnInfo(kv.key);
                if (info != null) {
                    Value v = kv.value;
                    if (ValueIn.CQ.equals(info.in)) {
                        tuple[info.pos] = convert(kv.key.getColumnQualifier().getBytes(), info.es);
                    }
                    else {
                        tuple[info.pos] = convert(v.get(), info.es);
                    }
                }
                info = this.columnMap.get(new ColumnSet(new Text(AccumuloMetadataProcessor.ROWID)));
                tuple[info.pos] = convert(kv.key.getRow().getBytes(), info.es);
            }
            return Arrays.asList(tuple);
        }

        private Object convert(byte[] content, ElementSymbol es) {
            return AccumuloDataTypeManager.deserialize(content, es.getType());
        }

        private ColumnInfo findColumnInfo(Key key) {
            // could not to do hash look up, as columns may be just based on CF or CF+CQ
            for(ColumnSet cs:columnMap.keySet()){
                if (cs.contains(key)) {
                    return this.columnMap.get(cs);
                }
            }
            return null;
        }

        public Map<ElementSymbol, Integer> getElementMap() {
            return this.elementMap;
        }
    }
}
