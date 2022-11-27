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

import java.io.IOException;
import java.sql.Array;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.function.FunctionDescriptor;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.ObjectTable;
import org.teiid.query.sql.lang.ObjectTable.ObjectColumn;
import org.teiid.query.util.CommandContext;

/**
 * Handles object table processing.
 */
public class ObjectTableNode extends SubqueryAwareRelationalNode {

    private static class ReflectiveArrayIterator implements Iterator<Object> {
        private int index;
        private Object array;
        private int length;

        ReflectiveArrayIterator(Object array) {
            this.array = array;
            length = java.lang.reflect.Array.getLength(this.array);
        }

        @Override
        public boolean hasNext() {
            return index < length;
        }

        @Override
        public Object next() {
            if (index >= length) {
                throw new NoSuchElementException();
            }
            return java.lang.reflect.Array.get(array, index++);
        }

    }

    private static final String TEIID_ROW_NUMBER = "teiid_row_number"; //$NON-NLS-1$
    private static final String TEIID_ROW = "teiid_row"; //$NON-NLS-1$
    private static final String TEIID_CONTEXT = "teiid_context"; //$NON-NLS-1$

    private ObjectTable table;
    private List<ObjectColumn> projectedColumns;

    //processing state
    private int rowCount = 0;
    private Object item;
    private Iterator<?> result;
    private SimpleScriptContext scriptContext;

    public ObjectTableNode(int nodeID) {
        super(nodeID);
    }

    @Override
    public void initialize(CommandContext context, BufferManager bufferManager,
            ProcessorDataManager dataMgr) {
        super.initialize(context, bufferManager, dataMgr);
        this.scriptContext = new SimpleScriptContext();
    }

    @Override
    public void open() throws TeiidComponentException, TeiidProcessingException {
        super.open();
        if (table.getScriptEngine() == null) {
            table.setScriptEngine(getContext().getMetadata().getScriptEngine(table.getScriptingLanguage()));
        }
        this.scriptContext.setAttribute(TEIID_CONTEXT, this.getContext(), ScriptContext.ENGINE_SCOPE);
    }

    @Override
    public synchronized void closeDirect() {
        if (this.scriptContext != null) {
            try {
                this.scriptContext.getErrorWriter().flush();
            } catch (IOException e) {
            }
            try {
                this.scriptContext.getWriter().flush();
            } catch (IOException e) {
            }
        }
        super.closeDirect();
        reset();
    }

    @Override
    public void reset() {
        super.reset();
        item = null;
        result = null;
        rowCount = 0;
        if (this.scriptContext != null) {
            this.scriptContext.getBindings(ScriptContext.ENGINE_SCOPE).clear();
        }
    }

    public void setTable(ObjectTable table) {
        this.table = table;
    }

    public void setProjectedColumns(List<ObjectColumn> projectedColumns) {
        this.projectedColumns = projectedColumns;
    }

    @Override
    public ObjectTableNode clone() {
        ObjectTableNode clone = new ObjectTableNode(getID());
        this.copyTo(clone);
        clone.setTable(table);
        clone.setProjectedColumns(projectedColumns);
        return clone;
    }

    @Override
    protected synchronized TupleBatch nextBatchDirect() throws BlockedException,
            TeiidComponentException, TeiidProcessingException {

        evaluate();

        while (!isBatchFull() && result.hasNext()) {
            if (item == null) {
                item = result.next();
                if (item == null) {
                    continue;
                }
                rowCount++;
            }
            addBatchRow(processRow());
        }
        if (!result.hasNext()) {
            terminateBatches();
        }
        return pullBatch();
    }

    private void evaluate() throws TeiidComponentException,
            ExpressionEvaluationException, BlockedException,
            TeiidProcessingException {
        if (result != null) {
            return;
        }
        setReferenceValues(this.table);
        Evaluator eval = getEvaluator(Collections.emptyMap());
        eval.evaluateParameters(this.table.getPassing(), null, scriptContext.getBindings(ScriptContext.ENGINE_SCOPE));

        Object value = evalScript(this.table.getCompiledScript(), this.table.getRowScript());
        if (value instanceof Iterable<?>) {
            result = ((Iterable<?>)value).iterator();
        } else if (value instanceof Iterator<?>) {
            result = (Iterator<?>)value;
        } else if (value != null && value.getClass().isArray()){
            result = new ReflectiveArrayIterator(value);
        } else if (value instanceof Array) {
            try {
                result = new ReflectiveArrayIterator(((Array)value).getArray());
            } catch (SQLException e) {
                throw new TeiidProcessingException(e);
            }
        } else {
            result = Arrays.asList(value).iterator();
        }
    }

    private Object evalScript(CompiledScript compiledScript, String script) throws TeiidProcessingException {
        try {
            if (compiledScript != null) {
                return compiledScript.eval(this.scriptContext);
            }
            return this.table.getScriptEngine().eval(script, this.scriptContext);
        } catch (ScriptException e) {
            throw new TeiidProcessingException(QueryPlugin.Event.TEIID31110, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31110, script, e.getMessage()));
        }
    }

    private List<?> processRow() throws ExpressionEvaluationException,
            TeiidComponentException, TeiidProcessingException {
        List<Object> tuple = new ArrayList<Object>(projectedColumns.size());
        this.scriptContext.setAttribute(TEIID_ROW, this.item, ScriptContext.ENGINE_SCOPE);
        this.scriptContext.setAttribute(TEIID_ROW_NUMBER, this.rowCount, ScriptContext.ENGINE_SCOPE);
        for (ObjectColumn proColumn : projectedColumns) {
            Object value = evalScript(proColumn.getCompiledScript(), proColumn.getPath());
            if (value == null) {
                if (proColumn.getDefaultExpression() != null) {
                    tuple.add(getEvaluator(Collections.emptyMap()).evaluate(proColumn.getDefaultExpression(), null));
                } else {
                    tuple.add(null);
                }
                continue;
            }
            value = FunctionDescriptor.importValue(value, proColumn.getSymbol().getType(), getContext());
            tuple.add(value);
        }
        item = null;
        return tuple;
    }

    @Override
    public Collection<? extends LanguageObject> getObjects() {
        return this.table.getPassing();
    }

    @Override
    public PlanNode getDescriptionProperties() {
        PlanNode props = super.getDescriptionProperties();
        AnalysisRecord.addLanaguageObjects(props, AnalysisRecord.PROP_TABLE_FUNCTION, Arrays.asList(this.table));
        return props;
    }

}