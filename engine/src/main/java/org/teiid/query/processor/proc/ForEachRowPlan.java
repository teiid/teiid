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

package org.teiid.query.processor.proc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.teiid.GeneratedKeys;
import org.teiid.client.xa.XATransactionException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.dqp.service.TransactionContext;
import org.teiid.dqp.service.TransactionContext.Scope;
import org.teiid.dqp.service.TransactionService;
import org.teiid.language.SQLConstants;
import org.teiid.query.QueryPlugin;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.processor.BatchCollector;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.QueryProcessor;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.util.VariableContext;
import org.teiid.query.util.CommandContext;

public class ForEachRowPlan extends ProcessorPlan {

    private ProcessorPlan queryPlan;
    private ProcedurePlan rowProcedure;
    private Map<ElementSymbol, Expression> params;
    private Map<Expression, Integer> lookupMap;
    private boolean singleRow;
    private boolean insert;

    private ProcessorDataManager dataMgr;
    private BufferManager bufferMgr;

    private QueryProcessor queryProcessor;
    private TupleSource tupleSource;
    private QueryProcessor rowProcessor;
    private List<?> currentTuple;
    private long updateCount;

    private TransactionContext planContext;
    private List<?> nextTuple;
    private boolean first = true;
    private boolean nextNull = false;
    private BatchCollector batchCollector;

    @Override
    public ProcessorPlan clone() {
        ForEachRowPlan clone = new ForEachRowPlan();
        clone.setQueryPlan(queryPlan.clone());
        clone.setRowProcedure((ProcedurePlan) rowProcedure.clone());
        clone.setParams(params);
        clone.setLookupMap(lookupMap);
        return clone;
    }

    @Override
    public void close() throws TeiidComponentException {
        if (this.queryProcessor != null) {
            this.queryProcessor.closeProcessing();
            if (this.rowProcessor != null) {
                this.rowProcessor.closeProcessing();
            }
        }
        if (this.tupleSource != null) {
            this.tupleSource.closeSource();
        }
        if (this.planContext != null) {
            TransactionService ts = this.getContext().getTransactionServer();
            try {
                ts.resume(planContext);
                ts.rollback(planContext);
                this.planContext = null;
            } catch (XATransactionException e) {
                throw new TeiidComponentException(QueryPlugin.Event.TEIID30165, e);
            }
        }
    }

    @Override
    public List<Expression> getOutputElements() {
        return Command.getUpdateCommandSymbol();
    }

    @Override
    public void initialize(CommandContext context,
            ProcessorDataManager dataMgr, BufferManager bufferMgr) {
        setContext(context);
        this.dataMgr = dataMgr;
        this.bufferMgr = bufferMgr;
        for (ElementSymbol elem : this.params.keySet()) {
            if (elem.getGroupSymbol().getName().equalsIgnoreCase(SQLConstants.NonReserved.KEY)) {
                this.insert = true;
                break;
            }
        }
    }

    @Override
    public TupleBatch nextBatch() throws BlockedException,
            TeiidComponentException, TeiidProcessingException {
        if (planContext != null) {
            this.getContext().getTransactionServer().resume(planContext);
        }
        try {
            while (true) {
                if (currentTuple == null) {
                    if (nextTuple != null) {
                        currentTuple = nextTuple;
                    } else if (!nextNull) {
                        if (tupleSource == null) {
                            //buffer the changeset from the processor plan
                            tupleSource = batchCollector.collectTuples().createIndexedTupleSource(true);
                        }
                        currentTuple = tupleSource.nextTuple();
                    }
                    if (currentTuple == null) {
                        this.tupleSource.closeSource();
                        if (this.planContext != null) {
                            TransactionService ts = this.getContext().getTransactionServer();
                            ts.commit(this.planContext);
                            this.planContext = null;
                        }
                        TupleBatch result = new TupleBatch(1, new List[] {Arrays.asList((int)Math.min(Integer.MAX_VALUE, updateCount))});
                        result.setTerminationFlag(true);
                        return result;
                    }
                }
                if (first) {
                    TransactionContext tc = this.getContext().getTransactionContext();
                    if (this.planContext == null && tc != null && tc.getTransactionType() == Scope.NONE) {
                        Boolean txnRequired = rowProcedure.requiresTransaction(false);
                        boolean start = false;
                        if (txnRequired == null) {
                            nextTuple = tupleSource.nextTuple();
                            if (nextTuple != null) {
                                start = true;
                            } else {
                                nextNull = true;
                            }
                        } else if (Boolean.TRUE.equals(txnRequired)) {
                            start = true;
                        }
                        if (start) {
                            this.getContext().getTransactionServer().begin(tc);
                            this.planContext = tc;
                        }
                    }
                    first = false;
                }
                if (this.rowProcessor == null) {
                    rowProcedure.reset();
                    CommandContext context = getContext().clone();
                    this.rowProcessor = new QueryProcessor(rowProcedure, context, this.bufferMgr, this.dataMgr);
                    Evaluator eval = new Evaluator(lookupMap, dataMgr, context);
                    for (Map.Entry<ElementSymbol, Expression> entry : this.params.entrySet()) {
                        Integer index = this.lookupMap.get(entry.getValue());
                        if (index != null) {
                            rowProcedure.getCurrentVariableContext().setValue(entry.getKey(), this.currentTuple.get(index));
                        } else {
                            rowProcedure.getCurrentVariableContext().setValue(entry.getKey(), eval.evaluate(entry.getValue(), this.currentTuple));
                        }
                    }
                }
                //save the variable context to get the key information
                VariableContext vc = rowProcedure.getCurrentVariableContext();
                //just getting the next batch is enough
                this.rowProcessor.nextBatch();
                if (insert) {
                    assignGeneratedKey(vc);
                }
                this.rowProcessor.closeProcessing();
                this.rowProcessor = null;
                this.currentTuple = null;
                this.updateCount++;
            }
        } finally {
            if (planContext != null) {
                this.getContext().getTransactionServer().suspend(planContext);
            }
        }
    }

    private void assignGeneratedKey(VariableContext vc) {
        LinkedHashMap<ElementSymbol, Object> key = null;
        for (Map.Entry<Object, Object> entry : vc.getVariableMap().entrySet()) {
            if (!(entry.getKey() instanceof ElementSymbol)) {
                continue;
            }
            ElementSymbol es = (ElementSymbol)entry.getKey();
            if (!es.getGroupSymbol().getName().equalsIgnoreCase(SQLConstants.NonReserved.KEY)) {
                continue;
            }
            Object value = entry.getValue();
            if (value == null) {
                return;
            }
            if (key == null) {
                key = new LinkedHashMap<ElementSymbol, Object>();
            }
            key.put(es, value);
        }
        if (key != null) {
            String[] names = new String[key.size()];
            Class<?>[] types = new Class<?>[key.size()];
            List<Object> values = new ArrayList<Object>(key.size());
            for (Map.Entry<ElementSymbol, Object> entry : key.entrySet()) {
                names[values.size()] = entry.getKey().getShortName();
                types[values.size()] = entry.getKey().getType();
                values.add(entry.getValue());
            }
            GeneratedKeys generatedKeys = getContext().returnGeneratedKeys(names, types);
            generatedKeys.addKey(values);
        }
    }

    @Override
    public void open() throws TeiidComponentException, TeiidProcessingException {
        TransactionContext tc = this.getContext().getTransactionContext();
        if (tc != null && tc.getTransactionType() == Scope.NONE && queryPlan != null
                && !Boolean.FALSE.equals(queryPlan.requiresTransaction(false))) {
            //start a transaction - if not each of the row plans will
            //be executed in it's own transaction, which is bad for performance

            //TODO: should probably allow non-atomic row plans
            //the parser accepts a trigger block without atomic
            //but the spec mandates it - and we treat it as atomic
            //either way

            //TODO: for non-transactional environments this will
            //trigger an error
            this.getContext().getTransactionServer().begin(tc);
            this.planContext = tc;
        }
        if (queryPlan != null) {
            queryProcessor = new QueryProcessor(queryPlan, getContext().clone(), this.bufferMgr, this.dataMgr);
            batchCollector = queryProcessor.createBatchCollector();
        }
    }

    public void setQueryPlan(ProcessorPlan queryPlan) {
        this.queryPlan = queryPlan;
    }

    public void setRowProcedure(ProcedurePlan rowProcedure) {
        this.rowProcedure = rowProcedure;
    }

    public void setParams(Map<ElementSymbol, Expression> params) {
        this.params = params;
    }

    public void setLookupMap(Map<Expression, Integer> symbolMap) {
        this.lookupMap = symbolMap;
    }

    @Override
    public void reset() {
        super.reset();
        this.queryPlan.reset();
        this.updateCount = 0;
        this.currentTuple = null;
        this.rowProcessor = null;
        this.queryProcessor = null;
        this.tupleSource = null;
        this.batchCollector = null;
        this.planContext = null;
        this.first = true;
        this.nextTuple = null;
        this.nextNull = false;
    }

    @Override
    public Boolean requiresTransaction(boolean transactionalReads) {
        Boolean requiresTxn = queryPlan.requiresTransaction(transactionalReads);
        if (!Boolean.FALSE.equals(requiresTxn)) {
            return true;
        }
        Boolean forEach = rowProcedure.requiresTransaction(transactionalReads);
        if (Boolean.TRUE.equals(forEach)) {
            return true;
        }
        if (forEach == null) {
            if (!singleRow) {
                return true;
            }
            return null;
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder val = new StringBuilder("ForEach "); //$NON-NLS-1$
        val.append(this.queryPlan).append("\n{\n"); //$NON-NLS-1$
        val.append(this.rowProcedure);
        val.append("}\n"); //$NON-NLS-1$
        return val.toString();
    }

    public void setTupleSource(TupleSource tupleSource) {
        this.tupleSource = tupleSource;
    }

    public void setSingleRow(boolean singleRow) {
        this.singleRow = singleRow;
    }

}
