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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.LRUCache;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.query.QueryPlugin;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.function.FunctionDescriptor;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.optimizer.relational.rules.CapabilitiesUtil;
import org.teiid.query.optimizer.relational.rules.RuleAssignOutputElements;
import org.teiid.query.processor.BatchCollector;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.QueryProcessor;
import org.teiid.query.processor.RegisterRequestParameter;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.lang.SubqueryContainer;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ContextReference;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.ExpressionSymbol;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.ScalarSubquery;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.util.ValueIterator;
import org.teiid.query.sql.util.VariableContext;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.util.CommandContext;


/**
 * <p>This utility handles the work of processing a subquery; certain types
 * of processor nodes will use an instance of this class to do that work.
 */
public class SubqueryAwareEvaluator extends Evaluator {

    private static final class SimpleProcessorPlan extends ProcessorPlan {
        private final Query command;
        private final FunctionDescriptor fd;
        private ProcessorDataManager dataMgr;
        TupleSource ts;
        private List<? extends Expression> output;
        private String schema;

        private SimpleProcessorPlan(Query command, String schema, FunctionDescriptor fd, List<? extends Expression> output) {
            this.command = command;
            this.fd = fd;
            this.output = output;
            this.schema = schema;
        }

        @Override
        public void initialize(CommandContext context,
                ProcessorDataManager dataMgr, BufferManager bufferMgr) {
            super.initialize(context, dataMgr, bufferMgr);
            this.dataMgr = dataMgr;
        }

        @Override
        public void open() throws TeiidComponentException, TeiidProcessingException {
            RegisterRequestParameter parameterObject = new RegisterRequestParameter();
            ts = dataMgr.registerRequest(getContext(), command, schema, parameterObject);
        }

        @Override
        public TupleBatch nextBatch() throws BlockedException,
                TeiidComponentException, TeiidProcessingException {
            ArrayList<List<?>> result = new ArrayList<List<?>>(2);
            List<?> list = ts.nextTuple();
            if (list != null) {
                result.add(list);
                list = ts.nextTuple();
                if (list != null) {
                    result.add(list);
                }
            }
            ts.closeSource();
            TupleBatch tb = new TupleBatch(1, result);
            tb.setTerminationFlag(true);
            return tb;
        }

        @Override
        public List getOutputElements() {
            return output;
        }

        @Override
        public void close() throws TeiidComponentException {
            if (ts != null) {
                ts.closeSource();
            }
            ts = null;
        }

        @Override
        public void reset() {
            ts = null;
        }

        @Override
        public ProcessorPlan clone() {
            return new SimpleProcessorPlan(command, schema, fd, output);
        }
    }

    @SuppressWarnings("serial")
    private final class LRUBufferCache extends LRUCache<List<?>, TupleBuffer> {

        private LRUCache<List<?>, TupleBuffer> spillOver;

        private LRUBufferCache(int maxSize, LRUCache<List<?>, TupleBuffer> spillOver) {
            super(maxSize);
            this.spillOver = spillOver;
        }

        protected boolean removeEldestEntry(Map.Entry<java.util.List<?>,TupleBuffer> eldest) {
            if (super.removeEldestEntry(eldest)) {
                if (spillOver != null && eldest.getValue().getRowCount() <= 2) {
                    spillOver.put(eldest.getKey(), eldest.getValue());
                } else {
                    eldest.getValue().remove();
                }
                return true;
            }
            return false;
        }

        @Override
        public void clear() {
            if (!isEmpty()) {
                for (TupleBuffer buffer : values()) {
                    buffer.remove();
                }
                super.clear();
            }
        }
    }

    public static class SubqueryState {
        QueryProcessor processor;
        BatchCollector collector;
        ProcessorPlan plan;
        List<Object> refValues;
        boolean comparable = true;
        public boolean blocked;

        void close(boolean removeBuffer) {
            if (processor == null) {
                return;
            }
            processor.requestCanceled();
            processor.closeProcessing();
            //check that the collector has it's own buffer
            if (removeBuffer && collector.getTupleBuffer() != null) {
                collector.getTupleBuffer().remove();
            }
            processor = null;
        }
    }

    //environment
    private BufferManager manager;

    //processing state
    private Map<String, SubqueryState> subqueries = new HashMap<String, SubqueryState>();
    private Map<Command, String> commands = new HashMap<Command, String>(); //TODO: could determine this ahead of time
    private LRUCache<List<?>, TupleBuffer> smallCache = new LRUBufferCache(1024, null);
    private LRUCache<List<?>, TupleBuffer> cache = new LRUBufferCache(512, smallCache);
    private int maxTuples = BufferManager.DEFAULT_PROCESSOR_BATCH_SIZE << 4;
    private int currentTuples = 0;

    private Map<Function, ScalarSubquery> functionState;
    private Map<List<?>, QueryProcessor> procedureState;

    public SubqueryAwareEvaluator(Map elements, ProcessorDataManager dataMgr,
            CommandContext context, BufferManager manager) {
        super(elements, dataMgr, context);
        this.manager = manager;
        //default to 16 batches
        if (this.manager != null) {
            this.maxTuples = this.manager.getProcessorBatchSize() << 4;
        }
        //TODO the number of cache entries and the max tuples should be based upon the reference count and types involved as well.
    }

    public void reset() {
        for (SubqueryState subQueryState : subqueries.values()) {
            subQueryState.plan.reset();
            subQueryState.close(true);
        }
        cache.clear();
        smallCache.clear();
        currentTuples = 0;
        if (this.functionState != null) {
            this.functionState.clear();
        }
    }

    public void close() {
        reset();
        commands.clear();
        subqueries.clear();
    }

    @Override
    protected ValueIterator evaluateSubquery(SubqueryContainer<?> container,
            List<?> tuple) throws TeiidProcessingException, BlockedException,
            TeiidComponentException {
        ContextReference ref = (ContextReference)container;
        String key = ref.getContextSymbol();
        SubqueryState state = this.subqueries.get(key);
        if (state == null) {
            String otherKey = commands.get(container.getCommand());
            if (otherKey != null) {
                state = this.subqueries.get(otherKey);
                if (state != null) {
                    key = otherKey;
                }
            }
        }
        if (state == null) {
            state = new SubqueryState();
            state.plan = container.getCommand().getProcessorPlan().clone();
            if (container.getCommand().getCorrelatedReferences() != null) {
                for (ElementSymbol es : container.getCommand().getCorrelatedReferences().getKeys()) {
                    if (DataTypeManager.isNonComparable(DataTypeManager.getDataTypeName(es.getType()))) {
                        state.comparable = false;
                        break;
                    }
                }
            }
            this.subqueries.put(key, state);
            this.commands.put(container.getCommand(), key);
        }
        SymbolMap correlatedRefs = container.getCommand().getCorrelatedReferences();
        VariableContext currentContext = null;
        boolean shouldClose = false;
        boolean deterministic = true;
        if (state.processor != null && correlatedRefs != null) {
            Determinism determinism = state.processor.getContext().getDeterminismLevel();
            deterministic = Determinism.COMMAND_DETERMINISTIC.compareTo(determinism) <= 0;
        }
        boolean removeBuffer = true;
        if (correlatedRefs != null) {
            currentContext = new VariableContext();
            for (Map.Entry<ElementSymbol, Expression> entry : container.getCommand().getCorrelatedReferences().asMap().entrySet()) {
                currentContext.setValue(entry.getKey(), evaluate(entry.getValue(), tuple));
            }
            List<Object> refValues = currentContext.getLocalValues();
            if (!refValues.equals(state.refValues)) {
                if (state.comparable && deterministic) {
                    if (state.processor != null) {
                        //cache the old value
                        TupleBuffer tb = state.collector.collectTuples();
                        //recheck determinism as the plan may not have been fully processed by the initial check
                        Determinism determinism = state.processor.getContext().getDeterminismLevel();
                        deterministic = Determinism.COMMAND_DETERMINISTIC.compareTo(determinism) <= 0;
                        if (deterministic) {
                            //allowed to track up to 4x the maximum results size
                            maxTuples = Math.max((int)Math.min(Integer.MAX_VALUE, tb.getRowCount() << 2), maxTuples);
                            ArrayList<Object> cacheKey = new ArrayList<Object>(state.refValues);
                            cacheKey.add(key);
                            tb.saveBatch(); //ensure that we aren't leaving large last batches in memory
                            this.cache.put(cacheKey, tb);
                            removeBuffer = false;
                            this.currentTuples += tb.getRowCount();
                            while (this.currentTuples > maxTuples && !cache.isEmpty()) {
                                Iterator<Map.Entry<List<?>, TupleBuffer>> i = this.cache.entrySet().iterator();
                                Map.Entry<List<?>, TupleBuffer> entry = i.next();
                                TupleBuffer buffer = entry.getValue();
                                if (buffer.getRowCount() <= 2) {
                                    this.smallCache.put(entry.getKey(), buffer);
                                } else {
                                    buffer.remove();
                                }
                                this.currentTuples -= buffer.getRowCount();
                                i.remove();
                            }
                        }
                    }
                    //find if we have cached values
                    List<Object> cacheKey = new ArrayList<Object>(refValues);
                    cacheKey.add(key);
                    TupleBuffer cachedResult = cache.get(cacheKey);
                    if (cachedResult == null) {
                        cachedResult = smallCache.get(cacheKey);
                    }
                    if (cachedResult != null) {
                        state.close(false);
                        return new TupleSourceValueIterator(cachedResult.createIndexedTupleSource(), 0);
                    }
                }
                state.refValues = refValues;
                shouldClose = true;
            }
        }
        if (shouldClose || (!deterministic && !state.blocked)) {
            state.close(removeBuffer);
        }
        state.blocked = true;
        if (state.processor == null) {
            CommandContext subContext = context.clone();
            state.plan.reset();
            state.processor = new QueryProcessor(state.plan, subContext, manager, this.dataMgr);
            if (currentContext != null) {
                state.processor.getContext().pushVariableContext(currentContext);
            }
            state.collector = state.processor.createBatchCollector();
        }
        TupleSourceValueIterator iter = new TupleSourceValueIterator(state.collector.collectTuples().createIndexedTupleSource(), 0);
        state.blocked = false;
        return iter;
    }

    /**
     * Implements procedure function handling.
     * TODO: cache results
     */
    @Override
    protected Object evaluateProcedure(Function function, List<?> tuple,
            Object[] values) throws TeiidComponentException, TeiidProcessingException {
        QueryProcessor qp = null;
        List<?> key = Arrays.asList(function, Arrays.asList(values));
        if (procedureState != null) {
            qp = this.procedureState.get(key);
        }
        if (qp == null) {
            String args = Collections.nCopies(values.length, '?').toString().substring(1);
            args = args.substring(0, args.length() - 1);
            String fullName = function.getFunctionDescriptor().getFullName();
            String call = String.format("call %1$s(%2$s)", fullName, args); //$NON-NLS-1$
            qp = this.context.getQueryProcessorFactory().createQueryProcessor(call, fullName, this.context, values);
            if (this.procedureState == null) {
                this.procedureState = new HashMap<List<?>, QueryProcessor>();
            }
            this.procedureState.put(key, qp);
        }

        //just in case validate the rows being returned
        TupleBatch tb = qp.nextBatch();
        TupleBatch next = tb;
        while (!next.getTerminationFlag()) {
            if (next.getEndRow() >= 2) {
                break;
            }
            next = qp.nextBatch();
        }
        if (next.getEndRow() >= 2) {
            throw new ExpressionEvaluationException(QueryPlugin.Event.TEIID30345, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30345, function));
        }

        Object result = null;
        if (next.getRowCount() > 0) {
            result = next.getTuples().get(0).get(0);
        }
        this.procedureState.remove(key);
        qp.closeProcessing();
        return result;
    }

    /**
     * Implements must pushdown function handling if supported by the source.
     *
     * The basic strategy is to create a dummy subquery to represent the evaluation
     */
    @Override
    protected Object evaluatePushdown(Function function, List<?> tuple,
            Object[] values) throws TeiidComponentException, TeiidProcessingException {
        final FunctionDescriptor fd = function.getFunctionDescriptor();
        if (fd.getMethod() == null) {
            throw new FunctionExecutionException(QueryPlugin.Event.TEIID30341, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30341, fd.getFullName()));
        }
        String schema = null;
        if (fd.getMethod().getParent() == null || !fd.getMethod().getParent().isPhysical()) {
            //find a suitable target
            CapabilitiesFinder capabiltiesFinder = this.context.getQueryProcessorFactory().getCapabiltiesFinder();
            //TODO: save this across invocations
            schema = RuleAssignOutputElements.findFunctionTarget(function, fd, capabiltiesFinder, this.context.getMetadata());
            if (schema == null) {
                throw new FunctionExecutionException(QueryPlugin.Event.TEIID30341, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30341, fd.getFullName()));
            }
        } else {
            if (!CapabilitiesUtil.supports(Capability.SELECT_WITHOUT_FROM, fd.getMethod().getParent(), context.getMetadata(), context.getQueryProcessorFactory().getCapabiltiesFinder())) {
                if (elements != null) {
                    Integer index = (Integer) elements.get(function);
                    if (index != null) {
                        return tuple.get(index.intValue());
                    }
                }
                throw new FunctionExecutionException(QueryPlugin.Event.TEIID30341, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30341, fd.getFullName()));
            }
            schema = fd.getSchema();
        }

        ScalarSubquery ss = null;
        if (functionState != null) {
            ss = functionState.get(function);
        }
        Expression[] functionArgs = new Expression[values.length];
        for(int i=0; i < values.length; i++) {
            functionArgs[i] = new Constant(values[i]);
        }
        if (ss == null) {
            final Query command = new Query();
            Select select = new Select();
            command.setSelect(select);
            Function f = new Function(function.getName(), functionArgs);
            f.setType(function.getType());
            f.setFunctionDescriptor(fd);
            select.addSymbol(f);
            ss = new ScalarSubquery(command);
            SymbolMap correlatedReferences = new SymbolMap();
            Collection<ElementSymbol> elements = ElementCollectorVisitor.getElements(function, true);
            if (!elements.isEmpty()) {
                for (ElementSymbol es : elements) {
                    correlatedReferences.addMapping(es, es);
                }
                command.setCorrelatedReferences(correlatedReferences);
            }
            command.setProcessorPlan(new SimpleProcessorPlan(command, schema, fd, Arrays.asList(new Constant(null, fd.getReturnType()))));
        } else {
            ((Function)((ExpressionSymbol)ss.getCommand().getProjectedSymbols().get(0)).getExpression()).setArgs(functionArgs);
        }
        if (functionState == null) {
            this.functionState = new HashMap<Function, ScalarSubquery>(2);
        }
        functionState.put(function, ss);
        return internalEvaluate(ss, tuple);
    }

}
