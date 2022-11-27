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

import static org.teiid.query.analysis.AnalysisRecord.*;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import javax.transaction.SystemException;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.client.ProcedureErrorInstructionException;
import org.teiid.client.plan.PlanNode;
import org.teiid.client.xa.XATransactionException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.BufferManager.TupleSourceType;
import org.teiid.common.buffer.IndexedTupleSource;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.ArrayImpl;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.Assertion;
import org.teiid.dqp.internal.process.DataTierTupleSource;
import org.teiid.dqp.service.TransactionContext;
import org.teiid.dqp.service.TransactionContext.Scope;
import org.teiid.dqp.service.TransactionService;
import org.teiid.events.EventDistributor;
import org.teiid.jdbc.TeiidSQLException;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Procedure;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.SupportConstants;
import org.teiid.query.processor.BatchIterator;
import org.teiid.query.processor.CollectionTupleSource;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.QueryProcessor;
import org.teiid.query.processor.RegisterRequestParameter;
import org.teiid.query.processor.proc.CreateCursorResultSetInstruction.Mode;
import org.teiid.query.processor.relational.SubqueryAwareEvaluator;
import org.teiid.query.processor.relational.SubqueryAwareRelationalNode;
import org.teiid.query.resolver.command.UpdateProcedureResolver;
import org.teiid.query.sql.ProcedureReservedWords;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.util.VariableContext;
import org.teiid.query.sql.visitor.ValueIteratorProviderCollectorVisitor;
import org.teiid.query.tempdata.TempTableStore;
import org.teiid.query.util.CommandContext;

/**
 */
public class ProcedurePlan extends ProcessorPlan implements ProcessorDataManager {

    private static class CursorState {
        QueryProcessor processor;
        IndexedTupleSource ts;
        List<?> currentRow;
        TupleBuffer resultsBuffer;
        public boolean returning;
        public boolean usesLocalTemp;
    }

    static final ElementSymbol ROWCOUNT =
        new ElementSymbol(ProcedureReservedWords.VARIABLES+"."+ProcedureReservedWords.ROWCOUNT); //$NON-NLS-1$

    static {
        ROWCOUNT.setType(DataTypeManager.DefaultDataClasses.INTEGER);
    }

    private Program originalProgram;

    // State initialized by processor
    private ProcessorDataManager parentDataMrg;
    private BufferManager bufferMgr;
    private int batchSize;

    private boolean done = false;
    private CursorState currentState;

    // Temp state for final results
    private TupleSource finalTupleSource;
    private int beginBatch = 1;
    private List<List<?>> batchRows;
    private boolean lastBatch = false;
    private LinkedHashMap<ElementSymbol, Expression> params;
    private boolean runInContext = true;
    private List<ElementSymbol> outParams;
    private QueryMetadataInterface metadata;

    private VariableContext cursorStates;
    private VariableContext currentVarContext;
    private VariableContext parentContext;

    private List outputElements;

    private SubqueryAwareEvaluator evaluator;

    // Stack of programs, with current program on top
    private Stack<Program> programs = new Stack<Program>();

    private boolean evaluatedParams;

    private int updateCount = Procedure.AUTO_UPDATECOUNT;

    private TransactionContext blockContext;
    /**
     * Resources cannot be held open across the txn boundary.  This list is a hack at ensuring the resources are closed.
     */
    private LinkedList<WeakReference<DataTierTupleSource>> txnTupleSources = new LinkedList<WeakReference<DataTierTupleSource>>();

    private boolean validateAccess;

    /**
     * Constructor for ProcedurePlan.
     */
    public ProcedurePlan(Program originalProgram) {
        this.originalProgram = originalProgram;
        createVariableContext();
    }

    public Program getOriginalProgram() {
        return originalProgram;
    }

    @Override
    public void initialize(CommandContext context, ProcessorDataManager dataMgr, BufferManager buffer) {
        this.bufferMgr = buffer;
        this.batchSize = bufferMgr.getProcessorBatchSize(getOutputElements());
        this.parentContext = context.getVariableContext();
        setContext(context.clone());
        this.parentDataMrg = dataMgr;
        if (evaluator == null) {
            this.evaluator = new SubqueryAwareEvaluator(Collections.emptyMap(), getDataManager(), getContext(), this.bufferMgr);
        }
    }

    public void reset() {
        super.reset();
        if (evaluator != null) {
            evaluator.reset();
        }
        evaluatedParams = false;
        if (parentContext != null) {
            super.getContext().setVariableContext(parentContext);
        }
        createVariableContext();
        CommandContext cc = super.getContext();
        if (cc != null) {
            //create fresh local state
            setContext(cc.clone());
        }
        done = false;
        currentState = null;
        finalTupleSource = null;
        beginBatch = 1;
        batchRows = null;
        lastBatch = false;
        //reset program stack
        programs.clear();
        LogManager.logTrace(org.teiid.logging.LogConstants.CTX_DQP, "ProcedurePlan reset"); //$NON-NLS-1$
    }

    public ProcessorDataManager getDataManager() {
        return this;
    }

    public void open() throws TeiidProcessingException, TeiidComponentException {
        if (!this.evaluatedParams) {
            if (this.outParams != null) {
                for (ElementSymbol param : this.outParams) {
                    setParameterValue(param, getCurrentVariableContext(), null);
                }
            }
            if (this.params != null) {
                for (Map.Entry<ElementSymbol, Expression> entry : this.params.entrySet()) {
                    ElementSymbol param = entry.getKey();
                    Expression expr = entry.getValue();

                    VariableContext context = getCurrentVariableContext();
                    if (context.getVariableMap().containsKey(param)) {
                        continue;
                    }
                    Object value = this.evaluateExpression(expr);

                    //check constraint
                    checkNotNull(param, value, metadata);
                    setParameterValue(param, context, value);
                }
                this.evaluator.close();
            } else if (runInContext) {
                //if there are no params, this needs to run in the current variable context
                this.currentVarContext.setParentContext(parentContext);
            }
            this.push(originalProgram);
        }
        this.evaluatedParams = true;
    }

    public static void checkNotNull(ElementSymbol param, Object value, QueryMetadataInterface metadata)
            throws TeiidComponentException, QueryMetadataException,
            QueryValidatorException {
        if (metadata.elementSupports(param.getMetadataID(), SupportConstants.Element.NULL)) {
            return;
        }
        if (metadata.isVariadic(param.getMetadataID())) {
            if (value instanceof ArrayImpl) {
                ArrayImpl av = (ArrayImpl)value;
                for (Object o : av.getValues()) {
                    if (o == null) {
                         throw new QueryValidatorException(QueryPlugin.Event.TEIID30164, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30164, param));
                    }
                }
            }
        } else if (value == null) {
            throw new QueryValidatorException(QueryPlugin.Event.TEIID30164, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30164, param));
        }
    }

    protected void setParameterValue(ElementSymbol param,
            VariableContext context, Object value) {
        context.setValue(param, value);
    }

    @Override
    public TupleBatch nextBatch() throws BlockedException,
            TeiidComponentException, TeiidProcessingException {
        if (blockContext != null) {
            this.getContext().getTransactionServer().resume(blockContext);
        }
        try {
            return nextBatchDirect();
        } finally {
            //ensure that the generatedkeys don't escape
            getContext().clearGeneratedKeys();
            if (blockContext != null) {
                this.getContext().getTransactionServer().suspend(blockContext);
            }
        }
    }

    /**
     * @see ProcessorPlan#nextBatch()
     */
    public TupleBatch nextBatchDirect()
        throws TeiidComponentException, TeiidProcessingException, BlockedException {

        // Already returned results?
        if(done) {
            // Already returned all results
            TupleBatch emptyTerminationBatch = new TupleBatch(beginBatch, new List[0]);
            emptyTerminationBatch.setTerminationFlag(true);
            return emptyTerminationBatch;

        }
        // First attempt to process
        if(this.finalTupleSource == null) {
            // Still need to process - this should either
            // throw a BlockedException or return a finalTupleSource
            this.finalTupleSource = processProcedure();
        }

        // Next, attempt to return batches if processing completed
        while(! isBatchFull()) {
            // May throw BlockedException and exit here
            List<?> tuple = this.finalTupleSource.nextTuple();
            if(tuple == null) {
                if (outParams != null) {
                    VariableContext vc = getCurrentVariableContext();
                    List<Object> paramTuple = Arrays.asList(new Object[this.getOutputElements().size()]);
                    int i = this.getOutputElements().size() - this.outParams.size();
                    for (ElementSymbol param : outParams) {
                        Object value = vc.getValue(param);
                        checkNotNull(param, value, metadata);
                        paramTuple.set(i++, value);
                    }
                    addBatchRow(paramTuple, true);
                }
                terminateBatches();
                done = true;
                break;
            }
            addBatchRow(tuple, false);
        }

        return pullBatch();
    }

    /**
     * <p>Process the procedure, using the stack of Programs supplied by the
     * ProcessorEnvironment.  With each pass through the loop, the
     * current Program is gotten off the top of the stack, and the
     * current instruction is gotten from that program; each call
     * to an instruction's process method may alter the Program
     * Stack and/or the current instruction pointer of a Program,
     * so it's important that this method's loop refer to the
     * call stack of the ProcessorEnvironment each time, and not
     * cache things in local variables.  If the current Program's
     * current instruction is null, then it's time to pop that
     * Program off the stack.
     *
     * @return List a single tuple containing one Integer: the update
     * count resulting from the procedure execution.
     */
    private TupleSource processProcedure()
        throws TeiidComponentException, TeiidProcessingException, BlockedException {

        // execute plan
        ProgramInstruction inst = null;

        while (!this.programs.empty()){
            Program program = peek();
            inst = program.getCurrentInstruction();
            if (inst == null){
                LogManager.logTrace(org.teiid.logging.LogConstants.CTX_DQP, "Finished program", program); //$NON-NLS-1$
                //look ahead to see if we need to process in place
                VariableContext vc = this.cursorStates.getParentContext();
                CursorState last = (CursorState) this.cursorStates.getValue(null);
                if(last != null){
                    if (last.resultsBuffer == null) {
                        if (last.usesLocalTemp || !txnTupleSources.isEmpty()) {
                            last.returning = true;
                        } else {
                            //check to see if we are returning from a loop - the processorplan can be reused, so we need to save this result
                            //
                            //the other strategy here would be to clone the plan on invocation and use different tracking rather
                            //than the plan itself
                            for (int i = 0; i < this.programs.size() - 1; i++) {
                                if (this.programs.get(i).getCurrentInstruction() instanceof RepeatedInstruction) {
                                    last.returning = true;
                                    break;
                                }
                            }
                        }
                        if (last.returning) {
                            last.resultsBuffer = bufferMgr.createTupleBuffer(last.processor.getOutputElements(), getContext().getConnectionId(), TupleSourceType.PROCESSOR);
                        }
                    }
                    if (last.returning) {
                        while (last.ts.hasNext()) {
                            List<?> tuple = last.ts.nextTuple();
                            last.resultsBuffer.addTuple(tuple);
                        }
                        last.resultsBuffer.close();
                        last.ts = last.resultsBuffer.createIndexedTupleSource(true);
                        last.returning = false;
                    }
                }
                this.pop(true);
                continue;
            }
            try {
                getContext().setCurrentTimestamp(System.currentTimeMillis());
                if (inst instanceof RepeatedInstruction) {
                    LogManager.logTrace(org.teiid.logging.LogConstants.CTX_DQP, "Executing repeated instruction", inst); //$NON-NLS-1$
                    RepeatedInstruction loop = (RepeatedInstruction)inst;
                    if (loop.testCondition(this)) {
                        LogManager.logTrace(org.teiid.logging.LogConstants.CTX_DQP, "Passed condition, executing program " + loop.getNestedProgram()); //$NON-NLS-1$
                        inst.process(this);
                        this.push(loop.getNestedProgram());
                        continue;
                    }
                    LogManager.logTrace(org.teiid.logging.LogConstants.CTX_DQP, "Exiting repeated instruction", inst); //$NON-NLS-1$
                    loop.postInstruction(this);
                } else {
                    LogManager.logTrace(org.teiid.logging.LogConstants.CTX_DQP, "Executing instruction", inst); //$NON-NLS-1$
                    inst.process(this);
                    this.evaluator.close();
                }
            } catch (TeiidComponentException e) {
                throw e;
            } catch (Exception e) {
                //processing or teiidsqlexception
                boolean atomic = program.isAtomic();
                while (program.getExceptionGroup() == null) {
                    this.pop(false);
                    if (this.programs.empty()) {
                        //reached the top without a handler, so throw
                        if (e instanceof TeiidProcessingException) {
                            throw (TeiidProcessingException)e;
                        }
                        throw new ProcedureErrorInstructionException(QueryPlugin.Event.TEIID30167, e);
                    }
                    program = peek();
                    atomic |= program.isAtomic();
                }
                try {
                    this.pop(false); //allow the current program to go out of scope
                    if (atomic) {
                        TransactionContext tc = this.getContext().getTransactionContext();
                        if (tc != null && tc.getTransactionType() != Scope.NONE) {
                            //a non-completing atomic block under a higher level transaction

                            //this will not work correctly until we support
                            //checkpoints/subtransactions
                            tc.getTransaction().setRollbackOnly();
                            getContext().addWarning(TeiidSQLException.create(e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31266)));
                        }
                    }
                } catch (IllegalStateException | SystemException| TeiidComponentException e1) {
                    LogManager.logDetail(LogConstants.CTX_DQP, "Caught exception while rolling back transaction", e1); //$NON-NLS-1$
                } catch (Throwable e1) {
                    LogManager.logWarning(LogConstants.CTX_DQP, e1);
                }
                if (e instanceof RuntimeException) {
                    LogManager.logWarning(LogConstants.CTX_DQP, e);
                } else {
                    LogManager.logDetail(LogConstants.CTX_DQP, "Caught exception in exception hanlding block", e); //$NON-NLS-1$
                }
                if (program.getExceptionProgram() == null) {
                    continue;
                }
                Program exceptionProgram = program.getExceptionProgram();
                this.push(exceptionProgram);
                TeiidSQLException tse = TeiidSQLException.create(e);
                GroupSymbol gs = new GroupSymbol(program.getExceptionGroup());
                this.currentVarContext.setValue(exceptionSymbol(gs, 0), tse.getSQLState());
                this.currentVarContext.setValue(exceptionSymbol(gs, 1), tse.getErrorCode());
                this.currentVarContext.setValue(exceptionSymbol(gs, 2), tse.getTeiidCode());
                this.currentVarContext.setValue(exceptionSymbol(gs, 3), tse);
                this.currentVarContext.setValue(exceptionSymbol(gs, 4), tse.getCause());
                continue;
            }
            program.incrementProgramCounter();
        }
        CursorState last = (CursorState) this.cursorStates.getValue(null);
        if(last == null){
            return CollectionTupleSource.createNullTupleSource();
        }
        return last.ts;
    }

    private ElementSymbol exceptionSymbol(GroupSymbol gs, int pos) {
        ElementSymbol es = UpdateProcedureResolver.exceptionGroup.get(pos).clone();
        es.setGroupSymbol(gs);
        return es;
    }

    public void close()
        throws TeiidComponentException {
        while (!programs.isEmpty()) {
            try {
                pop(false);
            } catch (TeiidComponentException e) {
                LogManager.logDetail(LogConstants.CTX_DQP, e, "Error closing program"); //$NON-NLS-1$
            }
        }
        if (this.evaluator != null) {
            this.evaluator.close();
        }
        if (this.cursorStates != null) {
            removeAllCursors(this.cursorStates);
            this.cursorStates = null;
        }
        this.txnTupleSources.clear();
        this.blockContext = null;
        this.currentVarContext = null;
    }

    public String toString() {
        return "ProcedurePlan:\n" + this.originalProgram; //$NON-NLS-1$
    }

    public ProcessorPlan clone(){
        ProcedurePlan plan = new ProcedurePlan(originalProgram.clone());
        plan.setOutputElements(this.getOutputElements());
        plan.setParams(params);
        plan.setOutParams(outParams);
        plan.setMetadata(metadata);
        plan.updateCount = updateCount;
        plan.runInContext = runInContext;
        return plan;
    }

    private void addBatchRow(List<?> row, boolean last) {
        if(this.batchRows == null) {
            this.batchRows = new ArrayList<List<?>>(this.batchSize/4);
        }
        if (!last && this.outParams != null) {
            List<Object> newRow = Arrays.asList(new Object[row.size() + this.outParams.size()]);
            for (int i = 0; i < row.size(); i++) {
                newRow.set(i, row.get(i));
            }
            row = newRow;
        }
        this.batchRows.add(row);
    }

    protected void terminateBatches() {
        this.lastBatch = true;
    }

    protected boolean isBatchFull() {
        return (this.batchRows != null) && (this.batchRows.size() == this.batchSize);
    }

    protected TupleBatch pullBatch() {
        TupleBatch batch = null;
        if(this.batchRows != null) {
            batch = new TupleBatch(this.beginBatch, this.batchRows);
            beginBatch += this.batchRows.size();
        } else {
            batch = new TupleBatch(this.beginBatch, Collections.EMPTY_LIST);
        }

        batch.setTerminationFlag(this.lastBatch);

        // Reset batch state
        this.batchRows = null;
        this.lastBatch = false;

        // Return batch
        return batch;
    }

    public PlanNode getDescriptionProperties() {
        PlanNode node = this.originalProgram.getDescriptionProperties();
        node.addProperty(PROP_OUTPUT_COLS, AnalysisRecord.getOutputColumnProperties(getOutputElements()));
        return node;
    }

    public void setMetadata( QueryMetadataInterface metadata ) {
        this.metadata = metadata;
    }

    public void setOutParams(List<ElementSymbol> outParams) {
        this.outParams = outParams;
    }

    public void setParams( LinkedHashMap<ElementSymbol, Expression> params ) {
        this.params = params;
    }

    private void createVariableContext() {
        this.currentVarContext = new VariableContext(runInContext && this.params == null);
        this.currentVarContext.setValue(ROWCOUNT, 0);
        this.cursorStates = new VariableContext(false);
        this.cursorStates.setValue(null, null);
    }

    /**
     * <p> Get the current <code>VariavleContext</code> on this environment.
     * The VariableContext is updated with variables and their values by
     * {@link ProgramInstruction}s that are part of the ProcedurePlan that use
     * this environment.
     * @return The current <code>VariariableContext</code>.
     */
    public VariableContext getCurrentVariableContext() {
        return this.currentVarContext;
    }

    /**
     *
     * @param command
     * @param rsName
     * @param procAssignments
     * @param mode
     * @param usesLocalTemp - only matters in HOLD mode
     * @throws TeiidComponentException
     * @throws TeiidProcessingException
     */
    public void executePlan(ProcessorPlan command, String rsName, Map<ElementSymbol, ElementSymbol> procAssignments, CreateCursorResultSetInstruction.Mode mode, boolean usesLocalTemp)
        throws TeiidComponentException, TeiidProcessingException {

        CursorState state = (CursorState) this.cursorStates.getValue(rsName);
        if (state == null || rsName == null) {
            if (this.currentState != null && this.currentState.processor.getProcessorPlan() != command) {
                //sanity check for non-deterministic paths
                removeState(this.currentState);
                this.currentState = null;
            }
            if (this.currentState == null) {
                //this may not be the first time the plan is being run
                command.reset();

                CommandContext subContext = getContext().clone();
                subContext.setVariableContext(this.currentVarContext);
                state = new CursorState();
                state.usesLocalTemp = usesLocalTemp;
                state.processor = new QueryProcessor(command, subContext, this.bufferMgr, this);
                state.ts = new BatchIterator(state.processor);
                if (mode == Mode.HOLD && procAssignments != null && state.processor.getOutputElements().size() - procAssignments.size() > 0) {
                    state.resultsBuffer = bufferMgr.createTupleBuffer(state.processor.getOutputElements().subList(0, state.processor.getOutputElements().size() - procAssignments.size()), getContext().getConnectionId(), TupleSourceType.PROCESSOR);
                } else if ((this.blockContext != null || this.programs.peek().isTrappingExceptions()) && (mode == Mode.HOLD || rsName != null)) {
                    state.resultsBuffer = bufferMgr.createTupleBuffer(state.processor.getOutputElements(), getContext().getConnectionId(), TupleSourceType.PROCESSOR);
                }
                this.currentState = state;
            }
            //force execution to the first batch to ensure that the plan is executed in the context of the procedure
            this.currentState.ts.hasNext();
            if (procAssignments != null) {
                //proc assignments force us to scroll through the entire results and save as we go
                while (this.currentState.ts.hasNext()) {
                    if (this.currentState.currentRow != null && this.currentState.resultsBuffer != null) {
                        this.currentState.resultsBuffer.addTuple(this.currentState.currentRow.subList(0, this.currentState.resultsBuffer.getSchema().size()));
                        this.currentState.currentRow = null;
                    }
                    this.currentState.currentRow = this.currentState.ts.nextTuple();
                }
                //process assignments
                Assertion.assertTrue(this.currentState.currentRow != null);
                for (Map.Entry<ElementSymbol, ElementSymbol> entry : procAssignments.entrySet()) {
                    if (entry.getValue() == null || !metadata.elementSupports(entry.getValue().getMetadataID(), SupportConstants.Element.UPDATE)) {
                        continue;
                    }
                    int index = this.currentState.processor.getOutputElements().indexOf(entry.getKey());
                    getCurrentVariableContext().setValue(entry.getValue(), DataTypeManager.transformValue(this.currentState.currentRow.get(index), entry.getValue().getType()));
                }
            } else if (this.currentState.resultsBuffer != null) {
                //result should be saved, typically to respect txn semantics
                while (this.currentState.ts.hasNext()) {
                    List<?> tuple = this.currentState.ts.nextTuple();
                    this.currentState.resultsBuffer.addTuple(tuple);
                }
                getCurrentVariableContext().setValue(ProcedurePlan.ROWCOUNT, 0);
            } else if (mode == Mode.UPDATE) {
                List<?> t = this.currentState.ts.nextTuple();
                if (this.currentState.ts.hasNext()) {
                    throw new AssertionError("Invalid update count result - more than 1 row returned"); //$NON-NLS-1$
                }
                removeState(this.currentState);
                this.currentState = null;
                int rowCount = 0;
                if (t != null) {
                    rowCount = (Integer)t.get(0);
                }
                getCurrentVariableContext().setValue(ProcedurePlan.ROWCOUNT, rowCount);
                return;
            }
            if (rsName == null && mode == Mode.NOHOLD) {
                //unnamed without hold
                //process fully, but don't save
                //TODO: could set the rowcount in this case
                while (this.currentState.ts.hasNext()) {
                    this.currentState.ts.nextTuple();
                }
                this.currentState = null;
                getCurrentVariableContext().setValue(ProcedurePlan.ROWCOUNT, 0);
                return;
            }
            if (this.currentState.resultsBuffer != null) {
                //close the results buffer and use a buffer backed tuplesource
                this.currentState.resultsBuffer.close();
                this.currentState.ts = this.currentState.resultsBuffer.createIndexedTupleSource(true);
            }
            CursorState old = (CursorState) this.cursorStates.setValue(rsName, this.currentState);
            if (old != null) {
                removeState(old);
            }
            this.currentState = null;
        }
    }

    /**
     * @param success
     * @throws TeiidComponentException
     */
    public void pop(boolean success) throws TeiidComponentException {
        this.evaluator.close();
        Program program = this.programs.pop();
        VariableContext vc = this.currentVarContext;
        VariableContext cs = this.cursorStates;
        try {
            this.currentVarContext = this.currentVarContext.getParentContext();
            this.cursorStates = this.cursorStates.getParentContext();
            TempTableStore tempTableStore = program.getTempTableStore();
            this.getContext().setTempTableStore(tempTableStore.getParentTempTableStore());
            tempTableStore.removeTempTables();
            if (program.startedTxn()) {
                TransactionService ts = this.getContext().getTransactionServer();
                TransactionContext tc = this.blockContext;
                this.blockContext = null;
                try {
                    ts.resume(tc);
                    for (WeakReference<DataTierTupleSource> ref : txnTupleSources) {
                        DataTierTupleSource dtts = ref.get();
                        if (dtts != null) {
                            dtts.fullyCloseSource();
                        }
                    }
                    this.txnTupleSources.clear();
                    if (success) {
                        ts.commit(tc);
                    } else {
                        ts.rollback(tc);
                    }
                } catch (XATransactionException e) {
                     throw new TeiidComponentException(QueryPlugin.Event.TEIID30165, e);
                }
            }
        } finally {
            removeAllCursors(cs);
        }
    }

    private void removeAllCursors(VariableContext cs) {
        if (this.currentState != null) {
            //in error situations the current state will still need cleared
            removeState(currentState);
            this.currentState = null;
        }
        for (Map.Entry<Object, Object> entry : cs.getVariableMap().entrySet()) {
            removeState((CursorState) entry.getValue());
        }
    }

    public void push(Program program) throws XATransactionException {
        this.evaluator.close();
        program.reset(this.getContext().getConnectionId());
        program.setTrappingExceptions(program.getExceptionGroup() != null || (!this.programs.isEmpty() && this.programs.peek().isTrappingExceptions()));
        TempTableStore tts = getTempTableStore();
        getContext().setTempTableStore(program.getTempTableStore());
        program.getTempTableStore().setParentTempTableStore(tts);
        this.programs.push(program);
        VariableContext context = new VariableContext(true);
        context.setParentContext(this.currentVarContext);
        this.currentVarContext = context;
        VariableContext cc = new VariableContext(true);
        cc.setParentContext(this.cursorStates);
        this.cursorStates = cc;

        if (program.isAtomic() && this.blockContext == null) {
            TransactionContext tc = this.getContext().getTransactionContext();
            if (tc != null && tc.getTransactionType() == Scope.NONE) {
                //need to toggle the atomic block flag prior to asking if txn required
                CommandContext tlc = CommandContext.getThreadLocalContext();
                boolean saved = false;
                if (tlc != null) {
                    saved = tlc.isAtomicBlock();
                    tlc.setAtomicBlock(true);
                }
                try {
                    if (Boolean.TRUE.equals(program.instructionsRequireTransaction(false))) {
                        //start a transaction
                        this.getContext().getTransactionServer().begin(tc);
                        this.blockContext = tc;
                        program.setStartedTxn(true);
                    }
                } finally {
                    if (tlc != null) {
                        tlc.setAtomicBlock(saved);
                    }
                }
            }
        }
    }

    public void incrementProgramCounter() throws TeiidComponentException {
        if (this.programs.isEmpty()) {
            return;
        }
        Program program = peek();
        ProgramInstruction instr = program.getCurrentInstruction();
        if (instr instanceof RepeatedInstruction) {
            RepeatedInstruction repeated = (RepeatedInstruction)instr;
            repeated.postInstruction(this);
        }
        peek().incrementProgramCounter();
    }

    public List<?> getCurrentRow(String rsName) throws TeiidComponentException {
        return getCursorState(rsName).currentRow;
    }

    public boolean iterateCursor(String rsName)
        throws TeiidComponentException, TeiidProcessingException {

        CursorState state = getCursorState(rsName);

        state.currentRow = state.ts.nextTuple();
        return (state.currentRow != null);
    }

    private CursorState getCursorState(String rsKey) throws TeiidComponentException {
        CursorState state = (CursorState) this.cursorStates.getValue(rsKey);
        if (state == null) {
             throw new TeiidComponentException(QueryPlugin.Event.TEIID30166, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30166, rsKey));
        }
        return state;
    }

    public void removeResults(String rsName) {
        CursorState state = (CursorState) this.cursorStates.remove(rsName);
        removeState(state);
    }

    private void removeState(CursorState state) {
        if (state != null) {
            state.processor.closeProcessing();
            if (state.resultsBuffer != null) {
                state.resultsBuffer.remove();
            }
        }
    }

    /**
     * Get the schema from the tuple source that
     * represents the columns in a result set
     * @param rsName the ResultSet name (not a temp group)
     * @return List of elements
     * @throws TeiidComponentException
     */
    public List getSchema(String rsName) throws TeiidComponentException {
        CursorState cursorState = getCursorState(rsName);
        // get the schema from the tuple source
        List schema = cursorState.processor.getOutputElements();
        return schema;
    }

    public boolean resultSetExists(String rsName) {
        return this.cursorStates.containsVariable(rsName);
    }

    public CommandContext getContext() {
        CommandContext context = super.getContext();
        if (evaluatedParams) {
            context.setVariableContext(currentVarContext);
        }
        return context;
    }

    public List getOutputElements() {
        return outputElements;
    }

    public void setOutputElements(List outputElements) {
        this.outputElements = outputElements;
    }

    /**
     * @return Returns the tempTableStore.
     * @since 5.5
     */
    public TempTableStore getTempTableStore() {
        if (this.programs.isEmpty()) {
            if (runInContext && params == null) {
                return getContext().getTempTableStore();
            }
            return null;
        }
        return this.peek().getTempTableStore();
    }

    boolean evaluateCriteria(Criteria condition) throws BlockedException, TeiidProcessingException, TeiidComponentException {
        evaluator.initialize(getContext(), getDataManager());
        return evaluator.evaluate(condition, Collections.emptyList());
    }

    Object evaluateExpression(Expression expression) throws BlockedException, TeiidProcessingException, TeiidComponentException {
        evaluator.initialize(getContext(), getDataManager());
        return evaluator.evaluate(expression, Collections.emptyList());
    }

    public Program peek() {
        if (this.programs.isEmpty()) {
            return null;
        }
        return programs.peek();
    }

    public void setUpdateCount(int updateCount) {
        this.updateCount = updateCount;
    }

    @Override
    public Boolean requiresTransaction(boolean transactionalReads) {
        Boolean paramRequires = false;
        if (params != null) {
            paramRequires = SubqueryAwareRelationalNode.requiresTransaction(transactionalReads, ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(params.values()));
            if (paramRequires != null && paramRequires) {
                return true;
            }
        }
        if ((transactionalReads && updateCount < 2) || updateCount == Procedure.AUTO_UPDATECOUNT) {
            Boolean programRequires = this.originalProgram.requiresTransaction(transactionalReads);
            if (programRequires == null) {
                return paramRequires==null?true:null;
            }
            if (programRequires) {
                return true;
            }
            return paramRequires;
        }
        if (updateCount == 0) {
            return paramRequires;
        }
        if (updateCount == 1) {
            return paramRequires==null?true:null;
        }
        return true;
    }

    /**
     * For procedures without explicit parameters, sets whether the
     * procedure should run in the parent variable context.
     * @param runInContext
     */
    public void setRunInContext(boolean runInContext) {
        this.runInContext = runInContext;
    }

    @Override
    public TupleSource registerRequest(CommandContext context, Command command,
            String modelName, RegisterRequestParameter parameterObject)
            throws TeiidComponentException, TeiidProcessingException {
        //programs will be empty for parameter evaluation
        TupleSource ts = parentDataMrg.registerRequest(context, command, modelName, parameterObject);
        if (blockContext != null && ts instanceof DataTierTupleSource) {
            txnTupleSources.add(new WeakReference<DataTierTupleSource>((DataTierTupleSource)ts));
        }
        return ts;
    }

    @Override
    public Object lookupCodeValue(CommandContext context, String codeTableName,
            String returnElementName, String keyElementName, Object keyValue)
            throws BlockedException, TeiidComponentException,
            TeiidProcessingException {
        return parentDataMrg.lookupCodeValue(context, codeTableName, returnElementName, keyElementName, keyValue);
    }

    @Override
    public EventDistributor getEventDistributor() {
        return parentDataMrg.getEventDistributor();
    }

    public void setValidateAccess(boolean b) {
        this.validateAccess = b;
    }

    public boolean isValidateAccess() {
        return validateAccess;
    }

}
