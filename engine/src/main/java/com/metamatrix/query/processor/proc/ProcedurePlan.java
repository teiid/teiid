/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package com.metamatrix.query.processor.proc;

import static com.metamatrix.query.analysis.AnalysisRecord.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.teiid.client.plan.PlanNode;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.api.exception.query.QueryValidatorException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.IndexedTupleSource;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleSource;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.execution.QueryExecPlugin;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.metadata.SupportConstants;
import com.metamatrix.query.processor.BatchIterator;
import com.metamatrix.query.processor.CollectionTupleSource;
import com.metamatrix.query.processor.ProcessorDataManager;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.processor.QueryProcessor;
import com.metamatrix.query.processor.TempTableDataManager;
import com.metamatrix.query.processor.program.Program;
import com.metamatrix.query.processor.program.ProgramInstruction;
import com.metamatrix.query.processor.relational.SubqueryAwareEvaluator;
import com.metamatrix.query.sql.ProcedureReservedWords;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.Reference;
import com.metamatrix.query.sql.util.VariableContext;
import com.metamatrix.query.tempdata.TempTableStore;
import com.metamatrix.query.tempdata.TempTableStoreImpl;
import com.metamatrix.query.util.CommandContext;
import com.metamatrix.query.util.ErrorMessageKeys;
/**
 */
public class ProcedurePlan extends ProcessorPlan {

	public static class CursorState {
		private QueryProcessor processor;
		private IndexedTupleSource ts;
		private List<?> currentRow;
	}
	
    private Program originalProgram;

	// State initialized by processor
	private ProcessorDataManager dataMgr;
	private ProcessorDataManager parentDataMrg;
    private BufferManager bufferMgr;
    private int batchSize;

    private boolean done = false;
    private CursorState currentState;

    // Temp state for final results
    private TupleSource finalTupleSource;
    private int beginBatch = 1;
    private List batchRows;
    private boolean lastBatch = false;
    private Map<ElementSymbol, Expression> params;
    private Map<ElementSymbol, Reference> implicitParams;
    private QueryMetadataInterface metadata;

    private Map<String, CursorState> cursorStates = new HashMap<String, CursorState>();

	private static ElementSymbol ROWS_UPDATED =
			new ElementSymbol(ProcedureReservedWords.VARIABLES+"."+ProcedureReservedWords.ROWS_UPDATED); //$NON-NLS-1$

	private static int NO_ROWS_UPDATED = 0;
	private VariableContext currentVarContext;
    private boolean isUpdateProcedure = true;

    private TupleSource lastTupleSource;
    
    private List outputElements;
    
    private TempTableStore tempTableStore;
    
    private LinkedList tempContext = new LinkedList();
	private SubqueryAwareEvaluator evaluator;
	
    // Stack of programs, with current program on top
    private Stack<Program> programs = new Stack<Program>();
    
    private boolean evaluatedParams;
    
    private boolean requiresTransaction = true;

    /**
     * Constructor for ProcedurePlan.
     */
    public ProcedurePlan(Program originalProgram) {
    	this.originalProgram = originalProgram;
    	this.programs.add(originalProgram);
    	createVariableContext();
    }
    
    public Program getOriginalProgram() {
		return originalProgram;
	}

    /**
     * @see ProcessorPlan#initialize(ProcessorDataManager, Object)
     */
    public void initialize(CommandContext context, ProcessorDataManager dataMgr, BufferManager bufferMgr) {       
        this.bufferMgr = bufferMgr;
        this.batchSize = bufferMgr.getProcessorBatchSize();
        setContext(context);
        this.dataMgr = dataMgr;
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
        cursorStates.clear();
        createVariableContext();
        lastTupleSource = null;
        
        done = false;
        currentState = null;

        finalTupleSource = null;
        beginBatch = 1;
        batchRows = null;
        lastBatch = false;

        //reset program stack
        originalProgram.resetProgramCounter();
        programs.clear();
    	programs.push(originalProgram);
		LogManager.logTrace(com.metamatrix.common.log.LogConstants.CTX_DQP, "ProcedurePlan reset"); //$NON-NLS-1$
    }

    public ProcessorDataManager getDataManager() {
        return this.dataMgr;
    }

    public void open() throws MetaMatrixProcessingException, MetaMatrixComponentException {
    	if (!this.evaluatedParams) {
    		if (this.params != null) { 
		        for (Map.Entry<ElementSymbol, Expression> entry : this.params.entrySet()) {
		            ElementSymbol param = entry.getKey();
		            Expression expr = entry.getValue();
		            
		            VariableContext context = getCurrentVariableContext();
		            Object value = this.evaluateExpression(expr);
		
		            //check constraint
		            if (value == null && !metadata.elementSupports(param.getMetadataID(), SupportConstants.Element.NULL)) {
		                throw new QueryValidatorException(QueryExecPlugin.Util.getString("ProcedurePlan.nonNullableParam", expr)); //$NON-NLS-1$
		            }
		            context.setValue(param, value);
		        }
    		}
    		if (this.implicitParams != null) {
    			for (Map.Entry<ElementSymbol, Reference> entry : this.implicitParams.entrySet()) {
		            VariableContext context = getCurrentVariableContext();
		            Object value = this.evaluateExpression(entry.getValue());
		            context.setValue(entry.getKey(), value);
				}
    		}
    		tempTableStore = new TempTableStoreImpl(bufferMgr, getContext().getConnectionID(), null);
            this.dataMgr = new TempTableDataManager(dataMgr, tempTableStore);
    	}
    	this.evaluatedParams = true;
    }

    /**
     * @see ProcessorPlan#nextBatch()
     */
    public TupleBatch nextBatch()
        throws MetaMatrixComponentException, MetaMatrixProcessingException, BlockedException {

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
            List tuple = this.finalTupleSource.nextTuple();
            if(tuple == null) {
                terminateBatches();
                done = true;
                break;
            }
            addBatchRow(tuple);
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
     * Program off the stack.</p>
     *
     * @return List a single tuple containing one Integer: the update
     * count resulting from the procedure execution.
     */
    private TupleSource processProcedure()
        throws MetaMatrixComponentException, MetaMatrixProcessingException, BlockedException {

        // execute plan
	    ProgramInstruction inst = null;

	    while (!this.programs.empty()){
            Program program = peek();
            inst = program.getCurrentInstruction();
	        if (inst == null){
	        	LogManager.logTrace(com.metamatrix.common.log.LogConstants.CTX_DQP, "Finished program", program); //$NON-NLS-1$
                this.pop();
                continue;
            }
            if (inst instanceof RepeatedInstruction) {
    	        LogManager.logTrace(com.metamatrix.common.log.LogConstants.CTX_DQP, "Executing repeated instruction", inst); //$NON-NLS-1$
                RepeatedInstruction loop = (RepeatedInstruction)inst;
                if (loop.testCondition(this)) {
                    LogManager.logTrace(com.metamatrix.common.log.LogConstants.CTX_DQP, "Passed condition, executing program " + loop.getNestedProgram()); //$NON-NLS-1$
                    inst.process(this);
                    this.push(loop.getNestedProgram());
                    continue;
                }
                LogManager.logTrace(com.metamatrix.common.log.LogConstants.CTX_DQP, "Exiting repeated instruction", inst); //$NON-NLS-1$
                loop.postInstruction(this);
            } else {
            	LogManager.logTrace(com.metamatrix.common.log.LogConstants.CTX_DQP, "Executing instruction", inst); //$NON-NLS-1$
                inst.process(this);
            }
            program.incrementProgramCounter();
	    }

        if(this.isUpdateProcedure){
            return this.getUpdateCountAsToupleSource();
        }

        if(lastTupleSource == null){
            return CollectionTupleSource.createNullTupleSource(null);
        }
        return lastTupleSource;
    }

    public void close()
        throws MetaMatrixComponentException {
        if (!this.cursorStates.isEmpty()) {
        	List<String> cursors = new ArrayList<String>(this.cursorStates.keySet());
        	for (String rsName : cursors) {
    			removeResults(rsName);
			}
        }
        if(getTempTableStore()!=null) {
        	getTempTableStore().removeTempTables();
        }
        if (this.evaluator != null) {
        	this.evaluator.close();
        }
        this.tempTableStore = null;
        this.dataMgr = parentDataMrg;
    }

    public String toString() {
        return "ProcedurePlan:\n" + this.originalProgram; //$NON-NLS-1$
    }

	public ProcessorPlan clone(){
        ProcedurePlan plan = new ProcedurePlan((Program)originalProgram.clone());
        plan.setUpdateProcedure(this.isUpdateProcedure());
        plan.setOutputElements(this.getOutputElements());
        plan.setParams(params);
        plan.setImplicitParams(implicitParams);
        plan.setMetadata(metadata);
        plan.requiresTransaction = requiresTransaction;
        return plan;
    }

    private void addBatchRow(List row) {
        if(this.batchRows == null) {
            this.batchRows = new ArrayList(this.batchSize);
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

    public void setParams( Map<ElementSymbol, Expression> params ) {
        this.params = params;
    }
    
    public void setImplicitParams(Map<ElementSymbol, Reference> implicitParams) {
		this.implicitParams = implicitParams;
	}
        
	private void createVariableContext() {
		this.currentVarContext = new VariableContext(true);
        this.currentVarContext.setValue(ROWS_UPDATED, new Integer(NO_ROWS_UPDATED));
	}

    private TupleSource getUpdateCountAsToupleSource() {
    	Object rowCount = currentVarContext.getValue(ROWS_UPDATED);
    	if(rowCount == null) {
			rowCount = new Integer(NO_ROWS_UPDATED);
    	}
        return CollectionTupleSource.createUpdateCountTupleSource((Integer)rowCount);
    }

    /**
     * <p> Get the current <code>VariavleContext</code> on this environment.
     * The VariableContext is updated with variables and their values by
     * {@link ProgramInstruction}s that are part of the ProcedurePlan that use
     * this environment.</p>
     * @return The current <code>VariariableContext</code>.
     */
    public VariableContext getCurrentVariableContext() {
		return this.currentVarContext;
    }

    public CursorState executePlan(ProcessorPlan command, String rsName)
        throws MetaMatrixComponentException, MetaMatrixProcessingException {
    	
        CursorState state = this.cursorStates.get(rsName.toUpperCase());
        if (state == null) {
        	if (this.currentState == null) {
		        //this may not be the first time the plan is being run
		        command.reset();
		
		        CommandContext subContext = (CommandContext) getContext().clone();
		        subContext.setVariableContext(this.currentVarContext);
		        subContext.setTempTableStore(getTempTableStore());
		        state = new CursorState();
		        state.processor = new QueryProcessor(command, subContext, this.bufferMgr, this.dataMgr);
		        state.ts = new BatchIterator(state.processor);
		        
	            //keep a reference to the tuple source
	            //it may be the last one
	            this.lastTupleSource = state.ts;
	            
	            this.currentState = state;
        	}
        	//force execution to the first batch
    		this.currentState.ts.hasNext();

	        this.cursorStates.put(rsName.toUpperCase(), this.currentState);
	        this.currentState = null;
        }
        return state;
    }
    
    /** 
     * @throws MetaMatrixComponentException 
     * @see com.metamatrix.query.processor.program.ProgramEnvironment#pop()
     */
    public void pop() throws MetaMatrixComponentException {
    	this.programs.pop();
        if (this.currentVarContext.getParentContext() != null) {
        	this.currentVarContext = this.currentVarContext.getParentContext();
        }
        Set current = getTempContext();

        Set tempTables = getLocalTempTables();

        tempTables.addAll(current);
        
        for (Iterator i = tempTables.iterator(); i.hasNext();) {
            this.tempTableStore.removeTempTableByName((String)i.next());
        }
        
        this.tempContext.removeLast();
    }
    
    /** 
     * @see com.metamatrix.query.processor.program.ProgramEnvironment#push(com.metamatrix.query.processor.program.Program)
     */
    public void push(Program program) {
    	program.resetProgramCounter();
        this.programs.push(program);
        VariableContext context = new VariableContext(true);
        context.setParentContext(this.currentVarContext);
        this.currentVarContext = context;
        
        Set current = getTempContext();
        
        Set tempTables = getLocalTempTables();
        
        current.addAll(tempTables);
        this.tempContext.add(new HashSet());
    }
    
    /** 
     * @see com.metamatrix.query.processor.program.ProgramEnvironment#incrementProgramCounter()
     */
    public void incrementProgramCounter() throws MetaMatrixComponentException {
        Program program = peek();
        ProgramInstruction instr = program.getCurrentInstruction();
        if (instr instanceof RepeatedInstruction) {
            RepeatedInstruction repeated = (RepeatedInstruction)instr;
            repeated.postInstruction(this);
        }
        peek().incrementProgramCounter();
    }

    /** 
     * @return
     */
    private Set getLocalTempTables() {
        Set tempTables = this.tempTableStore.getAllTempTables();
        
        //determine what was created in this scope
        for (int i = 0; i < tempContext.size() - 1; i++) {
            tempTables.removeAll((Set)tempContext.get(i));
        }
        return tempTables;
    }

    public Set getTempContext() {
        if (this.tempContext.isEmpty()) {
            tempContext.addLast(new HashSet());
        }
        return (Set)this.tempContext.getLast();
    }

    public List getCurrentRow(String rsName) throws MetaMatrixComponentException {
        return getCursorState(rsName.toUpperCase()).currentRow;
    }

    public boolean iterateCursor(String rsName)
        throws MetaMatrixComponentException, MetaMatrixProcessingException {

        String rsKey = rsName.toUpperCase();

        CursorState state = getCursorState(rsKey);
        
        state.currentRow = state.ts.nextTuple();
        return (state.currentRow != null);
    }

	private CursorState getCursorState(String rsKey) throws MetaMatrixComponentException {
		CursorState state = this.cursorStates.get(rsKey);
		if (state == null) {
			throw new MetaMatrixComponentException(QueryExecPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0037, rsKey));
		}
		return state;
	}

    public void removeResults(String rsName) throws MetaMatrixComponentException {
        String rsKey = rsName.toUpperCase();
        CursorState state = this.cursorStates.remove(rsKey);
        if (state != null) {
        	state.processor.closeProcessing();
        }
    }


    /**
     * Get the schema from the tuple source that
     * represents the columns in a result set
     * @param rsName the ResultSet name (not a temp group)
     * @return List of elements
     * @throws QueryProcessorException if the list of elements is null
     */
    public List getSchema(String rsName) throws MetaMatrixComponentException {

        // get the tuple source
        String rsKey = rsName.toUpperCase();
        
        CursorState cursorState = getCursorState(rsKey);
        // get the schema from the tuple source
        List schema = cursorState.ts.getSchema();
        return schema;
    }

    public boolean resultSetExists(String rsName) {
        String rsKey = rsName.toUpperCase();
        boolean exists = this.cursorStates.containsKey(rsKey);
        return exists;
    }

    public CommandContext getContext() {
    	CommandContext context = super.getContext();
    	if (evaluatedParams) {
    		context.setVariableContext(currentVarContext);
    	}
    	return context;
    }

    /**
     * @return
     */
    public boolean isUpdateProcedure() {
        return isUpdateProcedure;
    }

    /**
     * @param b
     */
    public void setUpdateProcedure(boolean b) {
        isUpdateProcedure = b;
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
        return this.tempTableStore;
    }

    boolean evaluateCriteria(Criteria condition) throws BlockedException, MetaMatrixProcessingException, MetaMatrixComponentException {
    	evaluator.initialize(getContext(), getDataManager());
		boolean result = evaluator.evaluate(condition, Collections.emptyList());
		this.evaluator.close();
		return result;
    }
    
    Object evaluateExpression(Expression expression) throws BlockedException, MetaMatrixProcessingException, MetaMatrixComponentException {
    	evaluator.initialize(getContext(), getDataManager());
    	Object result = evaluator.evaluate(expression, Collections.emptyList());
    	this.evaluator.close();
    	return result;
    }
               
    public Program peek() {
        return programs.peek();
    }
    
    public void setRequiresTransaction(boolean requiresTransaction) {
		this.requiresTransaction = requiresTransaction;
	}
    
    @Override
    public boolean requiresTransaction(boolean transactionalReads) {
    	//TODO: detect simple select case
    	return requiresTransaction || transactionalReads;
    }
    
}
