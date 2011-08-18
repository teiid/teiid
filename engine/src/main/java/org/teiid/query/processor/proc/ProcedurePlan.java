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

package org.teiid.query.processor.proc;

import static org.teiid.query.analysis.AnalysisRecord.*;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.client.plan.PlanNode;
import org.teiid.client.xa.XATransactionException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.IndexedTupleSource;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.TupleSource;
import org.teiid.common.buffer.BufferManager.TupleSourceType;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.Assertion;
import org.teiid.dqp.internal.process.DataTierTupleSource;
import org.teiid.dqp.service.TransactionContext;
import org.teiid.dqp.service.TransactionService;
import org.teiid.dqp.service.TransactionContext.Scope;
import org.teiid.events.EventDistributor;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.MetadataRepository;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.SupportConstants;
import org.teiid.query.processor.BatchIterator;
import org.teiid.query.processor.CollectionTupleSource;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.QueryProcessor;
import org.teiid.query.processor.relational.SubqueryAwareEvaluator;
import org.teiid.query.sql.ProcedureReservedWords;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.util.VariableContext;
import org.teiid.query.tempdata.TempTableStore;
import org.teiid.query.util.CommandContext;

/**
 */
public class ProcedurePlan extends ProcessorPlan {

	private static class CursorState {
		QueryProcessor processor;
		IndexedTupleSource ts;
		List<?> currentRow;
		TupleBuffer resultsBuffer;
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
    private List<Object> batchRows;
    private boolean lastBatch = false;
    private LinkedHashMap<ElementSymbol, Expression> params;
    private List<ElementSymbol> outParams;
    private Map<ElementSymbol, Reference> implicitParams;
    private QueryMetadataInterface metadata;

    private Map<String, CursorState> cursorStates = new HashMap<String, CursorState>();

	private static ElementSymbol ROWS_UPDATED =
			new ElementSymbol(ProcedureReservedWords.VARIABLES+"."+ProcedureReservedWords.ROWS_UPDATED); //$NON-NLS-1$

	static ElementSymbol ROWCOUNT =
		new ElementSymbol(ProcedureReservedWords.VARIABLES+"."+ProcedureReservedWords.ROWCOUNT); //$NON-NLS-1$
	
	static {
		ROWS_UPDATED.setType(DataTypeManager.DefaultDataClasses.INTEGER);
		ROWCOUNT.setType(DataTypeManager.DefaultDataClasses.INTEGER);
	}

	private VariableContext currentVarContext;
    private boolean isUpdateProcedure = true;

    private TupleSource lastTupleSource;
    
    private List outputElements;
    
	private SubqueryAwareEvaluator evaluator;
	
    // Stack of programs, with current program on top
    private Stack<Program> programs = new Stack<Program>();
    
    private boolean evaluatedParams;
    
    private boolean requiresTransaction = true;
    
    private TransactionContext blockContext;
    private boolean inTxn;
    /**
     * Resources cannot be held open across the txn boundary.  This list is a hack at ensuring the resources are closed.
     */
    private LinkedList<WeakReference<DataTierTupleSource>> txnTupleSources = new LinkedList<WeakReference<DataTierTupleSource>>();

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

    /**
     * @see ProcessorPlan#initialize(ProcessorDataManager, Object)
     */
    public void initialize(CommandContext context, ProcessorDataManager dataMgr, BufferManager bufferMgr) {       
        this.bufferMgr = bufferMgr;
        this.batchSize = bufferMgr.getProcessorBatchSize();
        setContext(context.clone());
        this.dataMgr = new ProcessorDataManager() {
			
			@Override
			public TupleSource registerRequest(CommandContext context, Command command,
					String modelName, String connectorBindingId, int nodeID, int limit)
					throws TeiidComponentException, TeiidProcessingException {
				TupleSource ts = parentDataMrg.registerRequest(context, command, modelName, connectorBindingId, nodeID, limit);
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
			public MetadataRepository getMetadataRepository() {
				return parentDataMrg.getMetadataRepository();
			}
			
			@Override
			public EventDistributor getEventDistributor() {
				return parentDataMrg.getEventDistributor();
			}
		};
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
        inTxn = false;
        //reset program stack
        programs.clear();
		LogManager.logTrace(org.teiid.logging.LogConstants.CTX_DQP, "ProcedurePlan reset"); //$NON-NLS-1$
    }

    public ProcessorDataManager getDataManager() {
        return this.dataMgr;
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
		            Object value = this.evaluateExpression(expr);
		
		            //check constraint
		            checkNotNull(param, value);
		            setParameterValue(param, context, value);
		        }
    		}
    		if (this.implicitParams != null) {
    			for (Map.Entry<ElementSymbol, Reference> entry : this.implicitParams.entrySet()) {
		            VariableContext context = getCurrentVariableContext();
		            Object value = this.evaluateExpression(entry.getValue());
		            context.setValue(entry.getKey(), value);
				}
    		}
    		this.push(originalProgram);
    	}
    	this.evaluatedParams = true;
    }

	private void checkNotNull(ElementSymbol param, Object value)
			throws TeiidComponentException, QueryMetadataException,
			QueryValidatorException {
		if (value == null && !metadata.elementSupports(param.getMetadataID(), SupportConstants.Element.NULL)) {
		    throw new QueryValidatorException(QueryPlugin.Util.getString("ProcedurePlan.nonNullableParam", param)); //$NON-NLS-1$
		}
	}

	protected void setParameterValue(ElementSymbol param,
			VariableContext context, Object value) {
		context.setValue(param, value);
	}
	
	@Override
	public TupleBatch nextBatch() throws BlockedException,
			TeiidComponentException, TeiidProcessingException {
		if (blockContext != null && !this.inTxn) {
			this.getContext().getTransactionServer().resume(blockContext);
			this.inTxn = true;
		} 
		try {
			return nextBatchDirect();
		} finally {
			if (blockContext != null) {
				this.inTxn = false;
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
            			checkNotNull(param, value);
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
     * Program off the stack.</p>
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
                this.pop(true);
                continue;
            }
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
            }
            program.incrementProgramCounter();
	    }

        if(this.isUpdateProcedure){
            return this.getUpdateCountAsToupleSource();
        }

        if(lastTupleSource == null){
            return CollectionTupleSource.createNullTupleSource();
        }
        return lastTupleSource;
    }

    public void close()
        throws TeiidComponentException {
        if (!this.cursorStates.isEmpty()) {
        	List<String> cursors = new ArrayList<String>(this.cursorStates.keySet());
        	for (String rsName : cursors) {
    			removeResults(rsName);
			}
        }
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
        this.dataMgr = parentDataMrg;
        this.txnTupleSources.clear();
        this.blockContext = null;
    }

    public String toString() {
        return "ProcedurePlan:\n" + this.originalProgram; //$NON-NLS-1$
    }

	public ProcessorPlan clone(){
        ProcedurePlan plan = new ProcedurePlan((Program)originalProgram.clone());
        plan.setUpdateProcedure(this.isUpdateProcedure());
        plan.setOutputElements(this.getOutputElements());
        plan.setParams(params);
        plan.setOutParams(outParams);
        plan.setImplicitParams(implicitParams);
        plan.setMetadata(metadata);
        plan.requiresTransaction = requiresTransaction;
        return plan;
    }

	private void addBatchRow(List<?> row, boolean last) {
        if(this.batchRows == null) {
            this.batchRows = new ArrayList<Object>(this.batchSize/4);
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
    
    public void setImplicitParams(Map<ElementSymbol, Reference> implicitParams) {
		this.implicitParams = implicitParams;
	}
        
	private void createVariableContext() {
		this.currentVarContext = new VariableContext(true);
        this.currentVarContext.setValue(ROWS_UPDATED, 0);
        this.currentVarContext.setValue(ROWCOUNT, 0);
	}

    private TupleSource getUpdateCountAsToupleSource() {
    	Object rowCount = currentVarContext.getValue(ROWS_UPDATED);
    	if(rowCount == null) {
			rowCount = 0;
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

    public void executePlan(ProcessorPlan command, String rsName, Map<ElementSymbol, ElementSymbol> procAssignments, boolean keepRs)
        throws TeiidComponentException, TeiidProcessingException {
    	
        CursorState state = this.cursorStates.get(rsName.toUpperCase());
        if (state == null) {
        	if (this.currentState == null) {
		        //this may not be the first time the plan is being run
		        command.reset();
		
		        CommandContext subContext = getContext().clone();
		        subContext.setVariableContext(this.currentVarContext);
		        state = new CursorState();
		        state.processor = new QueryProcessor(command, subContext, this.bufferMgr, this.dataMgr);
		        state.ts = new BatchIterator(state.processor);
		        if (procAssignments != null && state.processor.getOutputElements().size() - procAssignments.size() > 0) {
		        	state.resultsBuffer = bufferMgr.createTupleBuffer(state.processor.getOutputElements().subList(0, state.processor.getOutputElements().size() - procAssignments.size()), getContext().getConnectionID(), TupleSourceType.PROCESSOR);
		        } else if (this.blockContext != null) {
		        	state.resultsBuffer = bufferMgr.createTupleBuffer(state.processor.getOutputElements(), getContext().getConnectionID(), TupleSourceType.PROCESSOR);
		        }
	            this.currentState = state;
        	}
        	//force execution to the first batch
        	this.currentState.ts.hasNext();
        	if (procAssignments != null) {
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
            	//no resultset
            	if (this.currentState.resultsBuffer == null) {
            		this.currentState.processor.closeProcessing();
            		this.currentState = null;
            		return;
            	}
            	this.currentState.resultsBuffer.close();
            	this.currentState.ts = this.currentState.resultsBuffer.createIndexedTupleSource();
            } else if (this.blockContext != null) {
            	//process fully in a block transaction
            	while (this.currentState.ts.hasNext()) {
            		List<?> tuple = this.currentState.ts.nextTuple();
        			this.currentState.resultsBuffer.addTuple(tuple);
            	}
            	this.currentState.resultsBuffer.close();
            	this.currentState.ts = this.currentState.resultsBuffer.createIndexedTupleSource();
            }
	        this.cursorStates.put(rsName.toUpperCase(), this.currentState);
	        //keep a reference to the tuple source
            //it may be the last one
	        if (keepRs) {
	        	this.lastTupleSource = this.currentState.ts;
	        }
	        this.currentState = null;
        }
    }
    
    /** 
     * @param success
     * @throws TeiidComponentException 
     * @throws XATransactionException 
     */
    public void pop(boolean success) throws TeiidComponentException {
    	Program program = this.programs.pop();
        if (this.currentVarContext.getParentContext() != null) {
        	this.currentVarContext = this.currentVarContext.getParentContext();
        }
    	program.getTempTableStore().removeTempTables();
    	if (program.startedTxn() && this.blockContext != null) {
    		TransactionService ts = this.getContext().getTransactionServer();
    		TransactionContext tc = this.blockContext;
    		this.blockContext = null;
    		try {
	    		if (!inTxn) {
	    			this.getContext().getTransactionServer().resume(tc);
	    		}
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
    			throw new TeiidComponentException(e);
    		}
    	}
    }
    
    public void push(Program program) throws XATransactionException {
    	program.reset(this.getContext().getConnectionID());
    	TempTableStore tts = getTempTableStore();
		getContext().setTempTableStore(program.getTempTableStore());
		program.getTempTableStore().setParentTempTableStore(tts);
        this.programs.push(program);
        VariableContext context = new VariableContext(true);
        context.setParentContext(this.currentVarContext);
        this.currentVarContext = context;
        
        if (program.isAtomic()) {
        	TransactionContext tc = this.getContext().getTransactionContext();
        	if (tc != null && tc.getTransactionType() == Scope.NONE) {
        		//start a transaction
        		this.getContext().getTransactionServer().begin(tc);
        		this.inTxn = true;
        		this.blockContext = tc;
        		this.peek().setStartedTxn(true);
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
        return getCursorState(rsName.toUpperCase()).currentRow;
    }

    public boolean iterateCursor(String rsName)
        throws TeiidComponentException, TeiidProcessingException {

        String rsKey = rsName.toUpperCase();

        CursorState state = getCursorState(rsKey);
        
        state.currentRow = state.ts.nextTuple();
        return (state.currentRow != null);
    }

	private CursorState getCursorState(String rsKey) throws TeiidComponentException {
		CursorState state = this.cursorStates.get(rsKey);
		if (state == null) {
			throw new TeiidComponentException(QueryPlugin.Util.getString("ERR.015.006.0037", rsKey)); //$NON-NLS-1$
		}
		return state;
	}

    public void removeResults(String rsName) {
        String rsKey = rsName.toUpperCase();
        CursorState state = this.cursorStates.remove(rsKey);
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
     * @throws QueryProcessorException if the list of elements is null
     */
    public List getSchema(String rsName) throws TeiidComponentException {

        // get the tuple source
        String rsKey = rsName.toUpperCase();
        
        CursorState cursorState = getCursorState(rsKey);
        // get the schema from the tuple source
        List schema = cursorState.processor.getOutputElements();
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
    	if (this.programs.isEmpty()) {
    		return null;
    	}
        return this.peek().getTempTableStore();
    }

    boolean evaluateCriteria(Criteria condition) throws BlockedException, TeiidProcessingException, TeiidComponentException {
    	evaluator.initialize(getContext(), getDataManager());
		boolean result = evaluator.evaluate(condition, Collections.emptyList());
		this.evaluator.close();
		return result;
    }
    
    Object evaluateExpression(Expression expression) throws BlockedException, TeiidProcessingException, TeiidComponentException {
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
