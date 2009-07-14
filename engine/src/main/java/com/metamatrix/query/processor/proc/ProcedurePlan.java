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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.api.exception.query.QueryValidatorException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleSource;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.common.buffer.TupleSourceNotFoundException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.dqp.util.LogConstants;
import com.metamatrix.query.execution.QueryExecPlugin;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.metadata.SupportConstants;
import com.metamatrix.query.processor.BaseProcessorPlan;
import com.metamatrix.query.processor.DescribableUtil;
import com.metamatrix.query.processor.NullTupleSource;
import com.metamatrix.query.processor.ProcessorDataManager;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.processor.QueryProcessor;
import com.metamatrix.query.processor.TempTableDataManager;
import com.metamatrix.query.processor.program.Program;
import com.metamatrix.query.processor.program.ProgramInstruction;
import com.metamatrix.query.processor.program.ProgramUtil;
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
public class ProcedurePlan extends BaseProcessorPlan {

    private Program originalProgram;

	// State initialized by processor
	private ProcessorDataManager dataMgr;
    private BufferManager bufferMgr;
    private int batchSize;

    private boolean done = false;
    private QueryProcessor internalProcessor;
    private TupleSourceID internalResultID;

    // Temp state for final results
    private TupleSource finalTupleSource;
    private int beginBatch = 1;
    private List batchRows;
    private boolean lastBatch = false;
    private Map<ElementSymbol, Expression> params;
    private Map<ElementSymbol, Reference> implicitParams;
    private QueryMetadataInterface metadata;
    
    private Map tupleSourceMap = new HashMap();     // rsName -> TupleSource
    private Map tupleSourceIDMap = new HashMap();   // rsName -> TupleSourceID
    private Map currentRowMap = new HashMap();

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

    /**
     * Constructor for ProcedurePlan.
     */
    public ProcedurePlan(Program originalProgram) {
    	this.originalProgram = originalProgram;
    	this.programs.add(originalProgram);
    	createVariableContext();
    }

    /**
     * @see ProcessorPlan#initialize(ProcessorDataManager, Object)
     */
    public void initialize(CommandContext context, ProcessorDataManager dataMgr, BufferManager bufferMgr) {       
        this.bufferMgr = bufferMgr;
        this.batchSize = bufferMgr.getProcessorBatchSize();
        tempTableStore = new TempTableStoreImpl(bufferMgr, context.getConnectionID(), (TempTableStore)context.getTempTableStore());
        this.dataMgr = new TempTableDataManager(dataMgr, tempTableStore);
        setContext(context);
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
        tupleSourceMap.clear();
        tupleSourceIDMap.clear();
        currentRowMap.clear();
        createVariableContext();
        lastTupleSource = null;
        
        done = false;
        internalProcessor = null;
        internalResultID = null;

        finalTupleSource = null;
        beginBatch = 1;
        batchRows = null;
        lastBatch = false;

        //reset program stack
        originalProgram.resetProgramCounter();
        programs.clear();
    	programs.push(originalProgram);
		LogManager.logTrace(LogConstants.CTX_DQP, "ProcedurePlan reset"); //$NON-NLS-1$
    }

    public ProcessorDataManager getDataManager() {
        return this.dataMgr;
    }

    /**
     * Request for data from a node
     * @param command Command to execute from node
     * @return The <code>TupleSourceID</code> for the results
     */
    TupleSourceID registerRequest(ProcessorPlan subPlan, VariableContext currentVariableContext)
        throws MetaMatrixComponentException {
        
        if(this.internalProcessor != null){
            return this.internalResultID;
        }
        
        //this may not be the first time the plan is being run
        subPlan.reset();

        // Run query processor on command
        CommandContext subContext = (CommandContext) getContext().clone();
        subContext.setVariableContext(currentVariableContext);
        subContext.setTempTableStore(getTempTableStore());
        internalProcessor = new QueryProcessor(subPlan, subContext, this.bufferMgr, this.dataMgr);
        this.internalResultID = this.internalProcessor.getResultsID();
        return this.internalResultID;
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
	        	LogManager.logTrace(LogConstants.CTX_DQP, "Finished program", program); //$NON-NLS-1$
                this.pop();
                continue;
            }
            if (inst instanceof RepeatedInstruction) {
    	        LogManager.logTrace(LogConstants.CTX_DQP, "Executing repeated instruction", inst); //$NON-NLS-1$
                RepeatedInstruction loop = (RepeatedInstruction)inst;
                if (loop.testCondition(this)) {
                    LogManager.logTrace(LogConstants.CTX_DQP, "Passed condition, executing program " + loop.getNestedProgram()); //$NON-NLS-1$
                    inst.process(this);
                    this.push(loop.getNestedProgram());
                    continue;
                }
                LogManager.logTrace(LogConstants.CTX_DQP, "Exiting repeated instruction", inst); //$NON-NLS-1$
                loop.postInstruction(this);
            } else {
            	LogManager.logTrace(LogConstants.CTX_DQP, "Executing instruction", inst); //$NON-NLS-1$
                inst.process(this);
            }
            program.incrementProgramCounter();
	    }

        if(this.isUpdateProcedure){
            return this.getUpdateCountAsToupleSource();
        }

        if(lastTupleSource == null){
            return new NullTupleSource(null);
        }
        return lastTupleSource;
    }

    private TupleSource getResults()
        throws MetaMatrixComponentException, BlockedException, MetaMatrixProcessingException {

		TupleSource results;

        try {
            this.internalProcessor.process(Integer.MAX_VALUE); //TODO: put a better value here

            // didn't throw processor blocked, so must be done
            results = this.bufferMgr.getTupleSource(this.internalResultID);
        } catch(MetaMatrixComponentException e) {
            throw e;
        } catch (MetaMatrixProcessingException e) {
        	throw e;
        } catch(MetaMatrixCoreException e) {
            throw new MetaMatrixComponentException(e, ErrorMessageKeys.PROCESSOR_0023, QueryExecPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0023, e.getMessage()));
        }

        // clean up internal stuff
        this.internalProcessor = null;

        return results;
    }

    public void close()
        throws MetaMatrixComponentException {
        // Defect 14544 - remove the tuple batches for the internal tuple source from the buffer manager.
        // This is the last tuple source created by the procedure plan.
        if (internalResultID != null) {
            try {
                bufferMgr.removeTupleSource(internalResultID);
                internalResultID = null;
            } catch (Exception e) {
                // Ignore
            }
        }
        if(getTempTableStore()!=null) {
        	getTempTableStore().removeTempTables();
        }
        if (this.evaluator != null) {
        	this.evaluator.close();
        }
    }

    public String toString() {
        return "ProcedurePlan:\n" + ProgramUtil.programToString(this.originalProgram); //$NON-NLS-1$
    }

	public Object clone(){
        ProcedurePlan plan = new ProcedurePlan((Program)originalProgram.clone());
        plan.setUpdateProcedure(this.isUpdateProcedure());
        plan.setOutputElements(this.getOutputElements());
        plan.setParams(params);
        plan.setImplicitParams(implicitParams);
        plan.setMetadata(metadata);
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

    public Map getDescriptionProperties() {
        Map props = this.originalProgram.getDescriptionProperties();
        props.put(PROP_TYPE, "Procedure Plan"); //$NON-NLS-1$
        props.put(PROP_OUTPUT_COLS, DescribableUtil.getOutputColumnProperties(getOutputElements()));

        return props;
    }
    
    /** 
     * @see com.metamatrix.query.processor.ProcessorPlan#getChildPlans()
     * @since 4.2
     */
    public Collection getChildPlans() {
        return this.originalProgram.getChildPlans();
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

        final List updateResult = new ArrayList(1);
        updateResult.add(rowCount);

        return new UpdateCountTupleSource(updateResult);
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

    public void executePlan(ProcessorPlan command, String rsName)
        throws MetaMatrixComponentException, MetaMatrixProcessingException {
        boolean isExecSQLInstruction = rsName.equals(ExecSqlInstruction.RS_NAME);
        // Defect 14544: Close all non-final ExecSqlInstruction tuple sources before creating a new source.
        // This guarantees that the tuple source will be removed predictably from the buffer manager.
        if (isExecSQLInstruction) {
            removeResults(ExecSqlInstruction.RS_NAME);
        }
        
        TupleSourceID tsID = registerRequest(command, this.currentVarContext);
        TupleSource source = getResults();
        tupleSourceIDMap.put(rsName.toUpperCase(), tsID);
        tupleSourceMap.put(rsName.toUpperCase(), source);
        if(isExecSQLInstruction){
            //keep a reference to the tuple source
            //it may be the last one
            this.lastTupleSource = source;
        }
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
            removeResults((String)i.next());
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

    public List getCurrentRow(String rsName) {
        return (List) currentRowMap.get(rsName.toUpperCase());
    }

    public boolean iterateCursor(String rsName)
        throws MetaMatrixComponentException, MetaMatrixProcessingException {

        String rsKey = rsName.toUpperCase();

        TupleSource source = (TupleSource) tupleSourceMap.get(rsKey);
        if(source == null) {
            // TODO - throw exception?
            return false;
        }

        List row = source.nextTuple();
        currentRowMap.put(rsKey, row);
        return (row != null);
    }

    public void removeResults(String rsName) throws MetaMatrixComponentException {
        String rsKey = rsName.toUpperCase();
        TupleSource source = (TupleSource) tupleSourceMap.get(rsKey);
        if(source != null) {
            source.closeSource();
            TupleSourceID tsID = (TupleSourceID) tupleSourceIDMap.get(rsKey);
            try {
    			this.bufferMgr.removeTupleSource(tsID);
    		} catch (TupleSourceNotFoundException e) {
                throw new MetaMatrixComponentException(e, ErrorMessageKeys.PROCESSOR_0021, QueryExecPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0021, (String)null));
    		} catch (MetaMatrixComponentException e) {
                throw new MetaMatrixComponentException(e, ErrorMessageKeys.PROCESSOR_0022, QueryExecPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0022, (String) null));
    		}
            LogManager.logTrace(LogConstants.CTX_DQP, new Object[]{"removed tuple source", tsID, "for result set"}); //$NON-NLS-1$ //$NON-NLS-2$
            tupleSourceMap.remove(rsKey);
            tupleSourceIDMap.remove(rsKey);
            currentRowMap.remove(rsKey);
            this.tempTableStore.removeTempTableByName(rsKey);
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
        TupleSource source = (TupleSource) tupleSourceMap.get(rsKey);
        if(source == null){
            throw new MetaMatrixComponentException(QueryExecPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0037, rsName));
        }
        // get the schema from the tuple source
        List schema = source.getSchema();
        if(schema == null){
            throw new MetaMatrixComponentException(QueryExecPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0038));
        }

        return schema;
    }

    public boolean resultSetExists(String rsName) {
        String rsKey = rsName.toUpperCase();
        boolean exists = this.tupleSourceMap.containsKey(rsKey);
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
    	evaluator.setContext(getContext());
		boolean result = evaluator.evaluate(condition, Collections.emptyList());
		this.evaluator.close();
		return result;
    }
    
    Object evaluateExpression(Expression expression) throws BlockedException, MetaMatrixProcessingException, MetaMatrixComponentException {
    	evaluator.setContext(getContext());
    	Object result = evaluator.evaluate(expression, Collections.emptyList());
    	this.evaluator.close();
    	return result;
    }
               
    public Program peek() {
        return programs.peek();
    }
    
}
