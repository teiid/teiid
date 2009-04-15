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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
import com.metamatrix.query.eval.Evaluator;
import com.metamatrix.query.execution.QueryExecPlugin;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.metadata.SupportConstants;
import com.metamatrix.query.processor.BaseProcessorPlan;
import com.metamatrix.query.processor.DescribableUtil;
import com.metamatrix.query.processor.ProcessorDataManager;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.processor.QueryProcessor;
import com.metamatrix.query.processor.TempTableDataManager;
import com.metamatrix.query.processor.program.Program;
import com.metamatrix.query.processor.program.ProgramInstruction;
import com.metamatrix.query.processor.program.ProgramUtil;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.util.VariableContext;
import com.metamatrix.query.tempdata.TempTableStore;
import com.metamatrix.query.tempdata.TempTableStoreImpl;
import com.metamatrix.query.util.CommandContext;
import com.metamatrix.query.util.ErrorMessageKeys;
import com.metamatrix.query.util.LogConstants;
/**
 */
public class ProcedurePlan extends BaseProcessorPlan {

	// State passed during construction
	private ProcedureEnvironment env;
    //this reference should never be used for anything except toString method
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
    private Map params;
    private QueryMetadataInterface metadata;

    /**
     * Constructor for ProcedurePlan.
     */
    public ProcedurePlan(ProcedureEnvironment env) {
    	this.env = env;
        this.env.initialize(this);
		this.originalProgram = (Program)this.env.getProgramStack().peek();
    }

    /**
     * @see ProcessorPlan#initialize(ProcessorDataManager, Object)
     */
    public void initialize(CommandContext context, ProcessorDataManager dataMgr, BufferManager bufferMgr) {       
        this.bufferMgr = bufferMgr;
        this.batchSize = bufferMgr.getProcessorBatchSize();
        TempTableStoreImpl tempTableStore = new TempTableStoreImpl(bufferMgr, context.getConnectionID(), (TempTableStore)context.getTempTableStore());
        this.dataMgr = new TempTableDataManager(dataMgr, tempTableStore);
        env.setTempTableStore(tempTableStore);
        setContext(context);
    }

    public void reset() {
        super.reset();

        done = false;
        internalProcessor = null;
        internalResultID = null;

        finalTupleSource = null;
        beginBatch = 1;
        batchRows = null;
        lastBatch = false;

        //reset program stack
        originalProgram.resetProgramCounter();
        if(env.getProgramStack().empty()){
        	env.getProgramStack().push(originalProgram);
        }
        env.reset();
		LogManager.logTrace(LogConstants.CTX_QUERY_PLANNER, "ProcedurePlan reset"); //$NON-NLS-1$
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
        subContext.setTempTableStore(env.getTempTableStore());
        internalProcessor = new QueryProcessor(subPlan, subContext, this.bufferMgr, this.dataMgr);
        this.internalResultID = this.internalProcessor.getResultsID();
        return this.internalResultID;
    }

    /**
     * Method for ProcessorEnvironment to remove a tuple source when it is done with it.
     */
    void removeTupleSource(TupleSourceID tupleSourceID)
    throws MetaMatrixComponentException {
        try {
			this.bufferMgr.removeTupleSource(tupleSourceID);
		} catch (TupleSourceNotFoundException e) {
            throw new MetaMatrixComponentException(e, ErrorMessageKeys.PROCESSOR_0021, QueryExecPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0021, (String)null));
		} catch (MetaMatrixComponentException e) {
            throw new MetaMatrixComponentException(e, ErrorMessageKeys.PROCESSOR_0022, QueryExecPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0022, (String) null));
		}
        LogManager.logTrace(LogConstants.CTX_QUERY_PLANNER, new Object[]{"removed tuple source", tupleSourceID, "for result set"}); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Get list of resolved elements describing output columns for this plan.
     * @return List of SingleElementSymbol
     */
    public List getOutputElements() {
//        ArrayList output = new ArrayList(1);
//        ElementSymbol count = new ElementSymbol("Count"); //$NON-NLS-1$
//        count.setType(DataTypeManager.DefaultDataClasses.INTEGER);
//        output.add(count);
//        return output;
    	return env.getOutputElements();
    }

    public void open() throws MetaMatrixProcessingException, MetaMatrixComponentException {
        evaluateParams();
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

	    while (!this.env.getProgramStack().empty()){
            Program program = env.peek();
            inst = program.getCurrentInstruction();
	        if (inst == null){
	        	LogManager.logTrace(LogConstants.CTX_QUERY_PLANNER, "Finished program", program); //$NON-NLS-1$
                this.env.pop();
                continue;
            }
            if (inst instanceof RepeatedInstruction) {
    	        LogManager.logTrace(LogConstants.CTX_QUERY_PLANNER, "Executing repeated instruction", inst); //$NON-NLS-1$
                RepeatedInstruction loop = (RepeatedInstruction)inst;
                if (loop.testCondition(env)) {
                    LogManager.logTrace(LogConstants.CTX_QUERY_PLANNER, "Passed condition, executing program " + loop.getNestedProgram()); //$NON-NLS-1$
                    inst.process(env);
                    env.push(loop.getNestedProgram());
                    continue;
                }
                LogManager.logTrace(LogConstants.CTX_QUERY_PLANNER, "Exiting repeated instruction", inst); //$NON-NLS-1$
                loop.postInstruction(env);
            } else {
            	LogManager.logTrace(LogConstants.CTX_QUERY_PLANNER, "Executing instruction", inst); //$NON-NLS-1$
                inst.process(this.env);
            }
            program.incrementProgramCounter();
	    }

        return this.env.getFinalTupleSource();
    }


    public TupleSource getResults(TupleSourceID tupleID)
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
        if(env.getTempTableStore()!=null) {
        	env.getTempTableStore().removeTempTables();
        }
    }

    public String toString() {
        return "ProcedurePlan:\n" + ProgramUtil.programToString(this.originalProgram); //$NON-NLS-1$
    }

	/**
	 * The plan is only clonable in the pre-execution stage, not the execution state
 	 * (things like program state, result sets, etc). It's only safe to call that
 	 * method in between query processings, inother words, it's only safe to call
 	 * clone() on a plan after nextTuple() returns null, meaning the plan has
 	 * finished processing.
 	 */
	public Object clone(){
		ProcedureEnvironment clonedEnv = new ProcedureEnvironment();
    	clonedEnv.getProgramStack().push(originalProgram.clone());
        clonedEnv.setUpdateProcedure(this.env.isUpdateProcedure());
        clonedEnv.setOutputElements(this.env.getOutputElements());
        ProcedurePlan plan = new ProcedurePlan(clonedEnv);
        plan.setParams(params);
        plan.setMetadata(metadata);

        return plan;
    }

    protected void addBatchRow(List row) {
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

    public void setParams( Map params ) {
        this.params = params;
    }
        
    public void evaluateParams() throws BlockedException, MetaMatrixComponentException, MetaMatrixProcessingException {
        
        if ( params == null ) {
            return;
        }
        
        for (Iterator iter = params.entrySet().iterator(); iter.hasNext();) {
            Map.Entry entry = (Map.Entry)iter.next();
            ElementSymbol param = (ElementSymbol)entry.getKey();
            Expression expr = (Expression)entry.getValue();
            
            VariableContext context = env.getCurrentVariableContext();
            Object value = new Evaluator(null, null, getContext()).evaluate(expr, null);

            //check constraint
            if (value == null && !metadata.elementSupports(param.getMetadataID(), SupportConstants.Element.NULL)) {
                throw new QueryValidatorException(QueryExecPlugin.Util.getString("ProcedurePlan.nonNullableParam", expr)); //$NON-NLS-1$
            }
            context.setValue(param, value);
        } 

    }
}
