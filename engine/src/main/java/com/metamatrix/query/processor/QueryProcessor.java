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

package com.metamatrix.query.processor;

import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.common.buffer.TupleSourceNotFoundException;
import com.metamatrix.common.buffer.BufferManager.TupleSourceStatus;
import com.metamatrix.common.buffer.BufferManager.TupleSourceType;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.dqp.util.LogConstants;
import com.metamatrix.query.execution.QueryExecPlugin;
import com.metamatrix.query.util.CommandContext;
import com.metamatrix.query.util.TypeRetrievalUtil;

/**
 * TODO: combine overlapping responsibilities with RequestWorkItem
 */
public class QueryProcessor {
	
	public interface ProcessorFactory {
		QueryProcessor createQueryProcessor(String query, String recursionGroup, CommandContext commandContext) throws MetaMatrixProcessingException, MetaMatrixComponentException;
	}
	
	public interface BatchHandler {
		void batchProduced(TupleBatch batch) throws MetaMatrixProcessingException, MetaMatrixComponentException;
	}

	private TupleSourceID resultsID;
    private CommandContext context;
	private ProcessorDataManager dataMgr;
    private BufferManager bufferMgr;
	private ProcessorPlan processPlan;
    private boolean initialized = false;
    /** Flag that marks whether the request has been canceled. */
    private volatile boolean requestCanceled = false;
    private volatile boolean requestClosed = false;
    private static final int DEFAULT_WAIT = 50;       
    private boolean processorClosed = false;
    private BatchHandler batchHandler;
    private volatile int highestRow;
         
    /**
     * Construct a processor with all necessary information to process.
     * @param plan The plan to process
     * @param context The context that this plan is being processed in
     * @param bufferMgr The buffer manager that provides access to tuple sources
     * @param dataMgr The data manager that provides access to get data
     * @throws MetaMatrixComponentException 
     */
    public QueryProcessor(ProcessorPlan plan, CommandContext context, BufferManager bufferMgr, ProcessorDataManager dataMgr) throws MetaMatrixComponentException {
        this.context = context;
		this.dataMgr = dataMgr;
        this.bufferMgr = bufferMgr;
		this.processPlan = plan;

		// Add data manager to all nodes in tree
		this.processPlan.initialize(context, this.dataMgr, bufferMgr);
        this.resultsID = bufferMgr.createTupleSource(processPlan.getOutputElements(), TypeRetrievalUtil.getTypeNames(processPlan.getOutputElements()), context.getConnectionID(), TupleSourceType.PROCESSOR);
    }
    
    public CommandContext getContext() {
		return context;
	}
    
	public Object getProcessID() {
		return this.context.getProcessorID();
	}
	
	public TupleSourceID getResultsID() {
		return resultsID;
	}

    public ProcessorPlan getProcessorPlan() {
        return this.processPlan;
    }

	private void initialize()
		throws BlockedException, MetaMatrixComponentException, MetaMatrixProcessingException {

		// initialize if necessary
		if(! initialized) {
			// Open the top node for reading
			processPlan.open();

            initialized = true;
		}
	}

	/**
	 * Process fully blocking
	 * @throws MetaMatrixCoreException 
	 */
	public void process()
		throws BlockedException, MetaMatrixProcessingException, MetaMatrixComponentException {

        while(true) {
        	try {
        		if (process(Integer.MAX_VALUE)) {
            		break;
            	}
        	} catch(BlockedException e) {
                try {
                    Thread.sleep(DEFAULT_WAIT);
                } catch (InterruptedException err) {
                    throw new MetaMatrixComponentException(err);
                }
        	}             	
        }        
	}
    
	/**
	 * Process until time expires or a {@link BlockedException} occurs
	 * @param time
	 * @return
	 */
	public boolean process(long time)
		throws BlockedException, MetaMatrixProcessingException, MetaMatrixComponentException {
		
	    boolean done = false;
		
	    try {
			// Will only initialize the first time process() is called
			initialize();
	
			long start = System.currentTimeMillis();

            while(System.currentTimeMillis() - start < time && !requestClosed && !processorClosed) {
                checkState();

                TupleBatch batch = processPlan.nextBatch();
                flushBatch(batch);

            	if(batch.getTerminationFlag()) {
            		done = true;
            		break;
            	}
            }
        } catch (BlockedException e) {
            throw e;
        } catch (MetaMatrixException e) {
        	try {
        		closeProcessing();
        	} catch (MetaMatrixException e1){
        		LogManager.logDetail(LogConstants.CTX_DQP, e1, "Error closing processor"); //$NON-NLS-1$
        	}
        	if (e instanceof MetaMatrixProcessingException) {
        		throw (MetaMatrixProcessingException)e;
        	}
        	if (e instanceof MetaMatrixComponentException) {
        		throw (MetaMatrixComponentException)e;
        	}
        	throw new MetaMatrixComponentException(e);
        } finally {
            bufferMgr.releasePinnedBatches();
        }

		if(done || requestClosed) {
			closeProcessing();
			return true;
		} 
		return processorClosed;
	}
                   
    /**
     * Flush the batch by giving it to the buffer manager.
     */
    private void flushBatch(TupleBatch batch) throws MetaMatrixComponentException, MetaMatrixProcessingException {
		if(batch.getRowCount() > 0) {
			this.bufferMgr.addTupleBatch(this.resultsID, batch);
			this.highestRow = batch.getEndRow();
		}
		if (this.batchHandler != null && (batch.getRowCount() > 0 || batch.getTerminationFlag())) {
        	this.batchHandler.batchProduced(batch);
        }
    }
    
    /**
     * Close processing and clean everything up.  Should only be called by the same thread that called process.
     * @throws MetaMatrixComponentException 
     * @throws TupleSourceNotFoundException 
     */
    public void closeProcessing() throws TupleSourceNotFoundException, MetaMatrixComponentException  {
    	if (processorClosed) {
    		return;
    	}
    	if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
    		LogManager.logDetail(LogConstants.CTX_DQP, "QueryProcessor: closing processor"); //$NON-NLS-1$
    	}
        processorClosed = true;
    	    
        try {
        	processPlan.close();
        } finally {
            // Mark tuple source FULL
            this.bufferMgr.setStatus(this.resultsID, TupleSourceStatus.FULL);
        }
    }

    /**
     * Checks whether the request has been canceled of an error has occurred, and throws an exception if either of these conditions is true. 
     * @throws MetaMatrixException if the request has been canceled or an error occurred during processing.
     * @since 4.2
     */
    private void checkState() throws MetaMatrixException {
        if (requestCanceled) {
            throw new MetaMatrixProcessingException(QueryExecPlugin.Util.getString("QueryProcessor.request_cancelled", getProcessID())); //$NON-NLS-1$
        }
    }

    public List<Exception> getAndClearWarnings() {
        return this.processPlan.getAndClearWarnings();
    }
    
    /** 
     * Asynch shutdown of the QueryProcessor, which may trigger exceptions in the processing thread
     */
    public void requestCanceled() {
        this.requestCanceled = true;
    }
    
    /**
     * Asynch graceful shutdown of the QueryProcessor
     */
    public void requestClosed() {
        this.requestClosed = true;        
    }

	public void setBatchHandler(BatchHandler batchHandler) {
		this.batchHandler = batchHandler;
	}

	public int getHighestRow() {
		return highestRow;
	}    
}
