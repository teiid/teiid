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
import com.metamatrix.common.buffer.BufferManager.BufferReserveMode;
import com.metamatrix.common.buffer.BufferManager.TupleSourceType;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.LogConstants;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.query.execution.QueryExecPlugin;
import com.metamatrix.query.processor.BatchCollector.BatchProducer;
import com.metamatrix.query.util.CommandContext;

public class QueryProcessor implements BatchProducer {
	
	public static class ExpiredTimeSliceException extends MetaMatrixRuntimeException {
		
	}
	
	private static ExpiredTimeSliceException EXPIRED_TIME_SLICE = new ExpiredTimeSliceException();
	
	public interface ProcessorFactory {
		QueryProcessor createQueryProcessor(String query, String recursionGroup, CommandContext commandContext) throws MetaMatrixProcessingException, MetaMatrixComponentException;
	}
	
    private CommandContext context;
	private ProcessorDataManager dataMgr;
	private BufferManager bufferMgr;
	private ProcessorPlan processPlan;
    private boolean initialized = false;
    private int reserved;
    /** Flag that marks whether the request has been canceled. */
    private volatile boolean requestCanceled = false;
    private static final int DEFAULT_WAIT = 50;       
    private boolean processorClosed = false;
    
    private boolean nonBlocking = false;
         
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
		this.processPlan = plan;
		this.bufferMgr = bufferMgr;
		// Add data manager to all nodes in tree
		this.processPlan.initialize(context, this.dataMgr, bufferMgr);
    }
    
    public CommandContext getContext() {
		return context;
	}
    
	public Object getProcessID() {
		return this.context.getProcessorID();
	}
	
    public ProcessorPlan getProcessorPlan() {
        return this.processPlan;
    }

	public TupleBatch nextBatch()
		throws BlockedException, MetaMatrixProcessingException, MetaMatrixComponentException {
		
	    while (true) {
	    	try {
	    		return nextBatchDirect();
	    	} catch (ExpiredTimeSliceException e) {
	    		if (!nonBlocking) {
	    			throw e;
	    		}
	    	} catch (BlockedException e) {
	    		if (!nonBlocking) {
	    			throw e;
	    		}
	    		try {
	                Thread.sleep(DEFAULT_WAIT);
	            } catch (InterruptedException err) {
	                throw new MetaMatrixComponentException(err);
	            }
	    	}
	    }
	}
	
	private TupleBatch nextBatchDirect()
		throws BlockedException, MetaMatrixProcessingException, MetaMatrixComponentException {
		
	    boolean done = false;
	    TupleBatch result = null;
		
	    try {
	    	// initialize if necessary
			if(! initialized) {
				if (reserved == 0) {
					reserved = this.bufferMgr.reserveBuffers(this.bufferMgr.getSchemaSize(this.getOutputElements()), BufferReserveMode.FORCE);
				}
				// Open the top node for reading
				processPlan.open();
				initialized = true;
			}
	
			long currentTime = System.currentTimeMillis();
			Assertion.assertTrue(!processorClosed);
	        while(currentTime < context.getTimeSliceEnd()) {
	        	if (requestCanceled) {
	                throw new MetaMatrixProcessingException(QueryExecPlugin.Util.getString("QueryProcessor.request_cancelled", getProcessID())); //$NON-NLS-1$
	            }
	        	if (currentTime > context.getTimeoutEnd()) {
	        		throw new MetaMatrixProcessingException("Query timed out"); //$NON-NLS-1$
	        	}
	            result = processPlan.nextBatch();
	
	        	if(result.getTerminationFlag()) {
	        		done = true;
	        		break;
	        	}
	        	
	        	if (result.getRowCount() > 0) {
	        		break;
	        	}
	        	
	        }
	    } catch (BlockedException e) {
	    	throw e;
	    } catch (MetaMatrixException e) {
    		closeProcessing();
	    	if (e instanceof MetaMatrixProcessingException) {
	    		throw (MetaMatrixProcessingException)e;
	    	}
	    	if (e instanceof MetaMatrixComponentException) {
	    		throw (MetaMatrixComponentException)e;
	    	}
	    	throw new MetaMatrixComponentException(e);
	    }
		if(done) {
			closeProcessing();
		} 
	    if (result == null) {
	    	throw EXPIRED_TIME_SLICE;
	    }
		return result;
	}

	                   
    /**
     * Close processing and clean everything up.  Should only be called by the same thread that called process.
     */
    public void closeProcessing() {
    	if (processorClosed) {
    		return;
    	}
    	if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
    		LogManager.logDetail(LogConstants.CTX_DQP, "QueryProcessor: closing processor"); //$NON-NLS-1$
    	}
		this.bufferMgr.releaseBuffers(reserved);
		reserved = 0;
        processorClosed = true;
        try {
        	processPlan.close();
		} catch (MetaMatrixComponentException e1){
			LogManager.logDetail(LogConstants.CTX_DQP, e1, "Error closing processor"); //$NON-NLS-1$
		}
    }

    @Override
    public List getOutputElements() {
    	return this.processPlan.getOutputElements();
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
    
	public BatchCollector createBatchCollector() throws MetaMatrixComponentException {
		return new BatchCollector(this, this.bufferMgr.createTupleBuffer(this.processPlan.getOutputElements(), context.getConnectionID(), TupleSourceType.PROCESSOR));
	}
	
	public void setNonBlocking(boolean nonBlocking) {
		this.nonBlocking = nonBlocking;
	}
}
