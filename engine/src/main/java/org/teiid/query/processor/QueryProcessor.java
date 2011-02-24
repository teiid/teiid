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

package org.teiid.query.processor;

import java.util.List;

import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.BufferManager.BufferReserveMode;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.Assertion;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.query.QueryPlugin;
import org.teiid.query.processor.BatchCollector.BatchProducer;
import org.teiid.query.util.CommandContext;

/**
 * Driver for plan processing.
 */
public class QueryProcessor implements BatchProducer {
	
	public static class ExpiredTimeSliceException extends TeiidRuntimeException {
		private static final long serialVersionUID = 4585044674826578060L;
	}
	
	private static ExpiredTimeSliceException EXPIRED_TIME_SLICE = new ExpiredTimeSliceException();
	
	public interface ProcessorFactory {
		QueryProcessor createQueryProcessor(String query, String recursionGroup, CommandContext commandContext, Object... params) throws TeiidProcessingException, TeiidComponentException;
	}
	
    private CommandContext context;
	private ProcessorDataManager dataMgr;
	private BufferManager bufferMgr;
	private ProcessorPlan processPlan;
    private boolean initialized;
    private boolean open;
    private int reserved;
    /** Flag that marks whether the request has been canceled. */
    private volatile boolean requestCanceled;
    private static final int DEFAULT_WAIT = 50;       
    private boolean processorClosed;
         
    /**
     * Construct a processor with all necessary information to process.
     * @param plan The plan to process
     * @param context The context that this plan is being processed in
     * @param bufferMgr The buffer manager that provides access to tuple sources
     * @param dataMgr The data manager that provides access to get data
     * @throws TeiidComponentException 
     */
    public QueryProcessor(ProcessorPlan plan, CommandContext context, BufferManager bufferMgr, ProcessorDataManager dataMgr) throws TeiidComponentException {
        this.context = context;
		this.dataMgr = dataMgr;
		this.processPlan = plan;
		this.bufferMgr = bufferMgr;
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
		throws BlockedException, TeiidProcessingException, TeiidComponentException {
		
	    while (true) {
	    	long wait = DEFAULT_WAIT;
	    	try {
	    		return nextBatchDirect();
	    	} catch (BlockedException e) {
	    		if (!this.context.isNonBlocking()) {
	    			throw e;
	    		}
	    	}
    		try {
                Thread.sleep(wait);
            } catch (InterruptedException err) {
                throw new TeiidComponentException(err);
            }
	    }
	}
	
	private TupleBatch nextBatchDirect()
		throws BlockedException, TeiidProcessingException, TeiidComponentException {
		
	    boolean done = false;
	    TupleBatch result = null;
		
	    try {
	    	init(); 
			long currentTime = System.currentTimeMillis();
			Assertion.assertTrue(!processorClosed);
			
			//TODO: see if there is pending work before preempting
			
	        while(currentTime < context.getTimeSliceEnd() || context.isNonBlocking()) {
	        	if (requestCanceled) {
	                throw new TeiidProcessingException(QueryPlugin.Util.getString("QueryProcessor.request_cancelled", getProcessID())); //$NON-NLS-1$
	            }
	        	if (currentTime > context.getTimeoutEnd()) {
	        		throw new TeiidProcessingException("Query timed out"); //$NON-NLS-1$
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
	    } catch (TeiidException e) {
    		closeProcessing();
	    	if (e instanceof TeiidProcessingException) {
	    		throw (TeiidProcessingException)e;
	    	}
	    	if (e instanceof TeiidComponentException) {
	    		throw (TeiidComponentException)e;
	    	}
	    	throw new TeiidComponentException(e);
	    }
		if(done) {
			closeProcessing();
		} 
	    if (result == null) {
	    	throw EXPIRED_TIME_SLICE;
	    }
		return result;
	}

	private void init() throws TeiidComponentException, TeiidProcessingException {
		// initialize if necessary
		if(!initialized) {
			reserved = this.bufferMgr.reserveBuffers(this.bufferMgr.getSchemaSize(this.getOutputElements()), BufferReserveMode.FORCE);
			this.processPlan.initialize(context, this.dataMgr, bufferMgr);
			initialized = true;
		}
		
		if (!open) {
			// Open the top node for reading
			processPlan.open();
			open = true;
		}
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
		} catch (TeiidComponentException e1){
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
    
	public BatchCollector createBatchCollector() throws TeiidComponentException {
		return new BatchCollector(this, this.bufferMgr, this.context, false);
	}
	
	public void setNonBlocking(boolean nonBlocking) {
		this.context.setNonBlocking(nonBlocking);
	}

	@Override
	public TupleBuffer getFinalBuffer() throws BlockedException, TeiidComponentException, TeiidProcessingException {
		while (true) {
	    	long wait = DEFAULT_WAIT;
	    	try {
	    		init();
	    		return this.processPlan.getFinalBuffer();
	    	} catch (BlockedException e) {
	    		if (!this.context.isNonBlocking()) {
	    			throw e;
	    		}
	    	} catch (TeiidComponentException e) {
	    		closeProcessing();
	    		throw e;
	    	} catch (TeiidProcessingException e) {
	    		closeProcessing();
	    		throw e;
	    	}
    		try {
                Thread.sleep(wait);
            } catch (InterruptedException err) {
                throw new TeiidComponentException(err);
            }
	    }
	}

	@Override
	public boolean hasFinalBuffer() {
		return this.processPlan.hasFinalBuffer();
	}
	
	public BufferManager getBufferManager() {
		return bufferMgr;
	}
}
