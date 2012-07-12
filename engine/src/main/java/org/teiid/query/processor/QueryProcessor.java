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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.TupleSource;
import org.teiid.common.buffer.BufferManager.BufferReserveMode;
import org.teiid.common.buffer.BufferManager.TupleSourceType;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.Assertion;
import org.teiid.events.EventDistributor;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.query.QueryPlugin;
import org.teiid.query.processor.BatchCollector.BatchProducer;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.util.CommandContext;

/**
 * Driver for plan processing.
 */
public class QueryProcessor implements BatchProducer, ProcessorDataManager {

	public static class ExpiredTimeSliceException extends TeiidRuntimeException {
		private static final long serialVersionUID = 4585044674826578060L;
	}
	
	private static ExpiredTimeSliceException EXPIRED_TIME_SLICE = new ExpiredTimeSliceException();
	
	public interface ProcessorFactory {
		QueryProcessor createQueryProcessor(String query, String recursionGroup, CommandContext commandContext, Object... params) throws TeiidProcessingException, TeiidComponentException;
	}
	
    private class SharedState {
    	TupleBuffer tb;
    	TupleSource ts;
    	int id;
    	int expectedReaders;
    	
    	private void remove() {
    		ts.closeSource();
			tb.remove();
			tb = null;
			ts = null;
    	}
    }
    
	private final class BufferedTupleSource implements TupleSource {
		private int rowNumber = 1;
		private SharedState state;
		
		private BufferedTupleSource(SharedState state) {
			this.state = state;
		}

		@Override
		public List<?> nextTuple() throws TeiidComponentException,
				TeiidProcessingException {
			if (rowNumber <= state.tb.getRowCount()) {
				return state.tb.getBatch(rowNumber).getTuple(rowNumber++);
			}
			if (state.tb.isFinal()) {
				return null;
			}
			List<?> row = state.ts.nextTuple();
			if (row == null) {
				state.tb.setFinal(true);
			} else {
				this.state.tb.addTuple(row);
				rowNumber++;
			}
			return row;
		}

		@Override
		public void closeSource() {
			if (--state.expectedReaders == 0 && sharedStates != null && sharedStates.containsKey(state.id)) {
				state.remove();
				sharedStates.remove(state.id);
			}
		}
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
    private boolean continuous;
    private int rowOffset = 1;
    
    Map<Integer, SharedState> sharedStates;
         
    /**
     * Construct a processor with all necessary information to process.
     * @param plan The plan to process
     * @param context The context that this plan is being processed in
     * @param bufferMgr The buffer manager that provides access to tuple sources
     * @param dataMgr The data manager that provides access to get data
     * @throws TeiidComponentException 
     */
    public QueryProcessor(ProcessorPlan plan, CommandContext context, BufferManager bufferMgr, final ProcessorDataManager dataMgr) {
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
                 throw new TeiidComponentException(QueryPlugin.Event.TEIID30159, err);
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
	                 throw new TeiidProcessingException(QueryPlugin.Event.TEIID30160, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30160, getProcessID()));
	            }
	        	if (currentTime > context.getTimeoutEnd()) {
	        		 throw new TeiidProcessingException(QueryPlugin.Event.TEIID30161, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30161));
	        	}
	            result = processPlan.nextBatch();

	            if (continuous) {
	        		result.setRowOffset(rowOffset);
	        		rowOffset = result.getEndRow() + 1;

	        		if (result.getTerminationFlag()) {
	        			result.setTerminationFlag(false);
		        		this.processPlan.close();
		        		this.processPlan.reset();
		        		this.context.incrementReuseCount();
		        		this.open = false;	
	        		}
	            }
        		
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
	    	 throw new TeiidComponentException(QueryPlugin.Event.TEIID30162, e);
	    }
		if(done) {
			closeProcessing();
		} 
	    if (result == null) {
	    	throw EXPIRED_TIME_SLICE;
	    }
		return result;
	}

	public void init() throws TeiidComponentException, TeiidProcessingException {
		// initialize if necessary
		if(!initialized) {
			reserved = this.bufferMgr.reserveBuffers(this.bufferMgr.getSchemaSize(this.getOutputElements()), BufferReserveMode.FORCE);
			this.processPlan.initialize(context, this, bufferMgr);
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
    	if (sharedStates != null) {
    		for (SharedState ss : sharedStates.values()) {
				ss.remove();
			}
    		sharedStates = null;
    	}
		this.bufferMgr.releaseBuffers(reserved);
		reserved = 0;
        processorClosed = true;
        if (initialized) {
	        try {
	        	processPlan.close();
			} catch (TeiidComponentException e1){
				LogManager.logDetail(LogConstants.CTX_DQP, e1, "Error closing processor"); //$NON-NLS-1$
			}
        }
    }

    @Override
    public List getOutputElements() {
    	return this.processPlan.getOutputElements();
    }

    public List<Exception> getAndClearWarnings() {
        return this.context.getAndClearWarnings();
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
                 throw new TeiidComponentException(QueryPlugin.Event.TEIID30163, err);
            }
	    }
	}

	@Override
	public boolean hasFinalBuffer() {
		return !continuous && this.processPlan.hasFinalBuffer();
	}
	
	public BufferManager getBufferManager() {
		return bufferMgr;
	}
	
	public void setContinuous(boolean continuous) {
		this.continuous = continuous;
	}

	@Override
	public Object lookupCodeValue(CommandContext ctx, String codeTableName,
			String returnElementName, String keyElementName, Object keyValue)
			throws BlockedException, TeiidComponentException,
			TeiidProcessingException {
		return dataMgr.lookupCodeValue(ctx, codeTableName, returnElementName, keyElementName, keyValue);
	}
	
	@Override
	public EventDistributor getEventDistributor() {
		return dataMgr.getEventDistributor();
	}

	@Override
	public TupleSource registerRequest(CommandContext ctx, Command command,
			String modelName, RegisterRequestParameter parameterObject)
			throws TeiidComponentException, TeiidProcessingException {
		if (parameterObject.info == null) {
			return dataMgr.registerRequest(ctx, command, modelName, parameterObject);
		}
		//begin handling of shared commands
		if (sharedStates == null) {
			sharedStates = new HashMap<Integer, SharedState>();
		}
		SharedState state = sharedStates.get(parameterObject.info.id);
		if (state == null) {
			state = new SharedState();
			state.expectedReaders = parameterObject.info.sharingCount;
			state.tb = QueryProcessor.this.bufferMgr.createTupleBuffer(command.getProjectedSymbols(), ctx.getConnectionId(), TupleSourceType.PROCESSOR);
			state.ts = dataMgr.registerRequest(ctx, command, modelName, new RegisterRequestParameter(parameterObject.connectorBindingId, 0, -1));
			state.id = parameterObject.info.id;
			sharedStates.put(parameterObject.info.id, state);
		}
		return new BufferedTupleSource(state);
	}
}
