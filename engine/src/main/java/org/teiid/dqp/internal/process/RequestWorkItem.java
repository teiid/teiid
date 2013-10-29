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

package org.teiid.dqp.internal.process;

import java.lang.ref.WeakReference;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.teiid.client.RequestMessage;
import org.teiid.client.RequestMessage.ShowPlan;
import org.teiid.client.ResizingArrayList;
import org.teiid.client.ResultsMessage;
import org.teiid.client.lob.LobChunk;
import org.teiid.client.metadata.ParameterInfo;
import org.teiid.client.util.ResultsReceiver;
import org.teiid.client.xa.XATransactionException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager.TupleSourceType;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.dqp.internal.process.AuthorizationValidator.CommandType;
import org.teiid.dqp.internal.process.DQPCore.CompletionListener;
import org.teiid.dqp.internal.process.SessionAwareCache.CacheID;
import org.teiid.dqp.internal.process.ThreadReuseExecutor.PrioritizedRunnable;
import org.teiid.dqp.message.AtomicRequestID;
import org.teiid.dqp.message.RequestID;
import org.teiid.dqp.service.TransactionContext;
import org.teiid.dqp.service.TransactionContext.Scope;
import org.teiid.dqp.service.TransactionService;
import org.teiid.jdbc.EnhancedTimer.Task;
import org.teiid.jdbc.SQLStates;
import org.teiid.logging.CommandLogMessage.Event;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.parser.ParseInfo;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.processor.BatchCollector;
import org.teiid.query.processor.QueryProcessor;
import org.teiid.query.processor.QueryProcessor.ExpiredTimeSliceException;
import org.teiid.query.sql.lang.CacheHint;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.SPParameter;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.Symbol;
import org.teiid.query.util.CommandContext;
import org.teiid.query.util.GeneratedKeysImpl;

public class RequestWorkItem extends AbstractWorkItem implements PrioritizedRunnable {
	
	//TODO: this could be configurable
	private static final int OUTPUT_BUFFER_MAX_BATCHES = 8;
	private static final int CLIENT_FETCH_MAX_BATCHES = 3;
	
	public static final class MoreWorkTask implements Runnable {

		WeakReference<RequestWorkItem> ref;

    	public MoreWorkTask(RequestWorkItem workItem) {
    		ref = new WeakReference<RequestWorkItem>(workItem);
		}

		@Override
		public void run() {
			RequestWorkItem item = ref.get();
			if (item != null) {
				item.moreWork();
			}
		}
	}

	private final class WorkWrapper<T> implements
			DQPCore.CompletionListener<T> {
		
		boolean submitted;
		FutureWork<T> work;
		
		public WorkWrapper(FutureWork<T> work) {
			this.work = work;
		}

		@Override
		public void onCompletion(FutureWork<T> future) {
			WorkWrapper<?> nextWork = null;
			synchronized (queue) {
				if (!submitted) {
					return;
				}
				synchronized (RequestWorkItem.this) {
					if (isProcessing()) {
						totalThreads--;
						moreWork();
						return;
					}
				}
				nextWork = queue.pollFirst();
				if (nextWork == null) {
					totalThreads--;
				} else {
					nextWork.submitted = true;
				}
			}
			if (nextWork != null) {
				dqpCore.addWork(nextWork.work);
			}    		
		}
	}

	private enum ProcessingState {NEW, PROCESSING, CLOSE}
	private ProcessingState state = ProcessingState.NEW;
    
	private enum TransactionState {NONE, ACTIVE, DONE}
	private TransactionState transactionState = TransactionState.NONE;
	
	private int totalThreads;
	private LinkedList<WorkWrapper<?>> queue = new LinkedList<WorkWrapper<?>>();
	
	/*
	 * Obtained at construction time 
	 */
	protected final DQPCore dqpCore;
    final RequestMessage requestMsg;    
    final RequestID requestID;
    private Request request; //provides the processing plan, held on a temporary basis
    private final int processorTimeslice;
	private CacheID cid;
	private final TransactionService transactionService;
	private final DQPWorkContext dqpWorkContext;
	boolean active;
	
    /*
     * obtained during new
     */
    private volatile QueryProcessor processor;
    private BatchCollector collector;
    private Command originalCommand;
    private AnalysisRecord analysisRecord;
    private TransactionContext transactionContext;
    TupleBuffer resultsBuffer;
    private boolean returnsUpdateCount;
    
    /*
     * maintained during processing
     */
    private Throwable processingException;
    private Map<AtomicRequestID, DataTierTupleSource> connectorInfo = Collections.synchronizedMap(new HashMap<AtomicRequestID, DataTierTupleSource>(4));
    private volatile boolean doneProducingBatches;
    private volatile boolean isClosed;
    private volatile boolean isCanceled;
    private volatile boolean closeRequested;

	//results request
	private ResultsReceiver<ResultsMessage> resultsReceiver;
	private int begin;
	private int end;
    private TupleBatch savedBatch;
    private Map<Integer, LobWorkItem> lobStreams = Collections.synchronizedMap(new HashMap<Integer, LobWorkItem>(4));    
    
    /**The time when command begins processing on the server.*/
    private long processingTimestamp = System.currentTimeMillis();
    
    protected boolean useCallingThread;
    private volatile boolean hasThread;
    
    private Future<Void> cancelTask;
    private Future<Void> moreWorkTask;

	private boolean explicitSourceClose;
	private int schemaSize;
    
    public RequestWorkItem(DQPCore dqpCore, RequestMessage requestMsg, Request request, ResultsReceiver<ResultsMessage> receiver, RequestID requestID, DQPWorkContext workContext) {
        this.requestMsg = requestMsg;
        this.requestID = requestID;
        this.processorTimeslice = dqpCore.getProcessorTimeSlice();
        this.transactionService = dqpCore.getTransactionService();
        this.dqpCore = dqpCore;
        this.request = request;
        this.dqpWorkContext = workContext;
        this.requestResults(1, requestMsg.getFetchSize(), receiver);
    }
    
    private boolean isForwardOnly() {
    	return this.cid == null && requestMsg.getCursorType() == ResultSet.TYPE_FORWARD_ONLY;    	
    }
    
	/**
	 * Ask for results.
	 * @param beginRow
	 * @param endRow
	 */
    synchronized void requestResults(int beginRow, int endRow, ResultsReceiver<ResultsMessage> receiver) {
		if (this.resultsReceiver != null) {
			throw new IllegalStateException("Results already requested"); //$NON-NLS-1$\
		}
		this.resultsReceiver = receiver;
		this.begin = beginRow;
		this.end = endRow;
	}
    
	@Override
	protected boolean isDoneProcessing() {
		return isClosed;
	}
		
	@Override
	public void run() {
		hasThread = true;
		try {
			while (!isDoneProcessing()) {
				super.run();
				if (!useCallingThread) {
					break;
				}
				//should use the calling thread
				synchronized (this) {
					if (this.resultsReceiver == null) {
						break; //allow results to be processed by calling thread
					}
					if (this.getThreadState() == ThreadState.MORE_WORK) {
						continue;
					}
					try {
						wait();
					} catch (InterruptedException e) {
						try {
							requestCancel();
						} catch (TeiidComponentException e1) {
							 throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30543, e1);
						}
					}
				}
			}
		} finally {
			hasThread = false;
		}
	}

	@Override
	protected void resumeProcessing() {
		if (!this.useCallingThread) {
			dqpCore.addWork(this);
		}
	}
	
	/**
	 * Special call from request threads to allow resumption of processing by
	 * the calling thread.
	 */
	public void doMoreWork() {
		boolean run = false;
		synchronized (this) {
			moreWork();
			if (!useCallingThread || this.getThreadState() != ThreadState.MORE_WORK) {
				return;
			}
			run = !hasThread;
		}
		if (run) {
			//run outside of the lock
			LogManager.logDetail(LogConstants.CTX_DQP, "Restarting processing using the calling thread", requestID); //$NON-NLS-1$
			run();
		}
	}
	
	@Override
	protected void process() {
        LogManager.logDetail(LogConstants.CTX_DQP, "Request Thread", requestID, "with state", state); //$NON-NLS-1$ //$NON-NLS-2$
        try {
            if (this.state == ProcessingState.NEW) {
                state = ProcessingState.PROCESSING;
        		processNew();
                if (isCanceled) {
                	setCanceledException();
                    state = ProcessingState.CLOSE;
                } 
        	}
        	
            resume();
        	
            if (this.state == ProcessingState.PROCESSING) {
            	if (!this.closeRequested) {
            		processMore();
            	}
            	if (this.closeRequested) {
            		this.state = ProcessingState.CLOSE;
            	}
            }                  	            
        } catch (BlockedException e) {
        	if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
        		LogManager.logDetail(LogConstants.CTX_DQP, "Request Thread", requestID, "- processor blocked"); //$NON-NLS-1$ //$NON-NLS-2$
        	}
        	if (e == BlockedException.BLOCKED_ON_MEMORY_EXCEPTION || e instanceof ExpiredTimeSliceException) {
        		//requeue
        		this.moreWork();
        	}
        } catch (Throwable e) {
        	handleThrowable(e);
        } finally {
        	if (isClosed) {
        		/*
        		 * since there may be a client waiting notify them of a problem
        		 */
        		if (this.processingException == null) {
        			this.processingException = new IllegalStateException("Request is already closed"); //$NON-NLS-1$
        		}
        		sendError();
        	} else if (this.state == ProcessingState.CLOSE) {
        		close();
        	}
        	suspend();
        }
    }

	private void setCanceledException() {
		this.processingException = new TeiidProcessingException(QueryPlugin.Event.TEIID30563, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30563, this.requestID));
	}

	private void handleThrowable(Throwable e) {
		if (!isCanceled()) {
			dqpCore.logMMCommand(this, Event.ERROR, null);
		    //Case 5558: Differentiate between system level errors and
		    //processing errors.  Only log system level errors as errors, 
		    //log the processing errors as warnings only
		    if(e instanceof TeiidProcessingException) {                          
		    	Throwable cause = e;
		    	while (cause.getCause() != null && cause.getCause() != cause) {
		    		cause = cause.getCause();
		    	}
		    	StackTraceElement[] elems = cause.getStackTrace();
		    	Object elem = null;
		    	String causeMsg = cause.getMessage();
	    		if (elems.length > 0) {
		    		StackTraceElement ste = cause.getStackTrace()[0];
		    		if (ste.getLineNumber() > 0 && ste.getFileName() != null) {
		    			elem = ste.getFileName() + ":" + ste.getLineNumber(); //$NON-NLS-1$
		    		} else {
		    			elem = ste;
		    		}
		    		String msg = e.getMessage();
		    		if (causeMsg != null && cause != e && !msg.contains(causeMsg)) {
		    			elem = "'" + causeMsg + "' " + elem; //$NON-NLS-1$ //$NON-NLS-2$
		    		} 
		    	} else if (cause != e && causeMsg != null) {
		    		elem = "'" + cause.getMessage() + "'"; //$NON-NLS-1$ //$NON-NLS-2$
		    	}
		        String msg = QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30020, e.getMessage(), requestID, e.getClass().getSimpleName(), elem);
		    	if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
		    		LogManager.logWarning(LogConstants.CTX_DQP, e, msg);
		    	} else {
		    		LogManager.logWarning(LogConstants.CTX_DQP, msg + QueryPlugin.Util.getString("stack_info")); //$NON-NLS-1$		    		
		    	}
		    } else {
		        LogManager.logError(LogConstants.CTX_DQP, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30019, requestID));
		    }                             
		} else {
			LogManager.logDetail(LogConstants.CTX_DQP, e, "Request Thread", requestID, "- error occurred after cancel"); //$NON-NLS-1$ //$NON-NLS-2$
		}
		
		this.processingException = e;
		this.state = ProcessingState.CLOSE;
	}

	private void resume() throws XATransactionException {
		if (this.transactionState == TransactionState.ACTIVE) {
			this.transactionService.resume(this.transactionContext);
		}
	}

	private boolean isSuspendable() {
		return this.transactionContext.getTransaction() != null && !(this.useCallingThread && this.transactionContext.getTransactionType() == Scope.GLOBAL);
	}

	private void suspend() {
		if (this.transactionState != TransactionState.NONE && isSuspendable()) {
			try {
				this.transactionService.suspend(this.transactionContext);
			} catch (XATransactionException e) {
				LogManager.logDetail(LogConstants.CTX_DQP, e, "Error suspending active transaction"); //$NON-NLS-1$
			}
		}
	}

	protected void processMore() throws BlockedException, TeiidException {
		if (!doneProducingBatches) {
			synchronized (queue) {
				while (!queue.isEmpty() && totalThreads < dqpCore.getUserRequestSourceConcurrency()) {
					WorkWrapper<?> w = queue.removeFirst();
	        		dqpCore.addWork(w.work);
	        		w.submitted = true;
	        		totalThreads++;
	        	}
			}
			this.processor.getContext().setTimeSliceEnd(System.currentTimeMillis() + this.processorTimeslice);
			sendResultsIfNeeded(null);
			try {
				CommandContext.pushThreadLocalContext(this.processor.getContext());
				this.resultsBuffer = collector.collectTuples();
			} finally {
				CommandContext.popThreadLocalContext();
			}
			if (!doneProducingBatches) {
				done();
			}
		}
		if (this.transactionState == TransactionState.ACTIVE) {
			this.transactionState = TransactionState.DONE;
			if (transactionContext.getTransactionType() == TransactionContext.Scope.REQUEST) {
				/*
				 * TEIID-14 if we are done producing batches, then proactively close transactional 
				 * executions even ones that were intentionally kept alive. this may 
				 * break the read of a lob from a transactional source under a transaction 
				 * if the source does not support holding the clob open after commit
				 */
	        	for (DataTierTupleSource connectorRequest : getConnectorRequests()) {
	        		if (connectorRequest.isTransactional()) {
	        			connectorRequest.fullyCloseSource();
	        		}
	            }
				this.transactionService.commit(transactionContext);
			} else {
				suspend();
			}
		}
		sendResultsIfNeeded(null);
	}

	/**
	 * Client close is currently implemented as asynch.
	 * Any errors that occur will not make it to the client, instead we just log them here.
	 */
	protected void close() {
		int rowcount = -1;
		try {
			cancelCancelTask();
			if (moreWorkTask != null) {
				moreWorkTask.cancel(false);
				moreWorkTask = null;
			}
			if (this.resultsBuffer != null) {
				if (this.processor != null) {
					this.processor.closeProcessing();
				
					if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
				        LogManager.logDetail(LogConstants.CTX_DQP, "Removing tuplesource for the request " + requestID); //$NON-NLS-1$
				    }
					rowcount = resultsBuffer.getRowCount();
					if (this.cid == null || !this.doneProducingBatches) {
						resultsBuffer.remove();
					} else {
						try {
							this.resultsBuffer.persistLobs();
						} catch (TeiidComponentException e) {
							LogManager.logDetail(LogConstants.CTX_DQP, QueryPlugin.Util.getString("failed_to_cache")); //$NON-NLS-1$
						}
					}
					
					for (DataTierTupleSource connectorRequest : getConnectorRequests()) {
						connectorRequest.fullyCloseSource();
				    }
					
					CommandContext cc = this.processor.getContext();
					cc.close();
				}
	
				this.resultsBuffer = null;
				
				if (!this.lobStreams.isEmpty()) {
					List<LobWorkItem> lobs = null;
					synchronized (lobStreams) {
						lobs = new ArrayList<LobWorkItem>(this.lobStreams.values());
					}
					for (LobWorkItem lobWorkItem : lobs) {
						lobWorkItem.close();
					}
				}
			}
	
			if (this.transactionState == TransactionState.ACTIVE) { 
				this.transactionState = TransactionState.DONE;
	            if (transactionContext.getTransactionType() == TransactionContext.Scope.REQUEST) {
					try {
		        		this.transactionService.rollback(transactionContext);
		            } catch (XATransactionException e1) {
		                LogManager.logWarning(LogConstants.CTX_DQP, e1, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30028));
		            } 
				} else {
					suspend();
				}
			}
			
	        synchronized (this) {
		        if (this.processingException == null && this.resultsReceiver != null) {
		        	//sanity check to ensure that something will be sent to the client
		        	setCanceledException();
		        }
			}
		} catch (Throwable t) {
			handleThrowable(t);
		} finally {
			isClosed = true;
			
			dqpCore.removeRequest(this);
		    
			if (this.processingException != null) {
				sendError();			
			} else {
		        dqpCore.logMMCommand(this, Event.END, rowcount);
			}
		}
	}

	private void cancelCancelTask() {
		if (this.cancelTask != null) {
			this.cancelTask.cancel(false);
			this.cancelTask = null;
		}
	}

	protected void processNew() throws TeiidProcessingException, TeiidComponentException {
		SessionAwareCache<CachedResults> rsCache = dqpCore.getRsCache();
				
		boolean cachable = false;
		CacheID cacheId = null;
		boolean canUseCached = !requestMsg.getRequestOptions().isContinuous() && (requestMsg.useResultSetCache() || 
				getCacheHint() != null);
		
		if (rsCache != null) {
			if (!canUseCached) {
				LogManager.logDetail(LogConstants.CTX_DQP, requestID, "Non-cachable command."); //$NON-NLS-1$
			} else {
				ParseInfo pi = Request.createParseInfo(requestMsg);
				cacheId = new CacheID(this.dqpWorkContext, pi, requestMsg.getCommandString());
		    	cachable = cacheId.setParameters(requestMsg.getParameterValues());
				if (cachable) {
					//allow cache to be transactionally aware
					if (rsCache.isTransactional()) {
						TransactionContext tc = request.getTransactionContext(false);
						if (tc != null && tc.getTransactionType() != Scope.NONE) {
							initTransactionState(tc);
							resume();
						}
					}
					CachedResults cr = rsCache.get(cacheId);
					//check that there are enough cached results
					//TODO: possibly ignore max rows for caching
					if (cr != null && (cr.getRowLimit() == 0 || (requestMsg.getRowLimit() != 0 && requestMsg.getRowLimit() <= cr.getRowLimit()))) {
						this.resultsBuffer = cr.getResults();
						request.initMetadata();
						this.originalCommand = cr.getCommand(requestMsg.getCommandString(), request.metadata, pi);
						if (!request.validateAccess(requestMsg.getCommands(), this.originalCommand, CommandType.CACHED)) {
							doneProducingBatches();
							return;
						}
						LogManager.logDetail(LogConstants.CTX_DQP, requestID, "Cached result command to be modified, will not use the cached results", cacheId); //$NON-NLS-1$
					} 
				} else {
					LogManager.logDetail(LogConstants.CTX_DQP, requestID, "Parameters are not serializable - cache cannot be used for", cacheId); //$NON-NLS-1$
				}
			}
		}
		try {
			request.processRequest();
		} finally {
			analysisRecord = request.analysisRecord;
		}
		originalCommand = request.userCommand;
        if (cachable && (requestMsg.useResultSetCache() || originalCommand.getCacheHint() != null) && rsCache != null && originalCommand.areResultsCachable()) {
        	this.cid = cacheId;
        	//turn on the collection of data objects used
        	request.processor.getContext().setDataObjects(new HashSet<Object>(4));
        }
        request.processor.getContext().setWorkItem(this);
		processor = request.processor;
		this.dqpCore.logMMCommand(this, Event.PLAN, null);
		collector = new BatchCollector(processor, processor.getBufferManager(), this.request.context, isForwardOnly()) {
			
			int maxRows = 0;
			
			@Override
			protected void flushBatchDirect(TupleBatch batch, boolean add) throws TeiidComponentException,TeiidProcessingException {
				resultsBuffer = getTupleBuffer();
				if (maxRows == 0) {
					maxRows = OUTPUT_BUFFER_MAX_BATCHES * resultsBuffer.getBatchSize();
				}
				if (cid != null) {
					super.flushBatchDirect(batch, add);
				}
				synchronized (lobStreams) {
					if (resultsBuffer.isLobs()) {
						super.flushBatchDirect(batch, false);
					}
					if (batch.getTerminationFlag()) {
						done();
					}
					add = sendResultsIfNeeded(batch);
					if (cid != null) {
						return;
					}
					super.flushBatchDirect(batch, add);
					if (!add && !processor.hasBuffer(false)) {
						resultsBuffer.setRowCount(batch.getEndRow());
					}
					if (transactionState != TransactionState.ACTIVE && (requestMsg.getRequestOptions().isContinuous() || (useCallingThread && isForwardOnly()))) {
			        	synchronized (this) {
							if (resultsReceiver == null) {
					        	throw BlockedException.block(requestID, "Blocking to allow asynch processing"); //$NON-NLS-1$            	
							}
						}
			        	if (add) {
			        		throw new AssertionError("Should not add batch to buffer"); //$NON-NLS-1$
			        	}
			        }
					if (isForwardOnly() && add 
							&& !processor.hasBuffer(false) //restrict the buffer size for forward only results
							&& !batch.getTerminationFlag() 
							&& transactionState != TransactionState.ACTIVE
							&& resultsBuffer.getManagedRowCount() >= maxRows) {
						int timeOut = 500;
						if (!connectorInfo.isEmpty()) {
							if (explicitSourceClose) {
								for (DataTierTupleSource ts : getConnectorRequests()) {
									if (!ts.isExplicitClose()) {
										timeOut = 100;
										break;
									}
								}
							} else {
								timeOut = 100;	
							}
						}
						if (dqpCore.blockOnOutputBuffer(RequestWorkItem.this)) {
							if (moreWorkTask != null) {
								moreWorkTask.cancel(false);
								moreWorkTask = null;
							}
							if (getThreadState() != ThreadState.MORE_WORK) {
								//we schedule the work to ensure that an idle client won't just indefinitely hold resources
								moreWorkTask = scheduleWork(timeOut); 
							}
							throw BlockedException.block(requestID, "Blocking due to full results TupleBuffer", //$NON-NLS-1$
									this.getTupleBuffer(), "rows", this.getTupleBuffer().getManagedRowCount(), "batch size", this.getTupleBuffer().getBatchSize()); //$NON-NLS-1$ //$NON-NLS-2$ 
						} 
						if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
							LogManager.logDetail(LogConstants.CTX_DQP, requestID, "Exceeding buffer limit since there are pending active plans or this is using the calling thread."); //$NON-NLS-1$
						}
					}
				}
			}
		};
		if (!request.addedLimit && this.requestMsg.getRowLimit() > 0 && this.requestMsg.getRowLimit() < Integer.MAX_VALUE) {
			//covers maxrows for commands that already have a limit, are prepared, or are a stored procedure
        	this.collector.setRowLimit(this.requestMsg.getRowLimit());
    		this.collector.setSaveLastRow(request.isReturingParams());
        }
		this.resultsBuffer = collector.getTupleBuffer();
		if (this.resultsBuffer == null) {
			//This is just a dummy result it will get replaced by collector source
	    	resultsBuffer = this.processor.getBufferManager().createTupleBuffer(this.originalCommand.getProjectedSymbols(), this.request.context.getConnectionId(), TupleSourceType.FINAL);
		} else if (this.requestMsg.getRequestOptions().isContinuous()) {
			//TODO: this is based upon continuous being an embedded connection otherwise we have to do something like
			//forcing inlining, but truncating or erroring over a given size (similar to odbc handling)
			resultsBuffer.removeLobTracking();
		}
		initTransactionState(request.transactionContext);
		if (requestMsg.isNoExec()) {
		    doneProducingBatches();
            resultsBuffer.close();
            this.cid = null;
		}
	    this.returnsUpdateCount = request.returnsUpdateCount;
	    if (this.returnsUpdateCount && this.requestMsg.getRequestOptions().isContinuous()) {
			throw new IllegalStateException("Continuous requests are not allowed to be updates."); //$NON-NLS-1$
	    }
		request = null;
	}

	private void initTransactionState(TransactionContext tc) {
		transactionContext = tc;
		if (this.transactionContext != null && this.transactionContext.getTransactionType() != Scope.NONE) {
			if (this.requestMsg.getRequestOptions().isContinuous()) {
				throw new IllegalStateException("Continuous requests are not allowed to be transactional."); //$NON-NLS-1$
			}
			this.transactionState = TransactionState.ACTIVE;
		}
	}

	private CacheHint getCacheHint() {
		if (requestMsg.getCommand() != null) {
			return ((Command)requestMsg.getCommand()).getCacheHint();
		}
		return QueryParser.getQueryParser().parseCacheHint(requestMsg.getCommandString());
	}
	
	private void addToCache() {
		if (!doneProducingBatches || cid == null) {
			return;
		}
    	Determinism determinismLevel = processor.getContext().getDeterminismLevel();
    	CachedResults cr = new CachedResults();
    	cr.setCommand(originalCommand);
        cr.setResults(resultsBuffer, processor.getProcessorPlan());
        if (requestMsg.getRowLimit() > 0 && resultsBuffer.getRowCount() == requestMsg.getRowLimit() + (collector.isSaveLastRow()?1:0)) {
        	cr.setRowLimit(requestMsg.getRowLimit());
        }
        if (originalCommand.getCacheHint() != null) {
        	LogManager.logDetail(LogConstants.CTX_DQP, requestID, "Using cache hint", originalCommand.getCacheHint()); //$NON-NLS-1$
			resultsBuffer.setPrefersMemory(originalCommand.getCacheHint().isPrefersMemory());
        	if (originalCommand.getCacheHint().getDeterminism() != null) {
				determinismLevel = originalCommand.getCacheHint().getDeterminism();
				LogManager.logTrace(LogConstants.CTX_DQP, new Object[] { "Cache hint modified the query determinism from ",processor.getContext().getDeterminismLevel(), " to ", determinismLevel }); //$NON-NLS-1$ //$NON-NLS-2$
        	}	
        	//if not updatable, then remove the access info
        	if (!originalCommand.getCacheHint().isUpdatable(true)) {
        		cr.getAccessInfo().setSensitiveToMetadataChanges(false);
        		cr.getAccessInfo().getObjectsAccessed().clear();
        	}
        }            		
        
        if (determinismLevel.compareTo(Determinism.SESSION_DETERMINISTIC) <= 0) {
			LogManager.logInfo(LogConstants.CTX_DQP, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30008, originalCommand));
		}
        dqpCore.getRsCache().put(cid, determinismLevel, cr, originalCommand.getCacheHint() != null?originalCommand.getCacheHint().getTtl():null);
	}
	
	public SessionAwareCache<CachedResults> getRsCache() {
		return dqpCore.getRsCache();
	}

	/**
	 * Send results if they have been requested.  This should only be called from the processing thread.
	 */
	protected boolean sendResultsIfNeeded(TupleBatch batch) throws TeiidComponentException {
		ResultsMessage response = null;
		ResultsReceiver<ResultsMessage> receiver = null;
		boolean result = true;
		synchronized (this) {
			if (this.resultsReceiver == null) {
				if (this.transactionState != TransactionState.ACTIVE && (requestMsg.getRequestOptions().isContinuous() || (useCallingThread && isForwardOnly()))) {
					if (batch != null) {
						throw new AssertionError("batch has no handler"); //$NON-NLS-1$
					}
		        	throw BlockedException.block(requestID, "Blocking until client is ready"); //$NON-NLS-1$            	
		        }
				return result;
			}
			if (!this.requestMsg.getRequestOptions().isContinuous()) {
				if ((this.begin > (batch != null?batch.getEndRow():this.resultsBuffer.getRowCount()) && !doneProducingBatches)
						|| (this.transactionState == TransactionState.ACTIVE)) {
					return result;
				}
			
				if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
					LogManager.logDetail(LogConstants.CTX_DQP, "[RequestWorkItem.sendResultsIfNeeded] requestID:", requestID, "resultsID:", this.resultsBuffer, "done:", doneProducingBatches );   //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
				}
		
				boolean fromBuffer = false;
	    		int count = this.end - this.begin + 1;
	    		if (batch == null || !(batch.containsRow(this.begin) || (batch.getTerminationFlag() && batch.getEndRow() <= this.begin))) {
		    		if (savedBatch != null && savedBatch.containsRow(this.begin)) {
		    			batch = savedBatch;
		    		} else {
		    			batch = resultsBuffer.getBatch(begin);
		    			//fetch more than 1 batch from the buffer
		    			boolean first = true;
		    			int rowSize = resultsBuffer.getRowSizeEstimate();
		    			int batches = CLIENT_FETCH_MAX_BATCHES;
		    			if (rowSize > 0) {
		    				int totalSize = rowSize * resultsBuffer.getBatchSize();
		    				if (schemaSize == 0) {
		    					schemaSize = this.dqpCore.getBufferManager().getSchemaSize(this.originalCommand.getProjectedSymbols());
		    				}
		    				int multiplier = schemaSize/totalSize;
		    				if (multiplier > 1) {
		    					batches *= multiplier;
		    				}
		    			}
		    			for (int i = 1; i < batches && batch.getRowCount() + resultsBuffer.getBatchSize() <= count && !batch.getTerminationFlag(); i++) {
		    				TupleBatch next = resultsBuffer.getBatch(batch.getEndRow() + 1);
		    				if (next.getRowCount() == 0) {
		    					break;
		    				}
		    				if (first) {
		    					first = false;
		    					TupleBatch old = batch;
		    					batch = new TupleBatch(batch.getBeginRow(), new ResizingArrayList<List<?>>(batch.getTuples()));
		    					batch.setTermination(old.getTermination());
		    				}
		    				batch.getTuples().addAll(next.getTuples());
		    				batch.setTermination(next.getTermination());
		    			}
		    		}
		    		savedBatch = null;
		    		fromBuffer = true;
		    	}
	    		if (batch.getRowCount() > count) {
	    			int beginRow = isForwardOnly()?begin:Math.min(this.begin, batch.getEndRow() - count + 1);
	    			int endRow = Math.min(beginRow + count - 1, batch.getEndRow());
	    			boolean last = false;
	    			if (endRow == batch.getEndRow()) {
	    				last = batch.getTerminationFlag();
	    			} else if (fromBuffer && isForwardOnly()) {
	        			savedBatch = batch;
	    			}
	                List<List<?>> memoryRows = batch.getTuples();
	                batch = new TupleBatch(beginRow, memoryRows.subList(beginRow - batch.getBeginRow(), endRow - batch.getBeginRow() + 1));
	                batch.setTerminationFlag(last);
	    		} else if (!fromBuffer){
	    			result = !isForwardOnly();
	    		}
			} else if (batch == null) {
				return result;
			} else {
				result = false;
			}
	        int finalRowCount = (this.resultsBuffer.isFinal()&&!this.requestMsg.getRequestOptions().isContinuous())?this.resultsBuffer.getRowCount():(batch.getTerminationFlag()?batch.getEndRow():-1);
	        
	        response = createResultsMessage(batch.getTuples(), this.originalCommand.getProjectedSymbols());
	        response.setFirstRow(batch.getBeginRow());
	        if (batch.getTermination() == TupleBatch.ITERATION_TERMINATED) {
	        	response.setLastRow(batch.getEndRow() - 1);
	        } else {
	        	response.setLastRow(batch.getEndRow());
	        }
	        response.setUpdateResult(this.returnsUpdateCount);
	        //swap the result for the generated keys
	        if (this.returnsUpdateCount 
	        		&& this.processor.getContext().isReturnAutoGeneratedKeys() 
	        		&& finalRowCount == 1
	        		&& this.processor.getContext().getGeneratedKeys() != null) {
	        	GeneratedKeysImpl keys = this.processor.getContext().getGeneratedKeys();
	        	response.setColumnNames(keys.getColumnNames());
	        	String[] dataTypes = new String[keys.getColumnNames().length];
	        	for(int i=0; i<dataTypes.length; i++) {
	                dataTypes[i] = DataTypeManager.getDataTypeName(keys.getColumnTypes()[i]);
	            }
	        	response.setUpdateCount((Integer)response.getResultsList().get(0).get(0));
	        	response.setDataTypes(dataTypes);
	        	response.setResults(keys.getKeys());
	        	response.setLastRow(keys.getKeys().size());
	        	finalRowCount = response.getLastRow();
	        }
	        // set final row
	        response.setFinalRow(finalRowCount);
	        if (response.getLastRow() == finalRowCount) {
	        	response.setDelayDeserialization(false);
	        }
	
	        setWarnings(response);
	        
	        // If it is stored procedure, set parameters
	        if (originalCommand instanceof StoredProcedure) {
	        	StoredProcedure proc = (StoredProcedure)originalCommand;
	        	if (proc.returnParameters()) {
	        		response.setParameters(getParameterInfo(proc));
	        	}
	        }
	        /*
	         * mark the results sent at this point.
	         * communication exceptions will be treated as non-recoverable 
	         */
            receiver = this.resultsReceiver;
            this.resultsReceiver = null;    
		}
		cancelCancelTask();
        receiver.receiveResults(response);
        return result;
	}

	private void setWarnings(ResultsMessage response) {
		// send any warnings with the response object
		List<Throwable> responseWarnings = new ArrayList<Throwable>();
		if (this.processor != null) {
			List<Exception> currentWarnings = processor.getAndClearWarnings();
		    if (currentWarnings != null) {
		    	responseWarnings.addAll(currentWarnings);
		    }
		}
		response.setWarnings(responseWarnings);
	}
    
    public ResultsMessage createResultsMessage(List<? extends List<?>> batch, List<? extends Expression> columnSymbols) {
        String[] columnNames = new String[columnSymbols.size()];
        String[] dataTypes = new String[columnSymbols.size()];

        for(int i=0; i<columnSymbols.size(); i++) {
            Expression symbol = columnSymbols.get(i);
            columnNames[i] = Symbol.getShortName(Symbol.getOutputName(symbol));
            dataTypes[i] = DataTypeManager.getDataTypeName(symbol.getType());
        }
        ResultsMessage result = new ResultsMessage(batch, columnNames, dataTypes);
        result.setClientSerializationVersion(this.dqpWorkContext.getClientVersion().getClientSerializationVersion());
        result.setDelayDeserialization(this.requestMsg.isDelaySerialization() && this.originalCommand.returnsResultSet());
        setAnalysisRecords(result);
        return result;
    }
    
	private void setAnalysisRecords(ResultsMessage response) {
		if(analysisRecord != null) {
        	if (requestMsg.getShowPlan() != ShowPlan.OFF) {
        		if (processor != null) {
            		response.setPlanDescription(processor.getProcessorPlan().getDescriptionProperties());
        		}
        		if (analysisRecord.getAnnotations() != null && !analysisRecord.getAnnotations().isEmpty()) {
		            response.setAnnotations(analysisRecord.getAnnotations());
		            analysisRecord.getAnnotations().clear();
        		}
        	}
            if (requestMsg.getShowPlan() == ShowPlan.DEBUG) {
            	response.setDebugLog(analysisRecord.getDebugLog());
            	analysisRecord.stopDebugLog();
            }
        }
	}

    private void sendError() {
    	ResultsReceiver<ResultsMessage> receiver = null;
    	synchronized (this) {
    		receiver = this.resultsReceiver;
    		this.resultsReceiver = null;
    		if (receiver == null) {
    			LogManager.logDetail(LogConstants.CTX_DQP, processingException, "Unable to send error to client as results were already sent.", requestID); //$NON-NLS-1$
    			return;
    		}
    	}
		LogManager.logDetail(LogConstants.CTX_DQP, processingException, "Sending error to client", requestID); //$NON-NLS-1$
        ResultsMessage response = new ResultsMessage();
        Throwable exception = this.processingException;
        if (isCanceled) {
        	exception = addCancelCode(exception); 
        }
        setWarnings(response);
        response.setException(exception);
        setAnalysisRecords(response);
        receiver.receiveResults(response);
    }

	private Throwable addCancelCode(Throwable exception) {
		if (exception instanceof TeiidException) {
			TeiidException te = (TeiidException)exception;
			if (SQLStates.QUERY_CANCELED.equals(te.getCode())) {
				return exception;
			}
		}
		return new TeiidProcessingException(exception, SQLStates.QUERY_CANCELED);
	}
    
    private static List<ParameterInfo> getParameterInfo(StoredProcedure procedure) {
        List<ParameterInfo> paramInfos = new ArrayList<ParameterInfo>();
        
        for (SPParameter param : procedure.getParameters()) {
            ParameterInfo info = new ParameterInfo(param.getParameterType(), param.getResultSetColumns().size());
            paramInfos.add(info);
        }
        
        return paramInfos;
    }
    
    public void processLobChunkRequest(String id, int streamRequestId, ResultsReceiver<LobChunk> chunckReceiver) {
    	LobWorkItem workItem = null;
    	synchronized (lobStreams) {
            workItem = this.lobStreams.get(streamRequestId);
            if (workItem == null) {
            	workItem = new LobWorkItem(this, dqpCore, id, streamRequestId);
            	lobStreams.put(streamRequestId, workItem);
            }
		}
    	workItem.setResultsReceiver(chunckReceiver);
    	if (this.dqpWorkContext.useCallingThread()) {
    		workItem.run();
    	} else {
    		dqpCore.addWork(workItem);
    	}
    }
    
    public void removeLobStream(int streamRequestId) {
        this.lobStreams.remove(streamRequestId);
    } 
    
    public boolean requestCancel() throws TeiidComponentException {
    	synchronized (this) {
        	if (this.isCanceled || this.closeRequested) {
        		return false;
        	}
        	this.isCanceled = true;
		}
    	if (this.processor != null) {
    		this.processor.requestCanceled();
    	}
    	
        // Cancel Connector atomic requests 
        try {
        	for (DataTierTupleSource connectorRequest : getConnectorRequests()) {
                connectorRequest.cancelRequest();
            }
        } finally {
        	try {
	            if (transactionService != null) {
	                try {
	                    transactionService.cancelTransactions(requestID.getConnectionID(), true);
	                } catch (XATransactionException err) {
	                     throw new TeiidComponentException(QueryPlugin.Event.TEIID30544, err);
	                }
	            }
        	} finally {
        		this.moreWork();
        	}
        }
        return true;
    }
    
    public boolean requestAtomicRequestCancel(AtomicRequestID ari) throws TeiidComponentException {
    	// in the case that this does not support partial results; cancel
        // the original processor request.
        if(!requestMsg.supportsPartialResults()) {
        	return requestCancel();
        }
        
        DataTierTupleSource connectorRequest = this.connectorInfo.get(ari);
        if (connectorRequest != null) {
	        connectorRequest.cancelRequest();
        	return true;
        }
        
		LogManager.logDetail(LogConstants.CTX_DQP, "Connector request not found. AtomicRequestID=", ari); //$NON-NLS-1$ 
        return false;
    }
    
    public void requestClose() throws TeiidComponentException {
    	synchronized (this) {
        	if (this.state == ProcessingState.CLOSE || this.closeRequested) {
        		if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
        			LogManager.logDetail(LogConstants.CTX_DQP, "Request already closing" + requestID); //$NON-NLS-1$
        		}
        		return;
        	}
		}
    	if (!this.doneProducingBatches) {
    		this.requestCancel(); //pending work should be canceled for fastest clean up
    	}
    	this.closeRequested = true;
    	this.doMoreWork();
    }
    
    public void requestMore(int batchFirst, int batchLast, ResultsReceiver<ResultsMessage> receiver) {
    	this.requestResults(batchFirst, batchLast, receiver);
    	this.doMoreWork(); 
    }
    
    public void closeAtomicRequest(AtomicRequestID atomicRequestId) {
        connectorInfo.remove(atomicRequestId);
        LogManager.logTrace(LogConstants.CTX_DQP, new Object[] {"closed atomic-request:", atomicRequestId});  //$NON-NLS-1$
    }
    
	public void addConnectorRequest(AtomicRequestID atomicRequestId, DataTierTupleSource connInfo) {
		this.explicitSourceClose |= connInfo.isExplicitClose();
		connectorInfo.put(atomicRequestId, connInfo);
	}
    
	boolean isCanceled() {
		return isCanceled;
	}
	
	Command getOriginalCommand() throws TeiidProcessingException {
		if (this.originalCommand == null) {
			if (this.processingException != null) {
				 throw new TeiidProcessingException(QueryPlugin.Event.TEIID30545, this.processingException);
			} 
			throw new IllegalStateException("Original command is not available"); //$NON-NLS-1$
		}
		return this.originalCommand;
	}
	
	void setOriginalCommand(Command originalCommand) {
		this.originalCommand = originalCommand;
	}

	TransactionContext getTransactionContext() {
		return transactionContext;
	}
	
	Collection<DataTierTupleSource> getConnectorRequests() {
		synchronized (this.connectorInfo) {
			return new ArrayList<DataTierTupleSource>(this.connectorInfo.values());
		}
	}
	
	DataTierTupleSource getConnectorRequest(AtomicRequestID id) {
		return this.connectorInfo.get(id);
	}
	
	@Override
	public String toString() {
		return this.requestID.toString();
	}

	public DQPWorkContext getDqpWorkContext() {
		return dqpWorkContext;
	}
	
	public long getProcessingTimestamp() {
		return processingTimestamp;
	}
	
	@Override
	public void release() {
		try {
			requestCancel();
		} catch (TeiidComponentException e) {
			LogManager.logWarning(LogConstants.CTX_DQP, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30026,requestID));
		}
	}

	private void done() {
		doneProducingBatches();
		//TODO: we could perform more tracking to know what source lobs are in use
		if (this.resultsBuffer.getLobCount() == 0) {
			for (DataTierTupleSource connectorRequest : getConnectorRequests()) {
				connectorRequest.fullyCloseSource();
		    }
		}
		addToCache();
	}

	private void doneProducingBatches() {
		this.doneProducingBatches = true;
		synchronized (queue) {
			queue.clear();
		}
		dqpCore.finishProcessing(this);
	}
	
	@Override
	public int getPriority() {
		return (closeRequested || isCanceled) ? 0 : 1000;
	}
	
	@Override
	public long getCreationTime() {
		return processingTimestamp;
	}	
	
	<T> FutureWork<T> addHighPriorityWork(Callable<T> callable) {
		FutureWork<T> work = new FutureWork<T>(callable, PrioritizedRunnable.NO_WAIT_PRIORITY);
		dqpCore.addWork(work);
		return work;
	}
	
    <T> FutureWork<T> addWork(Callable<T> callable, CompletionListener<T> listener, int priority) {
    	FutureWork<T> work = new FutureWork<T>(callable, priority);
    	WorkWrapper<T> wl = new WorkWrapper<T>(work);
    	work.addCompletionListener(wl);
    	work.addCompletionListener(listener);
    	synchronized (queue) {
        	if (totalThreads < dqpCore.getUserRequestSourceConcurrency()) {
        		dqpCore.addWork(work);
        		totalThreads++;
        		wl.submitted = true;
        	} else {
    	    	queue.add(wl);
    	    	LogManager.logDetail(LogConstants.CTX_DQP, this.requestID, " reached max source concurrency of ", dqpCore.getUserRequestSourceConcurrency()); //$NON-NLS-1$
        	}
    	}
    	return work;
    }
    
    public Future<Void> scheduleWork(long delay) {
    	return dqpCore.scheduleWork(new MoreWorkTask(this), delay);
    }
    
    public void setCancelTask(Task cancelTask) {
		this.cancelTask = cancelTask;
	}
    
    public QueryProcessor getProcessor() {
		return processor;
	}
    
    public RequestID getRequestID() {
		return requestID;
	}

}