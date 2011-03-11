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

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import org.teiid.client.RequestMessage;
import org.teiid.client.ResultsMessage;
import org.teiid.client.SourceWarning;
import org.teiid.client.RequestMessage.ShowPlan;
import org.teiid.client.lob.LobChunk;
import org.teiid.client.metadata.ParameterInfo;
import org.teiid.client.util.ResultsReceiver;
import org.teiid.client.xa.XATransactionException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.BufferManager.TupleSourceType;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.dqp.internal.process.DQPCore.FutureWork;
import org.teiid.dqp.internal.process.SessionAwareCache.CacheID;
import org.teiid.dqp.internal.process.ThreadReuseExecutor.PrioritizedRunnable;
import org.teiid.dqp.message.AtomicRequestID;
import org.teiid.dqp.message.RequestID;
import org.teiid.dqp.service.TransactionContext;
import org.teiid.dqp.service.TransactionService;
import org.teiid.dqp.service.TransactionContext.Scope;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.logging.CommandLogMessage.Event;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.parser.ParseInfo;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.processor.BatchCollector;
import org.teiid.query.processor.QueryProcessor;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.SPParameter;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.symbol.SingleElementSymbol;

public class RequestWorkItem extends AbstractWorkItem implements PrioritizedRunnable {
	
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
    private Map<AtomicRequestID, DataTierTupleSource> connectorInfo = new ConcurrentHashMap<AtomicRequestID, DataTierTupleSource>(4);
    // This exception contains details of all the atomic requests that failed when query is run in partial results mode.
    private List<TeiidException> warnings = new LinkedList<TeiidException>();
    private volatile boolean doneProducingBatches;
    private volatile boolean isClosed;
    private volatile boolean isCanceled;
    private volatile boolean closeRequested;

	//results request
	private ResultsReceiver<ResultsMessage> resultsReceiver;
	private int begin;
	private int end;
    private TupleBatch savedBatch;
    private Map<Integer, LobWorkItem> lobStreams = new ConcurrentHashMap<Integer, LobWorkItem>(4);    
    
    /**The time when command begins processing on the server.*/
    private long processingTimestamp = System.currentTimeMillis();
    
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
	protected void resumeProcessing() {
		if (doneProducingBatches && !closeRequested && !isCanceled) {
			this.run(); // just run in the IO thread
		} else {
			dqpCore.addWork(this);
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
                	this.processingException = new TeiidProcessingException(QueryPlugin.Util.getString("QueryProcessor.request_cancelled", this.requestID)); //$NON-NLS-1$
                    state = ProcessingState.CLOSE;
                } 
        	}
        	
            resume();
        	
            if (this.state == ProcessingState.PROCESSING) {
            	processMore();
            	if (this.closeRequested) {
            		this.state = ProcessingState.CLOSE;
            	}
            }                  	            
        } catch (BlockedException e) {
            LogManager.logDetail(LogConstants.CTX_DQP, "Request Thread", requestID, "- processor blocked"); //$NON-NLS-1$ //$NON-NLS-2$
        } catch (QueryProcessor.ExpiredTimeSliceException e) {
            LogManager.logDetail(LogConstants.CTX_DQP, "Request Thread", requestID, "- time slice expired"); //$NON-NLS-1$ //$NON-NLS-2$
            this.moreWork();
        } catch (Throwable e) {
        	LogManager.logDetail(LogConstants.CTX_DQP, e, "Request Thread", requestID, "- error occurred"); //$NON-NLS-1$ //$NON-NLS-2$
            
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
                	if (elems.length > 0) {
                		elem = cause.getStackTrace()[0];
                	} else {
                		elem = cause.getMessage();
                	}
                    LogManager.logWarning(LogConstants.CTX_DQP, QueryPlugin.Util.getString("ProcessWorker.processing_error", e.getMessage(), requestID, e.getClass().getName(), elem)); //$NON-NLS-1$
                }else {
                    LogManager.logError(LogConstants.CTX_DQP, e, QueryPlugin.Util.getString("ProcessWorker.error", requestID)); //$NON-NLS-1$
                }                                
            }
            
            this.processingException = e;
            this.state = ProcessingState.CLOSE;
        } finally {
        	if (this.state == ProcessingState.CLOSE && !isClosed) {
        		attemptClose();
        	} else if (isClosed) {
        		/*
        		 * since there may be a client waiting notify them of a problem
        		 */
        		if (this.processingException == null) {
        			this.processingException = new IllegalStateException("Request is already closed"); //$NON-NLS-1$
        		}
        		sendError();
        	}
        	suspend();
        }
    }

	private void resume() throws XATransactionException {
		if (this.transactionState == TransactionState.ACTIVE && this.transactionContext.getTransaction() != null) {
			this.transactionService.resume(this.transactionContext);
		}
	}

	private void suspend() {
		try {
			this.transactionService.suspend(this.transactionContext);
		} catch (XATransactionException e) {
			LogManager.logDetail(LogConstants.CTX_DQP, e, "Error suspending active transaction"); //$NON-NLS-1$
		}
	}

	protected void processMore() throws BlockedException, TeiidException {
		if (!doneProducingBatches) {
			this.processor.getContext().setTimeSliceEnd(System.currentTimeMillis() + this.processorTimeslice);
			sendResultsIfNeeded(null);
			this.resultsBuffer = collector.collectTuples();
			if (!doneProducingBatches) {
				doneProducingBatches();
				addToCache();
			}
		}
		if (this.transactionState == TransactionState.ACTIVE) {
			/*
			 * TEIID-14 if we are done producing batches, then proactively close transactional 
			 * executions even ones that were intentionally kept alive. this may 
			 * break the read of a lob from a transactional source under a transaction 
			 * if the source does not support holding the clob open after commit
			 */
        	for (DataTierTupleSource connectorRequest : this.connectorInfo.values()) {
        		if (connectorRequest.isTransactional()) {
        			connectorRequest.fullyCloseSource();
        		}
            }
			this.transactionState = TransactionState.DONE;
			if (transactionContext.getTransactionType() == TransactionContext.Scope.REQUEST) {
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
	protected void attemptClose() {
		int rowcount = -1;
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
				
				for (DataTierTupleSource connectorRequest : this.connectorInfo.values()) {
					connectorRequest.fullyCloseSource();
			    }
			}

			this.resultsBuffer = null;
			
			for (LobWorkItem lobWorkItem : this.lobStreams.values()) {
				lobWorkItem.close();
			}
		}

		if (this.transactionState == TransactionState.ACTIVE) { 
			this.transactionState = TransactionState.DONE;
            if (transactionContext.getTransactionType() == TransactionContext.Scope.REQUEST) {
				try {
	        		this.transactionService.rollback(transactionContext);
	            } catch (XATransactionException e1) {
	                LogManager.logWarning(LogConstants.CTX_DQP, e1, QueryPlugin.Util.getString("ProcessWorker.failed_rollback")); //$NON-NLS-1$           
	            } 
			} else {
				suspend();
			}
		}
		
		isClosed = true;

		dqpCore.removeRequest(this);
	    
		if (this.processingException != null) {
			sendError();			
		} else {
	        dqpCore.logMMCommand(this, Event.END, rowcount);
		}
	}

	protected void processNew() throws TeiidProcessingException, TeiidComponentException {
		SessionAwareCache<CachedResults> rsCache = dqpCore.getRsCache();
				
		boolean cachable = false;
		CacheID cacheId = null;
		boolean canUseCached = (requestMsg.useResultSetCache() || 
				QueryParser.getQueryParser().parseCacheHint(requestMsg.getCommandString()) != null);
		
		if (rsCache != null) {
			if (!canUseCached) {
				LogManager.logDetail(LogConstants.CTX_DQP, requestID, "No cache directive"); //$NON-NLS-1$
			} else {
				ParseInfo pi = Request.createParseInfo(requestMsg);
				cacheId = new CacheID(this.dqpWorkContext, pi, requestMsg.getCommandString());
		    	cachable = cacheId.setParameters(requestMsg.getParameterValues());
				if (cachable) {
					CachedResults cr = rsCache.get(cacheId);
					if (cr != null) {
						this.resultsBuffer = cr.getResults();
						this.analysisRecord = cr.getAnalysisRecord();
						request.initMetadata();
						this.originalCommand = cr.getCommand(requestMsg.getCommandString(), request.metadata, pi);
						request.validateAccess(this.originalCommand);
						this.doneProducingBatches();
						return;
					} 
				} else {
					LogManager.logDetail(LogConstants.CTX_DQP, requestID, "Parameters are not serializable - cache cannot be used for", cacheId); //$NON-NLS-1$
				}
			}
		}
		request.processRequest();
		originalCommand = request.userCommand;
        if (cachable && (requestMsg.useResultSetCache() || originalCommand.getCacheHint() != null) && rsCache != null && originalCommand.areResultsCachable()) {
        	this.cid = cacheId;
        }
		processor = request.processor;
		collector = new BatchCollector(processor, processor.getBufferManager(), this.request.context, isForwardOnly()) {
			protected void flushBatchDirect(TupleBatch batch, boolean add) throws TeiidComponentException,TeiidProcessingException {
				resultsBuffer = getTupleBuffer();
				boolean added = false;
				if (cid != null) {
					super.flushBatchDirect(batch, add);
					added = true;
				}
				if (batch.getTerminationFlag()) {
					doneProducingBatches();
				}
				addToCache();
				add = sendResultsIfNeeded(batch);
				if (!added) {
					super.flushBatchDirect(batch, add);
					//restrict the buffer size for forward only results
					if (add && !processor.hasFinalBuffer()
							&& !batch.getTerminationFlag() 
							&& this.getTupleBuffer().getManagedRowCount() >= 20 * this.getTupleBuffer().getBatchSize()) {
						//requestMore will trigger more processing
						throw BlockedException.INSTANCE;
					}
				}
			}
		};
		this.resultsBuffer = collector.getTupleBuffer();
		if (this.resultsBuffer == null) {
			//This is just a dummy result it will get replaced by collector source
	    	resultsBuffer = this.processor.getBufferManager().createTupleBuffer(this.originalCommand.getProjectedSymbols(), this.request.context.getConnectionID(), TupleSourceType.FINAL);
		}
		analysisRecord = request.analysisRecord;
		analysisRecord.setQueryPlan(processor.getProcessorPlan().getDescriptionProperties());
		transactionContext = request.transactionContext;
		if (this.transactionContext != null && this.transactionContext.getTransactionType() != Scope.NONE) {
			this.transactionState = TransactionState.ACTIVE;
		}
		if (requestMsg.isNoExec()) {
		    doneProducingBatches();
            resultsBuffer.close();
            this.cid = null;
		}
	    this.returnsUpdateCount = request.returnsUpdateCount;
		request = null;
	}
	
	private void addToCache() {
		if (!doneProducingBatches || cid == null) {
			return;
		}
    	Determinism determinismLevel = processor.getContext().getDeterminismLevel();
    	CachedResults cr = new CachedResults();
    	cr.setCommand(originalCommand);
        cr.setAnalysisRecord(analysisRecord);
        cr.setResults(resultsBuffer);
        if (originalCommand.getCacheHint() != null) {
        	LogManager.logDetail(LogConstants.CTX_DQP, requestID, "Using cache hint", originalCommand.getCacheHint()); //$NON-NLS-1$
			resultsBuffer.setPrefersMemory(originalCommand.getCacheHint().getPrefersMemory());
        	if (originalCommand.getCacheHint().getDeterminism() != null) {
				determinismLevel = originalCommand.getCacheHint().getDeterminism();
				LogManager.logTrace(LogConstants.CTX_DQP, new Object[] { "Cache hint modified the query determinism from ",processor.getContext().getDeterminismLevel(), " to ", determinismLevel }); //$NON-NLS-1$ //$NON-NLS-2$
        	}		                
        }            		
        
        if (determinismLevel.compareTo(Determinism.SESSION_DETERMINISTIC) <= 0) {
			LogManager.logInfo(LogConstants.CTX_DQP, QueryPlugin.Util.getString("RequestWorkItem.cache_nondeterministic", originalCommand)); //$NON-NLS-1$
		}
        dqpCore.getRsCache().put(cid, determinismLevel, cr, originalCommand.getCacheHint() != null?originalCommand.getCacheHint().getTtl():null);
	}

	/**
	 * Send results if they have been requested.  This should only be called from the processing thread.
	 */
	protected boolean sendResultsIfNeeded(TupleBatch batch) throws TeiidComponentException {
		ResultsMessage response = null;
		ResultsReceiver<ResultsMessage> receiver = null;
		boolean result = true;
		synchronized (this) {
			if (this.resultsReceiver == null
					|| (this.begin > (batch != null?batch.getEndRow():this.resultsBuffer.getRowCount()) && !doneProducingBatches)
					|| (this.transactionState == TransactionState.ACTIVE)) {
				return result;
			}
		
			if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
				LogManager.logDetail(LogConstants.CTX_DQP, "[RequestWorkItem.sendResultsIfNeeded] requestID:", requestID, "resultsID:", this.resultsBuffer, "done:", doneProducingBatches );   //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			}
	
			//TODO: support fetching more than 1 batch
			boolean fromBuffer = false;
    		if (batch == null || !(batch.containsRow(this.begin) || (batch.getTerminationFlag() && batch.getEndRow() <= this.begin))) {
	    		if (savedBatch != null && savedBatch.containsRow(this.begin)) {
	    			batch = savedBatch;
	    		} else {
	    			batch = resultsBuffer.getBatch(begin);
	    		}
	    		savedBatch = null;
	    		fromBuffer = true;
	    	}
    		int count = this.end - this.begin + 1;
    		if (batch.getRowCount() > count) {
    			int beginRow = Math.min(this.begin, batch.getEndRow() - count + 1);
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
	        int finalRowCount = this.resultsBuffer.isFinal()?this.resultsBuffer.getRowCount():(batch.getTerminationFlag()?batch.getEndRow():-1);
	        
	        response = createResultsMessage(batch.getAllTuples(), this.originalCommand.getProjectedSymbols());
	        response.setFirstRow(batch.getBeginRow());
	        response.setLastRow(batch.getEndRow());
	        response.setUpdateResult(this.returnsUpdateCount);
	        // set final row
	        response.setFinalRow(finalRowCount);
	
	        // send any warnings with the response object
	        List<Throwable> responseWarnings = new ArrayList<Throwable>();
	        if (this.processor != null) {
				List<Exception> currentWarnings = processor.getAndClearWarnings();
			    if (currentWarnings != null) {
			    	responseWarnings.addAll(currentWarnings);
			    }
	        }
		    synchronized (warnings) {
	        	responseWarnings.addAll(this.warnings);
	        	this.warnings.clear();
		    }
	        response.setWarnings(responseWarnings);
	        
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
        receiver.receiveResults(response);
        return result;
	}
    
    public ResultsMessage createResultsMessage(List[] batch, List columnSymbols) {
        String[] columnNames = new String[columnSymbols.size()];
        String[] dataTypes = new String[columnSymbols.size()];

        for(int i=0; i<columnSymbols.size(); i++) {
            SingleElementSymbol symbol = (SingleElementSymbol) columnSymbols.get(i);
            columnNames[i] = SingleElementSymbol.getShortName(symbol.getOutputName());
            dataTypes[i] = DataTypeManager.getDataTypeName(symbol.getType());
        }
        ResultsMessage result = new ResultsMessage(requestMsg, batch, columnNames, dataTypes);
        setAnalysisRecords(result);
        return result;
    }
    
	private void setAnalysisRecords(ResultsMessage response) {
		if(analysisRecord != null) {
        	if (requestMsg.getShowPlan() != ShowPlan.OFF) {
        		if (processor != null) {
        			analysisRecord.setQueryPlan(processor.getProcessorPlan().getDescriptionProperties());
        		}
        		response.setPlanDescription(analysisRecord.getQueryPlan());
	            response.setAnnotations(analysisRecord.getAnnotations());
        	}
            if (requestMsg.getShowPlan() == ShowPlan.DEBUG) {
            	response.setDebugLog(analysisRecord.getDebugLog());
            }
        }
	}

    private void sendError() {
    	synchronized (this) {
    		if (this.resultsReceiver == null) {
    			LogManager.logDetail(LogConstants.CTX_DQP, processingException, "Unable to send error to client as results were already sent.", requestID); //$NON-NLS-1$
    			return;
    		}
    	}
		LogManager.logDetail(LogConstants.CTX_DQP, processingException, "Sending error to client", requestID); //$NON-NLS-1$
        ResultsMessage response = new ResultsMessage(requestMsg);
        response.setException(processingException);
        setAnalysisRecords(response);
        resultsReceiver.receiveResults(response);
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
            workItem = this.lobStreams.get(new Integer(streamRequestId));
            if (workItem == null) {
            	workItem = new LobWorkItem(this, dqpCore, id, streamRequestId);
            	lobStreams.put(new Integer(streamRequestId), workItem);
            }
		}
    	workItem.setResultsReceiver(chunckReceiver);
        dqpCore.addWork(workItem);
    }
    
    public void removeLobStream(int streamRequestId) {
        this.lobStreams.remove(new Integer(streamRequestId));
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
        	for (DataTierTupleSource connectorRequest : this.connectorInfo.values()) {
                connectorRequest.cancelRequest();
            }
        } finally {
        	try {
	            if (transactionService != null) {
	                try {
	                    transactionService.cancelTransactions(requestID.getConnectionID(), true);
	                } catch (XATransactionException err) {
	                    throw new TeiidComponentException(err);
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
    	this.closeRequested = true;
    	if (!this.doneProducingBatches) {
    		this.requestCancel(); //pending work should be canceled for fastest clean up
    	}
    	this.moreWork();
    }
    
    public void requestMore(int batchFirst, int batchLast, ResultsReceiver<ResultsMessage> receiver) {
    	this.requestResults(batchFirst, batchLast, receiver);
    	this.moreWork(); 
    }
    
    public void closeAtomicRequest(AtomicRequestID atomicRequestId) {
        connectorInfo.remove(atomicRequestId);
        LogManager.logTrace(LogConstants.CTX_DQP, new Object[] {"closed atomic-request:", atomicRequestId});  //$NON-NLS-1$
    }
    
	public void addConnectorRequest(AtomicRequestID atomicRequestId, DataTierTupleSource connInfo) {
		connectorInfo.put(atomicRequestId, connInfo);
	}
    
    /**
	 * <p>This method add information to the warning on the work item for the given
	 * <code>RequestID</code>. This method is called from <code>DataTierManager</code></p>
	 */
    public void addSourceFailureDetails(SourceWarning details) {
    	synchronized (warnings) {
			this.warnings.add(details);
    	}
	}
        
	boolean isCanceled() {
		return isCanceled;
	}
	
	Command getOriginalCommand() throws TeiidProcessingException {
		if (this.originalCommand == null) {
			if (this.processingException != null) {
				throw new TeiidProcessingException(this.processingException);
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
		return new LinkedList<DataTierTupleSource>(this.connectorInfo.values());
	}
	
	DataTierTupleSource getConnectorRequest(AtomicRequestID id) {
		return this.connectorInfo.get(id);
	}
	
	public List<TeiidException> getWarnings() {
		return warnings;
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
			LogManager.logWarning(LogConstants.CTX_DQP, e, "Failed to cancel " + requestID); //$NON-NLS-1$
		}
	}

	private void doneProducingBatches() {
		this.doneProducingBatches = true;
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
	
    <T> FutureWork<T> addWork(Callable<T> callable, int priority) {
    	FutureWork<T> work = new FutureWork<T>(callable, priority);
    	WorkWrapper<T> wl = new WorkWrapper<T>(work);
    	work.addCompletionListener(wl);
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
    
    void scheduleWork(Runnable r, int priority, long delay) {
    	dqpCore.scheduleWork(r, priority, delay);
    }

}