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
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.dqp.DQPPlugin;
import org.teiid.dqp.internal.process.SessionAwareCache.CacheID;
import org.teiid.dqp.message.AtomicRequestID;
import org.teiid.dqp.message.RequestID;
import org.teiid.dqp.service.TransactionContext;
import org.teiid.dqp.service.TransactionService;
import org.teiid.dqp.service.TransactionContext.Scope;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.logging.CommandLogMessage.Event;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.execution.QueryExecPlugin;
import org.teiid.query.processor.BatchCollector;
import org.teiid.query.processor.QueryProcessor;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.SPParameter;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.symbol.SingleElementSymbol;
import org.teiid.translator.DataNotAvailableException;


public class RequestWorkItem extends AbstractWorkItem {
	
	private enum ProcessingState {NEW, PROCESSING, CLOSE}
	private ProcessingState state = ProcessingState.NEW;
    
	private enum TransactionState {NONE, ACTIVE, DONE}
	private TransactionState transactionState = TransactionState.NONE;
	
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
    private boolean doneProducingBatches;
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
		dqpCore.addWork(this);
	}
	
	@Override
	protected void process() {
        LogManager.logDetail(LogConstants.CTX_DQP, "Request Thread", requestID, "with state", state); //$NON-NLS-1$ //$NON-NLS-2$
        try {
            if (this.state == ProcessingState.NEW) {
                state = ProcessingState.PROCESSING;
        		processNew();
                if (isCanceled) {
                	this.processingException = new TeiidProcessingException(QueryExecPlugin.Util.getString("QueryProcessor.request_cancelled", this.requestID)); //$NON-NLS-1$
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
        } catch (DataNotAvailableException e) { 
        	LogManager.logDetail(LogConstants.CTX_DQP, "Request Thread", requestID, "- data not available"); //$NON-NLS-1$ //$NON-NLS-2$
            this.dqpCore.scheduleWork(this, e.getRetryDelay());
        } catch (Throwable e) {
        	LogManager.logDetail(LogConstants.CTX_DQP, e, "Request Thread", requestID, "- error occurred"); //$NON-NLS-1$ //$NON-NLS-2$
            
            if (!isCanceled()) {
            	dqpCore.logMMCommand(this, Event.CANCEL, null);
                //Case 5558: Differentiate between system level errors and
                //processing errors.  Only log system level errors as errors, 
                //log the processing errors as warnings only
                if(e instanceof TeiidProcessingException) {                          
                	Throwable cause = e;
                	while (cause.getCause() != null && cause.getCause() != cause) {
                		cause = cause.getCause();
                	}
                	StackTraceElement elem = cause.getStackTrace()[0];
                    LogManager.logWarning(LogConstants.CTX_DQP, DQPPlugin.Util.getString("ProcessWorker.processing_error", e.getMessage(), requestID, e.getClass().getName(), elem)); //$NON-NLS-1$
                }else {
                    LogManager.logError(LogConstants.CTX_DQP, e, DQPPlugin.Util.getString("ProcessWorker.error", requestID)); //$NON-NLS-1$
                }                                
            }
            
            this.processingException = e;
            this.state = ProcessingState.CLOSE;
        } finally {
        	if (this.state == ProcessingState.CLOSE && !isClosed) {
        		attemptClose();
        		if (this.processingException != null) {
        			sendError();
        		}
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
			//there's no need to do this for xa transactions, as that is done by the workmanager
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
			collector.collectTuples();
		}
		if (doneProducingBatches) {
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
		} else {
			moreWork(false); // If the timeslice expired, then the processor can probably produce more batches.
			if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
				LogManager.logDetail(LogConstants.CTX_DQP, "############# PW EXITING on " + requestID + " - reenqueueing for more processing ###########"); //$NON-NLS-1$ //$NON-NLS-2$
			}
		}
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
	                LogManager.logWarning(LogConstants.CTX_DQP, e1, DQPPlugin.Util.getString("ProcessWorker.failed_rollback")); //$NON-NLS-1$           
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
		CacheID cacheId = new CacheID(this.dqpWorkContext, Request.createParseInfo(requestMsg), requestMsg.getCommandString());
    	cacheId.setParameters(requestMsg.getParameterValues());
		if (rsCache != null) {
			CachedResults cr = rsCache.get(cacheId);
			if (cr != null && (requestMsg.useResultSetCache() || cr.getCommand().isCache())) {
				this.resultsBuffer = cr.getResults();
				this.analysisRecord = cr.getAnalysisRecord();
				this.originalCommand = cr.getCommand();
				this.doneProducingBatches = true;
				return;
			}
		}
		request.processRequest();
		originalCommand = request.userCommand;
        if ((requestMsg.useResultSetCache() || originalCommand.isCache()) && rsCache != null && originalCommand.areResultsCachable()) {
        	this.cid = cacheId;
        }
		processor = request.processor;
		resultsBuffer = processor.createTupleBuffer();
		collector = new BatchCollector(processor, resultsBuffer) {
			protected void flushBatchDirect(TupleBatch batch, boolean add) throws TeiidComponentException,TeiidProcessingException {
				boolean added = false;
				if (cid != null || resultsBuffer.isLobs()) {
					super.flushBatchDirect(batch, add);
					added = true;
				}
				doneProducingBatches = batch.getTerminationFlag();
				if (doneProducingBatches && cid != null) {
			    	boolean sessionScope = processor.getContext().isSessionFunctionEvaluated();
	            	CachedResults cr = new CachedResults();
	            	cr.setCommand(originalCommand);
	                cr.setAnalysisRecord(analysisRecord);
	                cr.setResults(resultsBuffer);
	                dqpCore.getRsCache().put(cid, sessionScope, cr);
			    }
				add = sendResultsIfNeeded(batch);
				if (!added) {
					super.flushBatchDirect(batch, add);
				}
			}
		};
		resultsBuffer = collector.getTupleBuffer();
		resultsBuffer.setForwardOnly(isForwardOnly());
		analysisRecord = request.analysisRecord;
		analysisRecord.setQueryPlan(processor.getProcessorPlan().getDescriptionProperties());
		transactionContext = request.transactionContext;
		if (this.transactionContext != null && this.transactionContext.getTransactionType() != Scope.NONE) {
			this.transactionState = TransactionState.ACTIVE;
		}
		if (requestMsg.isNoExec()) {
		    doneProducingBatches = true;
            resultsBuffer.close();
            this.cid = null;
		}
	    this.returnsUpdateCount = request.returnsUpdateCount;
		request = null;
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
    		if (batch == null || !(batch.containsRow(this.begin))) {
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
        		int firstOffset = beginRow - batch.getBeginRow();
                List[] memoryRows = batch.getAllTuples();
                List[] rows = new List[count];
                System.arraycopy(memoryRows, firstOffset, rows, 0, endRow - beginRow + 1);
                batch = new TupleBatch(beginRow, rows);
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
        	if (this.isCanceled) {
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
    	this.requestCancel(); //pending work should be canceled for fastest clean up
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
	
}