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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.transaction.InvalidTransactionException;
import javax.transaction.SystemException;

import org.teiid.connector.xa.api.TransactionContext;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleBuffer;
import com.metamatrix.common.comm.api.ResultsReceiver;
import com.metamatrix.common.lob.LobChunk;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.common.xa.XATransactionException;
import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.dqp.DQPPlugin;
import com.metamatrix.dqp.exception.SourceWarning;
import com.metamatrix.dqp.message.AtomicRequestID;
import com.metamatrix.dqp.message.ParameterInfo;
import com.metamatrix.dqp.message.RequestID;
import com.metamatrix.dqp.message.RequestMessage;
import com.metamatrix.dqp.message.ResultsMessage;
import com.metamatrix.dqp.service.CommandLogMessage;
import com.metamatrix.dqp.service.TransactionService;
import com.metamatrix.dqp.util.LogConstants;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.analysis.QueryAnnotation;
import com.metamatrix.query.execution.QueryExecPlugin;
import com.metamatrix.query.processor.BatchCollector;
import com.metamatrix.query.processor.QueryProcessor;
import com.metamatrix.query.processor.BatchCollector.BatchHandler;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.Option;
import com.metamatrix.query.sql.lang.SPParameter;
import com.metamatrix.query.sql.lang.StoredProcedure;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;

public class RequestWorkItem extends AbstractWorkItem {
	
	private enum ProcessingState {NEW, PROCESSING, CLOSE}
	private ProcessingState state = ProcessingState.NEW;
    
	private enum TransactionState {NONE, ACTIVE, END, DONE}
	private TransactionState transactionState = TransactionState.NONE;
	
	/*
	 * Obtained at construction time 
	 */
    protected final DQPCore dqpCore;
    final RequestMessage requestMsg;    
    final RequestID requestID;
    protected Request request; //provides the processing plan, held on a temporary basis
    final private int processorTimeslice;
    //protected ResultSetCache rsCache;
	//protected CacheID cid;
	final private TransactionService transactionService;
	final DQPWorkContext dqpWorkContext;
	
	//results request
	private ResultsReceiver<ResultsMessage> resultsReceiver;
	private int begin;
	private int end;
        
    /*
     * obtained during new
     */
    private volatile QueryProcessor processor;
    private BatchCollector collector;
    protected Command originalCommand;
    private AnalysisRecord analysisRecord;
    private TransactionContext transactionContext;
    protected TupleBuffer resultsBuffer;
    private TupleBatch savedBatch;
    private Collection schemas;     // These are schemas associated with XML results
    private boolean returnsUpdateCount;
    
    /*
     * maintained during processing
     */
    private Throwable processingException;
    private Map<AtomicRequestID, DataTierTupleSource> connectorInfo = new ConcurrentHashMap<AtomicRequestID, DataTierTupleSource>(4);
    // This exception contains details of all the atomic requests that failed when query is run in partial results mode.
    private List<MetaMatrixException> warnings = new LinkedList<MetaMatrixException>();

    private boolean doneProducingBatches;
    protected boolean isClosed;
    private volatile boolean isCanceled;
    private volatile boolean closeRequested;
    
    private Map<Integer, LobWorkItem> lobStreams = Collections.synchronizedMap(new HashMap<Integer, LobWorkItem>(4));
    
    public RequestWorkItem(DQPCore dqpCore, RequestMessage requestMsg, Request request, ResultsReceiver<ResultsMessage> receiver, RequestID requestID, DQPWorkContext workContext) {
        this.requestMsg = requestMsg;
        this.requestID = requestID;
        this.processorTimeslice = dqpCore.getProcessorTimeSlice();
        /*this.rsCache = dqpCore.getRsCache();
        if (this.rsCache != null) {
        	this.cid = this.rsCache.createCacheID(workContext, requestMsg.getCommandString(), requestMsg.getParameterValues());
        }*/
        this.transactionService = dqpCore.getTransactionServiceDirect();
        this.dqpCore = dqpCore;
        this.request = request;
        this.dqpWorkContext = workContext;
        this.requestResults(1, requestMsg.getFetchSize(), receiver);
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
		DQPWorkContext.setWorkContext(this.dqpWorkContext);
		
        LogManager.logDetail(LogConstants.CTX_DQP, "############# PW PROCESSING on", requestID, "with state", state, "###########"); //$NON-NLS-1$ //$NON-NLS-2$  //$NON-NLS-3$
        
        try {
            if (this.state == ProcessingState.NEW) {
                state = ProcessingState.PROCESSING;
        		processNew();
                if (isCanceled) {
                	this.processingException = new MetaMatrixProcessingException(QueryExecPlugin.Util.getString("QueryProcessor.request_cancelled", this.requestID)); //$NON-NLS-1$
                    state = ProcessingState.CLOSE;
                } 
        	}
            if (this.state == ProcessingState.PROCESSING) {
            	processMore();
            	if (this.closeRequested) {
            		this.state = ProcessingState.CLOSE;
            	}
            }                  	            
        } catch (BlockedException e) {
            LogManager.logDetail(LogConstants.CTX_DQP, "############# PW EXITING on", requestID, "- processor blocked ###########"); //$NON-NLS-1$ //$NON-NLS-2$
        } catch (QueryProcessor.ExpiredTimeSliceException e) {
            LogManager.logDetail(LogConstants.CTX_DQP, "############# PW reenqueueing ", requestID, "- time slice expired ###########"); //$NON-NLS-1$ //$NON-NLS-2$
            this.moreWork();
        } catch (Throwable e) {
        	LogManager.logDetail(LogConstants.CTX_DQP, e, "############# PW EXITING on", requestID, "- error occurred ###########"); //$NON-NLS-1$ //$NON-NLS-2$
            
            if (!isCanceled()) {
            	logCommandError();
                //Case 5558: Differentiate between system level errors and
                //processing errors.  Only log system level errors as errors, 
                //log the processing errors as warnings only
                if(e instanceof MetaMatrixProcessingException) {                          
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
        	} else if (isClosed) {
        		/*
        		 * since there may be a client waiting notify them of a problem
        		 */
        		if (this.processingException == null) {
        			this.processingException = new IllegalStateException("Request is already closed"); //$NON-NLS-1$
        		}
        		sendError();
        	}        	    		        	
        }
    }

	protected void processMore() throws SystemException, BlockedException, MetaMatrixCoreException {
		this.processor.getContext().setTimeSliceEnd(System.currentTimeMillis() + this.processorTimeslice);
		if (!doneProducingBatches) {
			sendResultsIfNeeded(null);
			collector.collectTuples();
		    doneProducingBatches = this.resultsBuffer.isFinal();
		}
		if (doneProducingBatches) {
            /*if(rsCache != null && requestMsg.useResultSetCache() && originalCommand.areResultsCachable()){
            	boolean sessionScope = this.processor.getContext().isSessionFunctionEvaluated();
                CacheResults cr = new CacheResults();
            	cr.setCommand(originalCommand);
                cr.setAnalysisRecord(analysisRecord);
            }*/
			if (this.transactionState == TransactionState.ACTIVE) {
				boolean endState = true;
				/*
				 * TEIID-14 if we are done producing batches, then proactively close transactional 
				 * executions even ones that were intentionally kept alive. this may 
				 * break the read of a lob from a transactional source under a transaction 
				 * if the source does not support holding the clob open after commit
				 */
	        	for (DataTierTupleSource connectorRequest : this.connectorInfo.values()) {
	        		if (connectorRequest.isTransactional()) {
	        			connectorRequest.fullyCloseSource();
	        			endState = false;
	        		}
	            }
				if (endState) {
					this.transactionState = TransactionState.END;
				}
			}
			if (this.transactionState == TransactionState.END && transactionContext.getTransactionType() == TransactionContext.Scope.REQUEST) {
				this.transactionService.commit(transactionContext);
				this.transactionState = TransactionState.DONE;
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
			try {
				this.processor.closeProcessing();
			} catch (MetaMatrixComponentException e) {
				if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
					LogManager.logDetail(LogConstants.CTX_DQP, e, e.getMessage());
				}
			}
			
			if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
		        LogManager.logDetail(LogConstants.CTX_DQP, "Removing tuplesource for the request " + requestID); //$NON-NLS-1$
		    }
			rowcount = resultsBuffer.getRowCount();
		    resultsBuffer.remove();
			
			for (DataTierTupleSource connectorRequest : this.connectorInfo.values()) {
				try {
					connectorRequest.fullyCloseSource();
				} catch (MetaMatrixComponentException e) {
					if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
						LogManager.logDetail(LogConstants.CTX_DQP, e, e.getMessage());
					}
				}
		    }

			this.resultsBuffer = null;
		}

		if (this.transactionState == TransactionState.ACTIVE) {
			if (!this.connectorInfo.isEmpty()) {
				return; //wait for pending connector work
			}
			this.transactionState = TransactionState.END;
		} 
		
		if (this.transactionState == TransactionState.END && transactionContext.getTransactionType() == TransactionContext.Scope.REQUEST) {
			this.transactionState = TransactionState.DONE;
            try {
        		this.transactionService.rollback(transactionContext);
            } catch (XATransactionException e1) {
                LogManager.logWarning(LogConstants.CTX_DQP, e1, DQPPlugin.Util.getString("ProcessWorker.failed_rollback")); //$NON-NLS-1$           
            } catch (SystemException err) {
                LogManager.logWarning(LogConstants.CTX_DQP, err, DQPPlugin.Util.getString("ProcessWorker.failed_rollback")); //$NON-NLS-1$
            }
		}
		
		isClosed = true;

		dqpCore.removeRequest(this);
	    
		if (this.processingException != null) {
			sendError();			
		} else {
	        dqpCore.logMMCommand(this, false, false, rowcount);
		}
	}

	protected void processNew() throws MetaMatrixProcessingException, MetaMatrixComponentException {
		request.processRequest();
		originalCommand = request.userCommand;
		processor = request.processor;
		collector = processor.createBatchCollector();
		collector.setBatchHandler(new BatchHandler() {
			public void batchProduced(TupleBatch batch) throws MetaMatrixComponentException {
			    sendResultsIfNeeded(batch);
			}
		});
		resultsBuffer = collector.getTupleBuffer();
		if (requestMsg.getCursorType() == ResultSet.TYPE_FORWARD_ONLY) {
			resultsBuffer.setForwardOnly(true);
		}
		analysisRecord = request.analysisRecord;
		schemas = request.schemas;
		transactionContext = request.transactionContext;
		if (this.transactionContext != null && this.transactionContext.isInTransaction()) {
			this.transactionState = TransactionState.ACTIVE;
		}
		Option option = originalCommand.getOption();
		if (option != null && option.getPlanOnly()) {
		    doneProducingBatches = true;
            resultsBuffer.close();
		}
		
	    if (analysisRecord.recordQueryPlan()) {
	        analysisRecord.setQueryPlan(processor.getProcessorPlan().getDescriptionProperties());
	    }
	    this.returnsUpdateCount = request.returnsUpdateCount;
		request = null;
	}

	/**
	 * Send results if they have been requested.  This should only be called from the processing thread.
	 */
	protected void sendResultsIfNeeded(TupleBatch batch) throws MetaMatrixComponentException {
		ResultsMessage response = null;
		ResultsReceiver<ResultsMessage> receiver = null;
		
		synchronized (this) {
			if (this.resultsReceiver == null
					|| (this.begin > this.resultsBuffer.getRowCount() && !doneProducingBatches)
					|| (this.transactionState == TransactionState.ACTIVE)) {
				return;
			}
		}
		if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
			LogManager.logDetail(LogConstants.CTX_DQP, "[RequestWorkItem.sendResultsIfNeeded] requestID: " + requestID + " resultsID: " + this.resultsBuffer + " done: " + doneProducingBatches );   //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		}

    	if (batch == null || !(batch.getBeginRow() <= this.begin && batch.getEndRow() >= this.begin)) {
    		if (savedBatch != null && savedBatch.getBeginRow() <= this.begin && savedBatch.getEndRow() >= this.begin) {
    			batch = savedBatch;
    		} else {
    			batch = resultsBuffer.getBatch(begin);
    		}
    		//TODO: support fetching more than 1 batch
    		int count = this.end - this.begin + 1;
    		if (batch.getRowCount() > count) {
    			if (requestMsg.getCursorType() == ResultSet.TYPE_FORWARD_ONLY) {
    				savedBatch = batch;
    			}
    			int beginRow = Math.min(this.begin, batch.getEndRow() - count + 1);
    			int endRow = Math.min(beginRow + count - 1, batch.getEndRow());
        		int firstOffset = beginRow - batch.getBeginRow();
                List[] memoryRows = batch.getAllTuples();
                List[] rows = new List[count];
                System.arraycopy(memoryRows, firstOffset, rows, 0, endRow - beginRow + 1);
                batch = new TupleBatch(beginRow, rows);
    		}
    	}
        int finalRowCount = this.resultsBuffer.isFinal()?this.resultsBuffer.getRowCount():-1;
        
        response = createResultsMessage(requestMsg, batch.getAllTuples(), this.processor.getProcessorPlan().getOutputElements(), analysisRecord);
        response.setFirstRow(batch.getBeginRow());
        response.setLastRow(batch.getEndRow());
        response.setUpdateResult(this.returnsUpdateCount);
        // set final row
        response.setFinalRow(finalRowCount);

        // send any schemas associated with the results
        response.setSchemas(this.schemas);
        
        // send any warnings with the response object
        List<Throwable> responseWarnings = new ArrayList<Throwable>();
		List<Exception> currentWarnings = processor.getAndClearWarnings();
	    if (currentWarnings != null) {
	    	responseWarnings.addAll(currentWarnings);
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
        synchronized (this) {
            receiver = this.resultsReceiver;
            this.resultsReceiver = null;    
		}
        receiver.receiveResults(response);
	}
    
    public static ResultsMessage createResultsMessage(RequestMessage message, List[] batch, List columnSymbols, AnalysisRecord analysisRecord) {
        String[] columnNames = new String[columnSymbols.size()];
        String[] dataTypes = new String[columnSymbols.size()];

        for(int i=0; i<columnSymbols.size(); i++) {
            SingleElementSymbol symbol = (SingleElementSymbol) columnSymbols.get(i);
            columnNames[i] = SingleElementSymbol.getShortName(symbol.getOutputName());
            dataTypes[i] = DataTypeManager.getDataTypeName(symbol.getType());
        }
        
        ResultsMessage result = new ResultsMessage(message, batch, columnNames, dataTypes);
        setAnalysisRecords(result, analysisRecord);
        return result;
    }
    
	private static void setAnalysisRecords(ResultsMessage response, AnalysisRecord analysisRecord) {
        if(analysisRecord != null) {
            response.setPlanDescription(analysisRecord.getQueryPlan());
            response.setDebugLog(analysisRecord.getDebugLog());
            
            // Convert annotations to JDBC expected format - String[4]
            Collection anns = analysisRecord.getAnnotations();
            Collection converted = null;
            if(anns != null) {
                converted = new ArrayList(anns.size());
                Iterator annIter = anns.iterator();
                while(annIter.hasNext()) {
                    QueryAnnotation ann = (QueryAnnotation) annIter.next();
                    String[] jdbcAnn = new String[4];
                    jdbcAnn[0] = ann.getCategory();
                    jdbcAnn[1] = ann.getAnnotation();
                    jdbcAnn[2] = ann.getResolution();
                    jdbcAnn[3] = "" + ann.getPriority(); //$NON-NLS-1$
                    converted.add(jdbcAnn);
                } 
                response.setAnnotations(converted);
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
        setAnalysisRecords(response, analysisRecord);
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
    
    public boolean requestCancel() throws MetaMatrixComponentException {
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
	                } catch (InvalidTransactionException err) {
	                    LogManager.logWarning(LogConstants.CTX_DQP, "rollback failed for requestID=" + requestID.getConnectionID()); //$NON-NLS-1$
	                } catch (SystemException err) {
	                    throw new MetaMatrixComponentException(err);
	                }
	            }
        	} finally {
        		this.moreWork();
        	}
        }
        return true;
    }
    
    public boolean requestAtomicRequestCancel(AtomicRequestID ari) throws MetaMatrixComponentException {
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
    
    public void requestClose() throws MetaMatrixComponentException {
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
        
    /**
     * Log the command to the MM cmd log. 
     */
    private void logCommandError() {
        String transactionID = null;
        if (this.transactionContext != null && this.transactionContext.isInTransaction()) {
            transactionID = this.transactionContext.getTxnID();
        }
        CommandLogMessage message = new CommandLogMessage(System.currentTimeMillis(), requestID.toString(), transactionID == null ? null : transactionID, requestID.getConnectionID(), dqpWorkContext.getUserName(), dqpWorkContext.getVdbName(), dqpWorkContext.getVdbVersion(), -1, false, true);
        LogManager.log(MessageLevel.INFO, LogConstants.CTX_COMMANDLOGGING, message);
    }

	boolean isCanceled() {
		return isCanceled;
	}
	
	Command getOriginalCommand() throws MetaMatrixProcessingException {
		if (this.originalCommand == null) {
			if (this.processingException != null) {
				throw new MetaMatrixProcessingException(this.processingException);
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
	
	void setTransactionContext(TransactionContext transactionContext) {
		this.transactionContext = transactionContext;
	}
	
	Collection<DataTierTupleSource> getConnectorRequests() {
		return new LinkedList<DataTierTupleSource>(this.connectorInfo.values());
	}
	
	DataTierTupleSource getConnectorRequest(AtomicRequestID id) {
		return this.connectorInfo.get(id);
	}
	
	public List<MetaMatrixException> getWarnings() {
		return warnings;
	}

	@Override
	public String toString() {
		return this.requestID.toString();
	}

	public DQPWorkContext getDqpWorkContext() {
		return dqpWorkContext;
	}

}