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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.transaction.xa.Xid;

import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.Request.ProcessingState;
import org.teiid.adminapi.Request.ThreadState;
import org.teiid.adminapi.VDB.Status;
import org.teiid.adminapi.impl.RequestMetadata;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.adminapi.impl.TransactionMetadata;
import org.teiid.adminapi.impl.WorkerPoolStatisticsMetadata;
import org.teiid.client.DQP;
import org.teiid.client.RequestMessage;
import org.teiid.client.RequestMessage.StatementType;
import org.teiid.client.ResultsMessage;
import org.teiid.client.lob.LobChunk;
import org.teiid.client.metadata.MetadataResult;
import org.teiid.client.plan.PlanNode;
import org.teiid.client.util.ResultsFuture;
import org.teiid.client.util.ResultsReceiver;
import org.teiid.client.xa.XATransactionException;
import org.teiid.client.xa.XidImpl;
import org.teiid.common.buffer.BufferManager;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.Streamable;
import org.teiid.core.util.ApplicationInfo;
import org.teiid.core.util.ExecutorUtils;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.dqp.message.AtomicRequestMessage;
import org.teiid.dqp.message.RequestID;
import org.teiid.dqp.service.TransactionContext;
import org.teiid.dqp.service.TransactionContext.Scope;
import org.teiid.dqp.service.TransactionService;
import org.teiid.events.EventDistributor;
import org.teiid.jdbc.EnhancedTimer;
import org.teiid.logging.CommandLogMessage;
import org.teiid.logging.CommandLogMessage.Event;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.query.QueryPlugin;
import org.teiid.query.processor.QueryProcessor;
import org.teiid.query.tempdata.GlobalTableStoreImpl;
import org.teiid.query.tempdata.TempTableDataManager;
import org.teiid.query.tempdata.TempTableStore;
import org.teiid.query.tempdata.TempTableStore.TransactionMode;
import org.teiid.query.util.CommandContext;
import org.teiid.query.util.Options;

/**
 * Implements the core DQP processing.
 */
public class DQPCore implements DQP {
	
	public interface CompletionListener<T> {
		void onCompletion(FutureWork<T> future);
	}
	
	private TeiidExecutor processWorkerPool;
    
    // Resources
    private BufferManager bufferManager;
    private TempTableDataManager dataTierMgr;
    private SessionAwareCache<PreparedPlan> prepPlanCache;
    private SessionAwareCache<CachedResults> rsCache;
    private TransactionService transactionService;
    private EventDistributor eventDistributor;
    
    private DQPConfiguration config = new DQPConfiguration();
    
    private int chunkSize = Streamable.STREAMING_BATCH_SIZE_IN_BYTES;
    
	private Map<RequestID, RequestWorkItem> requests = new ConcurrentHashMap<RequestID, RequestWorkItem>();			
	private Map<String, ClientState> clientState = new ConcurrentHashMap<String, ClientState>();
    
    private int maxActivePlans = DQPConfiguration.DEFAULT_MAX_ACTIVE_PLANS;
    private int currentlyActivePlans;
    private int userRequestSourceConcurrency;
    private LinkedList<RequestWorkItem> waitingPlans = new LinkedList<RequestWorkItem>();
    private LinkedHashSet<RequestWorkItem> bufferFullPlans = new LinkedHashSet<RequestWorkItem>();
    private int maxWaitingPlans = 0;
	private AuthorizationValidator authorizationValidator;
	
	private EnhancedTimer cancellationTimer;
	private Options options;
    
    /**
     * perform a full shutdown and wait for 10 seconds for all threads to finish
     */
    public void stop() {
    	processWorkerPool.shutdownNow();
    	try {
			processWorkerPool.awaitTermination(10, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
		}
    	// TODO: Should we be doing more cleanup here??
		LogManager.logDetail(LogConstants.CTX_DQP, "Stopping the DQP"); //$NON-NLS-1$
    }
    
    /**
     * Return a list of {@link RequestMetadata} for the given session
     */
    public List<RequestMetadata> getRequestsForSession(String sessionId) {
    	ClientState state = getClientState(sessionId, false);
    	if (state == null) {
    		return Collections.emptyList();
    	}
        return buildRequestInfos(state.getRequests(), -1);
    }
    
    public ClientState getClientState(String key, boolean create) {
    	if (key == null) {
    		return null;
    	}
		ClientState state = clientState.get(key);
		if (state == null && create) {
			state = new ClientState(new TempTableStore(key, TransactionMode.ISOLATE_WRITES));
    		clientState.put(key, state);
		}
		return state;
    }

    /**
     * Return a list of all {@link RequestMetadata} 
     */
    public List<RequestMetadata> getRequests() {
		return buildRequestInfos(requests.keySet(), -1);
    } 
    
    public List<RequestMetadata> getLongRunningRequests(){
    	return buildRequestInfos(requests.keySet(), System.currentTimeMillis() - this.config.getQueryThresholdInMilli() );
    }

    private List<RequestMetadata> buildRequestInfos(Collection<RequestID> ids, long longRunningQueryThreshold) {
		List<RequestMetadata> results = new ArrayList<RequestMetadata>();
    	
		for (RequestID requestID : ids) {
            RequestWorkItem holder = requests.get(requestID);
            
            if(holder != null && !holder.isCanceled() && (longRunningQueryThreshold == -1 || holder.getProcessingTimestamp() < longRunningQueryThreshold)) {
            	RequestMetadata req = new RequestMetadata();
            	
            	req.setExecutionId(holder.requestID.getExecutionID());
            	req.setSessionId(holder.requestID.getConnectionID());
            	req.setCommand(holder.requestMsg.getCommandString());
            	req.setStartTime(holder.getProcessingTimestamp());
            	req.setState(holder.isCanceled()?ProcessingState.CANCELED:holder.isDoneProcessing()?ProcessingState.DONE:ProcessingState.PROCESSING);
            	switch (holder.getThreadState()) {
            	case DONE:
            	case IDLE:
            		req.setThreadState(ThreadState.IDLE);
            		break;
            	default:
            		if (holder.isProcessing()) {
            			req.setThreadState(ThreadState.RUNNING);
            		} else {
            			req.setThreadState(ThreadState.QUEUED);
            		}
            	}
            	if (holder.getTransactionContext() != null && holder.getTransactionContext().getTransactionType() != Scope.NONE) {
            		req.setTransactionId(holder.getTransactionContext().getTransactionId());
            	}

                for (DataTierTupleSource conInfo : holder.getConnectorRequests()) {
                    String connectorName = conInfo.getConnectorName();

                    if (connectorName == null) {
                    	continue;
                    }
                    // If the request has not yet completed processing, then
                    // add all the subrequest messages
                	AtomicRequestMessage arm = conInfo.getAtomicRequestMessage();
                	RequestMetadata info = new RequestMetadata();
                	if (conInfo.isQueued()) {
                		info.setThreadState(ThreadState.QUEUED);
                	} else if (conInfo.isRunning()) {
                		info.setThreadState(ThreadState.RUNNING);
                	} else {
                		info.setThreadState(ThreadState.IDLE);
                	}
                	info.setExecutionId(arm.getRequestID().getExecutionID());
                	info.setSessionId(holder.requestID.getConnectionID());
                	info.setCommand(arm.getCommand().toString());
                	info.setStartTime(arm.getProcessingTimestamp());
                	info.setSourceRequest(true);
                	info.setNodeId(arm.getAtomicRequestID().getNodeID());
                	info.setState(conInfo.isCanceled()?ProcessingState.CANCELED:conInfo.isDone()?ProcessingState.DONE:ProcessingState.PROCESSING);
        			results.add(info);
                }
                
            	results.add(req);
            }
        }
    	return results;
	}    

	public ResultsFuture<ResultsMessage> executeRequest(long reqID,RequestMessage requestMsg) throws TeiidProcessingException {
    	DQPWorkContext workContext = DQPWorkContext.getWorkContext();
    	checkActive(workContext);
		RequestID requestID = workContext.getRequestID(reqID);
		requestMsg.setFetchSize(Math.min(requestMsg.getFetchSize(), this.config.getMaxRowsFetchSize()));
		Request request = null;
	    if ( requestMsg.isPreparedStatement() || requestMsg.isCallableStatement() || requestMsg.getRequestOptions().isContinuous()) {
	    	request = new PreparedStatementRequest(prepPlanCache);
	    } else {
	    	request = new Request();
	    }
	    ClientState state = this.getClientState(workContext.getSessionId(), true);
	    if (state.session == null) {
	    	state.session = workContext.getSession();
	    }
	    request.initialize(requestMsg, bufferManager,
				dataTierMgr, transactionService, state.sessionTables,
				workContext, this.prepPlanCache);
	    request.setOptions(options);
	    request.setExecutor(this.processWorkerPool);
		request.setResultSetCacheEnabled(this.rsCache != null);
		request.setAuthorizationValidator(this.authorizationValidator);
		request.setUserRequestConcurrency(this.getUserRequestSourceConcurrency());
        ResultsFuture<ResultsMessage> resultsFuture = new ResultsFuture<ResultsMessage>();
        final RequestWorkItem workItem = new RequestWorkItem(this, requestMsg, request, resultsFuture.getResultsReceiver(), requestID, workContext);
    	logMMCommand(workItem, Event.NEW, null); 
        addRequest(requestID, workItem, state);
        long timeout = workContext.getVDB().getQueryTimeout();
        timeout = Math.min(timeout>0?timeout:Long.MAX_VALUE, config.getQueryTimeout()>0?config.getQueryTimeout():Long.MAX_VALUE);
        if (timeout < Long.MAX_VALUE) {
        	final long finalTimeout = timeout;
        	workItem.setCancelTask(this.cancellationTimer.add(new Runnable() {
				WeakReference<RequestWorkItem> workItemRef = new WeakReference<RequestWorkItem>(workItem);
				@Override
				public void run() {
					try {
						RequestWorkItem wi = workItemRef.get();
						if (wi != null) {
							LogManager.logInfo(LogConstants.CTX_DQP, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31096, wi.requestID, finalTimeout));
							wi.requestCancel();
						}
					} catch (TeiidComponentException e) {
						LogManager.logError(LogConstants.CTX_DQP, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30018));
					}
				}
			}, timeout));
        }
        boolean runInThread = requestMsg.isSync();
        synchronized (waitingPlans) {
			if (runInThread || currentlyActivePlans <= maxActivePlans) {
				startActivePlan(workItem, !runInThread);
			} else {
				if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
		            LogManager.logDetail(LogConstants.CTX_DQP, workItem.requestID, "Queuing plan, since max plans has been reached.");  //$NON-NLS-1$
		        }  
				if (!bufferFullPlans.isEmpty()) {
	        		Iterator<RequestWorkItem> id = bufferFullPlans.iterator();
	        		RequestWorkItem bufferFull = id.next();
	        		id.remove();
					if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
			            LogManager.logDetail(LogConstants.CTX_DQP, bufferFull.requestID, "Restarting plan with full buffer, since there is a pending active plan.");  //$NON-NLS-1$
			        }  
	        		bufferFull.moreWork();
	        	}
				waitingPlans.add(workItem);
				maxWaitingPlans = Math.max(this.maxWaitingPlans, waitingPlans.size());
			}
		}
        if (runInThread) {
        	workItem.useCallingThread = true;
        	workItem.run();
        }
        return resultsFuture;
    }
	
	public ResultsFuture<ResultsMessage> processCursorRequest(long reqID,
			int batchFirst, int fetchSize) throws TeiidProcessingException {
        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
            LogManager.logDetail(LogConstants.CTX_DQP, "DQP process cursor request", batchFirst, fetchSize);  //$NON-NLS-1$
        }
		DQPWorkContext workContext = DQPWorkContext.getWorkContext();
        ResultsFuture<ResultsMessage> resultsFuture = new ResultsFuture<ResultsMessage>();
		RequestWorkItem workItem = getRequestWorkItem(workContext.getRequestID(reqID));
		workItem.requestMore(batchFirst, batchFirst + Math.min(fetchSize, this.config.getMaxRowsFetchSize()) - 1, resultsFuture.getResultsReceiver());
		return resultsFuture;
	}

	void addRequest(RequestID requestID, RequestWorkItem workItem, ClientState state) {
		this.requests.put(requestID, workItem);
		state.addRequest(requestID);
	}

	private void startActivePlan(RequestWorkItem workItem, boolean addToQueue) {
		boolean continuous = workItem.requestMsg.getRequestOptions().isContinuous();
		workItem.active = !continuous;
		if (addToQueue) {
			this.addWork(workItem);
		}
		if (!continuous) {
			this.currentlyActivePlans++;
		}
	}
	
    void finishProcessing(final RequestWorkItem workItem) {
    	synchronized (waitingPlans) {
    		if (!workItem.active) {
        		return;
        	}
        	workItem.active = false;
    		currentlyActivePlans--;
    		bufferFullPlans.remove(workItem.requestID);
			if (!waitingPlans.isEmpty()) {
				RequestWorkItem work = waitingPlans.remove();
				startActivePlan(work, true);
			}
		}
    }
    
    public int getActivePlanCount() {
    	return this.currentlyActivePlans;
    }
    
    public boolean blockOnOutputBuffer(RequestWorkItem item) {
    	synchronized (waitingPlans) {
    		if (!waitingPlans.isEmpty()) {
    			return false;
    		}
    		if (item.useCallingThread || item.getDqpWorkContext().getSession().isEmbedded()) {
    			return false;
    		}
    		this.bufferFullPlans.add(item);
		}
    	return true;
    }
    
    public int getWaitingPlanCount() {
    	return waitingPlans.size();
    }
    
    public int getMaxWaitingPlanWatermark() {
    	return this.maxWaitingPlans;
    }
    
    void removeRequest(final RequestWorkItem workItem) {
    	finishProcessing(workItem);
    	this.requests.remove(workItem.requestID);
    	ClientState state = getClientState(workItem.getDqpWorkContext().getSessionId(), false);
    	if (state != null) {
    		state.removeRequest(workItem.requestID);
    	}
    }
    
    void addWork(Runnable work) {
		this.processWorkerPool.execute(work);
    }
    
    Future<Void> scheduleWork(final Runnable r, long delay) {
    	return this.cancellationTimer.add(r, delay);
    }
    
	public ResultsFuture<?> closeLobChunkStream(int lobRequestId,
			long requestId, String streamId)
			throws TeiidProcessingException {
        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
            LogManager.logDetail(LogConstants.CTX_DQP, "Request to close the Lob stream with Stream id="+streamId+" instance id="+lobRequestId);  //$NON-NLS-1$//$NON-NLS-2$
        }   
        DQPWorkContext workContext = DQPWorkContext.getWorkContext();
        RequestWorkItem workItem = safeGetWorkItem(workContext.getRequestID(requestId));
        if (workItem != null) {
	        workItem.removeLobStream(lobRequestId);
        }
        return ResultsFuture.NULL_FUTURE;
    }
	    
	public ResultsFuture<LobChunk> requestNextLobChunk(int lobRequestId,
			long requestId, String streamId)
			throws TeiidProcessingException {
        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
            LogManager.logDetail(LogConstants.CTX_DQP, "Request for next Lob chunk with Stream id="+streamId+" instance id="+lobRequestId);  //$NON-NLS-1$//$NON-NLS-2$
        }  
        RequestWorkItem workItem = getRequestWorkItem(DQPWorkContext.getWorkContext().getRequestID(requestId));
        ResultsFuture<LobChunk> resultsFuture = new ResultsFuture<LobChunk>();
        workItem.processLobChunkRequest(streamId, lobRequestId, resultsFuture.getResultsReceiver());
        return resultsFuture;
    }
    
    RequestWorkItem getRequestWorkItem(RequestID reqID) throws TeiidProcessingException {
    	RequestWorkItem result = this.requests.get(reqID);
    	if (result == null) {
    		 throw new TeiidProcessingException(QueryPlugin.Event.TEIID30495, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30495, reqID));
    	}
    	return result;
    }
    
	RequestWorkItem safeGetWorkItem(Object processorID) {
    	return this.requests.get(processorID);
	}
	
    public WorkerPoolStatisticsMetadata getWorkerPoolStatistics() {
    	return this.processWorkerPool.getStats();
    }
           
    public void terminateSession(String sessionId) {
        // sometimes there will not be any atomic requests pending, in that
        // situation we still need to clear the master request from our map
        ClientState state = this.clientState.remove(sessionId);
        if (state != null) {
	        for (RequestID reqId : state.getRequests()) {
	            try {
	                cancelRequest(reqId);
	            } catch (TeiidComponentException err) {
	                LogManager.logWarning(LogConstants.CTX_DQP, err, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30026,reqId));
				}
	        }
	        SessionMetadata session = state.session;
	        if (session != null) {
	        	GlobalTableStoreImpl store = CommandContext.removeSessionScopedStore(session);
	        	if (store != null) {
	        		try {
						store.getTempTableStore().removeTempTables();
					} catch (TeiidComponentException e) {
		                LogManager.logDetail(LogConstants.CTX_DQP, e, "Error removing session scoped temp tables"); //$NON-NLS-1$
					}
	        	}
	        }
        }
        
        try {
            transactionService.cancelTransactions(sessionId, false);
        } catch (XATransactionException err) {
            LogManager.logWarning(LogConstants.CTX_DQP, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30005,sessionId));
        } 
    }

    public boolean cancelRequest(String sessionId, long executionId) throws TeiidComponentException {
    	RequestID requestID = new RequestID(sessionId, executionId);
    	return cancelRequest(requestID);
    }
    
    public PlanNode getPlan(String sessionId, long executionId) {
    	RequestID requestID = new RequestID(sessionId, executionId);
        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
            LogManager.logDetail(LogConstants.CTX_DQP, "getPlan for requestID=" + requestID); //$NON-NLS-1$
        }
        RequestWorkItem workItem = safeGetWorkItem(requestID);
        if (workItem == null) {
        	return null;
        }
        QueryProcessor qp = workItem.getProcessor();
        if (qp == null) {
        	return null;
        }
        return qp.getProcessorPlan().getDescriptionProperties();
    }
    
    private boolean cancelRequest(RequestID requestID) throws TeiidComponentException {
        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
            LogManager.logDetail(LogConstants.CTX_DQP, "cancelQuery for requestID=" + requestID); //$NON-NLS-1$
        }
        
        boolean markCancelled = false;
        
        RequestWorkItem workItem = safeGetWorkItem(requestID);
        if (workItem != null) {
        	markCancelled = workItem.requestCancel();
        }
    	if (markCancelled) {
            logMMCommand(workItem, Event.CANCEL, null);
    	} else {
    		LogManager.logDetail(LogConstants.CTX_DQP, QueryPlugin.Util.getString("DQPCore.failed_to_cancel")); //$NON-NLS-1$
    	}
        return markCancelled;
    }
    
	public ResultsFuture<?> closeRequest(long requestId) throws TeiidProcessingException, TeiidComponentException {
        DQPWorkContext workContext = DQPWorkContext.getWorkContext();
        closeRequest(workContext.getRequestID(requestId));
        return ResultsFuture.NULL_FUTURE;
	}
    
    /**
     * Close the request with given ID 
     * @param requestID
     * @throws TeiidComponentException 
     */
    void closeRequest(RequestID requestID) throws TeiidComponentException {
        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
            LogManager.logDetail(LogConstants.CTX_DQP, "closeQuery for requestID=" + requestID); //$NON-NLS-1$
        }
        
        RequestWorkItem workItem = safeGetWorkItem(requestID);
        if (workItem != null) {
        	workItem.requestClose();
        } else {
        	LogManager.logDetail(LogConstants.CTX_DQP, requestID + " close call ignored as the request has already been removed."); //$NON-NLS-1$
        }
    }
    
	public Collection<TransactionMetadata> getTransactions() {
		return this.transactionService.getTransactions();
	}
	
	public void terminateTransaction(String xid) throws AdminException {
		this.transactionService.terminateTransaction(xid);
	}	
	
    void logMMCommand(RequestWorkItem workItem, Event status, Integer rowCount) {
    	if ((status != Event.PLAN && !LogManager.isMessageToBeRecorded(LogConstants.CTX_COMMANDLOGGING, MessageLevel.DETAIL))
    			|| (status == Event.PLAN && !LogManager.isMessageToBeRecorded(LogConstants.CTX_COMMANDLOGGING, MessageLevel.TRACE))) {
    		return;
    	}
    	
        RequestMessage msg = workItem.requestMsg;
        DQPWorkContext workContext = DQPWorkContext.getWorkContext();
        RequestID rID = workItem.requestID;
    	String txnID = null;
		TransactionContext tc = workItem.getTransactionContext();
		if (tc != null && tc.getTransactionType() != Scope.NONE) {
			txnID = tc.getTransactionId();
		}
    	String appName = workContext.getAppName();
        // Log to request log
        CommandLogMessage message = null;
        if (status == Event.NEW) {
            message = new CommandLogMessage(System.currentTimeMillis(), rID.toString(), txnID, workContext.getSessionId(), appName, workContext.getUserName(), workContext.getVdbName(), workContext.getVdbVersion(), msg.getCommandString());
        } else {
            QueryProcessor qp = workItem.getProcessor();
            PlanNode plan = null;
            if (LogManager.isMessageToBeRecorded(LogConstants.CTX_COMMANDLOGGING, MessageLevel.TRACE) && qp != null) {
            	plan = qp.getProcessorPlan().getDescriptionProperties();
            }
            message = new CommandLogMessage(System.currentTimeMillis(), rID.toString(), txnID, workContext.getSessionId(), workContext.getUserName(), workContext.getVdbName(), workContext.getVdbVersion(), rowCount, status, plan);
        }
        LogManager.log(status == Event.PLAN?MessageLevel.TRACE:MessageLevel.DETAIL, LogConstants.CTX_COMMANDLOGGING, message);
    }
    
    public TempTableDataManager getDataTierManager() {
    	return this.dataTierMgr;
    }
    
	public BufferManager getBufferManager() {
		return bufferManager;
	}

	public TransactionService getTransactionService() {
		return transactionService;
	}

	SessionAwareCache<CachedResults> getRsCache() {
		return rsCache;
	}
	
	int getProcessorTimeSlice() {
		return this.config.getTimeSliceInMilli();
	}	
	
	int getChunkSize() {
		return chunkSize;
	}
	
	public void start(DQPConfiguration theConfig) {
		this.config = theConfig;
        this.authorizationValidator = config.getAuthorizationValidator();
        this.chunkSize = config.getLobChunkSizeInKB() * 1024;

        this.processWorkerPool = config.getTeiidExecutor();
        //we don't want cancellations waiting on normal processing, so they get a small dedicated pool
        //TODO: overflow to the worker pool
        Executor timeoutExecutor = ExecutorUtils.newFixedThreadPool(3, "Server Side Timeout"); //$NON-NLS-1$
        this.cancellationTimer = new EnhancedTimer(timeoutExecutor, timeoutExecutor);
        this.maxActivePlans = config.getMaxActivePlans();
        
        if (this.maxActivePlans > config.getMaxThreads()) {
        	LogManager.logWarning(LogConstants.CTX_DQP, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30006, this.maxActivePlans, config.getMaxThreads()));
        	this.maxActivePlans = config.getMaxThreads();
        }

        //hack to set the max active plans
        this.bufferManager.setMaxActivePlans(this.maxActivePlans);
        try {
			this.bufferManager.initialize();
		} catch (TeiidComponentException e) {
			 throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30496, e);
		}
        
        this.userRequestSourceConcurrency = config.getUserRequestSourceConcurrency();
        if (this.userRequestSourceConcurrency < 1) {
        	this.userRequestSourceConcurrency = Math.min(config.getMaxThreads(), 2*config.getMaxThreads()/this.maxActivePlans);
        }
        
        DataTierManagerImpl processorDataManager = new DataTierManagerImpl(this, this.bufferManager, this.config.isDetectingChangeEvents());
        processorDataManager.setEventDistributor(eventDistributor);
		dataTierMgr = new TempTableDataManager(processorDataManager, this.bufferManager, this.rsCache);
		dataTierMgr.setExecutor(new TempTableDataManager.RequestExecutor() {
			
			@Override
			public void execute(String command, List<?> parameters) {
				final String sessionId = DQPWorkContext.getWorkContext().getSessionId();
				RequestMessage request = new RequestMessage(command);
				request.setParameterValues(parameters);
				request.setStatementType(StatementType.PREPARED);
				ResultsFuture<ResultsMessage> result;
				try {
					result = executeRequest(0, request);
				} catch (TeiidProcessingException e) {
					throw new TeiidRuntimeException(e);
				}
				result.addCompletionListener(new ResultsFuture.CompletionListener<ResultsMessage>() {

					@Override
					public void onCompletion(
							ResultsFuture<ResultsMessage> future) {
						terminateSession(sessionId);
					}
					
				});
			}
		});
        dataTierMgr.setEventDistributor(eventDistributor);
        //for now options are scoped to the engine - vdb scoping is a todo
        options = new Options();
        options.setProperties(System.getProperties());
        PropertiesUtils.setBeanProperties(options, options.getProperties(), "org.teiid", true); //$NON-NLS-1$
        LogManager.logDetail(LogConstants.CTX_DQP, "DQPCore started maxThreads", this.config.getMaxThreads(), "maxActivePlans", this.maxActivePlans, "source concurrency", this.userRequestSourceConcurrency); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}
	
	public void setBufferManager(BufferManager mgr) {
		this.bufferManager = mgr;
	}
	
	public void setTransactionService(TransactionService service) {
		this.transactionService = service;
	}
	
	public void setEventDistributor(EventDistributor eventDistributor) {
		this.eventDistributor = eventDistributor;
	}
	
	@Override
	public boolean cancelRequest(long requestID)
			throws TeiidProcessingException, TeiidComponentException {
		DQPWorkContext workContext = DQPWorkContext.getWorkContext();
		return this.cancelRequest(workContext.getRequestID(requestID));
	}
	
	// local txn
	public ResultsFuture<?> begin() throws XATransactionException {
    	String threadId = DQPWorkContext.getWorkContext().getSessionId();
    	this.getTransactionService().begin(threadId);
    	return ResultsFuture.NULL_FUTURE;
	}
	
	// local txn
	public ResultsFuture<?> commit() throws XATransactionException {
		final String threadId = DQPWorkContext.getWorkContext().getSessionId();
		Callable<Void> processor = new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				getTransactionService().commit(threadId);
				return null;
			}
		};
		return addWork(processor, 0);
	}
	
	// local txn
	public ResultsFuture<?> rollback() throws XATransactionException {
		final String threadId = DQPWorkContext.getWorkContext().getSessionId();
		Callable<Void> processor = new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				getTransactionService().rollback(threadId);
				return null;
			}
		};
		return addWork(processor, 0);
	}

	// global txn
	public ResultsFuture<?> commit(final XidImpl xid, final boolean onePhase) throws XATransactionException {
		Callable<Void> processor = new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				DQPWorkContext workContext = DQPWorkContext.getWorkContext();
				getTransactionService().commit(workContext.getSessionId(), xid, onePhase, workContext.getSession().isEmbedded());
				return null;
			}
		};
		return addWork(processor, 0);
	}
	// global txn
	public ResultsFuture<?> end(XidImpl xid, int flags) throws XATransactionException {
		DQPWorkContext workContext = DQPWorkContext.getWorkContext();
		this.getTransactionService().end(workContext.getSessionId(), xid, flags, workContext.getSession().isEmbedded());
		return ResultsFuture.NULL_FUTURE;
	}
	// global txn
	public ResultsFuture<?> forget(XidImpl xid) throws XATransactionException {
		DQPWorkContext workContext = DQPWorkContext.getWorkContext();
		this.getTransactionService().forget(workContext.getSessionId(), xid, workContext.getSession().isEmbedded());
		return ResultsFuture.NULL_FUTURE;
	}
		
	// global txn
	public ResultsFuture<Integer> prepare(final XidImpl xid) throws XATransactionException {
		Callable<Integer> processor = new Callable<Integer>() {
			@Override
			public Integer call() throws Exception {
				DQPWorkContext workContext = DQPWorkContext.getWorkContext();
				return getTransactionService().prepare(workContext.getSessionId(),xid, workContext.getSession().isEmbedded());
			}
		};
		return addWork(processor, 10);
	}

	private <T> ResultsFuture<T> addWork(final Callable<T> processor, int priority) {
		final ResultsFuture<T> result = new ResultsFuture<T>();
		final ResultsReceiver<T> receiver = result.getResultsReceiver();
		Runnable r = new Runnable() {

			@Override
			public void run() {
				try {
					receiver.receiveResults(processor.call());
				} catch (Throwable t) {
					receiver.exceptionOccurred(t);
				}
			}
		};
		FutureWork<T> work = new FutureWork<T>(r, null, priority);
		if (DQPWorkContext.getWorkContext().useCallingThread()) {
			work.run();
		} else {
			this.addWork(work);
		}
		return result;
	}
	
	// global txn
	public ResultsFuture<Xid[]> recover(int flag) throws XATransactionException {
		ResultsFuture<Xid[]> result = new ResultsFuture<Xid[]>();
		DQPWorkContext workContext = DQPWorkContext.getWorkContext();
		result.getResultsReceiver().receiveResults(this.getTransactionService().recover(flag, workContext.getSession().isEmbedded()));
		return result;
	}
	// global txn
	public ResultsFuture<?> rollback(final XidImpl xid) throws XATransactionException {
		Callable<Void> processor = new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				DQPWorkContext workContext = DQPWorkContext.getWorkContext();
				getTransactionService().rollback(workContext.getSessionId(),xid, workContext.getSession().isEmbedded());
				return null;
			}
		};
		return addWork(processor, 0);
	}
	// global txn
	public ResultsFuture<?> start(final XidImpl xid, final int flags, final int timeout)
			throws XATransactionException {
		final DQPWorkContext workContext = DQPWorkContext.getWorkContext();
		if (workContext.getSession().isEmbedded()) {
		    //must be a synch call regardless since the txn should be associated with the thread.
			getTransactionService().start(workContext.getSessionId(), xid, flags, timeout, true);
			return ResultsFuture.NULL_FUTURE;
		}
		Callable<Void> processor = new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				getTransactionService().start(workContext.getSessionId(), xid, flags, timeout, false);
				return null;
			}
		};
		return addWork(processor, 100);
	}

	public MetadataResult getMetadata(long requestID)
			throws TeiidComponentException, TeiidProcessingException {
		DQPWorkContext workContext = DQPWorkContext.getWorkContext();
		MetaDataProcessor processor = new MetaDataProcessor(this, this.prepPlanCache,  workContext.getVdbName(), workContext.getVdbVersion());
		return processor.processMessage(workContext.getRequestID(requestID), workContext, null, true);
	}

	public MetadataResult getMetadata(long requestID, String preparedSql,
			boolean allowDoubleQuotedVariable)
			throws TeiidComponentException, TeiidProcessingException {
		DQPWorkContext workContext = DQPWorkContext.getWorkContext();
		checkActive(workContext);
		MetaDataProcessor processor = new MetaDataProcessor(this, this.prepPlanCache, workContext.getVdbName(), workContext.getVdbVersion());
		return processor.processMessage(workContext.getRequestID(requestID), workContext, preparedSql, allowDoubleQuotedVariable);
	}

	private void checkActive(DQPWorkContext workContext)
			throws TeiidProcessingException {
		if (workContext.getVDB().getStatus() != Status.ACTIVE) {
			throw new TeiidProcessingException(QueryPlugin.Event.TEIID31099, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31099, workContext.getVDB()));
		}
	}
	
	public boolean isExceptionOnMaxSourceRows() {
		return this.config.isExceptionOnMaxSourceRows();
	}
	
	public int getMaxSourceRows() {
		return this.config.getMaxSourceRows();
	}
	
	public int getMaxRowsFetchSize() {
		return this.config.getMaxRowsFetchSize();
	}
	
	public void setResultsetCache(SessionAwareCache<CachedResults> cache) {
		this.rsCache = cache;
	}
	
	public void setPreparedPlanCache(SessionAwareCache<PreparedPlan> cache) {
		this.prepPlanCache = cache;
	}	
	
	public int getUserRequestSourceConcurrency() {
		return userRequestSourceConcurrency;
	}
	
	void setUserRequestSourceConcurrency(int userRequestSourceConcurrency) {
		this.userRequestSourceConcurrency = userRequestSourceConcurrency;
	}
	
	public int getMaxActivePlans() {
		return maxActivePlans;
	}
	
	SessionAwareCache<PreparedPlan> getPrepPlanCache() {
		return prepPlanCache;
	}
	
	public String getRuntimeVersion() {
		return ApplicationInfo.getInstance().getBuildNumber();
	}	
}