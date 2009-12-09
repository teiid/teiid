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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import javax.transaction.InvalidTransactionException;
import javax.transaction.SystemException;
import javax.transaction.xa.Xid;

import org.teiid.connector.xa.api.TransactionContext;
import org.teiid.dqp.internal.cache.CacheID;
import org.teiid.dqp.internal.cache.CacheResults;
import org.teiid.dqp.internal.cache.DQPContextCache;
import org.teiid.dqp.internal.cache.ResultSetCache;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.cache.CacheFactory;
import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.application.ApplicationService;
import com.metamatrix.common.application.DQPConfigSource;
import com.metamatrix.common.application.ServiceLoader;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.lob.LobChunk;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.queue.WorkerPool;
import com.metamatrix.common.queue.WorkerPoolFactory;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.common.xa.MMXid;
import com.metamatrix.common.xa.XATransactionException;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.dqp.DQPPlugin;
import com.metamatrix.dqp.client.ClientSideDQP;
import com.metamatrix.dqp.client.MetadataResult;
import com.metamatrix.dqp.client.ResultsFuture;
import com.metamatrix.dqp.embedded.DQPEmbeddedProperties;
import com.metamatrix.dqp.internal.datamgr.ConnectorID;
import com.metamatrix.dqp.message.AtomicRequestID;
import com.metamatrix.dqp.message.AtomicRequestMessage;
import com.metamatrix.dqp.message.RequestID;
import com.metamatrix.dqp.message.RequestMessage;
import com.metamatrix.dqp.message.ResultsMessage;
import com.metamatrix.dqp.service.BufferService;
import com.metamatrix.dqp.service.CommandLogMessage;
import com.metamatrix.dqp.service.ConfigurationService;
import com.metamatrix.dqp.service.DQPServiceNames;
import com.metamatrix.dqp.service.DataService;
import com.metamatrix.dqp.service.MetadataService;
import com.metamatrix.dqp.service.TransactionService;
import com.metamatrix.dqp.service.VDBService;
import com.metamatrix.dqp.util.LogConstants;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.processor.ProcessorDataManager;
import com.metamatrix.query.tempdata.TempTableStoreImpl;
import com.metamatrix.server.serverapi.RequestInfo;

/**
 * Implements the core DQP processing.
 */
@Singleton
public class DQPCore implements ClientSideDQP {
	
	static class ClientState {
		List<RequestID> requests;
		TempTableStoreImpl tempTableStoreImpl;
		
		public ClientState(TempTableStoreImpl tableStoreImpl) {
			this.tempTableStoreImpl = tableStoreImpl;
		}
		
		public synchronized void addRequest(RequestID requestID) {
			if (requests == null) {
				requests = new LinkedList<RequestID>();
			}
			requests.add(requestID);
		}
		
		public synchronized List<RequestID> getRequests() {
			if (requests == null) {
				return Collections.emptyList();
			}
			return new ArrayList<RequestID>(requests);
		}

		public synchronized void removeRequest(RequestID requestID) {
			if (requests != null) {
				requests.remove(requestID);
			}
		}
		
	}
	
    //Constants
    private static final int DEFAULT_MAX_CODE_TABLE_RECORDS = 10000;
    private static final int DEFAULT_MAX_CODE_TABLES = 200;
    private static final int DEFAULT_MAX_CODE_RECORDS = 200000;
    private static final int DEFAULT_FETCH_SIZE = 2000;
    private static final int DEFAULT_PROCESSOR_TIMESLICE = 2000;
    private static final String PROCESS_PLAN_QUEUE_NAME = "QueryProcessorQueue"; //$NON-NLS-1$
    private static final int DEFAULT_MAX_PROCESS_WORKERS = 15;

    // System properties for Code Table
    private int maxCodeTableRecords = DEFAULT_MAX_CODE_TABLE_RECORDS;
    private int maxCodeTables = DEFAULT_MAX_CODE_TABLES;
    private int maxCodeRecords = DEFAULT_MAX_CODE_RECORDS;
    
    private int maxFetchSize = DEFAULT_FETCH_SIZE;
    
    // Resources
    private BufferManager bufferManager;
    private ProcessorDataManager dataTierMgr;
    private PreparedPlanCache prepPlanCache;
    private TransactionService transactionService;
    private MetadataService metadataService;
    private ResultSetCache rsCache;
    
    // Query worker pool for processing plans
    private WorkerPool processWorkerPool;
    private int processorTimeslice = DEFAULT_PROCESSOR_TIMESLICE;
    private boolean processorDebugAllowed;
    
    private int chunkSize = 0;
    
	private Map<RequestID, RequestWorkItem> requests = new ConcurrentHashMap<RequestID, RequestWorkItem>();			
	private Map<String, ClientState> clientState = Collections.synchronizedMap(new HashMap<String, ClientState>());
	private DQPContextCache contextCache;
    private ServiceLoader loader = new ServiceLoader();
    private CacheFactory cacheFactory;
    
    private ApplicationEnvironment environment = new ApplicationEnvironment();

    /**
     * perform a full shutdown and wait for 10 seconds for all threads to finish
     * @throws ApplicationLifecycleException 
     */
    public void stop() throws ApplicationLifecycleException {
		LogManager.logDetail(LogConstants.CTX_DQP, "Stopping the DQP"); //$NON-NLS-1$
    	processWorkerPool.shutdownNow();
    	try {
			processWorkerPool.awaitTermination(10, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
		}
    	contextCache.shutdown();
    	this.environment.stop();
    	
    	if (cacheFactory != null) {
    		cacheFactory.destroy();
    	}
    }
    
    /**
     * Return a list of {@link RequestInfo} for the given session
     */
    public List<RequestInfo> getRequestsByClient(String clientConnection) {
    	ClientState state = getClientState(clientConnection, false);
    	if (state == null) {
    		return Collections.emptyList();
    	}
        return buildRequestInfos(state.getRequests());
    }
    
    public ClientState getClientState(String key, boolean create) {
    	synchronized (clientState) {
    		ClientState state = clientState.get(key);
    		if (state == null && create) {
    			state = new ClientState(new TempTableStoreImpl(bufferManager, key, null));
        		clientState.put(key, state);
    		}
    		return state;
		}
    }

    /**
     * Return a list of all {@link RequestInfo} 
     */
    public List<RequestInfo> getRequests() {
		return buildRequestInfos(requests.keySet());
    } 

    private List<RequestInfo> buildRequestInfos(Collection<RequestID> ids) {
		List<RequestInfo> results = new ArrayList<RequestInfo>();
    	for (RequestID requestID : ids) {
            RequestWorkItem holder = requests.get(requestID);
            
            if(holder != null && !holder.isCanceled()) {
            	RequestInfo req = new RequestInfo(holder.requestID, holder.requestMsg.getCommandString(), holder.requestMsg.getSubmittedTimestamp(), holder.requestMsg.getProcessingTimestamp());
            	req.setSessionToken(holder.dqpWorkContext.getSessionToken());
            	if (holder.getTransactionContext() != null && holder.getTransactionContext().isInTransaction()) {
            		req.setTransactionId(holder.getTransactionContext().getTxnID());
            	}

                for (DataTierTupleSource conInfo : holder.getConnectorRequests()) {
                    ConnectorID connectorID = conInfo.getConnectorId();

                    if (connectorID == null) {
                    	continue;
                    }
                    // If the request has not yet completed processing, then
                    // add all the subrequest messages
                	AtomicRequestMessage arm = conInfo.getAtomicRequestMessage();
                	RequestInfo info = new RequestInfo(arm.getRequestID(), arm.getCommand().toString(), arm.getSubmittedTimestamp(), arm.getProcessingTimestamp());
                	info.setSessionToken(holder.dqpWorkContext.getSessionToken());
                	info.setConnectorBindingUUID(arm.getConnectorBindingID());
        			info.setNodeID(arm.getAtomicRequestID().getNodeID());
        			info.setExecutionID(arm.getAtomicRequestID().getExecutionId());
        			results.add(info);
                }
                results.add(req);
            }
        }
    	return results;
	}    

	public ResultsFuture<ResultsMessage> executeRequest(long reqID,
			RequestMessage requestMsg) {
    	DQPWorkContext workContext = DQPWorkContext.getWorkContext();
		RequestID requestID = workContext.getRequestID(reqID);
		requestMsg.markProcessingStart();
		requestMsg.setFetchSize(Math.min(requestMsg.getFetchSize(), maxFetchSize));
		Request request = null;
	    if ( requestMsg.isPreparedStatement() || requestMsg.isCallableStatement()) {
	    	request = new PreparedStatementRequest(prepPlanCache);
	    } else {
	    	request = new Request();
	    }
	    ClientState state = this.getClientState(workContext.getConnectionID(), true);
	    request.initialize(requestMsg, environment, bufferManager,
				dataTierMgr, transactionService, processorDebugAllowed,
				state.tempTableStoreImpl, workContext,
				chunkSize);
		
        RequestWorkItem workItem = null;
        
        ResultsFuture<ResultsMessage> resultsFuture = new ResultsFuture<ResultsMessage>();
        
        if(this.rsCache != null && requestMsg.useResultSetCache()) {
        	CacheID cID = this.rsCache.createCacheID(workContext, requestMsg.getCommandString(), requestMsg.getParameterValues());
            CacheResults cr = this.rsCache.getResults(cID, new int[]{1, 1});
            if (cr != null) {
            	workItem = new CachedRequestWorkItem(this, requestMsg, request, resultsFuture.getResultsReceiver(), requestID, workContext, cr.getCommand());
            }
        }
        if (workItem == null) {
            workItem = new RequestWorkItem(this, requestMsg, request, resultsFuture.getResultsReceiver(), requestID, workContext);
        }
        
    	logMMCommand(workItem, true, false, 0); //TODO: there is no transaction at this point 
        addRequest(requestID, workItem, state);
        
        this.addWork(workItem);      
        return resultsFuture;
    }
	
	public ResultsFuture<ResultsMessage> processCursorRequest(long reqID,
			int batchFirst, int fetchSize) throws MetaMatrixProcessingException {
        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
            LogManager.logDetail(LogConstants.CTX_DQP, "DQP process cursor request from " + batchFirst);  //$NON-NLS-1$
        }
		DQPWorkContext workContext = DQPWorkContext.getWorkContext();
        ResultsFuture<ResultsMessage> resultsFuture = new ResultsFuture<ResultsMessage>();
		RequestWorkItem workItem = getRequestWorkItem(workContext.getRequestID(reqID));
		workItem.requestMore(batchFirst, batchFirst + Math.min(fetchSize, maxFetchSize) - 1, resultsFuture.getResultsReceiver());
		return resultsFuture;
	}

	void addRequest(RequestID requestID, RequestWorkItem workItem, ClientState state) {
		this.requests.put(requestID, workItem);
		state.addRequest(requestID);
	}
    
    void removeRequest(final RequestWorkItem workItem) {
    	this.requests.remove(workItem.requestID);
    	ClientState state = getClientState(workItem.getDqpWorkContext().getConnectionID(), false);
    	if (state != null) {
    		state.removeRequest(workItem.requestID);
    	}
    	contextCache.removeRequestScopedCache(workItem.requestID.toString());
    }
           
    void addWork(Runnable r) {
    	this.processWorkerPool.execute(r);
    }
    
	public ResultsFuture<?> closeLobChunkStream(int lobRequestId,
			long requestId, String streamId)
			throws MetaMatrixProcessingException {
        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
            LogManager.logDetail(LogConstants.CTX_DQP, "Request to close the Lob stream with Stream id="+streamId+" instance id="+lobRequestId);  //$NON-NLS-1$//$NON-NLS-2$
        }   
        DQPWorkContext workContext = DQPWorkContext.getWorkContext();
        RequestWorkItem workItem = safeGetWorkItem(workContext.getRequestID(requestId));
        if (workItem != null) {
	        workItem.removeLobStream(lobRequestId);
        }
        return null;
    }
	    
	public ResultsFuture<LobChunk> requestNextLobChunk(int lobRequestId,
			long requestId, String streamId)
			throws MetaMatrixProcessingException {
        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
            LogManager.logDetail(LogConstants.CTX_DQP, "Request for next Lob chunk with Stream id="+streamId+" instance id="+lobRequestId);  //$NON-NLS-1$//$NON-NLS-2$
        }  
        RequestWorkItem workItem = getRequestWorkItem(DQPWorkContext.getWorkContext().getRequestID(requestId));
        ResultsFuture<LobChunk> resultsFuture = new ResultsFuture<LobChunk>();
        workItem.processLobChunkRequest(streamId, lobRequestId, resultsFuture.getResultsReceiver());
        return resultsFuture;
    }
    
    /**
     * Cancels a node in the request. (This request is called by the 
     * client directly using the admin API), so if this does not support
     * partial results then remove the original request.
     * @throws MetaMatrixComponentException 
     */
    public void cancelAtomicRequest(AtomicRequestID requestID) throws MetaMatrixComponentException {                    
        RequestWorkItem workItem = safeGetWorkItem(requestID.getRequestID());
        if (workItem == null) {
    		LogManager.logDetail(LogConstants.CTX_DQP, "Could not cancel", requestID, "parent request does not exist"); //$NON-NLS-1$ //$NON-NLS-2$
        	return;
        }
        workItem.requestAtomicRequestCancel(requestID);
    }
    
    RequestWorkItem getRequestWorkItem(RequestID reqID) throws MetaMatrixProcessingException {
    	RequestWorkItem result = this.requests.get(reqID);
    	if (result == null) {
    		throw new MetaMatrixProcessingException(DQPPlugin.Util.getString("DQPCore.The_request_has_been_closed.", reqID));//$NON-NLS-1$
    	}
    	return result;
    }
    
	RequestWorkItem safeGetWorkItem(Object processorID) {
    	return this.requests.get(processorID);
	}

    /**
     * Returns a list of QueueStats objects that represent the queues in
     * this service.
     * If there are no queues, an empty Collection is returned.
     */
    public Collection getQueueStatistics() {
        if ( this.processWorkerPool == null ) {
            return Collections.EMPTY_LIST;
        }

        return Arrays.asList(processWorkerPool.getStats());
    }

    /**
     * Returns a QueueStats object that represent the queue in
     * this service.
     * If there is no queue with the given name, an empty Collection is returned.
     */
    public Collection getQueueStatistics(String name) {
        if ( !name.equalsIgnoreCase(PROCESS_PLAN_QUEUE_NAME)) {
            return Collections.EMPTY_LIST;
        }
        return getQueueStatistics();
    }
    

            
    /**
     * Cancel and close all requests associated with the clientConnection/session. Also runs a final cleanup any caches within
     * the session's scope.
     * @param clientConnection
     * @param sendCancellationsToClient Notify the client that each request has been closed.
     * @throws MetaMatrixComponentException
     * @since 4.2
     */
    public void terminateConnection(String sessionId) throws MetaMatrixComponentException {
    	        
        // sometimes there will not be any atomic requests pending, in that
        // situation we still need to clear the master request from our map
        ClientState state = getClientState(sessionId, false);
        if (state != null) {
	        for (RequestID reqId : state.getRequests()) {
	            try {
	                cancelRequest(reqId);
	            } catch (MetaMatrixComponentException err) {
	                LogManager.logWarning(LogConstants.CTX_DQP, err, "Failed to cancel " + reqId); //$NON-NLS-1$
				}
	        }
        }
        
        // cleanup the buffer manager
        try {
            bufferManager.removeTupleSources(sessionId);
        } catch (Exception e) {
            LogManager.logWarning(LogConstants.CTX_DQP, e, "Failed to remove buffered tuples for connection " + sessionId); //$NON-NLS-1$
        }
        
        if (transactionService != null) {
            try {
                transactionService.cancelTransactions(sessionId, false);
            } catch (InvalidTransactionException err) {
                LogManager.logWarning(LogConstants.CTX_DQP, "rollback failed for requestID=" + sessionId); //$NON-NLS-1$
            } catch (SystemException err) {
                throw new MetaMatrixComponentException(err);
            }
        }
        contextCache.removeSessionScopedCache(sessionId);
    }

    public boolean cancelRequest(RequestID requestID)
        throws MetaMatrixComponentException {
        
        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
            LogManager.logDetail(LogConstants.CTX_DQP, "cancelQuery for requestID=" + requestID); //$NON-NLS-1$
        }
        
        boolean markCancelled = false;
        
        RequestWorkItem workItem = safeGetWorkItem(requestID);
        if (workItem != null) {
        	markCancelled = workItem.requestCancel();
        }
    	if (markCancelled) {
            logMMCommand(workItem, false, true, 0);
    	} else {
    		LogManager.logDetail(LogConstants.CTX_DQP, DQPPlugin.Util.getString("DQPCore.failed_to_cancel")); //$NON-NLS-1$
    	}
        return markCancelled;
    }
    
	public ResultsFuture<?> closeRequest(long requestId) throws MetaMatrixProcessingException {
        DQPWorkContext workContext = DQPWorkContext.getWorkContext();
        closeRequest(workContext.getRequestID(requestId));
        return null;
	}
    
    /**
     * Close the request with given ID 
     * @param requestID
     */
    void closeRequest(RequestID requestID) {
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
    
    public void clearPlanCache(){
        LogManager.logInfo(LogConstants.CTX_DQP, DQPPlugin.Util.getString("DQPCore.Clearing_prepared_plan_cache")); //$NON-NLS-1$
        this.prepPlanCache.clearAll();
    }

    public void clearCodeTableCache(){
        LogManager.logInfo(LogConstants.CTX_DQP, DQPPlugin.Util.getString("DQPCore.Clearing_code_table_cache")); //$NON-NLS-1$
        this.dataTierMgr.clearCodeTables();
    }

	public void clearResultSetCache() {
		//clear cache in server
		if(rsCache != null){
			rsCache.clear();
		}
	}
    
    void logMMCommand(RequestWorkItem workItem, boolean isBegin, boolean isCancel, int rowCount) {
    	if (!LogManager.isMessageToBeRecorded(LogConstants.CTX_COMMANDLOGGING, MessageLevel.INFO)) {
    		return;
    	}
    	
        RequestMessage msg = workItem.requestMsg;
        DQPWorkContext workContext = DQPWorkContext.getWorkContext();
        RequestID rID = new RequestID(workContext.getConnectionID(), msg.getExecutionId());
        String command = null;
    	if(isBegin && !isCancel){
    		command = msg.getCommandString();
    	}
    	String txnID = null;
		TransactionContext tc = workItem.getTransactionContext();
		if (tc != null) {
			txnID = tc.getTxnID();
		}
    	String appName = workContext.getAppName();
        // Log to request log
        short point = isBegin? CommandLogMessage.CMD_POINT_BEGIN:CommandLogMessage.CMD_POINT_END;
        short status = CommandLogMessage.CMD_STATUS_NEW;
        if(!isBegin){
        	if(isCancel){
        		status = CommandLogMessage.CMD_STATUS_CANCEL;
        	}else{
        		status = CommandLogMessage.CMD_STATUS_END;
        	}
        }
        CommandLogMessage message = null;
        if (point == CommandLogMessage.CMD_POINT_BEGIN) {
            message = new CommandLogMessage(System.currentTimeMillis(), rID.toString(), txnID, workContext.getConnectionID(), appName, workContext.getUserName(), workContext.getVdbName(), workContext.getVdbVersion(), command);
        } else {
            boolean isCancelled = false;
            boolean errorOccurred = false;

            if (status == CommandLogMessage.CMD_STATUS_CANCEL) {
                isCancelled = true;
            } else if (status == CommandLogMessage.CMD_STATUS_ERROR) {
                errorOccurred = true;
            }
            message = new CommandLogMessage(System.currentTimeMillis(), rID.toString(), txnID, workContext.getConnectionID(), workContext.getUserName(), workContext.getVdbName(), workContext.getVdbVersion(), rowCount, isCancelled, errorOccurred);
        }
        LogManager.log(MessageLevel.INFO, LogConstants.CTX_COMMANDLOGGING, message);
    }
    
    ProcessorDataManager getDataTierManager() {
    	return this.dataTierMgr;
    }
    
    public void setDataTierManager(ProcessorDataManager dataTierMgr) {
    	this.dataTierMgr = dataTierMgr;
    }

	BufferManager getBufferManager() {
		return bufferManager;
	}

	public TransactionService getTransactionService() {
		if (transactionService == null) {
			throw new MetaMatrixRuntimeException("Transactions are not enabled"); //$NON-NLS-1$
		}
		return transactionService;
	}
	
	public TransactionService getTransactionServiceDirect() {
		return transactionService;
	}

	ResultSetCache getRsCache() {
		return rsCache;
	}
	
	int getProcessorTimeSlice() {
		return this.processorTimeslice;
	}	
	
	int getChunkSize() {
		return chunkSize;
	}

	
    /* 
     * @see com.metamatrix.common.application.Application#initialize(java.util.Properties)
     */
    public void start(DQPConfigSource configSource) throws ApplicationInitializationException {
        
        // Load services into DQP
        for(int i=0; i<DQPServiceNames.ALL_SERVICES.length; i++) {
            
        	final String serviceName = DQPServiceNames.ALL_SERVICES[i];
            final Class<? extends ApplicationService> type = DQPServiceNames.ALL_SERVICE_CLASSES[i];

            ApplicationService appService = configSource.getServiceInstance(type);
        	if (appService == null) {
        		LogManager.logInfo(LogConstants.CTX_DQP, DQPPlugin.Util.getString("DQPLauncher.InstallService_ServiceIsNull", serviceName)); //$NON-NLS-1$
        		continue;
        	}
        	
        	appService = loader.loadService(serviceName, appService);
        	String loggingContext = DQPServiceNames.SERVICE_LOGGING_CONTEXT[i];
			if (loggingContext != null) {
				appService = (ApplicationService)LogManager.createLoggingProxy(loggingContext, appService, new Class[] {type}, MessageLevel.DETAIL);
			}
            
			appService.initialize(configSource.getProperties());
        	this.environment.installService(serviceName, appService);
            LogManager.logInfo(LogConstants.CTX_DQP, DQPPlugin.Util.getString("DQPLauncher.InstallService_ServiceInstalled", serviceName)); //$NON-NLS-1$
        }
        
		ConfigurationService cs = (ConfigurationService)this.environment.findService(DQPServiceNames.CONFIGURATION_SERVICE);
		Properties p = configSource.getProperties();
		if (cs != null) {
			p = cs.getSystemProperties();
		}
		start(p);
    }
    
	
	public void start(Properties props) {
		PropertiesUtils.setBeanProperties(this, props, null);
		
        this.processorTimeslice = PropertiesUtils.getIntProperty(props, DQPEmbeddedProperties.PROCESS_TIMESLICE, DEFAULT_PROCESSOR_TIMESLICE);
        this.maxFetchSize = PropertiesUtils.getIntProperty(props, DQPEmbeddedProperties.MAX_FETCH_SIZE, DEFAULT_FETCH_SIZE);
        this.processorDebugAllowed = PropertiesUtils.getBooleanProperty(props, DQPEmbeddedProperties.PROCESSOR_DEBUG_ALLOWED, true);
        this.maxCodeTableRecords = PropertiesUtils.getIntProperty(props, DQPEmbeddedProperties.MAX_CODE_TABLE_RECORDS_PER_TABLE, DEFAULT_MAX_CODE_TABLE_RECORDS);
        this.maxCodeTables = PropertiesUtils.getIntProperty(props, DQPEmbeddedProperties.MAX_CODE_TABLES, DEFAULT_MAX_CODE_TABLES);
        this.maxCodeRecords = PropertiesUtils.getIntProperty(props, DQPEmbeddedProperties.MAX_CODE_TABLE_RECORDS, DEFAULT_MAX_CODE_RECORDS);
        
        this.chunkSize = PropertiesUtils.getIntProperty(props, DQPEmbeddedProperties.STREAMING_BATCH_SIZE, 10) * 1024;
        
        //result set cache
        if(PropertiesUtils.getBooleanProperty(props, DQPEmbeddedProperties.USE_RESULTSET_CACHE, false)){ 
			this.rsCache = new ResultSetCache();
			PropertiesUtils.setBeanProperties(this.rsCache, props, "ResultSetCache"); //$NON-NLS-1$
			this.rsCache.start(cacheFactory);
        }

        //prepared plan cache
        int maxSizeTotal = PropertiesUtils.getIntProperty(props, DQPEmbeddedProperties.MAX_PLAN_CACHE_SIZE, PreparedPlanCache.DEFAULT_MAX_SIZE_TOTAL);
        prepPlanCache = new PreparedPlanCache(maxSizeTotal);
		
        // Processor debug flag
        LogManager.logInfo(LogConstants.CTX_DQP, DQPPlugin.Util.getString("DQPCore.Processor_debug_allowed_{0}", this.processorDebugAllowed)); //$NON-NLS-1$
                        
        //get buffer manager
        BufferService bufferService = (BufferService) this.environment.findService(DQPServiceNames.BUFFER_SERVICE);
        bufferManager = bufferService.getBufferManager();
        contextCache = bufferService.getContextCache();

        transactionService = (TransactionService )this.environment.findService(DQPServiceNames.TRANSACTION_SERVICE);
        metadataService = (MetadataService) this.environment.findService(DQPServiceNames.METADATA_SERVICE);

        // Create the worker pools to tie the queues together
        processWorkerPool = WorkerPoolFactory.newWorkerPool(PROCESS_PLAN_QUEUE_NAME, PropertiesUtils.getIntProperty(props, DQPEmbeddedProperties.PROCESS_POOL_MAX_THREADS, DEFAULT_MAX_PROCESS_WORKERS)); 
 
        dataTierMgr = new DataTierManagerImpl(this,
                                            (DataService) this.environment.findService(DQPServiceNames.DATA_SERVICE),
                                            (VDBService) this.environment.findService(DQPServiceNames.VDB_SERVICE),
                                            (BufferService) this.environment.findService(DQPServiceNames.BUFFER_SERVICE),
                                            metadataService,
                                            this.maxCodeTables,
                                            this.maxCodeRecords,
                                            this.maxCodeTableRecords);        
	}
	
	public List getXmlSchemas(String docName) throws MetaMatrixComponentException,
			QueryMetadataException {
		DQPWorkContext workContext = DQPWorkContext.getWorkContext();
        QueryMetadataInterface metadata = metadataService.lookupMetadata(workContext.getVdbName(), workContext.getVdbVersion());

        Object groupID = metadata.getGroupID(docName);
        return metadata.getXMLSchemas(groupID);
	}

	public void cancelRequest(long requestID)
			throws MetaMatrixProcessingException, MetaMatrixComponentException {
		DQPWorkContext workContext = DQPWorkContext.getWorkContext();
		this.cancelRequest(workContext.getRequestID(requestID));
	}
	
	public void begin() throws XATransactionException {
    	String threadId = DQPWorkContext.getWorkContext().getConnectionID();
        try {
			this.getTransactionService().begin(threadId);
		} catch (SystemException e) {
			throw new XATransactionException(e);
		}
	}
	
	public void commit() throws XATransactionException {
		String threadId = DQPWorkContext.getWorkContext().getConnectionID();
        try {
			this.getTransactionService().commit(threadId);
		} catch (SystemException e) {
			throw new XATransactionException(e);
		}
	}
	
	public void rollback() throws XATransactionException {
		try {
			this.getTransactionService().rollback(
					DQPWorkContext.getWorkContext().getConnectionID());
		} catch (SystemException e) {
			throw new XATransactionException(e);
		}
	}

	public void commit(MMXid xid, boolean onePhase)
			throws XATransactionException {
		String threadId = DQPWorkContext.getWorkContext().getConnectionID();
		this.getTransactionService().commit(threadId, xid, onePhase);
	}

	public void end(MMXid xid, int flags) throws XATransactionException {
		String threadId = DQPWorkContext.getWorkContext().getConnectionID();
		this.getTransactionService().end(threadId, xid, flags);
	}

	public void forget(MMXid xid) throws XATransactionException {
		String threadId = DQPWorkContext.getWorkContext().getConnectionID();
		this.getTransactionService().forget(threadId, xid);
	}
	
	public int prepare(MMXid xid) throws XATransactionException {
		return this.getTransactionService().prepare(
				DQPWorkContext.getWorkContext().getConnectionID(),
				xid);
	}
	
	public Xid[] recover(int flag) throws XATransactionException {
		return this.getTransactionService().recover(flag);
	}

	public void rollback(MMXid xid) throws XATransactionException {
		this.getTransactionService().rollback(
				DQPWorkContext.getWorkContext().getConnectionID(),
				xid);
	}

	public void start(MMXid xid, int flags, int timeout)
			throws XATransactionException {
		String threadId = DQPWorkContext.getWorkContext().getConnectionID();
		this.getTransactionService().start(threadId, xid, flags, timeout);
	}

	public MetadataResult getMetadata(long requestID)
			throws MetaMatrixComponentException, MetaMatrixProcessingException {
		DQPWorkContext workContext = DQPWorkContext.getWorkContext();
		MetaDataProcessor processor = new MetaDataProcessor(this.metadataService, this, this.prepPlanCache, this.environment, workContext.getVdbName(), workContext.getVdbVersion());
		return processor.processMessage(workContext.getRequestID(requestID), workContext, null, true);
	}

	public MetadataResult getMetadata(long requestID, String preparedSql,
			boolean allowDoubleQuotedVariable)
			throws MetaMatrixComponentException, MetaMatrixProcessingException {
		DQPWorkContext workContext = DQPWorkContext.getWorkContext();
		MetaDataProcessor processor = new MetaDataProcessor(this.metadataService, this, this.prepPlanCache, this.environment, workContext.getVdbName(), workContext.getVdbVersion());
		return processor.processMessage(workContext.getRequestID(requestID), workContext, preparedSql, allowDoubleQuotedVariable);
	}
	
	public ApplicationEnvironment getEnvironment() {
		return environment;
	}
	
	@Inject
	public void setCacheFactory(CacheFactory cacheFactory) {
		this.cacheFactory = cacheFactory;
		this.environment.setCacheFactory(cacheFactory);
	}
	
	public void setEnvironment(ApplicationEnvironment environment) {
		this.environment = environment;
	}
}