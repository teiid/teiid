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
import org.teiid.dqp.internal.cache.DQPContextCache;
import org.teiid.dqp.internal.cache.ResultSetCache;
import org.teiid.dqp.internal.cache.ResultSetCacheUtil;

import com.google.inject.Singleton;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.common.application.Application;
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
import com.metamatrix.core.util.LRUCache;
import com.metamatrix.dqp.DQPPlugin;
import com.metamatrix.dqp.ResourceFinder;
import com.metamatrix.dqp.client.ClientSideDQP;
import com.metamatrix.dqp.client.MetadataResult;
import com.metamatrix.dqp.client.ResultsFuture;
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
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities;
import com.metamatrix.query.processor.ProcessorDataManager;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.server.serverapi.RequestInfo;
import com.metamatrix.vdb.runtime.VDBKey;

/**
 * Implements the core DQP processing.
 */
@Singleton
public class DQPCore extends Application implements ClientSideDQP {
	
	static class ConnectorCapabilitiesCache  {
		
		private Map<VDBKey, Map<String, SourceCapabilities>> cache = new LRUCache<VDBKey, Map<String, SourceCapabilities>>(1000);
		
		Map<String, SourceCapabilities> getVDBConnectorCapabilities(
				DQPWorkContext workContext) {
			VDBKey key = new VDBKey(workContext.getVdbName(), workContext.getVdbVersion());
			Map<String, SourceCapabilities> vdbCapabilties = null;
			synchronized (this.cache) {
				vdbCapabilties = cache.get(key);
				if (vdbCapabilties == null) {
					vdbCapabilties = new ConcurrentHashMap<String, SourceCapabilities>();
					this.cache.put(key, vdbCapabilties);
				}
			}
			return vdbCapabilties;
		}
	}
	
    //Constants
    private static final int DEFAULT_MAX_CODE_TABLE_RECORDS = 10000;
    private static final int DEFAULT_MAX_CODE_TABLES = 20;
    private static final int DEFAULT_PROCESSOR_TIMESLICE = 2000;
    private static final String PROCESS_PLAN_QUEUE_NAME = "QueryProcessorQueue"; //$NON-NLS-1$
    private static final int DEFAULT_MAX_PROCESS_WORKERS = 15;
    private static final String DEFAULT_MAX_RESULTSET_CACHE_SIZE = "50"; //$NON-NLS-1$
    private static final String DEFAULT_MAX_RESULTSET_CACHE_AGE = "3600000"; //$NON-NLS-1$

    // System properties for Code Table
    private int maxCodeTableRecords = DEFAULT_MAX_CODE_TABLE_RECORDS;
    private int maxCodeTables = DEFAULT_MAX_CODE_TABLES;
    
    private int maxFetchSize = 20000;
    
    // Resources
    private ConnectorCapabilitiesCache connectorCapabilitiesCache = new ConnectorCapabilitiesCache();
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
    
	private TempTableStoresHolder tempTableStoresHolder;
    private int chunkSize = 0;
    
	private Map<RequestID, RequestWorkItem> requests = Collections.synchronizedMap(new HashMap<RequestID, RequestWorkItem>());			
	private Map<String, List<RequestID>> requestsByClients = Collections.synchronizedMap(new HashMap<String, List<RequestID>>());
	private DQPContextCache contextCache;
    private ServiceLoader loader = new ServiceLoader();

    /**
     * perform a full shutdown and wait for 10 seconds for all threads to finish
     * @throws ApplicationLifecycleException 
     */
	@Override
    public void stop() throws ApplicationLifecycleException {
    	processWorkerPool.shutdownNow();
    	try {
			processWorkerPool.awaitTermination(10, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
		}
    	contextCache.shutdown();
    	super.stop();
    }
    
    /**
     * Return a list of {@link RequestInfo} for the given session
     */
    public List<RequestInfo> getRequestsByClient(String clientConnection) {
        List<RequestID> ids = this.requestsByClients.get(clientConnection);

        return buildRequestInfos(ids);
    }

    /**
     * Return a list of all {@link RequestInfo} 
     */
    public List<RequestInfo> getRequests() {
        List<RequestID> copies = null;
		synchronized(requests) {
            copies = new ArrayList<RequestID>(requests.keySet());
        }
		
		return buildRequestInfos(copies);
    } 

    private List<RequestInfo> buildRequestInfos(List<RequestID> ids) {
		if(ids == null) {
			return Collections.emptyList();
		}
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
		Map<String, SourceCapabilities> vdbCapabilties = this.connectorCapabilitiesCache.getVDBConnectorCapabilities(workContext);
		requestMsg.setFetchSize(Math.min(requestMsg.getFetchSize(), maxFetchSize));
		Request request = null;
	    if ( requestMsg.isPreparedStatement() || requestMsg.isCallableStatement()) {
	    	request = new PreparedStatementRequest(prepPlanCache);
	    } else {
	    	request = new Request();
	    }
	    request.initialize(requestMsg, getEnvironment(), bufferManager,
				dataTierMgr, vdbCapabilties, transactionService,
				processorDebugAllowed, this.tempTableStoresHolder
						.getTempTableStore(workContext.getConnectionID()),
				workContext, chunkSize);
		
        RequestWorkItem workItem = null;
        
        ResultsFuture<ResultsMessage> resultsFuture = new ResultsFuture<ResultsMessage>();
        
        if (areResultsInCache(requestMsg)) {
            CacheID cID = ResultSetCacheUtil.createCacheID(requestMsg, this.rsCache);
            Command command = this.rsCache.getResults(cID, new int[]{1, 1}).getCommand();
        	workItem = new CachedRequestWorkItem(this, requestMsg, request, resultsFuture.getResultsReceiver(), requestID, workContext, command);
        }
        else {            
            workItem = new RequestWorkItem(this, requestMsg, request, resultsFuture.getResultsReceiver(), requestID, workContext);
        }
        
    	logMMCommand(workItem, true, false, 0); //TODO: there is no transaction at this point 
        addRequest(requestID, workItem);
        
        this.addWork(workItem);      
        return resultsFuture;
    }
	
	public ResultsFuture<ResultsMessage> processCursorRequest(long reqID,
			int batchFirst, int batchLast) throws MetaMatrixProcessingException {
        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
            LogManager.logDetail(LogConstants.CTX_DQP, "DQP process cursor request from " + batchFirst + " to " + batchLast);  //$NON-NLS-1$//$NON-NLS-2$
        }
		DQPWorkContext workContext = DQPWorkContext.getWorkContext();
        ResultsFuture<ResultsMessage> resultsFuture = new ResultsFuture<ResultsMessage>();
		RequestWorkItem workItem = getRequestWorkItem(workContext.getRequestID(reqID));
		workItem.requestMore(batchFirst, batchLast, resultsFuture.getResultsReceiver());
		return resultsFuture;
	}

	void addRequest(RequestID requestID, RequestWorkItem workItem) {
		this.requests.put(requestID, workItem);
        synchronized (requestsByClients) {
            List<RequestID> clientRequests = this.requestsByClients.get(workItem.getDqpWorkContext().getConnectionID());
            if (clientRequests == null) {
            	clientRequests = new LinkedList<RequestID>();
            	this.requestsByClients.put(workItem.getDqpWorkContext().getConnectionID(), clientRequests);
            }
            clientRequests.add(requestID);
		}
	}
    
    void removeRequest(final RequestWorkItem workItem) {
    	this.requests.remove(workItem.requestID);
    	synchronized (requestsByClients) {
        	List<RequestID> clientRequests = this.requestsByClients.get(workItem.getDqpWorkContext().getConnectionID());
        	if (clientRequests != null) {
        		clientRequests.remove(workItem.requestID);
        	}
		}
    	contextCache.removeRequestScopedCache(workItem.requestID.toString());
    }
           
    boolean areResultsInCache(final RequestMessage requestMsg) {
        if(this.rsCache == null){
            return false;
        }
        if(!requestMsg.useResultSetCache()){
            return false;
        }
        CacheID cID = ResultSetCacheUtil.createCacheID(requestMsg, this.rsCache);
        return rsCache.hasResults(cID);        
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
        RequestWorkItem workItem = getRequestWorkItem(workContext.getRequestID(requestId));
        workItem.removeLobStream(lobRequestId);
        ResultsFuture<Void> resultsFuture = new ResultsFuture<Void>();
        resultsFuture.getResultsReceiver().receiveResults(null);
        return resultsFuture;
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
    		throw new MetaMatrixProcessingException(DQPPlugin.Util.getString("DQPCore.The_request_has_been_cancelled.", reqID));//$NON-NLS-1$
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
        List<RequestID> requestIds = requestsByClients.get(sessionId);
        if (requestIds != null) {
            synchronized (requestsByClients) {
            	requestIds = new ArrayList<RequestID>(requestIds);
    		}
	        for (RequestID reqId : requestIds) {
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
        	installService(serviceName, appService);
            LogManager.logInfo(LogConstants.CTX_DQP, DQPPlugin.Util.getString("DQPLauncher.InstallService_ServiceInstalled", serviceName)); //$NON-NLS-1$
        }
        
		ConfigurationService cs = (ConfigurationService)this.getEnvironment().findService(DQPServiceNames.CONFIGURATION_SERVICE);
		Properties p = configSource.getProperties();
		if (cs != null) {
			p = cs.getSystemProperties();
		}
		start(p);
    }
    
	
	public void start(Properties props) {
		ApplicationEnvironment env = this.getEnvironment();
		
		PropertiesUtils.setBeanProperties(this, props, null);
        
        this.chunkSize = PropertiesUtils.getIntProperty(props, DQPConfigSource.STREAMING_BATCH_SIZE, 10) * 1024;
        
        //result set cache
        if(PropertiesUtils.getBooleanProperty(props, DQPConfigSource.USE_RESULTSET_CACHE, false)){ 
        	Properties rsCacheProps = new Properties();
        	rsCacheProps.setProperty(ResultSetCache.RS_CACHE_MAX_SIZE, props.getProperty(DQPConfigSource.MAX_RESULTSET_CACHE_SIZE, DEFAULT_MAX_RESULTSET_CACHE_SIZE));
        	rsCacheProps.setProperty(ResultSetCache.RS_CACHE_MAX_AGE, props.getProperty(DQPConfigSource.MAX_RESULTSET_CACHE_AGE, DEFAULT_MAX_RESULTSET_CACHE_AGE));
        	rsCacheProps.setProperty(ResultSetCache.RS_CACHE_SCOPE, props.getProperty(DQPConfigSource.RESULTSET_CACHE_SCOPE, ResultSetCache.RS_CACHE_SCOPE_VDB)); 
			this.rsCache = new ResultSetCache(rsCacheProps, ResourceFinder.getCacheFactory());
        }

        //prepared plan cache
        int maxSizeTotal = PropertiesUtils.getIntProperty(props, DQPConfigSource.MAX_PLAN_CACHE_SIZE, PreparedPlanCache.DEFAULT_MAX_SIZE_TOTAL);
        prepPlanCache = new PreparedPlanCache(maxSizeTotal);
		
        // Processor debug flag
        LogManager.logInfo(LogConstants.CTX_DQP, DQPPlugin.Util.getString("DQPCore.Processor_debug_allowed_{0}", this.processorDebugAllowed)); //$NON-NLS-1$
                        
        //get buffer manager
        BufferService bufferService = (BufferService) env.findService(DQPServiceNames.BUFFER_SERVICE);
        bufferManager = bufferService.getBufferManager();
        contextCache = bufferService.getContextCache();

        transactionService = (TransactionService )env.findService(DQPServiceNames.TRANSACTION_SERVICE);
        metadataService = (MetadataService) env.findService(DQPServiceNames.METADATA_SERVICE);

        // Create the worker pools to tie the queues together
        processWorkerPool = WorkerPoolFactory.newWorkerPool(PROCESS_PLAN_QUEUE_NAME, PropertiesUtils.getIntProperty(props, DQPConfigSource.PROCESS_POOL_MAX_THREADS, DEFAULT_MAX_PROCESS_WORKERS)); 
 
        tempTableStoresHolder = new TempTableStoresHolder(bufferManager);
        
        dataTierMgr = new DataTierManagerImpl(this,
                                            (DataService) env.findService(DQPServiceNames.DATA_SERVICE),
                                            (VDBService) env.findService(DQPServiceNames.VDB_SERVICE),
                                            (BufferService) env.findService(DQPServiceNames.BUFFER_SERVICE),
                                            this.maxCodeTables,
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
		MetaDataProcessor processor = new MetaDataProcessor(this.metadataService, this, this.prepPlanCache, getEnvironment(), this.tempTableStoresHolder);
		return processor.processMessage(workContext.getRequestID(requestID), workContext, null, true);
	}

	public MetadataResult getMetadata(long requestID, String preparedSql,
			boolean allowDoubleQuotedVariable)
			throws MetaMatrixComponentException, MetaMatrixProcessingException {
		DQPWorkContext workContext = DQPWorkContext.getWorkContext();
		MetaDataProcessor processor = new MetaDataProcessor(this.metadataService, this, this.prepPlanCache, getEnvironment(), this.tempTableStoresHolder);
		return processor.processMessage(workContext.getRequestID(requestID), workContext, preparedSql, allowDoubleQuotedVariable);
	}
	
	public void setMaxFetchSize(int maxFetchSize) {
		this.maxFetchSize = maxFetchSize;
	}
	
	public void setProcessorDebugAllowed(boolean processorDebugAllowed) {
		this.processorDebugAllowed = processorDebugAllowed;
	}
	
	public void setMaxCodeTableRecords(int maxCodeTableRecords) {
		this.maxCodeTableRecords = maxCodeTableRecords;
	}
	
	public void setMaxCodeTables(int maxCodeTables) {
		this.maxCodeTables = maxCodeTables;
	}
	
	public void setProcessorTimeslice(int processorTimeslice) {
		this.processorTimeslice = processorTimeslice;
	}

}