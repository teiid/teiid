/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.dqp.internal.process;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import com.metamatrix.api.exception.ComponentNotAvailableException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.TupleSource;
import com.metamatrix.common.comm.api.ResultsReceiver;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.core.id.UUID;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.dqp.DQPPlugin;
import com.metamatrix.dqp.internal.datamgr.ConnectorID;
import com.metamatrix.dqp.message.AtomicRequestID;
import com.metamatrix.dqp.message.AtomicRequestMessage;
import com.metamatrix.dqp.message.AtomicResultsMessage;
import com.metamatrix.dqp.message.RequestID;
import com.metamatrix.dqp.message.RequestMessage;
import com.metamatrix.dqp.service.BufferService;
import com.metamatrix.dqp.service.DataService;
import com.metamatrix.dqp.service.MetadataService;
import com.metamatrix.dqp.service.VDBService;
import com.metamatrix.dqp.util.LogConstants;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.resolver.QueryResolver;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.From;
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.lang.Select;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.util.CommandContext;

public class DataTierManagerImpl implements DataTierManager {

	// Resources
	private DQPCore requestMgr;
    private DataService dataService;
    private MetadataService metadataService;
    private VDBService vdbService;
    private BufferService bufferService;

    // Code table limits
    private int maxCodeTableRecords;

	// Processor state
    private CodeTableCache codeTableCache;
    
    private static class CodeTableFailure {
    	long time = System.currentTimeMillis();
    	Throwable exception;
    }
    
    private ConcurrentHashMap<String, CodeTableFailure> codeTableCacheFailures = new ConcurrentHashMap<String, CodeTableFailure>();
    
    public DataTierManagerImpl(DQPCore requestMgr,
        DataService dataService, MetadataService metadataService, VDBService vdbService, 
        BufferService bufferService, int maxCodeTables,
        int maxCodeTableRecords) {

		this.requestMgr = requestMgr;
        this.dataService = dataService;
        this.metadataService = metadataService;
        this.vdbService = vdbService;
        this.maxCodeTableRecords = maxCodeTableRecords;
        this.bufferService = bufferService;

        this.codeTableCache = new CodeTableCache(maxCodeTables);
	}

	public TupleSource registerRequest(Object processorId, Command command,
			String modelName, int nodeID) throws MetaMatrixComponentException, MetaMatrixProcessingException {
		RequestWorkItem workItem = requestMgr.getRequestWorkItem((RequestID)processorId, true);
		AtomicRequestMessage aqr = createRequest(processorId, command,
				modelName, nodeID);
        DataTierTupleSource tupleSource = new DataTierTupleSource(aqr.getCommand().getProjectedSymbols(), aqr, this, aqr.getConnectorID(), workItem);
        tupleSource.open();
        return tupleSource;
	}

	private AtomicRequestMessage createRequest(Object processorId,
			Command command, String modelName, int nodeID)
			throws MetaMatrixProcessingException, MetaMatrixComponentException {
		RequestWorkItem workItem = requestMgr.getRequestWorkItem((RequestID)processorId, true);
		
	    RequestMessage request = workItem.requestMsg;
		// build the atomic request based on original request + context info
        AtomicRequestMessage aqr = new AtomicRequestMessage(request, workItem.getDqpWorkContext(), nodeID);
        aqr.markSubmissionStart();
        aqr.setCommand(command);
        aqr.setModelName(modelName);
        aqr.setUseResultSetCache(request.useResultSetCache());
        aqr.setPartialResults(request.supportsPartialResults());
        if (nodeID >= 0) {
        	aqr.setTransactionContext(workItem.getTransactionContext());
        }
        aqr.setFetchSize(this.bufferService.getBufferManager().getConnectorBatchSize());

        // Set routing name
        String binding = modelName;
        if(! modelName.startsWith(UUID.PROTOCOL)) {
            List bindings = vdbService.getConnectorBindingNames(workItem.getDqpWorkContext().getVdbName(), workItem.getDqpWorkContext().getVdbVersion(), modelName);
            if (bindings == null || bindings.isEmpty()) {
                // this should not happen, but it did occur when setting up the SystemAdmin models
                throw new MetaMatrixComponentException(DQPPlugin.Util.getString("DataTierManager.could_not_obtain_connector_binding", new Object[]{modelName, workItem.getDqpWorkContext().getVdbName(), workItem.getDqpWorkContext().getVdbVersion() })); //$NON-NLS-1$
            }
            binding = (String)bindings.get(0); 
            Assertion.isNotNull(binding, "could not obtain connector id"); //$NON-NLS-1$
        }
        aqr.setConnectorBindingID(binding);
        // Select any connector instance for this connector binding
        ConnectorID connectorID = this.dataService.selectConnector(binding);
        // if we had this as null before
        aqr.setConnectorID(connectorID);
		return aqr;
	}
	
	String getConnectorName(String connectorBindingID) {
        try {
            return vdbService.getConnectorName(connectorBindingID);
        } catch (Throwable t) {
            // OK
        }
        return connectorBindingID;
	}
	
	void executeRequest(AtomicRequestMessage aqr, ConnectorID connectorId,
			ResultsReceiver<AtomicResultsMessage> receiver)
			throws MetaMatrixComponentException {
		this.dataService.executeRequest(aqr, connectorId, receiver);
	}

	public void closeRequest(AtomicRequestID request, ConnectorID connectorId)
			throws MetaMatrixComponentException {
		this.dataService.closeRequest(request, connectorId);
	}
	
	public void cancelRequest(AtomicRequestID request, ConnectorID connectorId)
		throws MetaMatrixComponentException {
		this.dataService.cancelRequest(request, connectorId);
	}

	void requestBatch(AtomicRequestID request, ConnectorID connectorId)
			throws MetaMatrixComponentException {
		this.dataService.requestBatch(request, connectorId);
	}

    /** 
     * Notify each waiting request that the code table data is now available.
     * @param requests
     * @since 4.2
     */
    private void notifyWaitingCodeTableRequests(Collection requests) {
        if (requests != null) {
            for (Iterator reqIter = requests.iterator(); reqIter.hasNext();) {
                RequestWorkItem workItem = requestMgr.safeGetWorkItem(reqIter.next());
                if (workItem != null) {
                	workItem.moreWork();
                }
            }
        }
    }        
    
    public Object lookupCodeValue(
        CommandContext context,
        String codeTableName,
        String returnElementName,
        String keyElementName,
        Object keyValue)
        throws BlockedException, MetaMatrixComponentException {

        int existsCode = this.codeTableCache.cacheExists(codeTableName, returnElementName, keyElementName, context);
        if(existsCode == CodeTableCache.CACHE_EXISTS ) {
            return this.codeTableCache.lookupValue(codeTableName, returnElementName, keyElementName, keyValue);
        } else if(existsCode == CodeTableCache.CACHE_NOT_EXIST ) {
        	String failureKey = codeTableName.toLowerCase();
        	CodeTableFailure failure = codeTableCacheFailures.get(failureKey);
        	//retry rate of 5 seconds
        	if (failure != null && System.currentTimeMillis() - failure.time < 5000) {
    			throw new MetaMatrixComponentException(failure.exception);
        	} 
            try {
				registerCodeTableRequest(context, codeTableName, returnElementName, keyElementName);
			} catch (MetaMatrixProcessingException e) {
				//TODO: this is not right
				throw new MetaMatrixComponentException(e);
			}
        } else if(existsCode == CodeTableCache.CACHE_OVERLOAD) {
			throw new MetaMatrixComponentException("ERR.018.005.0099", DQPPlugin.Util.getString("ERR.018.005.0099")); //$NON-NLS-1$ //$NON-NLS-2$

        } // else, CACHE_LOADING - do nothing other than block

        throw BlockedException.INSTANCE;
    }

    void registerCodeTableRequest(
        CommandContext context,
        final String codeTableName,
        String returnElementName,
        String keyElementName)
        throws MetaMatrixComponentException, MetaMatrixProcessingException {

        // Look up request information based on processor ID (which is really RequestID)
        RequestID requestID = (RequestID) context.getProcessorID();
        RequestWorkItem workItem = this.requestMgr.getRequestWorkItem(requestID, true);
        
        Query query = null;
        String modelName = null;

        // Look up metadata for this request
        QueryMetadataInterface metadata = null;
        try {
            metadata = metadataService.lookupMetadata(workItem.getDqpWorkContext().getVdbName(), workItem.getDqpWorkContext().getVdbVersion());

            // Construct the command objects
            query = new Query();
            Select select = new Select();
            select.addSymbol(new ElementSymbol(keyElementName));
            select.addSymbol(new ElementSymbol(returnElementName));
            query.setSelect(select);
            From from = new From();
            from.addGroup(new GroupSymbol(codeTableName));
            query.setFrom(from);

            // Resolve the command
            QueryResolver.resolveCommand(query, metadata);

            // Get the routing name and modelID from the group
            GroupSymbol group = (GroupSymbol) query.getFrom().getGroups().get(0);
            Object modelID = metadata.getModelID(group.getMetadataID());
            modelName = metadata.getFullName(modelID);
        } catch(QueryMetadataException e) {
            String msg = DQPPlugin.Util.getString("DataTierManager.Unable_to_get_metadata."); //$NON-NLS-1$
            throw new ComponentNotAvailableException(e, msg);
        } catch(QueryResolverException e) {
            String msg = DQPPlugin.Util.getString("DataTierManager.Unable_to_resolve_query."); //$NON-NLS-1$
            throw new ComponentNotAvailableException(e, msg);
        }

        int nodeId = this.codeTableCache.createCacheRequest(codeTableName, returnElementName, keyElementName, requestID);
        final AtomicRequestMessage aqr = createRequest(context.getProcessorID(), query, modelName, nodeId);
        this.executeRequest(aqr, aqr.getConnectorID(), new ResultsReceiver<AtomicResultsMessage>() {

			public void exceptionOccurred(Throwable e) {
		        // Notify code table and waiting requests that this lookup failed.
		        Collection requests = codeTableCache.errorLoadingCache(aqr.getRequestID(), aqr.getAtomicRequestID().getNodeID());
		        CodeTableFailure failure = new CodeTableFailure();
		        failure.exception = e;
		        codeTableCacheFailures.put(codeTableName.toLowerCase(), failure);
		        try {
					closeRequest(aqr.getAtomicRequestID(), aqr.getConnectorID());
				} catch (MetaMatrixComponentException e1) {
					LogManager.logDetail(LogConstants.CTX_DQP, e1, "Exception closing code table request"); //$NON-NLS-1$
				}
		        notifyWaitingCodeTableRequests(requests);
			}

			public void receiveResults(AtomicResultsMessage response) {
				if (response.isRequestClosed()) {
					return;
				}
				// Determine whether the results should be added to code table cache
            	// Depends on size of results and available memory and system parameters
            	boolean codeTableLoadable = response.getLastRow() < maxCodeTableRecords;

            	if (!codeTableLoadable) {
                    LogManager.logWarning(LogConstants.CTX_DQP, DQPPlugin.Util.getString("DataTierManager.Unable_to_load_code_table_for_requestID_{0}_of_and_nodeID_of_{1}_because_result_sizes_exceeds_the_allowed_parameter_-_MaxCodeTableRecords.", new Object[]{aqr.getRequestID(), new Integer(aqr.getAtomicRequestID().getNodeID())})); //$NON-NLS-1$                    
                    MetaMatrixComponentException me = new MetaMatrixComponentException("ERR.018.005.0100", DQPPlugin.Util.getString("ERR.018.005.0100", aqr.getRequestID(), new Integer(aqr.getAtomicRequestID().getNodeID()))); //$NON-NLS-1$ //$NON-NLS-2$                    
                    exceptionOccurred(me);
            	} else {
					// Add value to codeTableCache
               		codeTableCache.loadTable(aqr.getRequestID(), aqr.getAtomicRequestID().getNodeID(), response.getResults());
                    if(response.getFinalRow() < 0) {
                        try {
                        	requestBatch(aqr.getAtomicRequestID(), aqr.getConnectorID());
                        } catch(MetaMatrixComponentException e) {
                            LogManager.logError(LogConstants.CTX_DQP, e, e.getMessage());
                        	exceptionOccurred(e);
                        }
                    } else {
                        // Set the loading complete
                        Collection requests = codeTableCache.markCacheLoaded(aqr.getRequestID(), aqr.getAtomicRequestID().getNodeID());
                        notifyWaitingCodeTableRequests(requests);
        		        try {
        					closeRequest(aqr.getAtomicRequestID(), aqr.getConnectorID());
        				} catch (MetaMatrixComponentException e1) {
        					LogManager.logDetail(LogConstants.CTX_DQP, "Exception closing code table request"); //$NON-NLS-1$
        				}
                    }
                }
			}
        });
    }

    /* (non-Javadoc)
	 * @see com.metamatrix.dqp.internal.process.DataTierManager#clearCodeTables()
	 */
    public void clearCodeTables() {
        this.codeTableCache.clearAll();
    }

}
