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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleSource;
import com.metamatrix.common.comm.api.ResultsReceiver;
import com.metamatrix.common.log.LogManager;
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
import com.metamatrix.dqp.service.VDBService;
import com.metamatrix.dqp.util.LogConstants;
import com.metamatrix.query.processor.ProcessorDataManager;
import com.metamatrix.query.processor.QueryProcessor;
import com.metamatrix.query.sql.ReservedWords;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.util.CommandContext;

public class DataTierManagerImpl implements ProcessorDataManager {

	// Resources
	private DQPCore requestMgr;
    private DataService dataService;
    private VDBService vdbService;
    private BufferService bufferService;

    // Code table limits
    private int maxCodeTableRecords;

	// Processor state
    private CodeTableCache codeTableCache;
    
    public DataTierManagerImpl(DQPCore requestMgr,
        DataService dataService, VDBService vdbService, BufferService bufferService, 
        int maxCodeTables, int maxCodeTableRecords) {

		this.requestMgr = requestMgr;
        this.dataService = dataService;
        this.vdbService = vdbService;
        this.maxCodeTableRecords = maxCodeTableRecords;
        this.bufferService = bufferService;

        this.codeTableCache = new CodeTableCache(maxCodeTables);
	}

	public TupleSource registerRequest(Object processorId, Command command,
			String modelName, String connectorBindingId, int nodeID) throws MetaMatrixComponentException, MetaMatrixProcessingException {
		RequestWorkItem workItem = requestMgr.getRequestWorkItem((RequestID)processorId);
		AtomicRequestMessage aqr = createRequest(processorId, command, modelName, connectorBindingId, nodeID);
        DataTierTupleSource tupleSource = new DataTierTupleSource(aqr.getCommand().getProjectedSymbols(), aqr, this, aqr.getConnectorID(), workItem);
        tupleSource.open();
        return tupleSource;
	}

	private AtomicRequestMessage createRequest(Object processorId,
			Command command, String modelName, String connectorBindingId, int nodeID)
			throws MetaMatrixProcessingException, MetaMatrixComponentException {
		RequestWorkItem workItem = requestMgr.getRequestWorkItem((RequestID)processorId);
		
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
        if (connectorBindingId == null) {
        	List bindings = vdbService.getConnectorBindingNames(workItem.getDqpWorkContext().getVdbName(), workItem.getDqpWorkContext().getVdbVersion(), modelName);
	        if (bindings == null || bindings.size() != 1) {
	            // this should not happen, but it did occur when setting up the SystemAdmin models
	            throw new MetaMatrixComponentException(DQPPlugin.Util.getString("DataTierManager.could_not_obtain_connector_binding", new Object[]{modelName, workItem.getDqpWorkContext().getVdbName(), workItem.getDqpWorkContext().getVdbVersion() })); //$NON-NLS-1$
	        }
	        connectorBindingId = (String)bindings.get(0); 
	        Assertion.isNotNull(connectorBindingId, "could not obtain connector id"); //$NON-NLS-1$
        }
        aqr.setConnectorBindingID(connectorBindingId);
        // Select any connector instance for this connector binding
        ConnectorID connectorID = this.dataService.selectConnector(connectorBindingId);
        // if we had this as null before
        aqr.setConnectorID(connectorID);
		return aqr;
	}
	
	String getConnectorName(String connectorBindingID) {
        try {
            return vdbService.getConnectorName(connectorBindingID);
        } catch (MetaMatrixComponentException t) {
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
        throws BlockedException, MetaMatrixComponentException, MetaMatrixProcessingException {

        switch (this.codeTableCache.cacheExists(codeTableName, returnElementName, keyElementName, context)) {
        	case CACHE_NOT_EXIST:
	        	registerCodeTableRequest(context, codeTableName, returnElementName, keyElementName);
        	case CACHE_EXISTS:
	        	return this.codeTableCache.lookupValue(codeTableName, returnElementName, keyElementName, keyValue, context);
	        case CACHE_OVERLOAD:
	        	throw new MetaMatrixProcessingException("ERR.018.005.0099", DQPPlugin.Util.getString("ERR.018.005.0099")); //$NON-NLS-1$ //$NON-NLS-2$
	        default:
	            throw BlockedException.INSTANCE;
        }
    }

    void registerCodeTableRequest(
        final CommandContext context,
        final String codeTableName,
        String returnElementName,
        String keyElementName)
        throws MetaMatrixComponentException, MetaMatrixProcessingException {

        String query = ReservedWords.SELECT + ' ' + keyElementName + " ," + returnElementName + ' ' + ReservedWords.FROM + ' ' + codeTableName; //$NON-NLS-1$ 
        
        final Integer codeRequestId = this.codeTableCache.createCacheRequest(codeTableName, returnElementName, keyElementName, context);

        boolean success = false;
        QueryProcessor processor = null;
        try {
            processor = context.getQueryProcessorFactory().createQueryProcessor(query, codeTableName.toUpperCase(), context);

            processor.setBatchHandler(new QueryProcessor.BatchHandler() {
            	@Override
            	public void batchProduced(TupleBatch batch) throws MetaMatrixProcessingException {
    				// Determine whether the results should be added to code table cache
                	// Depends on size of results and available memory and system parameters

                	if (batch.getEndRow() > maxCodeTableRecords) {
                        throw new MetaMatrixProcessingException("ERR.018.005.0100", DQPPlugin.Util.getString("ERR.018.005.0100", context.getProcessorID(), codeRequestId)); //$NON-NLS-1$ //$NON-NLS-2$                    
                	}
               		codeTableCache.loadTable(codeRequestId, batch.getAllTuples());
            	}
            });

        	//process lookup as fully blocking
        	processor.process();
        	success = true;
        } finally {
        	Collection requests = null;
        	if (success) {
                requests = codeTableCache.markCacheLoaded(codeRequestId);
        	} else {
        		requests = codeTableCache.errorLoadingCache(codeRequestId);        		
        	}
        	notifyWaitingCodeTableRequests(requests);
        	if (processor != null) {
	            try {
	            	this.bufferService.getBufferManager().removeTupleSource(processor.getResultsID());
	    		} catch (MetaMatrixComponentException e1) {
	    			LogManager.logDetail(LogConstants.CTX_DQP, "Exception closing code table request"); //$NON-NLS-1$
	    		}
        	}
        }
    }

    /* (non-Javadoc)
	 * @see com.metamatrix.dqp.internal.process.DataTierManager#clearCodeTables()
	 */
    public void clearCodeTables() {
        this.codeTableCache.clearAll();
    }

}
