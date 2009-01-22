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

package com.metamatrix.dqp.internal.datamgr.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.comm.api.ResultsReceiver;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.data.api.AsynchQueryCommandExecution;
import com.metamatrix.data.api.AsynchQueryExecution;
import com.metamatrix.data.api.Batch;
import com.metamatrix.data.api.BatchedExecution;
import com.metamatrix.data.api.BatchedUpdatesExecution;
import com.metamatrix.data.api.Connection;
import com.metamatrix.data.api.Connector;
import com.metamatrix.data.api.ConnectorCapabilities;
import com.metamatrix.data.api.Execution;
import com.metamatrix.data.api.ProcedureExecution;
import com.metamatrix.data.api.SynchQueryCommandExecution;
import com.metamatrix.data.api.SynchQueryExecution;
import com.metamatrix.data.api.UpdateExecution;
import com.metamatrix.data.basic.BasicBatch;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.IBatchedUpdates;
import com.metamatrix.data.language.IBulkInsert;
import com.metamatrix.data.language.ICommand;
import com.metamatrix.data.language.IParameter;
import com.metamatrix.data.language.IProcedure;
import com.metamatrix.data.language.IQuery;
import com.metamatrix.data.language.IQueryCommand;
import com.metamatrix.data.language.ISetQuery;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;
import com.metamatrix.data.xa.api.XAConnection;
import com.metamatrix.data.xa.api.XAConnector;
import com.metamatrix.dqp.DQPPlugin;
import com.metamatrix.dqp.internal.datamgr.language.LanguageBridgeFactory;
import com.metamatrix.dqp.internal.datamgr.metadata.MetadataFactory;
import com.metamatrix.dqp.internal.datamgr.metadata.RuntimeMetadataImpl;
import com.metamatrix.dqp.internal.process.AbstractWorkItem;
import com.metamatrix.dqp.internal.process.DQPWorkContext;
import com.metamatrix.dqp.message.AtomicRequestID;
import com.metamatrix.dqp.message.AtomicRequestMessage;
import com.metamatrix.dqp.message.AtomicResultsMessage;
import com.metamatrix.dqp.spi.TrackerLogConstants;
import com.metamatrix.dqp.util.LogConstants;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.metadata.TempMetadataAdapter;
import com.metamatrix.query.metadata.TempMetadataStore;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;

public abstract class ConnectorWorkItem extends AbstractWorkItem {
	
	private static class NeedsClosedException extends Exception {}
	
    /* Permanent state members */
    protected AtomicRequestID id;
    protected ConnectorManager manager;
    protected AtomicRequestMessage requestMsg;
    protected boolean isTransactional;
    
    /* Created on new request */
    protected Connection connection;
    protected ExecutionContextImpl securityContext;
    private int executionMode;
    protected volatile Execution execution;
    private ICommand translatedCommand;
        
    /* End state information */    
    private boolean lastBatch;
    private int rowCount;
    
    protected enum RequestState {
    	NEW, MORE, CLOSE
    }
        
    protected RequestState requestState = RequestState.NEW;
    
    private volatile boolean isCancelled;
    private volatile boolean moreRequested;
    private volatile boolean closeRequested;
    private boolean isClosed;

    ResultsReceiver<AtomicResultsMessage> resultsReceiver;
    
    ConnectorWorkItem(AtomicRequestMessage message, ConnectorManager manager, ResultsReceiver<AtomicResultsMessage> resultsReceiver) {
        this.id = message.getAtomicRequestID();
        this.requestMsg = message;
        this.isTransactional = manager.getConnector() instanceof XAConnector && message.isTransactional();
        this.manager = manager;
        this.resultsReceiver = resultsReceiver;
    }

    private void createConnection(Connector connector, QueryMetadataInterface queryMetadata) throws ConnectorException, MetaMatrixComponentException {
        LogManager.logTrace(LogConstants.CTX_CONNECTOR, new Object[] {id, "creating connection for atomic-request"});  //$NON-NLS-1$
        AtomicRequestID requestID = this.requestMsg.getAtomicRequestID();
        this.securityContext = new ExecutionContextImpl(requestMsg.getWorkContext().getVdbName(),
                                                       requestMsg.getWorkContext().getVdbVersion(),
                                                       requestMsg.getWorkContext().getUserName(),
                                                       requestMsg.getWorkContext().getTrustedPayload(),
                                                       requestMsg.getExecutionPayload(),                                                                       
                                                       requestMsg.getWorkContext().getConnectionID(),                                                                      
                                                       requestMsg.getConnectorID().getID(),
                                                       requestMsg.getRequestID().toString(),
                                                       Integer.toString(requestID.getNodeID()),
                                                       Integer.toString(requestID.getExecutionId()),
                                                       requestMsg.useResultSetCache()
                                                       && (requestMsg.getCommand()).areResultsCachable()
                                                       ); 
        if (isTransactional){
    		connection = ((XAConnector)connector).getXAConnection(this.securityContext, requestMsg.getTransactionContext());
    		if (!(connection instanceof XAConnection)){
                throw new ConnectorException(DQPPlugin.Util.getString("ConnectorRequestState.invalidConnectionType")); //$NON-NLS-1$                    		                    			
    		} 
    		this.securityContext.setTransactional(true);
    	} else {
    	    if (requestMsg.isTransactional() && requestMsg.getCommand().updatingModelCount(queryMetadata) > 0) {
    	        throw new ConnectorException(DQPPlugin.Util.getString("ConnectorWorker.transactionNotSupported")); //$NON-NLS-1$
    	    }
    	    connection = connector.getConnection(this.securityContext);
    	}
    }
            
    protected void process() {
    	DQPWorkContext.setWorkContext(this.requestMsg.getWorkContext());
        ClassLoader contextloader = Thread.currentThread().getContextClassLoader();
    	Thread.currentThread().setContextClassLoader(manager.getConnector().getClass().getClassLoader());
    	boolean success = true;
    	try {
    		checkForCloseEvent();
    		switch (this.requestState) {
	    		case NEW:
    				createExecution();
		    		//prior to processing new, mark me as MORE
		        	if (this.requestState == RequestState.NEW) {
		        		this.requestState = RequestState.MORE;
		        		checkForCloseEvent();
			        	processNewRequest();
		        	}
		        	break;
	    		case MORE:
	    			processMoreRequest();
	    			break;
	    		case CLOSE:
	    			return;
    		}
			if (lastBatch && !this.securityContext.keepExecutionAlive()) {
				this.requestState = RequestState.CLOSE;
			}
		} catch (NeedsClosedException e) {
    		this.requestState = RequestState.CLOSE;
    	} catch (Throwable t){
    		success = false;
    		this.requestState = RequestState.CLOSE;
        	handleError(t);
        } finally {
        	try {
	        	if (this.requestState == RequestState.CLOSE) {
	    			processClose(success);
	        	} 
        	} finally {
            	Thread.currentThread().setContextClassLoader(contextloader);
        	}
        }
    }

	private void checkForCloseEvent() throws NeedsClosedException {
		if (this.isCancelled || this.closeRequested) {
			throw new NeedsClosedException();
		}
	}
    
    public void requestCancel() {
    	try {
            LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.id, "Processing CANCEL request"}); //$NON-NLS-1$
            asynchCancel();
            this.manager.logSRCCommand(this.requestMsg, this.securityContext, TrackerLogConstants.CMD_STATUS.CANCEL, -1);
        } catch (ConnectorException e) {
            LogManager.logWarning(LogConstants.CTX_CONNECTOR, e, DQPPlugin.Util.getString("Cancel_request_failed", this.id)); //$NON-NLS-1$
        } finally {
    		moreWork();
        }
    }
    
    public synchronized void requestMore() {
    	Assertion.assertTrue(!this.moreRequested, "More already requested"); //$NON-NLS-1$
    	this.moreRequested = true;
    	Assertion.assertTrue(!this.lastBatch, "More should not be requested after the last batch"); //$NON-NLS-1$
    	assert this.requestState != RequestState.NEW : "More should not be requested during NEW"; //$NON-NLS-1$
		moreWork();
    }
    
    public synchronized void requestClose() {
    	if (this.requestState == RequestState.CLOSE || this.closeRequested) {
    		LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.id, "Already closing request"}); //$NON-NLS-1$
    		return;
    	}
    	this.closeRequested = true;
    	moreWork();
    }
    
    private void handleError(Throwable t) {
        manager.logSRCCommand(this.requestMsg, this.securityContext, TrackerLogConstants.CMD_STATUS.ERROR, -1);
        
        String msg = DQPPlugin.Util.getString("ConnectorWorker.process_failed", this.id); //$NON-NLS-1$
        if (isCancelled) {            
            LogManager.logDetail(LogConstants.CTX_CONNECTOR, msg);
        } else if (t instanceof ConnectorException || t instanceof MetaMatrixProcessingException) {
        	LogManager.logWarning(LogConstants.CTX_CONNECTOR, t, msg);
        } else {
            LogManager.logError(LogConstants.CTX_CONNECTOR, t, msg);
        }    

        if (!(t instanceof CommunicationException)) {
            if (t instanceof ConnectorException) {
                t = new ConnectorException(t, DQPPlugin.Util.getString("ConnectorWorker.error_occurred", this.manager.getName(), t.getMessage())); //$NON-NLS-1$
            }        	
            this.resultsReceiver.exceptionOccurred(t);
        }
    }
    
    protected void processClose(boolean success) {
    	this.isClosed = true;
    	LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.id, "Processing Close :", this.requestMsg.getCommand()}); //$NON-NLS-1$
    	if (success) {
            manager.logSRCCommand(this.requestMsg, this.securityContext, TrackerLogConstants.CMD_STATUS.END, this.rowCount);
        }
        try {
	        if (execution != null) {
	            execution.close();
	            LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.id, "Closed execution"}); //$NON-NLS-1$                    
	        }	        
	    } catch (ConnectorException e) {
	        LogManager.logWarning(LogConstants.CTX_CONNECTOR, e.getMessage());
        } catch (Throwable e) {
            LogManager.logError(LogConstants.CTX_CONNECTOR, e, e.getMessage());
        } finally {
        	try {
                if (connection != null) {
                    connection.release();
                    LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.id, "Closed connection"}); //$NON-NLS-1$
                }
            } finally {
                manager.removeState(this.id);
                sendClose();
            }
        }        
    }

	protected void sendClose() {
		AtomicResultsMessage response = new AtomicResultsMessage(this.requestMsg);
		response.setRequestClosed(true);
		this.resultsReceiver.receiveResults(response);
	}
    
    protected void processNewRequest() throws ConnectorException, CommunicationException {
    	// Execute query
        Batch aBatch = null;
        switch(executionMode) {
            case ConnectorCapabilities.EXECUTION_MODE.SYNCH_QUERY:
            {
            	((SynchQueryExecution)execution).execute((IQuery)translatedCommand, this.requestMsg.getFetchSize());
                break;
            }
            case ConnectorCapabilities.EXECUTION_MODE.SYNCH_QUERYCOMMAND:
            {
                ((SynchQueryCommandExecution)execution).execute((IQueryCommand)translatedCommand, this.requestMsg.getFetchSize());
                break;
            }
            case ConnectorCapabilities.EXECUTION_MODE.UPDATE:
            case ConnectorCapabilities.EXECUTION_MODE.BULK_INSERT:                
            {
                int count = ((UpdateExecution)execution).execute(translatedCommand);
                aBatch = new BasicBatch();
                aBatch.addRow(Arrays.asList(new Object[] { new Integer(count)}));
                aBatch.setLast();
                break;
            }
            case ConnectorCapabilities.EXECUTION_MODE.BATCHED_UPDATES:
            {
                List updates = ((IBatchedUpdates)translatedCommand).getUpdateCommands();
                ICommand[] updatesArray = (ICommand[])updates.toArray(new ICommand[updates.size()]); 
                int[] count = ((BatchedUpdatesExecution)execution).execute(updatesArray);
                aBatch = new BasicBatch();
                for (int i = 0; i < count.length; i++) {
                    aBatch.addRow(Arrays.asList(new Object[] { new Integer(count[i])}));
                }
                aBatch.setLast();
                break;
            }
            case ConnectorCapabilities.EXECUTION_MODE.PROCEDURE:
            {
                ((ProcedureExecution)execution).execute((IProcedure)translatedCommand, this.requestMsg.getFetchSize());
                break;
            }
            case ConnectorCapabilities.EXECUTION_MODE.ASYNCH_QUERY:
            {
                ((AsynchQueryExecution)execution).executeAsynch((IQuery)translatedCommand, this.requestMsg.getFetchSize());
                break;
            }
            case ConnectorCapabilities.EXECUTION_MODE.ASYNCH_QUERYCOMMAND:
            {
                ((AsynchQueryCommandExecution)execution).executeAsynch((IQueryCommand)translatedCommand, this.requestMsg.getFetchSize());
                break;
            }
            default:
            {
                Assertion.failed(DQPPlugin.Util.getString("ConnectorWorker.Unable_to_open_connector_execution")); //$NON-NLS-1$
            }
        }
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.id, "Executed command"}); //$NON-NLS-1$

        if(isBatchedExecution()) {
            aBatch = getNextBatch((BatchedExecution) execution, translatedCommand);
            LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.id, "Obtained first batch, row count:" + aBatch.getRowCount()}); //$NON-NLS-1$            
        }

        handleBatch(aBatch);
    }

	protected void createExecution() throws MetaMatrixComponentException,
			ConnectorException, MetaMatrixProcessingException {
    	LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.requestMsg.getAtomicRequestID(), "Processing NEW request:", this.requestMsg.getCommand()}); //$NON-NLS-1$                                     

		QueryMetadataInterface queryMetadata = new TempMetadataAdapter(manager.getMetadataService().lookupMetadata(this.requestMsg.getWorkContext().getVdbName(), this.requestMsg.getWorkContext().getVdbVersion()), new TempMetadataStore());
        createConnection(manager.getConnector(), queryMetadata);
        
        LogManager.logTrace(LogConstants.CTX_CONNECTOR, new Object[] {id, "creating execution for atomic-request"});  //$NON-NLS-1$

        // Translate the command
        Command command = this.requestMsg.getCommand();
        LanguageBridgeFactory factory = new LanguageBridgeFactory(queryMetadata);
        this.translatedCommand = factory.translate(command);

        RuntimeMetadata rmd = new RuntimeMetadataImpl(new MetadataFactory(queryMetadata));

        // Determine execution mode and remember it
        if(this.translatedCommand instanceof IQuery) {
            if(connection.getCapabilities().supportsExecutionMode(ConnectorCapabilities.EXECUTION_MODE.SYNCH_QUERY)) {
                this.executionMode = ConnectorCapabilities.EXECUTION_MODE.SYNCH_QUERY;
            } else if(connection.getCapabilities().supportsExecutionMode(ConnectorCapabilities.EXECUTION_MODE.ASYNCH_QUERY)) {
                this.executionMode = ConnectorCapabilities.EXECUTION_MODE.ASYNCH_QUERY;
            } else if(connection.getCapabilities().supportsExecutionMode(ConnectorCapabilities.EXECUTION_MODE.SYNCH_QUERYCOMMAND)) {
                this.executionMode = ConnectorCapabilities.EXECUTION_MODE.SYNCH_QUERYCOMMAND;
            } else if(connection.getCapabilities().supportsExecutionMode(ConnectorCapabilities.EXECUTION_MODE.ASYNCH_QUERYCOMMAND)) {
                this.executionMode = ConnectorCapabilities.EXECUTION_MODE.ASYNCH_QUERYCOMMAND;
            }
        } else if (this.translatedCommand instanceof ISetQuery) { 
            if(connection.getCapabilities().supportsExecutionMode(ConnectorCapabilities.EXECUTION_MODE.SYNCH_QUERYCOMMAND)) {
                this.executionMode = ConnectorCapabilities.EXECUTION_MODE.SYNCH_QUERYCOMMAND;
            } else if(connection.getCapabilities().supportsExecutionMode(ConnectorCapabilities.EXECUTION_MODE.ASYNCH_QUERYCOMMAND)) {
                this.executionMode = ConnectorCapabilities.EXECUTION_MODE.ASYNCH_QUERYCOMMAND;
            } else {
                Assertion.failed("unsupported execution mode"); //$NON-NLS-1$
            }
        } else if(this.translatedCommand instanceof IProcedure) {
            this.executionMode = ConnectorCapabilities.EXECUTION_MODE.PROCEDURE;  
        } else if(this.translatedCommand instanceof IBatchedUpdates) {
            this.executionMode = ConnectorCapabilities.EXECUTION_MODE.BATCHED_UPDATES;  
        } else if (this.translatedCommand instanceof IBulkInsert){
            this.executionMode = ConnectorCapabilities.EXECUTION_MODE.BULK_INSERT;
        } else {
            this.executionMode = ConnectorCapabilities.EXECUTION_MODE.UPDATE;  
        }
        
        // Create the execution based on mode
        execution = connection.createExecution(this.executionMode, this.securityContext, rmd);
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.requestMsg.getAtomicRequestID(), "Obtained execution"}); //$NON-NLS-1$      
        //Log the Source Command (Must be after obtaining the execution context)
        manager.logSRCCommand(this.requestMsg, this.securityContext, TrackerLogConstants.CMD_STATUS.NEW, -1); 
	}
    
    protected void handleBatch(Batch aBatch) 
        throws ConnectorException, CommunicationException {

        LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.id, "Sending results from connector"}); //$NON-NLS-1$
        lastBatch = aBatch.isLast();
            
        this.rowCount += aBatch.getRowCount();
        
        if (lastBatch) {
            LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.id, "Obtained last batch, total row count:", rowCount}); //$NON-NLS-1$
        }    

        // Check for max result rows exceeded
        if(manager.getMaxResultRows() != 0 && this.rowCount > manager.getMaxResultRows()){
            if (manager.isExceptionOnMaxRows()) {
                String msg = DQPPlugin.Util.getString("ConnectorWorker.MaxResultRowsExceed", manager.getMaxResultRows()); //$NON-NLS-1$
                throw new ConnectorException(msg);
            }
            LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.id, "Exceeded max, returning", manager.getMaxResultRows()}); //$NON-NLS-1$
        	this.rowCount = manager.getMaxResultRows();
        	List[] origResults = aBatch.getResults();
            List[] newResults = new List[manager.getMaxResultRows()];
            System.arraycopy(origResults, 0, newResults, 0, manager.getMaxResultRows());
            List newResultsList = Arrays.asList(newResults);
            Batch newBatch = new BasicBatch(newResultsList);
            newBatch.setLast();
            aBatch = newBatch;
        }

    	sendResults(aBatch, (requestMsg.getCommand()).getProjectedSymbols());
    }

    private void processMoreRequest() throws ConnectorException, CommunicationException {
    	Assertion.assertTrue(this.moreRequested, "More was not requested");
    	this.moreRequested = false;
    	LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.id, "Processing MORE request"}); //$NON-NLS-1$

        Assertion.assertTrue(isBatchedExecution(), DQPPlugin.Util.getString("ConnectorWorker.ConnectorWorker_expecting_an_Execution_of_type_SynchExecution,_got", execution));
        
        Batch aBatch = ((BatchedExecution) execution).nextBatch();                        
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.id, "Obtained batch, row count:" + aBatch.getRowCount()}); //$NON-NLS-1$

        handleBatch(aBatch);
    }

	private boolean isBatchedExecution() {
		return executionMode == ConnectorCapabilities.EXECUTION_MODE.SYNCH_QUERY || 
                executionMode == ConnectorCapabilities.EXECUTION_MODE.PROCEDURE ||
                executionMode == ConnectorCapabilities.EXECUTION_MODE.ASYNCH_QUERY ||
                executionMode == ConnectorCapabilities.EXECUTION_MODE.SYNCH_QUERYCOMMAND ||
                executionMode == ConnectorCapabilities.EXECUTION_MODE.ASYNCH_QUERYCOMMAND;
	}    
            
    private Batch getNextBatch(BatchedExecution batchedExecution, ICommand command) throws ConnectorException{
        if(command instanceof IProcedure){
            return getNextProcedureBatch((ProcedureExecution)batchedExecution, (IProcedure)command);
        }
        return batchedExecution.nextBatch();
    }

    static Batch getNextProcedureBatch(ProcedureExecution execution, IProcedure command) throws ConnectorException {
        List results = new ArrayList();

        List params = command.getParameters();
        
        int paramCols = 0;
        int resultSetCols = 0;
        if(params != null && !params.isEmpty()){
            Iterator iter = params.iterator();
            while(iter.hasNext()){
                IParameter param = (IParameter)iter.next();
                if (param.getDirection() == IParameter.RESULT_SET) {
                    resultSetCols = param.getMetadataID().getChildIDs().size();
                } else if(param.getDirection() == IParameter.RETURN || param.getDirection() == IParameter.OUT || param.getDirection() == IParameter.INOUT){
                    paramCols += 1;
                }
            }
        }
        
        Batch abatch = execution.nextBatch();
        boolean atLast = false;
        if (abatch.getResults() != null && abatch.getResults().length > 0) {
            for(int i = 0; i < abatch.getResults().length; i++){
                results.add(new ArrayList(abatch.getResults()[i]));
            }
            
            int resultSetSize = ((List)results.get(0)).size();
            
            if (resultSetSize != resultSetCols) {
                throw new ConnectorException(DQPPlugin.Util.getString("ConnectorWorker.ConnectorWorker_result_set_unexpected_columns", new Object[] {command, new Integer(resultSetCols), new Integer(resultSetSize)})); //$NON-NLS-1$
            }
            
            if(paramCols != 0){
                final List resultsPadding = Arrays.asList(new Object[paramCols]);
                //pad null to the current results
                Iterator iter = results.iterator();
                while(iter.hasNext()){
                    ((List)iter.next()).addAll(resultsPadding);
                }
            }
        } else {
            atLast = true;
        }
        
        if (atLast || abatch.isLast()) {
            return prepareLastProcedureBatch(execution, results, params, paramCols, resultSetCols);
        }
        
        BasicBatch batch = new BasicBatch(results);
        return batch;
    }

    private static Batch prepareLastProcedureBatch(ProcedureExecution execution,
                                            List results,
                                            List params,
                                            int paramCols,
                                            int resultSetCols) throws ConnectorException {
        List outParamValues = new ArrayList(params.size());
                
        if(!params.isEmpty()){
            Iterator iter = params.iterator();
            //return
            while(iter.hasNext()){
                IParameter param = (IParameter)iter.next();
                if(param.getDirection() == IParameter.RETURN){
                    outParamValues.add(execution.getOutputValue(param));
                }
            }
            //out, inout
            iter = params.iterator();
            while(iter.hasNext()){
                IParameter param = (IParameter)iter.next();
                if(param.getDirection() == IParameter.OUT || param.getDirection() == IParameter.INOUT){
                    outParamValues.add(execution.getOutputValue(param));
                }
            }

            //add out/return values
            Iterator i = outParamValues.iterator();
            for(int index = resultSetCols; i.hasNext(); index++){
                Object[] newRow = new Object[paramCols + resultSetCols];
                newRow[index] = i.next();
                results.add(Arrays.asList(newRow));
            }
        }
        BasicBatch batch = new BasicBatch(results);
        batch.setLast();
        return batch;
    }

    private void sendResults(Batch batch, List elements) 
        throws CommunicationException {
        
        int currentRowCount = batch.getRowCount();
        if ( !lastBatch && currentRowCount == 0 ) {
            // Defect 13366 - Should send all batches, even if they're zero size.
            // Log warning if received a zero-size non-last batch from the connector.
            LogManager.logWarning(LogConstants.CTX_CONNECTOR, DQPPlugin.Util.getString("ConnectorWorker.zero_size_non_last_batch", requestMsg.getConnectorID())); //$NON-NLS-1$
        }

        AtomicResultsMessage response = createResultsMessage(this.requestMsg, batch.getResults(), elements);
        response.setFirstRow(rowCount + 1 - currentRowCount);
        response.setLastRow(rowCount);
        
        // if we need to keep the execution alive, then we can not support
        // implicit close.
        response.setSupportsImplicitClose(!this.securityContext.keepExecutionAlive());
        response.setTransactional(this.securityContext.isTransactional());

        if ( lastBatch ) {
            response.setFinalRow(rowCount);
            response.setPartialResults(false);
        } else {
            response.setFinalRow(-1);
            response.setPartialResults(true);
        }

        this.resultsReceiver.receiveResults(response);
    }
    
    public static AtomicResultsMessage createResultsMessage(AtomicRequestMessage message, List[] batch, List columnSymbols) {
        String[] columnNames = new String[columnSymbols.size()];
        String[] dataTypes = new String[columnSymbols.size()];

        for(int i=0; i<columnSymbols.size(); i++) {
            SingleElementSymbol symbol = (SingleElementSymbol) columnSymbols.get(i);
            columnNames[i] = symbol.getShortName();
            dataTypes[i] = DataTypeManager.getDataTypeName(symbol.getType());
        }
        
        return new AtomicResultsMessage(message, batch, columnNames, dataTypes);
    }    
            
    void asynchCancel() throws ConnectorException {
    	if (!this.isCancelled) {
	    	this.isCancelled = true;
	        if(execution != null) {
	            execution.cancel();
	        }            
	        LogManager.logDetail(LogConstants.CTX_CONNECTOR, DQPPlugin.Util.getString("DQPCore.The_atomic_request_has_been_cancelled", this.id)); //$NON-NLS-1$
    	}
    }
    
    boolean isCancelled() {
    	return this.isCancelled;
    }

	@Override
	protected boolean isDoneProcessing() {
		return isClosed;
	}
	
	@Override
	public String toString() {
		return this.id.toString();
	}

}