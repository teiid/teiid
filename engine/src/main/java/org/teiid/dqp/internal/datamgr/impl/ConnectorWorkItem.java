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

package org.teiid.dqp.internal.datamgr.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.connector.api.Connection;
import org.teiid.connector.api.Connector;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.DataNotAvailableException;
import org.teiid.connector.api.Execution;
import org.teiid.connector.api.ProcedureExecution;
import org.teiid.connector.api.ResultSetExecution;
import org.teiid.connector.api.UpdateExecution;
import org.teiid.connector.language.ICommand;
import org.teiid.connector.language.IProcedure;
import org.teiid.connector.language.IQueryCommand;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;
import org.teiid.connector.xa.api.XAConnector;
import org.teiid.dqp.internal.datamgr.language.LanguageBridgeFactory;
import org.teiid.dqp.internal.datamgr.metadata.RuntimeMetadataImpl;
import org.teiid.dqp.internal.process.AbstractWorkItem;
import org.teiid.dqp.internal.process.DQPWorkContext;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.comm.api.ResultsReceiver;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.common.types.TransformationException;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.dqp.DQPPlugin;
import com.metamatrix.dqp.message.AtomicRequestID;
import com.metamatrix.dqp.message.AtomicRequestMessage;
import com.metamatrix.dqp.message.AtomicResultsMessage;
import com.metamatrix.dqp.service.CommandLogMessage;
import com.metamatrix.dqp.util.LogConstants;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.metadata.TempMetadataAdapter;
import com.metamatrix.query.metadata.TempMetadataStore;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.StoredProcedure;
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
    protected volatile ResultSetExecution execution;
    protected ProcedureBatchHandler procedureBatchHandler;
    private ICommand translatedCommand;
    private Class<?>[] schema;
    private List<Integer> convertToRuntimeType;
    private List<Integer> convertToDesiredRuntimeType;
        
    /* End state information */    
    protected boolean lastBatch;
    protected int rowCount;
    
    protected enum RequestState {
    	NEW, MORE, CLOSE
    }
        
    protected RequestState requestState = RequestState.NEW;
    
    private volatile boolean isCancelled;
    private volatile boolean moreRequested;
    private volatile boolean closeRequested;
    private boolean isClosed;

    protected ResultsReceiver<AtomicResultsMessage> resultsReceiver;
    
    ConnectorWorkItem(AtomicRequestMessage message, ConnectorManager manager, ResultsReceiver<AtomicResultsMessage> resultsReceiver) {
        this.id = message.getAtomicRequestID();
        this.requestMsg = message;
        this.manager = manager;
        this.resultsReceiver = resultsReceiver;
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
                Integer.toString(requestID.getExecutionId())
                );
        this.securityContext.setBatchSize(this.requestMsg.getFetchSize());
        this.securityContext.setContextCache(manager.getContextCache());
    }

    protected void createConnection(Connector connector, QueryMetadataInterface queryMetadata) throws ConnectorException, MetaMatrixComponentException {
        LogManager.logTrace(LogConstants.CTX_CONNECTOR, new Object[] {id, "creating connection for atomic-request"});  //$NON-NLS-1$
         
        if (requestMsg.isTransactional()){
        	if (manager.isXa()) {
	    		connection = ((XAConnector)connector).getXAConnection(this.securityContext, requestMsg.getTransactionContext());
	    		this.securityContext.setTransactional(true);
	    		this.isTransactional = true;
	    		return;
        	} 
    	    if (!manager.isImmutable() && requestMsg.getCommand().updatingModelCount(queryMetadata) > 0) {
    	        throw new ConnectorException(DQPPlugin.Util.getString("ConnectorWorker.transactionNotSupported")); //$NON-NLS-1$
    	    }
    	}
    	connection = connector.getConnection(this.securityContext);
    }
            
    protected void process() {
    	DQPWorkContext.setWorkContext(this.requestMsg.getWorkContext());
        ClassLoader contextloader = Thread.currentThread().getContextClassLoader();
    	Thread.currentThread().setContextClassLoader(this.manager.getClassloader());
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
            this.manager.logSRCCommand(this.requestMsg, this.securityContext, CommandLogMessage.CMD_STATUS_CANCEL, -1);
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
        manager.logSRCCommand(this.requestMsg, this.securityContext, CommandLogMessage.CMD_STATUS_ERROR, -1);
        
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
            manager.logSRCCommand(this.requestMsg, this.securityContext, CommandLogMessage.CMD_STATUS_END, this.rowCount);
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
                    connection.close();
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
    
    protected void processNewRequest() throws ConnectorException {
    	// Execute query
    	this.execution.execute();
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.id, "Executed command"}); //$NON-NLS-1$

        handleBatch();
    }

	protected void createExecution() throws MetaMatrixComponentException,
			ConnectorException, MetaMatrixProcessingException {
    	LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.requestMsg.getAtomicRequestID(), "Processing NEW request:", this.requestMsg.getCommand()}); //$NON-NLS-1$                                     

		QueryMetadataInterface queryMetadata = new TempMetadataAdapter(manager.getMetadataService().lookupMetadata(this.requestMsg.getWorkContext().getVdbName(), this.requestMsg.getWorkContext().getVdbVersion()), new TempMetadataStore());
        createConnection(manager.getConnector(), queryMetadata);
        
        LogManager.logTrace(LogConstants.CTX_CONNECTOR, new Object[] {id, "creating execution for atomic-request"});  //$NON-NLS-1$

        // Translate the command
        Command command = this.requestMsg.getCommand();
		List<SingleElementSymbol> symbols = this.requestMsg.getCommand().getProjectedSymbols();
		this.schema = new Class[symbols.size()];
		this.convertToDesiredRuntimeType = new ArrayList<Integer>(symbols.size());
		this.convertToRuntimeType = new ArrayList<Integer>(symbols.size());
		for (int i = 0; i < schema.length; i++) {
			SingleElementSymbol symbol = symbols.get(i);
			this.schema[i] = symbol.getType();
			this.convertToDesiredRuntimeType.add(i);
			this.convertToRuntimeType.add(i);
		}

        LanguageBridgeFactory factory = new LanguageBridgeFactory(queryMetadata);
        this.translatedCommand = factory.translate(command);

        RuntimeMetadata rmd = new RuntimeMetadataImpl(queryMetadata);
        
        // Create the execution based on mode
        final Execution exec = connection.createExecution(this.translatedCommand, this.securityContext, rmd);
        if (this.translatedCommand instanceof IProcedure) {
        	Assertion.isInstanceOf(this.execution, ProcedureExecution.class, "IProcedure Executions are expected to be ProcedureExecutions"); //$NON-NLS-1$
        	this.execution = (ProcedureExecution)exec;
        	StoredProcedure proc = (StoredProcedure)command;
        	if (proc.returnParameters()) {
        		this.procedureBatchHandler = new ProcedureBatchHandler((IProcedure)this.translatedCommand, (ProcedureExecution)this.execution);
        	}
        } else if (this.translatedCommand instanceof IQueryCommand){
        	Assertion.isInstanceOf(this.execution, ResultSetExecution.class, "IQueryCommand Executions are expected to be ResultSetExecutions"); //$NON-NLS-1$
        	this.execution = (ResultSetExecution)exec;
        } else {
        	Assertion.isInstanceOf(this.execution, UpdateExecution.class, "Update Executions are expected to be UpdateExecutions"); //$NON-NLS-1$
        	this.execution = new ResultSetExecution() {
        		private int[] results;
        		private int index;
        		
        		@Override
        		public void cancel() throws ConnectorException {
        			exec.cancel();
        		}
        		@Override
        		public void close() throws ConnectorException {
        			exec.close();
        		}
        		@Override
        		public void execute() throws ConnectorException {
        			exec.execute();
        		}
        		@Override
        		public List<?> next() throws ConnectorException,
        				DataNotAvailableException {
        			if (results == null) {
        				results = ((UpdateExecution)exec).getUpdateCounts();
        			}
        			if (index < results.length) {
        				return Arrays.asList(results[index++]);
        			}
        			return null;
        		}
        	};
        }
        
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.requestMsg.getAtomicRequestID(), "Obtained execution"}); //$NON-NLS-1$      
        //Log the Source Command (Must be after obtaining the execution context)
        manager.logSRCCommand(this.requestMsg, this.securityContext, CommandLogMessage.CMD_STATUS_NEW, -1); 
	}
    
    protected void handleBatch() 
        throws ConnectorException {
    	Assertion.assertTrue(!this.lastBatch);
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.id, "Sending results from connector"}); //$NON-NLS-1$
        int batchSize = 0;
        List<List> rows = new ArrayList<List>(batchSize/4);
        boolean sendResults = true;
    	try {
	        while (batchSize < this.requestMsg.getFetchSize()) {
        		List row = this.execution.next();
            	if (row == null) {
            		this.lastBatch = true;
            		break;
            	}
            	
            	this.rowCount += 1;
            	batchSize++;
            	if (this.procedureBatchHandler != null) {
            		row = this.procedureBatchHandler.padRow(row);
            	}
            	
            	correctTypes(row);
            	rows.add(row);
	            // Check for max result rows exceeded
	            if(manager.getMaxResultRows() != 0 && this.rowCount >= manager.getMaxResultRows()){
	                if (this.rowCount == manager.getMaxResultRows() && !manager.isExceptionOnMaxRows()) {
		                LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.id, "Exceeded max, returning", manager.getMaxResultRows()}); //$NON-NLS-1$
		        		this.lastBatch = true;
		        		break;
	            	} else if (this.rowCount > manager.getMaxResultRows() && manager.isExceptionOnMaxRows()) {
	                    String msg = DQPPlugin.Util.getString("ConnectorWorker.MaxResultRowsExceed", manager.getMaxResultRows()); //$NON-NLS-1$
	                    throw new ConnectorException(msg);
	                }
	            }
	        }
    	} catch (DataNotAvailableException e) {
    		if (rows.size() == 0) {
    			sendResults = dataNotAvailable(e.getRetryDelay());
    		}
    	}
                
        if (lastBatch) {
        	if (this.procedureBatchHandler != null) {
        		List row = this.procedureBatchHandler.getParameterRow();
        		if (row != null) {
        			correctTypes(row);
        			rows.add(row);
        			this.rowCount++;
        		}
        	}
            LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.id, "Obtained last batch, total row count:", rowCount}); //$NON-NLS-1$
        }   
        
        if (sendResults) {
        	sendResults(rows);
        }
    }

	protected void sendResults(List<List> rows) {
		int currentRowCount = rows.size();
		if ( !lastBatch && currentRowCount == 0 ) {
		    // Defect 13366 - Should send all batches, even if they're zero size.
		    // Log warning if received a zero-size non-last batch from the connector.
		    LogManager.logWarning(LogConstants.CTX_CONNECTOR, DQPPlugin.Util.getString("ConnectorWorker.zero_size_non_last_batch", requestMsg.getConnectorID())); //$NON-NLS-1$
		}

		AtomicResultsMessage response = createResultsMessage(this.requestMsg, rows.toArray(new List[currentRowCount]), requestMsg.getCommand().getProjectedSymbols());
		
		// if we need to keep the execution alive, then we can not support
		// implicit close.
		response.setSupportsImplicitClose(!this.securityContext.keepExecutionAlive());
		response.setTransactional(this.securityContext.isTransactional());
		response.setWarnings(this.securityContext.getWarnings());

		if ( lastBatch ) {
		    response.setFinalRow(rowCount);
		} 
		this.resultsReceiver.receiveResults(response);
	}

	private void correctTypes(List row) throws ConnectorException {
		//TODO: add a proper source schema
		for (int i = convertToRuntimeType.size() - 1; i >= 0; i--) {
			int index = convertToRuntimeType.get(i);
			Object value = row.get(index);
			if (value != null) {
				Object result = DataTypeManager.convertToRuntimeType(value);
				if (DataTypeManager.isLOB(result.getClass())) {
					this.securityContext.keepExecutionAlive(true);
				}
				if (value == result && !DataTypeManager.DefaultDataClasses.OBJECT.equals(this.schema[index])) {
					convertToRuntimeType.remove(i);
				}
				row.set(index, result);
			}
		}
		//TODO: add a proper intermediate schema
		for (int i = convertToDesiredRuntimeType.size() - 1; i >= 0; i--) {
			int index = convertToDesiredRuntimeType.get(i);
			Object value = row.get(index);
			if (value != null) {
				Object result;
				try {
					result = DataTypeManager.transformValue(value, value.getClass(), this.schema[index]);
				} catch (TransformationException e) {
					throw new ConnectorException(e);
				}
				if (value == result) {
					convertToDesiredRuntimeType.remove(i);
				}
				row.set(index, result);
			}
		}
	}
    
    protected abstract boolean dataNotAvailable(long delay);
    
    private void processMoreRequest() throws ConnectorException {
    	Assertion.assertTrue(this.moreRequested, "More was not requested"); //$NON-NLS-1$
    	this.moreRequested = false;
    	LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.id, "Processing MORE request"}); //$NON-NLS-1$

        handleBatch();
    }
            
    public static AtomicResultsMessage createResultsMessage(AtomicRequestMessage message, List[] batch, List columnSymbols) {
        String[] dataTypes = new String[columnSymbols.size()];

        for(int i=0; i<columnSymbols.size(); i++) {
            SingleElementSymbol symbol = (SingleElementSymbol) columnSymbols.get(i);
            dataTypes[i] = DataTypeManager.getDataTypeName(symbol.getType());
        }
        
        return new AtomicResultsMessage(message, batch, dataTypes);
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