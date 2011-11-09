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

package org.teiid.dqp.internal.datamgr;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.resource.ResourceException;

import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.client.ResizingArrayList;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.util.Assertion;
import org.teiid.dqp.internal.process.RequestWorkItem;
import org.teiid.dqp.message.AtomicRequestID;
import org.teiid.dqp.message.AtomicRequestMessage;
import org.teiid.dqp.message.AtomicResultsMessage;
import org.teiid.language.Call;
import org.teiid.language.QueryExpression;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.CommandLogMessage.Event;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.query.QueryPlugin;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.resource.spi.WrappedConnection;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.Execution;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;


public class ConnectorWorkItem implements ConnectorWork {
	
	/* Permanent state members */
	private AtomicRequestID id;
    private ConnectorManager manager;
    private AtomicRequestMessage requestMsg;
    private ExecutionFactory connector;
    private QueryMetadataInterface queryMetadata;
    
    /* Created on new request */
    private Object connection;
    private Object connectionFactory;
    private ExecutionContextImpl securityContext;
    private volatile ResultSetExecution execution;
    private ProcedureBatchHandler procedureBatchHandler;
    private org.teiid.language.Command translatedCommand;
    private int expectedColumns;
        
    /* End state information */    
    private boolean lastBatch;
    private int rowCount;
    private boolean error;
    
    private AtomicBoolean isCancelled = new AtomicBoolean();
    
    ConnectorWorkItem(AtomicRequestMessage message, ConnectorManager manager) {
        this.id = message.getAtomicRequestID();
        this.requestMsg = message;
        this.manager = manager;
        AtomicRequestID requestID = this.requestMsg.getAtomicRequestID();
        this.securityContext = new ExecutionContextImpl(requestMsg.getWorkContext().getVdbName(),
                requestMsg.getWorkContext().getVdbVersion(),                
                requestMsg.getExecutionPayload(),                                                                       
                requestMsg.getWorkContext().getSessionId(),                                                                      
                requestMsg.getConnectorName(),
                requestMsg.getRequestID().toString(),
                Integer.toString(requestID.getNodeID()),
                Integer.toString(requestID.getExecutionId())
                );
        this.securityContext.setGeneralHint(message.getGeneralHint());
        this.securityContext.setHint(message.getHint());
        this.securityContext.setUser(requestMsg.getWorkContext().getSubject());
        this.securityContext.setBatchSize(this.requestMsg.getFetchSize());
        this.securityContext.setSession(requestMsg.getWorkContext().getSession());
        
        this.connector = manager.getExecutionFactory();
    	VDBMetaData vdb = requestMsg.getWorkContext().getVDB();
    	this.queryMetadata = vdb.getAttachment(QueryMetadataInterface.class);
        this.queryMetadata = new TempMetadataAdapter(this.queryMetadata, new TempMetadataStore());
		this.securityContext.setTransactional(requestMsg.isTransactional());
    }
    
    @Override
    public void setRequestWorkItem(RequestWorkItem item) {
    	this.securityContext.setRequestWorkItem(item);
    }
    
    public AtomicRequestID getId() {
		return id;
	}
    
    public void cancel() {
    	try {
            LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.id, "Processing CANCEL request"}); //$NON-NLS-1$
            if (this.isCancelled.compareAndSet(false, true)) {
                this.manager.logSRCCommand(this.requestMsg, this.securityContext, Event.CANCEL, -1);
    	        if(execution != null) {
    	            execution.cancel();
    	        }            
    	        LogManager.logDetail(LogConstants.CTX_CONNECTOR, QueryPlugin.Util.getString("DQPCore.The_atomic_request_has_been_cancelled", this.id)); //$NON-NLS-1$
        	}
        } catch (TranslatorException e) {
            LogManager.logWarning(LogConstants.CTX_CONNECTOR, e, QueryPlugin.Util.getString("Cancel_request_failed", this.id)); //$NON-NLS-1$
        }
    }
    
    public AtomicResultsMessage more() throws TranslatorException {
    	LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.id, "Processing MORE request"}); //$NON-NLS-1$
    	try {
    		return handleBatch();
    	} catch (Throwable t) {
    		throw handleError(t);
    	}
    }
    
    public void close() {
    	this.securityContext.setRequestWorkItem(null);
    	LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.id, "Processing Close :", this.requestMsg.getCommand()}); //$NON-NLS-1$
    	if (!error) {
            manager.logSRCCommand(this.requestMsg, this.securityContext, Event.END, this.rowCount);
        }
        try {
	        if (execution != null) {
	            execution.close();
	            LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.id, "Closed execution"}); //$NON-NLS-1$                    
	        }	        
        } catch (Throwable e) {
            LogManager.logError(LogConstants.CTX_CONNECTOR, e, e.getMessage());
        } finally {
        	try {
        		this.connector.closeConnection(connection, connectionFactory);
        	} catch (Throwable e) {
        		LogManager.logError(LogConstants.CTX_CONNECTOR, e, e.getMessage());
        	} finally {
        		manager.removeState(this.id);
        	}
		    LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.id, "Closed connection"}); //$NON-NLS-1$
        } 
    }
    
    private TranslatorException handleError(Throwable t) {
    	if (t instanceof DataNotAvailableException) {
    		throw (DataNotAvailableException)t;
    	}
    	error = true;
    	if (t instanceof RuntimeException && t.getCause() != null) {
    		t = t.getCause();
    	}
        manager.logSRCCommand(this.requestMsg, this.securityContext, Event.ERROR, null);
        
        String msg = QueryPlugin.Util.getString("ConnectorWorker.process_failed", this.id); //$NON-NLS-1$
        if (isCancelled.get()) {            
            LogManager.logDetail(LogConstants.CTX_CONNECTOR, msg);
        } else if (t instanceof TranslatorException || t instanceof TeiidProcessingException) {
        	LogManager.logWarning(LogConstants.CTX_CONNECTOR, t, msg);
        } else {
            LogManager.logError(LogConstants.CTX_CONNECTOR, t, msg);
        } 
		if (t instanceof TranslatorException) {
			return (TranslatorException)t;
		}
		if (t instanceof RuntimeException) {
			throw (RuntimeException)t;
		}
		return new TranslatorException(t);
    }
    
	public AtomicResultsMessage execute() throws TranslatorException, BlockedException {
        if(isCancelled()) {
    		throw new TranslatorException("Request canceled"); //$NON-NLS-1$
    	}
        
    	LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.requestMsg.getAtomicRequestID(), "Processing NEW request:", this.requestMsg.getCommand()}); //$NON-NLS-1$                                     
    	try {
	    	this.connectionFactory = this.manager.getConnectionFactory();
	        this.connection = this.connector.getConnection(this.connectionFactory, securityContext);

	        Object unwrapped = null;
			if (connection instanceof WrappedConnection) {
				try {
					unwrapped = ((WrappedConnection)connection).unwrap();
				} catch (ResourceException e) {
					throw new TranslatorException(QueryPlugin.Util.getString("failed_to_unwrap_connection")); //$NON-NLS-1$
				}	
			}
			
	        // Translate the command
	        Command command = this.requestMsg.getCommand();
	        this.expectedColumns = command.getProjectedSymbols().size();
	        LanguageBridgeFactory factory = new LanguageBridgeFactory(queryMetadata);
	        this.translatedCommand = factory.translate(command);
	
	        RuntimeMetadata rmd = new RuntimeMetadataImpl(queryMetadata);
	        
	        // Create the execution based on mode
	        final Execution exec = connector.createExecution(this.translatedCommand, this.securityContext, rmd, (unwrapped == null) ? this.connection:unwrapped);
	        if (this.translatedCommand instanceof Call) {
	        	this.execution = Assertion.isInstanceOf(exec, ProcedureExecution.class, "Call Executions are expected to be ProcedureExecutions"); //$NON-NLS-1$
	        	StoredProcedure proc = (StoredProcedure)command;
	        	if (proc.returnParameters()) {
	        		this.procedureBatchHandler = new ProcedureBatchHandler((Call)this.translatedCommand, (ProcedureExecution)exec);
	        	}
	        } else if (this.translatedCommand instanceof QueryExpression){
	        	this.execution = Assertion.isInstanceOf(exec, ResultSetExecution.class, "QueryExpression Executions are expected to be ResultSetExecutions"); //$NON-NLS-1$
	        } else {
	        	Assertion.isInstanceOf(exec, UpdateExecution.class, "Update Executions are expected to be UpdateExecutions"); //$NON-NLS-1$
	        	this.execution = new ResultSetExecution() {
	        		private int[] results;
	        		private int index;
	        		
	        		@Override
	        		public void cancel() throws TranslatorException {
	        			exec.cancel();
	        		}
	        		@Override
	        		public void close() {
	        			exec.close();
	        		}
	        		@Override
	        		public void execute() throws TranslatorException {
	        			exec.execute();
	        		}
	        		@Override
	        		public List<?> next() throws TranslatorException,
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
	        manager.logSRCCommand(this.requestMsg, this.securityContext, Event.NEW, null); 
	        
	        // Execute query
	    	this.execution.execute();
	        LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.id, "Executed command"}); //$NON-NLS-1$
	
	        return handleBatch();
    	} catch (Throwable t) {
    		throw handleError(t);
    	}
	}
    
    protected AtomicResultsMessage handleBatch() throws TranslatorException {
    	Assertion.assertTrue(!this.lastBatch);
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.id, "Getting results from connector"}); //$NON-NLS-1$
        int batchSize = 0;
        List<List<?>> rows = new ResizingArrayList<List<?>>(batchSize/4);
        
        try {
	        while (batchSize < this.requestMsg.getFetchSize()) {
	        	
        		List<?> row = this.execution.next();
            	if (row == null) {
            		this.lastBatch = true;
            		break;
            	}
            	if (row.size() != this.expectedColumns) {
            		throw new AssertionError("Inproper results returned.  Expected " + this.expectedColumns + " columns, but was " + row.size()); //$NON-NLS-1$ //$NON-NLS-2$
        		}
            	this.rowCount += 1;
            	batchSize++;
            	if (this.procedureBatchHandler != null) {
            		row = this.procedureBatchHandler.padRow(row);
            	}
            	
            	rows.add(row);
	            // Check for max result rows exceeded
	            if(this.requestMsg.getMaxResultRows() > -1 && this.rowCount >= this.requestMsg.getMaxResultRows()){
	                if (this.rowCount == this.requestMsg.getMaxResultRows() && !this.requestMsg.isExceptionOnMaxRows()) {
		                LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.id, "Exceeded max, returning", this.requestMsg.getMaxResultRows()}); //$NON-NLS-1$
		        		this.lastBatch = true;
		        		break;
	            	} else if (this.rowCount > this.requestMsg.getMaxResultRows() && this.requestMsg.isExceptionOnMaxRows()) {
	                    String msg = QueryPlugin.Util.getString("ConnectorWorker.MaxResultRowsExceed", this.requestMsg.getMaxResultRows()); //$NON-NLS-1$
	                    throw new TranslatorException(msg);
	                }
	            }
	        }
    	} catch (DataNotAvailableException e) {
    		if (rows.size() == 0 && this.rowCount != 0) {
    			throw e;
    		}
    	}
                
        if (lastBatch) {
        	if (this.procedureBatchHandler != null) {
        		List<?> row = this.procedureBatchHandler.getParameterRow();
        		if (row != null) {
        			rows.add(row);
        			this.rowCount++;
        		}
        	}
            LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.id, "Obtained last batch, total row count:", rowCount}); //$NON-NLS-1$\
        }  else {
        	LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.id, "Obtained results from connector, current row count:", rowCount}); //$NON-NLS-1$
        }
        
    	int currentRowCount = rows.size();
		if ( !lastBatch && currentRowCount == 0 ) {
		    // Defect 13366 - Should send all batches, even if they're zero size.
		    // Log warning if received a zero-size non-last batch from the connector.
		    LogManager.logWarning(LogConstants.CTX_CONNECTOR, QueryPlugin.Util.getString("ConnectorWorker.zero_size_non_last_batch", requestMsg.getConnectorName())); //$NON-NLS-1$
		}

		AtomicResultsMessage response = createResultsMessage(rows.toArray(new List[currentRowCount]));
		
		// if we need to keep the execution alive, then we can not support implicit close.
		response.setSupportsImplicitClose(!this.securityContext.keepExecutionAlive());
		response.setTransactional(this.securityContext.isTransactional());
		response.setWarnings(this.securityContext.getWarnings());
		response.setSupportsCloseWithLobs(this.connector.areLobsUsableAfterClose());

		if ( lastBatch ) {
		    response.setFinalRow(rowCount);
		} 
		return response;
	}

    public static AtomicResultsMessage createResultsMessage(List[] batch) {
        return new AtomicResultsMessage(batch);
    }    
            
    boolean isCancelled() {
    	return this.isCancelled.get();
    }

	@Override
	public String toString() {
		return this.id.toString();
	}
	
	@Override
	public boolean isDataAvailable() {
		return this.securityContext.isDataAvailable();
	}

}