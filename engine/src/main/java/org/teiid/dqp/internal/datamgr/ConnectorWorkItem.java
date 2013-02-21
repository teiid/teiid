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
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.util.Assertion;
import org.teiid.dqp.internal.process.RequestWorkItem;
import org.teiid.dqp.message.AtomicRequestID;
import org.teiid.dqp.message.AtomicRequestMessage;
import org.teiid.dqp.message.AtomicResultsMessage;
import org.teiid.language.Call;
import org.teiid.logging.CommandLogMessage.Event;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.query.QueryPlugin;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.QueryCommand;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.resource.spi.WrappedConnection;
import org.teiid.translator.*;

public class ConnectorWorkItem implements ConnectorWork {
	
	/* Permanent state members */
	private AtomicRequestID id;
    private ConnectorManager manager;
    private AtomicRequestMessage requestMsg;
    private ExecutionFactory<Object, Object> connector;
    private RuntimeMetadataImpl queryMetadata;
    
    /* Created on new request */
    private Object connection;
    private Object connectionFactory;
    private ExecutionContextImpl securityContext;
    private volatile ResultSetExecution execution;
    private ProcedureBatchHandler procedureBatchHandler;
    private int expectedColumns;
        
    /* End state information */    
    private boolean lastBatch;
    private int rowCount;
    private boolean error;
    
    private AtomicBoolean isCancelled = new AtomicBoolean();
	private org.teiid.language.Command translatedCommand;
	
	private DataNotAvailableException dnae;
    
    ConnectorWorkItem(AtomicRequestMessage message, ConnectorManager manager) throws TeiidComponentException {
        this.id = message.getAtomicRequestID();
        this.requestMsg = message;
        this.manager = manager;
        AtomicRequestID requestID = this.requestMsg.getAtomicRequestID();
        this.securityContext = new ExecutionContextImpl(message.getCommandContext(),                                                                      
                requestMsg.getConnectorName(),
                Integer.toString(requestID.getNodeID()),
                Integer.toString(requestID.getExecutionId())
                );
        this.securityContext.setGeneralHint(message.getGeneralHint());
        this.securityContext.setHint(message.getHint());
        this.securityContext.setBatchSize(this.requestMsg.getFetchSize());
        this.securityContext.setSession(requestMsg.getWorkContext().getSession());
        
        this.connector = manager.getExecutionFactory();
    	VDBMetaData vdb = requestMsg.getWorkContext().getVDB();
    	QueryMetadataInterface qmi = vdb.getAttachment(QueryMetadataInterface.class);
        qmi = new TempMetadataAdapter(qmi, new TempMetadataStore());
        this.queryMetadata = new RuntimeMetadataImpl(qmi);
		this.securityContext.setTransactional(requestMsg.isTransactional());
        LanguageBridgeFactory factory = new LanguageBridgeFactory(this.queryMetadata);
        factory.setConvertIn(!this.connector.supportsInCriteria());
        factory.setSupportsConcat2(manager.getCapabilities().supportsFunction(SourceSystemFunctions.CONCAT2));
        translatedCommand = factory.translate(message.getCommand());
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
            LogManager.logWarning(LogConstants.CTX_CONNECTOR, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30024, this.id));
        }
    }
    
    public AtomicResultsMessage more() throws TranslatorException {
    	if (this.dnae != null) {
    		//clear the exception if it has been set
    		DataNotAvailableException e = this.dnae;
    		this.dnae = null;
    		throw e;
    	}
    	LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.id, "Processing MORE request"}); //$NON-NLS-1$
    	try {
    		return handleBatch();
    	} catch (Throwable t) {
    		throw handleError(t);
    	}
    }
    
    public synchronized void close() {
    	this.securityContext.setRequestWorkItem(null);
    	if (!manager.removeState(this.id)) {
    		return; //already closed
    	}
    	LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.id, "Processing Close :", this.requestMsg.getCommand()}); //$NON-NLS-1$
    	if (!error) {
            manager.logSRCCommand(this.requestMsg, this.securityContext, Event.END, this.rowCount);
        }
        try {
	        if (execution != null) {
	            execution.close();
	            LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.id, "Closed execution"}); //$NON-NLS-1$
	            if (execution instanceof ReusableExecution<?>) {
		        	this.requestMsg.getCommandContext().putReusableExecution(this.manager.getId(), (ReusableExecution<?>) execution);
		        }
	        }	        
        } catch (Throwable e) {
            LogManager.logError(LogConstants.CTX_CONNECTOR, e, e.getMessage());
        } finally {
        	if (this.connection != null) {
	        	try {
	        		this.connector.closeConnection(connection, connectionFactory);
	        	} catch (Throwable e) {
	        		LogManager.logError(LogConstants.CTX_CONNECTOR, e, e.getMessage());
	        	}
			    LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.id, "Closed connection"}); //$NON-NLS-1$
        	}
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
    
	public synchronized void execute() throws TranslatorException {
        if(isCancelled()) {
    		 throw new TranslatorException(QueryPlugin.Event.TEIID30476, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30476));
    	}
    	try {
	        if (this.execution == null) {
	        	if (this.connection == null) {
		        	LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.requestMsg.getAtomicRequestID(), "Processing NEW request:", this.requestMsg.getCommand()}); //$NON-NLS-1$                                     
		    		try {
		    			this.connectionFactory = this.manager.getConnectionFactory();
		    		} catch (TranslatorException e) {
		    			if (this.connector.isSourceRequired()) {
		    				throw e;
		    			}
		    		}
			    	if (this.connectionFactory != null) {
			    		this.connection = this.connector.getConnection(this.connectionFactory, securityContext);
			    	} 
			    	if (this.connection == null && this.connector.isSourceRequired()) {
			    		throw new TranslatorException(QueryPlugin.Event.TEIID31108, QueryPlugin.Util.getString("datasource_not_found", this.manager.getConnectionName())); //$NON-NLS-1$);
			    	}
	        	}
	
		        Object unwrapped = null;
				if (connection instanceof WrappedConnection) {
					try {
						unwrapped = ((WrappedConnection)connection).unwrap();
					} catch (ResourceException e) {
						 throw new TranslatorException(QueryPlugin.Event.TEIID30477, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30477));
					}	
				}
	
		        // Translate the command
		        Command command = this.requestMsg.getCommand();
		        this.expectedColumns = command.getProjectedSymbols().size();
		        if (command instanceof StoredProcedure) {
		        	this.expectedColumns = ((StoredProcedure)command).getResultSetColumns().size();
		        }
	
				Execution exec = this.requestMsg.getCommandContext().getReusableExecution(this.manager.getId());
				if (exec != null) {
					((ReusableExecution)exec).reset(translatedCommand, this.securityContext, connection);
				} else {
			        exec = connector.createExecution(translatedCommand, this.securityContext, queryMetadata, (unwrapped == null) ? this.connection:unwrapped);
				}
		        setExecution(command, translatedCommand, exec);
				
		        LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.requestMsg.getAtomicRequestID(), "Obtained execution"}); //$NON-NLS-1$      
		        //Log the Source Command (Must be after obtaining the execution context)
		        manager.logSRCCommand(this.requestMsg, this.securityContext, Event.NEW, null); 
	    	}
	        // Execute query
	    	this.execution.execute();
	        LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.id, "Executed command"}); //$NON-NLS-1$
    	} catch (Throwable t) {
    		throw handleError(t);
    	}
	}

	private void setExecution(Command command,
			org.teiid.language.Command translatedCommand, final Execution exec) {
		if (translatedCommand instanceof Call) {
			this.execution = Assertion.isInstanceOf(exec, ProcedureExecution.class, "Call Executions are expected to be ProcedureExecutions"); //$NON-NLS-1$
			StoredProcedure proc = (StoredProcedure)command;
			if (proc.returnParameters()) 			{
				this.procedureBatchHandler = new ProcedureBatchHandler((Call)translatedCommand, (ProcedureExecution)exec);
			}
		} else if (command instanceof QueryCommand){
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
	                     throw new TranslatorException(QueryPlugin.Event.TEIID30478, msg);
	                }
	            }
	        }
    	} catch (DataNotAvailableException e) {
    		if (rows.size() == 0) {
    			throw e;
    		}
    		if (e.getWaitUntil() != null) {
    			//we have an await until that we need to enforce 
    			this.dnae = e;
    		}
    		//else we can just ignore the delay
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
		    LogManager.logWarning(LogConstants.CTX_CONNECTOR, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30004, requestMsg.getConnectorName()));
		}

		AtomicResultsMessage response = createResultsMessage(rows.toArray(new List[currentRowCount]));
		
		// if we need to keep the execution alive, then we can not support implicit close.
		response.setSupportsImplicitClose(!this.securityContext.keepExecutionAlive());
		response.setWarnings(this.securityContext.getWarnings());
		if (this.securityContext.getCacheDirective() != null) {
			response.setScope(this.securityContext.getCacheDirective().getScope());
		}

		if ( lastBatch ) {
		    response.setFinalRow(rowCount);
		} 
		return response;
	}
    
    @Override
    public boolean areLobsUsableAfterClose() {
    	return this.connector.areLobsUsableAfterClose();
    }

    public static AtomicResultsMessage createResultsMessage(List<?>[] batch) {
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
	
	@Override
	public boolean copyLobs() {
		return this.connector.isCopyLobs();
	}
	
	@Override
	public CacheDirective getCacheDirective() throws TranslatorException {
		CacheDirective cd = connector.getCacheDirective(this.translatedCommand, this.securityContext, this.queryMetadata);
		this.securityContext.setCacheDirective(cd);
		return cd;
	}

	@Override
	public boolean isForkable() {
		return this.connector.isForkable();
	}

}