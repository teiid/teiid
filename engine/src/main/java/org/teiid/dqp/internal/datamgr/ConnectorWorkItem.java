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

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.activation.DataSource;
import javax.resource.ResourceException;
import javax.xml.stream.XMLStreamException;
import javax.xml.transform.Source;
import javax.xml.transform.stax.StAXSource;
import javax.xml.transform.stream.StreamSource;

import org.teiid.GeometryInputSource;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.client.ResizingArrayList;
import org.teiid.client.util.ExceptionUtil;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.FileStore;
import org.teiid.common.buffer.FileStoreInputStreamFactory;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.BlobImpl;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.types.SQLXMLImpl;
import org.teiid.core.types.StandardXMLTranslator;
import org.teiid.core.types.Streamable;
import org.teiid.core.types.TransformationException;
import org.teiid.core.types.XMLType;
import org.teiid.core.util.Assertion;
import org.teiid.core.util.ReaderInputStream;
import org.teiid.dqp.internal.process.SaveOnReadInputStream;
import org.teiid.dqp.message.AtomicRequestID;
import org.teiid.dqp.message.AtomicRequestMessage;
import org.teiid.dqp.message.AtomicResultsMessage;
import org.teiid.language.BatchedCommand;
import org.teiid.language.BatchedUpdates;
import org.teiid.language.Call;
import org.teiid.logging.CommandLogMessage.Event;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.GeometryUtils;
import org.teiid.query.function.source.XMLSystemFunctions;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.optimizer.capabilities.SourceCapabilities;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.processor.CollectionTupleSource;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.QueryCommand;
import org.teiid.query.sql.lang.SourceHint;
import org.teiid.query.sql.lang.SourceHint.SpecificHint;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.util.CommandContext;
import org.teiid.resource.spi.WrappedConnection;
import org.teiid.translator.*;
import org.teiid.translator.ExecutionFactory.TransactionSupport;
import org.teiid.util.XMLInputStream;

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
    private volatile boolean lastBatch;
    private long rowCount;
    
    private AtomicBoolean isCancelled = new AtomicBoolean();
	private org.teiid.language.Command translatedCommand;
	
	private DataNotAvailableException dnae;
	
    private FileStore lobStore;
    private byte[] lobBuffer;
    private boolean[] convertToRuntimeType;
    private boolean[] convertToDesiredRuntimeType;
    private boolean[] isLob;
    private Class<?>[] schema;
	private boolean explicitClose;
	
	private boolean copyLobs;
	private boolean areLobsUsableAfterClose;
	
	private TeiidException conversionError;
	
	private ThreadCpuTimer timer = new ThreadCpuTimer();
	
	private boolean unmodifiableList;
	
	ConnectorWorkItem(AtomicRequestMessage message, ConnectorManager manager) throws TeiidComponentException {
        this.id = message.getAtomicRequestID();
        this.requestMsg = message;
        this.manager = manager;
        AtomicRequestID requestID = this.requestMsg.getAtomicRequestID();
        this.securityContext = new ExecutionContextImpl(message.getCommandContext(),                                                                      
                requestMsg.getConnectorName(),
                Integer.toString(requestID.getNodeID()),
                Integer.toString(requestID.getExecutionId()),
                this);
        SourceHint hint = message.getCommand().getSourceHint();
        if (hint != null) {
	        this.securityContext.setGeneralHints(hint.getGeneralHints());
	        SpecificHint specificHint = hint.getSpecificHint(message.getConnectorName());
	        if (specificHint != null) {
	        	this.securityContext.setHints(specificHint.getHints());
	        }
        }
        this.securityContext.setBatchSize(this.requestMsg.getFetchSize());
        this.securityContext.setSession(requestMsg.getWorkContext().getSession());
        
        this.connector = manager.getExecutionFactory();
    	VDBMetaData vdb = requestMsg.getWorkContext().getVDB();
    	QueryMetadataInterface qmi = vdb.getAttachment(QueryMetadataInterface.class);
        qmi = new TempMetadataAdapter(qmi, new TempMetadataStore());
        this.queryMetadata = new RuntimeMetadataImpl(qmi);
        this.securityContext.setRuntimeMetadata(this.queryMetadata);
		this.securityContext.setTransactional(requestMsg.isTransactional());
        LanguageBridgeFactory factory = new LanguageBridgeFactory(this.queryMetadata);
        try {
			SourceCapabilities capabilities = manager.getCapabilities();
			//set other properties once the capabilities have been obtained
			factory.setSupportsConcat2(capabilities.supportsFunction(SourceSystemFunctions.CONCAT2));
			//read directly from the connector
	        factory.setConvertIn(!this.connector.supportsInCriteria());
	        factory.setMaxInPredicateSize((Integer) capabilities.getSourceProperty(Capability.MAX_IN_CRITERIA_SIZE));
	        factory.setCommandContext(requestMsg.getCommandContext());
	        factory.setExcludeWithName((String) capabilities.getSourceProperty(Capability.EXCLUDE_COMMON_TABLE_EXPRESSION_NAME));
		} catch (TranslatorException e) {
			throw new TeiidComponentException(e);
		}
        translatedCommand = factory.translate(message.getCommand());
        List<Expression> symbols = this.requestMsg.getCommand().getProjectedSymbols();
		this.schema = new Class[symbols.size()];
        this.convertToDesiredRuntimeType = new boolean[symbols.size()];
		this.convertToRuntimeType = new boolean[symbols.size()];
		this.isLob = new boolean[symbols.size()];
		for (int i = 0; i < symbols.size(); i++) {
			Expression symbol = symbols.get(i);
			this.schema[i] = symbol.getType();
			this.convertToDesiredRuntimeType[i] = true;
			this.convertToRuntimeType[i] = true;
			this.isLob[i] = DataTypeManager.isLOB(this.schema[i]);
		}
		this.areLobsUsableAfterClose = this.connector.areLobsUsableAfterClose();
		this.copyLobs = this.connector.isCopyLobs();
    }
    
    @Override
    public AtomicRequestID getId() {
		return id;
	}
    
    @Override
    public void cancel(boolean abnormal) {
    	if (lastBatch) {
    		return;
    	}
    	try {
			if (this.isCancelled.compareAndSet(false, true)) {
				LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.id, "Processing CANCEL request"}); //$NON-NLS-1$
            	Execution ex = this.execution;
            	if(ex != null) {
            		if (abnormal) {
                		this.manager.logSRCCommand(this.requestMsg, this.securityContext, Event.CANCEL, -1l, null);
            		}
    	            ex.cancel();
    	            LogManager.logDetail(LogConstants.CTX_CONNECTOR, QueryPlugin.Util.getString("DQPCore.The_atomic_request_has_been_cancelled", this.id)); //$NON-NLS-1$
            	}
        	}
        } catch (TranslatorException e) {
            LogManager.logWarning(LogConstants.CTX_CONNECTOR, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30024, this.id));
        }
    }
    
    public synchronized AtomicResultsMessage more() throws TranslatorException {
    	if (this.execution == null) {
    		return null; //already closed
    	}
    	if (this.dnae != null) {
    		//clear the exception if it has been set
    		DataNotAvailableException e = this.dnae;
    		this.dnae = null;
    		throw e;
    	}
    	if (this.conversionError != null) {
    		throw handleError(this.conversionError);
    	}
    	LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.id, "Processing MORE request"}); //$NON-NLS-1$
    	try {
        	timer.start();
    		return handleBatch();
    	} catch (Throwable t) {
    		throw handleError(t);
    	} finally {
    		timer.stop();
    	}
    }
    
    public synchronized void close() {
    	lobBuffer = null;
    	if (lobStore != null) {
    		lobStore.remove();
    		lobStore = null;
    	}
    	if (!manager.removeState(this.id)) {
    		return; //already closed
    	}
    	LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.id, "Processing Close :", this.requestMsg.getCommand()}); //$NON-NLS-1$
        try {
        	timer.start();
	        if (execution != null) {
	            execution.close();
	            LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.id, "Closed execution"}); //$NON-NLS-1$
	            if (execution instanceof ReusableExecution<?>) {
		        	this.requestMsg.getCommandContext().putReusableExecution(this.manager.getId(), (ReusableExecution<?>) execution);
		        }
	            execution = null;
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
        	Long time = timer.stop();
            manager.logSRCCommand(this.requestMsg, this.securityContext, Event.END, this.rowCount, time);
        } 
    }
    
    private TranslatorException handleError(Throwable t) {
    	if (t instanceof DataNotAvailableException) {
    		throw (DataNotAvailableException)t;
    	}
    	if (t instanceof RuntimeException && t.getCause() != null) {
    		t = t.getCause();
    	}
        
        String msg = QueryPlugin.Util.getString("ConnectorWorker.process_failed", this.id); //$NON-NLS-1$
        if (isCancelled.get()) {            
            LogManager.logDetail(LogConstants.CTX_CONNECTOR, msg);
        } else {
            manager.logSRCCommand(this.requestMsg, this.securityContext, Event.ERROR, null, null);

        	Throwable toLog = t;
        	if (this.requestMsg.getCommandContext().getOptions().isSanitizeMessages() && !LogManager.isMessageToBeRecorded(LogConstants.CTX_CONNECTOR, MessageLevel.DETAIL)) {
	    		toLog = ExceptionUtil.sanitize(toLog, true);
	    	}
        	if (toLog instanceof TranslatorException || toLog instanceof TeiidProcessingException) {
	        	LogManager.logWarning(LogConstants.CTX_CONNECTOR, toLog, msg);
	        } else {
	            LogManager.logError(LogConstants.CTX_CONNECTOR, toLog, msg);
	        } 
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
        timer.start();
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
						 throw new TranslatorException(QueryPlugin.Event.TEIID30477, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30477, this.manager.getConnectionName()));
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
		        manager.logSRCCommand(this.requestMsg, this.securityContext, Event.NEW, null, null); 
	    	}
	        // Execute query
	    	this.execution.execute();
	        LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {this.id, "Executed command"}); //$NON-NLS-1$
    	} catch (Throwable t) {
    		throw handleError(t);
    	} finally {
    		timer.stop();
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
			final boolean singleUpdateCount = connector.returnsSingleUpdateCount() 
					&& (translatedCommand instanceof BatchedUpdates || (translatedCommand instanceof BatchedCommand && ((BatchedCommand)translatedCommand).getParameterValues() != null));
			
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
					if (singleUpdateCount) {
						if (index++ < results[0]) {
							return CollectionTupleSource.UPDATE_ROW;
						}
						return null;
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
            	try {
	            	try {
	            		if (unmodifiableList) {
	                		row = new ArrayList<Object>(row);
	            		}
						row = correctTypes(row);
	            	} catch (UnsupportedOperationException | ArrayStoreException e) {
	            		//it's generally expected that the returned list from 
	            		//the translator should be modifiable, but we should be lax
	            		if (unmodifiableList) {
	            			throw e;
	            		}
	            		unmodifiableList = true;
	            		row = new ArrayList<Object>(row);
	            		row = correctTypes(row);
	            	}
				} catch (TeiidException e) {
					conversionError = e;
					break;
				}
            	if (this.procedureBatchHandler != null) {
            		row = this.procedureBatchHandler.padRow(row);
            	}
            	this.rowCount += 1;
            	batchSize++;
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
        			try {
						row = correctTypes(row);
	        			rows.add(row);
	        			this.rowCount += 1;
					} catch (TeiidException e) {
						lastBatch = false;
						conversionError = e;
					}
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
		response.setSupportsImplicitClose(!this.securityContext.keepExecutionAlive() && !explicitClose);
		response.setWarnings(this.securityContext.getWarnings());
		if (this.securityContext.getCacheDirective() != null) {
			response.setScope(this.securityContext.getCacheDirective().getScope());
		}
		if (this.securityContext.getScope() != null && 
				(response.getScope() == null || response.getScope().compareTo(this.securityContext.getScope()) > 0)) {
			response.setScope(this.securityContext.getScope());
		}

		if ( lastBatch ) {
		    response.setFinalRow(rowCount);
		} 
		return response;
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
	public CacheDirective getCacheDirective() throws TranslatorException {
		CacheDirective cd = connector.getCacheDirective(this.translatedCommand, this.securityContext, this.queryMetadata);
		this.securityContext.setCacheDirective(cd);
		return cd;
	}

	@Override
	public boolean isForkable() {
		return this.connector.isForkable() 
				&& (!this.requestMsg.isTransactional() || this.connector.getTransactionSupport() != TransactionSupport.XA);
	}
	
	@Override
	public boolean isThreadBound() {
		return this.connector.isThreadBound();
	}
	
	private List<?> correctTypes(List row) throws TeiidException {
		//TODO: add a proper intermediate schema
		for (int i = 0; i < row.size(); i++) {
			try {
				Object value = row.get(i);
				if (value == null) {
					continue;
				}
				if (convertToRuntimeType[i]) {
					Object result = convertToRuntimeType(requestMsg.getBufferManager(), value, this.schema[i], this.requestMsg.getCommandContext());
					if (value == result && !DataTypeManager.DefaultDataClasses.OBJECT.equals(this.schema[i])) {
						convertToRuntimeType[i] = false;
					} else {
						if (!explicitClose && isLob[i] && !copyLobs && !areLobsUsableAfterClose && DataTypeManager.isLOB(result.getClass()) 
								&& DataTypeManager.isLOB(DataTypeManager.convertToRuntimeType(value, false).getClass())) {
							explicitClose = true;
						}				
						row.set(i, result);
						value = result;
					}
				}
				if (convertToDesiredRuntimeType[i]) {
					if (value != null) {
						Object result = DataTypeManager.transformValue(value, value.getClass(), this.schema[i]);
						if (isLob[i] && copyLobs) {
							if (lobStore == null) {
								lobStore = requestMsg.getBufferManager().createFileStore("lobs"); //$NON-NLS-1$
								lobBuffer = new byte[1 << 14];
							}
							requestMsg.getBufferManager().persistLob((Streamable<?>) result, lobStore, lobBuffer);
						} else if (value == result) {
							convertToDesiredRuntimeType[i] = false;
							continue;
						}
						row.set(i, result);
					}
				} else if (DataTypeManager.isValueCacheEnabled()) {
					row.set(i, DataTypeManager.getCanonicalValue(value));
				}
			} catch (TeiidComponentException e) {
				throw new TeiidComponentException(QueryPlugin.Event.TEIID31176, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31176, this.requestMsg.getCommand().getProjectedSymbols().get(i), DataTypeManager.getDataTypeName(this.schema[i])));
			} catch (TransformationException e) {
				throw new TeiidException(QueryPlugin.Event.TEIID31176, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31176, this.requestMsg.getCommand().getProjectedSymbols().get(i), DataTypeManager.getDataTypeName(this.schema[i])));
			}
		}
		return row;
	}
	
	static Object convertToRuntimeType(BufferManager bm, Object value, Class<?> desiredType, CommandContext context) throws TransformationException {
		if (desiredType != DataTypeManager.DefaultDataClasses.XML || !(value instanceof Source)) {
			if (value instanceof InputStreamFactory) {
				return new BlobType(new BlobImpl((InputStreamFactory)value));
			}
			if (value instanceof DataSource) {
				FileStore fs = bm.createFileStore("bytes"); //$NON-NLS-1$
				//TODO: guess at the encoding from the content type
				FileStoreInputStreamFactory fsisf = new FileStoreInputStreamFactory(fs, Streamable.ENCODING);
	
				try {
					SaveOnReadInputStream is = new SaveOnReadInputStream(((DataSource)value).getInputStream(), fsisf);
					if (context != null) {
						context.addCreatedLob(fsisf);
					}
					return new BlobType(new BlobImpl(is.getInputStreamFactory()));
				} catch (IOException e) {
					throw new TransformationException(QueryPlugin.Event.TEIID30500, e, e.getMessage());
				}
			}
			if (value instanceof GeometryInputSource) {
				GeometryInputSource gis = (GeometryInputSource)value;
				try {
					InputStream is = gis.getEwkb();
					if (is != null) {
						return GeometryUtils.geometryFromEwkb(is, gis.getSrid());
					}
				} catch (Exception e) {
					throw new TransformationException(e);
				}
				try {
					Reader r = gis.getGml();
					if (r != null) {
						return GeometryUtils.geometryFromGml(r, gis.getSrid());
					}
				} catch (Exception e) {
					throw new TransformationException(e);
				}
			}
		}
		if (value instanceof Source) {
			if (!(value instanceof InputStreamFactory)) {
				if (value instanceof StreamSource) {
					StreamSource ss = (StreamSource)value;
					InputStream is = ss.getInputStream();
					Reader r = ss.getReader();
					if (is == null && r != null) {
						is = new ReaderInputStream(r, Streamable.CHARSET);
					}
					final FileStore fs = bm.createFileStore("xml"); //$NON-NLS-1$
					final FileStoreInputStreamFactory fsisf = new FileStoreInputStreamFactory(fs, Streamable.ENCODING);

					value = new SaveOnReadInputStream(is, fsisf).getInputStreamFactory();
					if (context != null) {
						context.addCreatedLob(fsisf);
					}
				} else if (value instanceof StAXSource) {
					//TODO: do this lazily.  if the first access to get the STaXSource, then 
					//it's more efficient to let the processing happen against STaX
					StAXSource ss = (StAXSource)value;
					try {
						final FileStore fs = bm.createFileStore("xml"); //$NON-NLS-1$
						final FileStoreInputStreamFactory fsisf = new FileStoreInputStreamFactory(fs, Streamable.ENCODING);
						value = new SaveOnReadInputStream(new XMLInputStream(ss, XMLSystemFunctions.getOutputFactory(true)), fsisf).getInputStreamFactory();
						if (context != null) {
							context.addCreatedLob(fsisf);
						}
					} catch (XMLStreamException e) {
						throw new TransformationException(e);
					}
				} else {
					//maybe dom or some other source we want to get out of memory
					StandardXMLTranslator sxt = new StandardXMLTranslator((Source)value);
					SQLXMLImpl sqlxml;
					try {
						sqlxml = XMLSystemFunctions.saveToBufferManager(bm, sxt, context);
					} catch (TeiidComponentException e) {
						 throw new TransformationException(e);
					} catch (TeiidProcessingException e) {
						 throw new TransformationException(e);
					}
					return new XMLType(sqlxml);	
				}
			}
			return new XMLType(new SQLXMLImpl((InputStreamFactory)value));
		}
		return DataTypeManager.convertToRuntimeType(value, desiredType != DataTypeManager.DefaultDataClasses.OBJECT);
	}

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof ConnectorWork))
            return false;
        ConnectorWork other = (ConnectorWork) obj;
        if (id == null) {
            if (other.getId() != null)
                return false;
        } else if (!id.equals(other.getId()))
            return false;
        return true;
    }

	public void logCommand(Object... command) {
		this.manager.logSRCCommand(this.requestMsg, securityContext, Event.SOURCE, null, null, command);
	}

}