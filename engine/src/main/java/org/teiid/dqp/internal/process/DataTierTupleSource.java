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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.activation.DataSource;
import javax.xml.transform.Source;

import org.teiid.client.SourceWarning;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.FileStore;
import org.teiid.common.buffer.FileStoreInputStreamFactory;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
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
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.dqp.internal.datamgr.ConnectorWork;
import org.teiid.dqp.internal.process.DQPCore.CompletionListener;
import org.teiid.dqp.internal.process.DQPCore.FutureWork;
import org.teiid.dqp.message.AtomicRequestMessage;
import org.teiid.dqp.message.AtomicResultsMessage;
import org.teiid.events.EventDistributor;
import org.teiid.metadata.Table;
import org.teiid.query.function.source.XMLSystemFunctions;
import org.teiid.query.processor.relational.RelationalNodeUtil;
import org.teiid.query.sql.lang.BatchedUpdateCommand;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.ProcedureContainer;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.SingleElementSymbol;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.TranslatorException;


/**
 * This tuple source impl can only be used once; once it is closed, it 
 * cannot be reopened and reused.
 * 
 * TODO: the handling of DataNotAvailable is awkward.
 * In the multi-threaded case we'd like to not even
 * notify the parent plan and just schedule the next poll. 
 */
public class DataTierTupleSource implements TupleSource, CompletionListener<AtomicResultsMessage> {
	
    // Construction state
    private final AtomicRequestMessage aqr;
    private final RequestWorkItem workItem;
    private final ConnectorWork cwi;
    private final DataTierManagerImpl dtm;
    
    private boolean[] convertToRuntimeType;
    private boolean[] convertToDesiredRuntimeType;
    private Class<?>[] schema;
    
    private int limit = -1;
    
    // Data state
    private int index;
    private int rowsProcessed;
    private AtomicResultsMessage arm;
    private AtomicBoolean closed = new AtomicBoolean();
    private volatile boolean canAsynchClose;
    private volatile boolean canceled;
    private volatile boolean cancelAsynch;
    private boolean executed;
    private volatile boolean done;
    private boolean explicitClose;
    
    private volatile FutureWork<AtomicResultsMessage> futureResult;
    private volatile boolean running;
    
    public DataTierTupleSource(AtomicRequestMessage aqr, RequestWorkItem workItem, ConnectorWork cwi, DataTierManagerImpl dtm, int limit) {
        this.aqr = aqr;
        this.workItem = workItem;
        this.cwi = cwi;
        this.dtm = dtm;
        this.limit = limit;
		List<SingleElementSymbol> symbols = this.aqr.getCommand().getProjectedSymbols();
		this.schema = new Class[symbols.size()];
        this.convertToDesiredRuntimeType = new boolean[symbols.size()];
		this.convertToRuntimeType = new boolean[symbols.size()];
		for (int i = 0; i < symbols.size(); i++) {
			SingleElementSymbol symbol = symbols.get(i);
			this.schema[i] = symbol.getType();
			this.convertToDesiredRuntimeType[i] = true;
			this.convertToRuntimeType[i] = true;
		}
        
    	Assertion.isNull(workItem.getConnectorRequest(aqr.getAtomicRequestID()));
        workItem.addConnectorRequest(aqr.getAtomicRequestID(), this);
        if (!aqr.isSerial()) {
        	addWork();
        }
    }

	private void addWork() {
		this.canAsynchClose = true;
		futureResult = workItem.addWork(new Callable<AtomicResultsMessage>() {
			@Override
			public AtomicResultsMessage call() throws Exception {
				return getResults();
			}
		}, this, 100);
	}

	private List<?> correctTypes(List<Object> row) throws TransformationException {
		//TODO: add a proper intermediate schema
		for (int i = 0; i < row.size(); i++) {
			Object value = row.get(i);
			if (value == null) {
				continue;
			}
			if (convertToRuntimeType[i]) {
				boolean lob = !arm.supportsCloseWithLobs() && DataTypeManager.isLOB(value.getClass());
				Object result = convertToRuntimeType(value, this.schema[i]);
				if (value == result && !DataTypeManager.DefaultDataClasses.OBJECT.equals(this.schema[i])) {
					convertToRuntimeType[i] = false;
				} else {
					if (lob && DataTypeManager.isLOB(result.getClass()) && DataTypeManager.isLOB(this.schema[i])) {
						explicitClose = true;
					}				
					row.set(i, result);
					value = result;
				}
			}
			if (convertToDesiredRuntimeType[i]) {
				if (value != null) {
					Object result = DataTypeManager.transformValue(value, value.getClass(), this.schema[i]);
					if (value == result) {
						convertToDesiredRuntimeType[i] = false;
						continue;
					}
					row.set(i, result);
				}
			} else {
				row.set(i, DataTypeManager.getCanonicalValue(value));
			}
		}
		return row;
	}

	private Object convertToRuntimeType(Object value, Class<?> desiredType) throws TransformationException {
		if (value instanceof DataSource && (!(value instanceof Source) || desiredType != DataTypeManager.DefaultDataClasses.XML)) {
			if (value instanceof InputStreamFactory) {
				return new BlobType(new BlobImpl((InputStreamFactory)value));
			}
			FileStore fs = dtm.getBufferManager().createFileStore("bytes"); //$NON-NLS-1$
			//TODO: guess at the encoding from the content type
			FileStoreInputStreamFactory fsisf = new FileStoreInputStreamFactory(fs, Streamable.ENCODING);
			
			try {
				ObjectConverterUtil.write(fsisf.getOuputStream(), ((DataSource)value).getInputStream(), -1);
			} catch (IOException e) {
				throw new TransformationException(e, e.getMessage());
			}
			return new BlobType(new BlobImpl(fsisf));
		}
		if (value instanceof Source) {
			if (value instanceof InputStreamFactory) {
				return new XMLType(new SQLXMLImpl((InputStreamFactory)value));
			}
			StandardXMLTranslator sxt = new StandardXMLTranslator((Source)value);
			SQLXMLImpl sqlxml;
			try {
				sqlxml = XMLSystemFunctions.saveToBufferManager(dtm.getBufferManager(), sxt);
			} catch (TeiidComponentException e) {
				throw new TeiidRuntimeException(e);
			} catch (TeiidProcessingException e) {
				throw new TeiidRuntimeException(e);
			}
			return new XMLType(sqlxml);
		}
		return DataTypeManager.convertToRuntimeType(value);
	}

    public List<?> nextTuple() throws TeiidComponentException, TeiidProcessingException {
    	while (true) {
    		if (arm == null) {
    			AtomicResultsMessage results = null;
    			try {
	    			if (futureResult != null || !aqr.isSerial()) {
	    				results = asynchGet();
	    			} else {
	    				results = getResults();
	    			}
	    			//check for update events
	    			if (index == 0 && this.dtm.detectChangeEvents()) {
	    				Command command = aqr.getCommand();
	    				int commandIndex = 0;
	    				if (RelationalNodeUtil.isUpdate(command)) {
	    					long ts = System.currentTimeMillis();
	    					checkForUpdates(results, command, dtm.getEventDistributor(), commandIndex, ts);
	    				} else if (command instanceof BatchedUpdateCommand) {
	    					long ts = System.currentTimeMillis();
	    					BatchedUpdateCommand bac = (BatchedUpdateCommand)command;
	    					for (Command uc : bac.getUpdateCommands()) {
	    						checkForUpdates(results, uc, dtm.getEventDistributor(), commandIndex++, ts);
	    					}
	    				}
	    			}
    			} catch (TranslatorException e) {
    				results = exceptionOccurred(e, true);
    			} catch (DataNotAvailableException e) {
    				if (e.getRetryDelay() >= 0) {
	    				workItem.scheduleWork(new Runnable() {
	    					@Override
	    					public void run() {
								workItem.moreWork();
	    					}
	    				}, 10, e.getRetryDelay());
    				} else if (this.cwi.isDataAvailable()) {
    					continue; 
    				}
    				throw BlockedException.block(aqr.getAtomicRequestID(), "Blocking on DataNotAvailableException"); //$NON-NLS-1$
    			} 
    			receiveResults(results);
    		}
	    	if (index < arm.getResults().length) {
	    		if (limit-- == 0) {
	    			this.done = true;
	    			arm = null;
	    			return null;
	    		}
	            return correctTypes(this.arm.getResults()[index++]);
	        }
	    	arm = null;
	    	if (isDone()) {
	    		return null;
	    	}
    	}
    }

	private void checkForUpdates(AtomicResultsMessage results, Command command,
			EventDistributor distributor, int commandIndex, long ts) {
		if (!RelationalNodeUtil.isUpdate(command) || !(command instanceof ProcedureContainer)) {
			return;
		}
		ProcedureContainer pc = (ProcedureContainer)aqr.getCommand();
		GroupSymbol gs = pc.getGroup();
		Integer zero = Integer.valueOf(0);
		if (results.getResults().length <= commandIndex || zero.equals(results.getResults()[commandIndex].get(0))) {
			return;
		}
		Object metadataId = gs.getMetadataID();
		if (metadataId == null) {
			return;
		}
		if (!(metadataId instanceof Table)) {
			return;
		} 
		Table t = (Table)metadataId;
		t.setLastDataModification(ts);
		if (distributor != null) {
			distributor.dataModification(this.workItem.getDqpWorkContext().getVdbName(), this.workItem.getDqpWorkContext().getVdbVersion(), t.getParent().getName(), t.getName());
		}
	}

	private AtomicResultsMessage asynchGet()
			throws BlockedException, TeiidProcessingException,
			TeiidComponentException, TranslatorException {
		if (futureResult == null) {
			addWork();
		}
		if (!futureResult.isDone()) {
			throw BlockedException.block(aqr.getAtomicRequestID(), "Blocking on source query"); //$NON-NLS-1$
		}
		FutureWork<AtomicResultsMessage> currentResults = futureResult;
		futureResult = null;
		AtomicResultsMessage results = null;
		try {
			results = currentResults.get();
			if (results.getFinalRow() < 0) {
				addWork();
			}
		} catch (InterruptedException e) {
			throw new TeiidRuntimeException(e);
		} catch (ExecutionException e) {
			if (e.getCause() instanceof TeiidProcessingException) {
				throw (TeiidProcessingException)e.getCause();
			}
			if (e.getCause() instanceof TeiidComponentException) {
				throw (TeiidComponentException)e.getCause();
			}
			if (e.getCause() instanceof TranslatorException) {
				throw (TranslatorException)e.getCause();
			}
			if (e.getCause() instanceof RuntimeException) {
				throw (RuntimeException)e.getCause();
			}
			//shouldn't happen
			throw new RuntimeException(e);
		}
		return results;
	}

	AtomicResultsMessage getResults()
			throws BlockedException, TeiidComponentException,
			TranslatorException {
		AtomicResultsMessage results = null;
		if (cancelAsynch) {
			return null;
		}
		running = true;
		if (!executed) {
			results = cwi.execute();
			executed = true;
		} else {
			results = cwi.more();
		}
		return results;
	}
    
    public boolean isQueued() {
    	FutureWork<AtomicResultsMessage> future = futureResult;
    	return !running && future != null && !future.isDone();
    }

	public boolean isDone() {
		return done;
	}
    
    public boolean isRunning() {
		return running;
	}
    
    public void fullyCloseSource() {
		cancelAsynch = true;
    	if (closed.compareAndSet(false, true)) {
	    	workItem.closeAtomicRequest(this.aqr.getAtomicRequestID());
	    	if (aqr.isSerial()) {
	    		this.cwi.close();
	    	} else if (!canAsynchClose) {
	    		workItem.addHighPriorityWork(new Callable<Void>() {
    				@Override
    				public Void call() throws Exception {
    					cwi.close();
    					return null;
    				}
    			});
	    	}
    	}
    }
    
    public boolean isCanceled() {
		return canceled;
	}
    
    public void cancelRequest() {
    	this.canceled = true;
		this.cwi.cancel();
    }

    /**
     * @see TupleSource#closeSource()
     */
    public void closeSource() {
    	cancelAsynch = true;
    	if (!explicitClose) {
        	fullyCloseSource();
    	}
    }

    AtomicResultsMessage exceptionOccurred(TranslatorException exception, boolean removeState) throws TeiidComponentException, TeiidProcessingException {
    	if (removeState) {
			fullyCloseSource();
		}
    	if(workItem.requestMsg.supportsPartialResults()) {
			AtomicResultsMessage emptyResults = new AtomicResultsMessage(new List[0]);
			emptyResults.setWarnings(Arrays.asList((Exception)exception));
			emptyResults.setFinalRow(this.rowsProcessed);
			return emptyResults;
		} 
		if (exception.getCause() instanceof TeiidComponentException) {
			throw (TeiidComponentException)exception.getCause();
		}
		if (exception.getCause() instanceof TeiidProcessingException) {
			throw (TeiidProcessingException)exception.getCause();
		}
		throw new TeiidProcessingException(exception, this.getConnectorName() + ": " + exception.getMessage()); //$NON-NLS-1$
	}

	void receiveResults(AtomicResultsMessage response) {
		this.arm = response;
		explicitClose |= !arm.supportsImplicitClose();
        rowsProcessed += response.getResults().length;
        index = 0;
		if (response.getWarnings() != null) {
			for (Exception warning : response.getWarnings()) {
				SourceWarning sourceFailure = new SourceWarning(this.aqr.getModelName(), aqr.getConnectorName(), warning, true);
		        workItem.addSourceFailureDetails(sourceFailure);
			}
		}
		if (response.getFinalRow() >= 0) {
    		done = true;
    	}
	}
	
	public AtomicRequestMessage getAtomicRequestMessage() {
		return aqr;
	}

	public String getConnectorName() {
		return this.aqr.getConnectorName();
	}
	
	public boolean isTransactional() {
		return this.aqr.isTransactional();
	}

	@Override
	public void onCompletion(FutureWork<AtomicResultsMessage> future) {
		if (!cancelAsynch) {
			workItem.moreWork(); //this is not necessary in some situations with DataNotAvailable
		}
		canAsynchClose = false;
		if (closed.get()) {
			cwi.close();
		}
		running = false;		
	}
	
}
