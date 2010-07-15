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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.teiid.client.SourceWarning;
import org.teiid.client.util.ResultsFuture;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.Assertion;
import org.teiid.dqp.internal.datamgr.impl.ConnectorWork;
import org.teiid.dqp.message.AtomicRequestMessage;
import org.teiid.dqp.message.AtomicResultsMessage;
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
public class DataTierTupleSource implements TupleSource {

    // Construction state
    private final AtomicRequestMessage aqr;
    private final RequestWorkItem workItem;
    private final ConnectorWork cwi;
    private final DataTierManagerImpl dtm;
    
    // Data state
    private int index;
    private int rowsProcessed;
    private volatile AtomicResultsMessage arm;
    private boolean closed;
    private volatile boolean canceled;
    private boolean executed;
    private volatile boolean done;
    
    private volatile ResultsFuture<AtomicResultsMessage> futureResult;
    private volatile boolean running;
    
    public DataTierTupleSource(AtomicRequestMessage aqr, RequestWorkItem workItem, ConnectorWork cwi, DataTierManagerImpl dtm) {
        this.aqr = aqr;
        this.workItem = workItem;
        this.cwi = cwi;
        this.dtm = dtm;
    	Assertion.isNull(workItem.getConnectorRequest(aqr.getAtomicRequestID()));
        workItem.addConnectorRequest(aqr.getAtomicRequestID(), this);
        if (!aqr.isTransactional()) {
        	addWork();
        }
    }

	private void addWork() {
		futureResult = dtm.addWork(new Callable<AtomicResultsMessage>() {
			@Override
			public AtomicResultsMessage call() throws Exception {
				return getResults();
			}
		}, 100);
		futureResult.addCompletionListener(new ResultsFuture.CompletionListener<AtomicResultsMessage>() {
			public void onCompletion(ResultsFuture<AtomicResultsMessage> future) {
				workItem.moreWork();
			}
		});
	}

    public List getSchema() {
        return this.aqr.getCommand().getProjectedSymbols();
    }

    public List<?> nextTuple() throws TeiidComponentException, TeiidProcessingException {
    	while (true) {
    		if (arm == null) {
    			AtomicResultsMessage results = null;
    			try {
	    			if (futureResult != null || !aqr.isTransactional()) {
	    				results = asynchGet();
	    			} else {
	    				results = getResults();
	    			}
    			} catch (TranslatorException e) {
    				exceptionOccurred(e, true);
    			} catch (DataNotAvailableException e) {
    				dtm.scheduleWork(new Runnable() {
    					@Override
    					public void run() {
							workItem.moreWork();
    					}
    				}, 10, e.getRetryDelay());
    				throw BlockedException.INSTANCE;
    			} 
    			receiveResults(results);
    		}
	    	if (index < arm.getResults().length) {
	            return this.arm.getResults()[index++];
	        }
	    	arm = null;
	    	if (isDone()) {
	    		return null;
	    	}
    	}
    }

	private AtomicResultsMessage asynchGet()
			throws BlockedException, TeiidProcessingException,
			TeiidComponentException, TranslatorException {
		if (futureResult == null) {
			addWork();
		}
		if (!futureResult.isDone()) {
			throw BlockedException.INSTANCE;
		}
		ResultsFuture<AtomicResultsMessage> currentResults = futureResult;
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

	private AtomicResultsMessage getResults()
			throws BlockedException, TeiidComponentException,
			TranslatorException {
		AtomicResultsMessage results = null;
		try {
			running = true;
			if (!executed) {
				results = cwi.execute();
				executed = true;
			} else {
				results = cwi.more();
			}
		} finally {
			running = false;
		}
		return results;
	}
    
    public boolean isQueued() {
    	ResultsFuture<AtomicResultsMessage> future = futureResult;
    	return !running && future != null && !future.isDone();
    }

	public boolean isDone() {
		return done;
	}
    
    public boolean isRunning() {
		return running;
	}
    
    public void fullyCloseSource() {
    	if (!closed) {
    		if (cwi != null) {
		    	workItem.closeAtomicRequest(this.aqr.getAtomicRequestID());
		    	if (!aqr.isTransactional()) {
		    		if (futureResult != null && !futureResult.isDone()) {
		    			futureResult.addCompletionListener(new ResultsFuture.CompletionListener<AtomicResultsMessage>() {
		    				@Override
		    				public void onCompletion(
		    						ResultsFuture<AtomicResultsMessage> future) {
		    					cwi.close(); // there is a small chance that this will be done in the processing thread
		    				}
						});
		    		} else {
		    			dtm.addWork(new Callable<Void>() {
		    				@Override
		    				public Void call() throws Exception {
		    					cwi.close();
		    					return null;
		    				}
		    			}, 0);
		    		}
		    	} else {
		    		this.cwi.close();
		    	}
    		}
			closed = true;
    	}
    }
    
    public boolean isCanceled() {
		return canceled;
	}
    
    public void cancelRequest() {
    	this.canceled = true;
    	if (this.cwi != null) {
    		this.cwi.cancel();
    	}
    }

    /**
     * @see TupleSource#closeSource()
     */
    public void closeSource() {
    	if (this.arm == null || this.arm.supportsImplicitClose()) {
        	fullyCloseSource();
    	}
    }

    void exceptionOccurred(TranslatorException exception, boolean removeState) throws TeiidComponentException, TeiidProcessingException {
    	if (removeState) {
			fullyCloseSource();
		}
    	if(workItem.requestMsg.supportsPartialResults()) {
			AtomicResultsMessage emptyResults = new AtomicResultsMessage(new List[0], null);
			emptyResults.setWarnings(Arrays.asList((Exception)exception));
			emptyResults.setFinalRow(this.rowsProcessed);
			receiveResults(emptyResults);
		} else {
    		if (exception.getCause() instanceof TeiidComponentException) {
    			throw (TeiidComponentException)exception.getCause();
    		}
    		if (exception.getCause() instanceof TeiidProcessingException) {
    			throw (TeiidProcessingException)exception.getCause();
    		}
    		throw new TeiidProcessingException(exception);
		}	
	}

	void receiveResults(AtomicResultsMessage response) {
		this.arm = response;
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
	public int available() {
		if (this.arm == null) {
			return 0;
		}
		return this.arm.getResults().length - index;
	}
	
}
