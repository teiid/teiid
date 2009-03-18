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

import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.TupleSource;
import com.metamatrix.common.comm.api.ResultsReceiver;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.dqp.exception.SourceWarning;
import com.metamatrix.dqp.internal.datamgr.ConnectorID;
import com.metamatrix.dqp.message.AtomicRequestMessage;
import com.metamatrix.dqp.message.AtomicResultsMessage;

/**
 * This tuple source impl can only be used once; once it is closed, it 
 * cannot be reopened and reused.
 */
public class DataTierTupleSource implements TupleSource, ResultsReceiver<AtomicResultsMessage> {

    // Construction state
    private final List schema;
    private final AtomicRequestMessage aqr;
    private final DataTierManagerImpl dataMgr;
    private final ConnectorID connectorId;
    private final RequestWorkItem workItem;
    
    // Data state
    private List[] currentBatch;
    private List[] nextBatch;
    private int currentBatchCount = 0;
    private int index = 0;
    private boolean nextBatchIsLast = false;
    private volatile boolean isLast = false;
    private int rowsProcessed = 0;
    private boolean waitingForData = false;
    private Throwable exception;
    private volatile boolean supportsImplicitClose;
    private volatile boolean isTransactional;
    
    /**
     * Constructor for DataTierTupleSource.
     */
    public DataTierTupleSource(List schema, AtomicRequestMessage aqr, DataTierManagerImpl dataMgr, ConnectorID connectorID, RequestWorkItem workItem) {
        this.schema = schema;       
        this.aqr = aqr;
        this.dataMgr = dataMgr;
        this.connectorId = connectorID;
        this.workItem = workItem;
    }

    /**
     * @see TupleSource#getSchema()
     */
    public List getSchema() {
        return this.schema;
    }

    public List nextTuple() throws MetaMatrixComponentException, MetaMatrixProcessingException {
    	if (index < currentBatchCount) {
            return this.currentBatch[index++];
        } else if (isLast) {
            return null;
        } else {
            // We're past the end of the current batch, so switch to the next
            switchBatch();
            // If the new batch is empty
            if (currentBatchCount == 0) {
                if (isLast) {
                    return null;
                }
                throw BlockedException.INSTANCE;
            }
            return currentBatch[index++];
        }
    }
    
    public void open() {
        Assertion.isNull(workItem.getConnectorRequest(aqr.getAtomicRequestID()));
        workItem.addConnectorRequest(aqr.getAtomicRequestID(), this);
        synchronized (this) {
        	this.waitingForData = true;
	        try {
	        	this.dataMgr.executeRequest(aqr, this.connectorId, this);
	        } catch (MetaMatrixComponentException e) {
	        	exceptionOccurred(e);
	        }
        }
    }
    
    /**
     * Switches to the next batch.
     * @throws BlockedException if we're still waiting for data from the connector
     * @throws MetaMatrixComponentException if the request for the next batch failed.
     * @since 4.3
     */
    private synchronized void switchBatch() throws MetaMatrixComponentException, MetaMatrixProcessingException {
    	if (exception != null) {
    		if (exception instanceof MetaMatrixComponentException) {
    			throw (MetaMatrixComponentException)exception;
    		}
    		if (exception instanceof MetaMatrixProcessingException) {
    			throw (MetaMatrixProcessingException)exception;
    		}
    		throw new MetaMatrixComponentException(exception);
    	}
    	boolean shouldBlock = true;
        if (nextBatch != null) {
            // Switch the current batch
            this.currentBatch = this.nextBatch;
            this.isLast = this.nextBatchIsLast;
            this.currentBatchCount = this.currentBatch.length;
            this.index = 0;
            this.nextBatch = null;
            shouldBlock = false;
        } 
        // Request the next batch immediately
        if (!this.isLast && !waitingForData) {
            this.dataMgr.requestBatch(this.aqr.getAtomicRequestID(), connectorId);
            
            // update waitingForData
            this.waitingForData = true;
        }
        if (shouldBlock) {
        	throw BlockedException.INSTANCE;
        }
    }
    
    public void fullyCloseSource() throws MetaMatrixComponentException {
    	this.dataMgr.closeRequest(aqr.getAtomicRequestID(), connectorId);
    }
    
    public void cancelRequest() throws MetaMatrixComponentException {
    	this.dataMgr.cancelRequest(aqr.getAtomicRequestID(), connectorId);
    }

    /**
     * @see TupleSource#closeSource()
     */
    public void closeSource() throws MetaMatrixComponentException {
    	if (this.supportsImplicitClose) {
    		this.dataMgr.closeRequest(aqr.getAtomicRequestID(), connectorId);
    	}
    }

	public void exceptionOccurred(Throwable e) {
		synchronized (this) {
			if(workItem.requestMsg.supportsPartialResults()) {
				nextBatch = new List[0];
				nextBatchIsLast = true;
		        String connectorBindingName = dataMgr.getConnectorName(aqr.getConnectorBindingID());
		        SourceWarning sourceFailure = new SourceWarning(this.aqr.getModelName(), connectorBindingName, e, true);
		        workItem.addSourceFailureDetails(sourceFailure);
			} else {
				this.exception = e;
			}	
			waitingForData = false;
		}
		workItem.closeAtomicRequest(aqr.getAtomicRequestID());
		this.workItem.moreWork();
	}

	public void receiveResults(AtomicResultsMessage response) {
		boolean removeRequest = false;
		synchronized (this) {
	        // check to see if this is close of the atomic request message.
	        if (response.isRequestClosed()) {
	        	removeRequest = true;
	        } else {
		        supportsImplicitClose = response.supportsImplicitClose();
		        isTransactional = response.isTransactional();
		        
		        nextBatch = response.getResults();    
				nextBatchIsLast = response.getFinalRow() >= 0;
		        rowsProcessed += nextBatch.length;
	        }
	        // reset waitingForData flag
	        waitingForData = false;
		}
		if (response.getWarnings() != null) {
	        String connectorBindingName = dataMgr.getConnectorName(aqr.getConnectorBindingID());
			for (Exception warning : response.getWarnings()) {
				SourceWarning sourceFailure = new SourceWarning(this.aqr.getModelName(), connectorBindingName, warning, true);
		        workItem.addSourceFailureDetails(sourceFailure);
			}
		}
		if (removeRequest) {
        	workItem.closeAtomicRequest(this.aqr.getAtomicRequestID());
		}
		this.workItem.moreWork();
	}
	
	public AtomicRequestMessage getAtomicRequestMessage() {
		return aqr;
	}

	public ConnectorID getConnectorId() {
		return connectorId;
	}
	
	public boolean isTransactional() {
		return this.isTransactional;
	}
	
}
