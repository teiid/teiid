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

import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.TupleSource;
import com.metamatrix.common.comm.api.ResultsReceiver;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.dqp.exception.SourceFailureDetails;
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
    private boolean supportsImplicitClose;
    
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
	        } catch (Exception e) {
	        	exceptionOccurred(e);
	        }
        }
    }
    
    /**
     * Send request for more data but use waitingForData flag to avoid requesting batches multiple times
     * @throws MetaMatrixComponentException if an unexpected error occurs
     */
    private void requestBatch() throws MetaMatrixComponentException {
        // check that the same request hasn't already been sent
        if(! waitingForData) { 

            // determine next batch to request
            int nextRow = this.rowsProcessed + 1;
            
            // send request
            this.aqr.setNextRow(nextRow);
            this.dataMgr.requestBatch(this.aqr.getAtomicRequestID(), connectorId);
            
            // update waitingForData
            this.waitingForData = true;
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
        if (nextBatch == null) {
            // If we don't have a next batch, request it.
            requestBatch();
            throw BlockedException.INSTANCE;
        }
        // Switch the current batch
        this.currentBatch = this.nextBatch;
        this.isLast = this.nextBatchIsLast;
        this.currentBatchCount = this.currentBatch.length;
        this.index = 0;
        this.nextBatch = null;
        

        // Request the next batch immediately
        if (!this.isLast) {
            requestBatch();
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
		        SourceFailureDetails sourceFailure = new SourceFailureDetails(this.aqr.getModelName(), connectorBindingName, new MetaMatrixComponentException(e));
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
		        aqr.setProcessingTimestamp(response.getProcessingTimestamp());
		        
		        nextBatch = response.getResults();    
				nextBatchIsLast = response.getFinalRow() >= 0;
		        rowsProcessed += nextBatch.length;
	        }
	        // reset waitingForData flag
	        waitingForData = false;
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

}
