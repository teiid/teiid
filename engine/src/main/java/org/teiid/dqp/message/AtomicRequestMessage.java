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

/*
 */
package org.teiid.dqp.message;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

import org.teiid.client.RequestMessage;
import org.teiid.common.buffer.BufferManager;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.dqp.service.TransactionContext;
import org.teiid.dqp.service.TransactionContext.Scope;
import org.teiid.query.sql.lang.Command;


/**
 * This class defines a request message for the Connector layer. This is uniquely identified
 * by AtomicRequestID.
 */
public class AtomicRequestMessage implements Serializable {
    static final long serialVersionUID = -4060941593023225562L;

	/**
	 * static counter to keep track of the Execution count.
	 */
	private static AtomicInteger EXECUTION_COUNT = new AtomicInteger(0);

	// atomic request id (request-id + node-id + execution-count)
	private AtomicRequestID atomicRequestId;

	/**
	 * The connectorBindingID that identifies the connector needed for this
	 * query.
	 */
	private String connectorName;

	/**
	 * Name of model where the connector is bound to
	 */
	private String modelName;

	// Transaction context for the current request
	private TransactionContext txnContext;

	// command to execute
	private Command command;

	// results fetch size
	private int fetchSize = BufferManager.DEFAULT_CONNECTOR_BATCH_SIZE;

	// The time when command begins processing on the server.
	private long processingTimestamp = System.currentTimeMillis();

    private boolean partialResultsFlag;
    
    private RequestID requestID;
    private Serializable executionPayload;
    
    private boolean exceptionOnMaxRows;
    private int maxRows;
    
    private boolean serial;
    
    private DQPWorkContext workContext;
    
    public AtomicRequestMessage() {
        // This is only to honor the externalizable interface..
	}
    
    public AtomicRequestMessage(RequestMessage requestMessage, DQPWorkContext parent, int nodeId){
    	this.executionPayload = requestMessage.getExecutionPayload();
    	this.workContext = parent;
    	this.requestID = new RequestID(parent.getSessionId(), requestMessage.getExecutionId());
        this.atomicRequestId = new AtomicRequestID(this.requestID, nodeId, EXECUTION_COUNT.getAndIncrement());
    }
    
    public int getMaxResultRows() {
		return maxRows;
	}
    
    public void setMaxResultRows(int maxRows) {
		this.maxRows = maxRows;
	}
    
    public boolean isExceptionOnMaxRows() {
		return exceptionOnMaxRows;
	}
    
    public void setExceptionOnMaxRows(boolean exceptionOnMaxRows) {
		this.exceptionOnMaxRows = exceptionOnMaxRows;
	}

    public AtomicRequestID getAtomicRequestID() {
        return this.atomicRequestId;
    }
          
    public String getConnectorName() {
        return connectorName;
    }

    public void setConnectorName(String string) {
        connectorName = string;
    }

    public String getModelName() {
        return this.modelName;
    }

    public void setModelName(String modelName) {
        this.modelName = modelName;
    }
    
    public TransactionContext getTransactionContext() {
        return txnContext;
    }

    public void setTransactionContext(TransactionContext context) {
        txnContext = context;
    }
    
    public boolean isSerial() {
    	return serial || isTransactional();
    }
    
    public void setSerial(boolean serial) {
		this.serial = serial;
	}

    public boolean isTransactional(){
        return this.txnContext != null && this.txnContext.getTransactionType() != Scope.NONE;
    }    
	
	public Command getCommand() {
		return command;
	}

	public void setCommand(Command command) {
		this.command = command;
	}

	public int getFetchSize() {
		return fetchSize;
	}

	public void setFetchSize(int fetchSize) {
		if (fetchSize < 1) {
			throw new IllegalArgumentException("fetch size must be positive"); //$NON-NLS-1$
		}
		this.fetchSize = fetchSize;
	}   

    /**
     * Get time that the request was assigned a unique ID by the server.
     * @return timestamp in millis
     */
    public long getProcessingTimestamp() {
        return processingTimestamp;
    }

    public boolean supportsPartialResults() {
        return partialResultsFlag;
    }

    public void setPartialResults(boolean partial) {
        partialResultsFlag = partial;
    }

    public String toString() {
        return atomicRequestId.toString();
    }

	public void setExecutionPayload(Serializable executionPayload) {
		this.executionPayload = executionPayload;
	}

	public Serializable getExecutionPayload() {
		return executionPayload;
	}

	public void setRequestID(RequestID requestID) {
		this.requestID = requestID;
	}

	public RequestID getRequestID() {
		return requestID;
	}

	public void setWorkContext(DQPWorkContext workContext) {
		this.workContext = workContext;
	}

	public DQPWorkContext getWorkContext() {
		return workContext;
	}

}
