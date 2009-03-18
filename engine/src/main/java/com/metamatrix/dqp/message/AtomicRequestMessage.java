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
package com.metamatrix.dqp.message;

import java.io.Serializable;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import org.teiid.connector.xa.api.TransactionContext;
import org.teiid.dqp.internal.process.DQPWorkContext;

import com.metamatrix.common.buffer.impl.BufferConfig;
import com.metamatrix.dqp.internal.datamgr.ConnectorID;
import com.metamatrix.query.sql.lang.Command;

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
	private String connectorBindingID;

	/**
	 * Name of model where the connector is bound to
	 */
	private String modelName;

	/**
	 * For cancel and update operations, the id of the data connector which
	 * originally handled the request.
	 */
	private ConnectorID connectorID;

	// Transaction context for the current request
	private TransactionContext txnContext;

	// command to execute
	private Command command;

	// results fetch size
	private int fetchSize = BufferConfig.DEFAULT_CONNECTOR_BATCH_SIZE;

	// The time when the command was created by the client
	private Date submittedTimestamp;

	// The time when command begins processing on the server.
	private Date processingTimestamp;

	// whether to use ResultSet cache if there is one
	private boolean useResultSetCache;
    
    private boolean partialResultsFlag;
    
    private RequestID requestID;
    private Serializable executionPayload;
    
    private DQPWorkContext workContext;
    
    public AtomicRequestMessage() {
        // This is only to honor the externalizable interface..
	}
    
    public AtomicRequestMessage(RequestMessage requestMessage, DQPWorkContext parent, int nodeId){
    	this.executionPayload = requestMessage.getExecutionPayload();
    	this.workContext = parent;
    	this.requestID = new RequestID(parent.getConnectionID(), requestMessage.getExecutionId());
        this.atomicRequestId = new AtomicRequestID(this.requestID, nodeId, EXECUTION_COUNT.getAndIncrement());
    }

    public AtomicRequestID getAtomicRequestID() {
        return this.atomicRequestId;
    }
          
    public String getConnectorBindingID() {
        return connectorBindingID;
    }

    public ConnectorID getConnectorID() {
        return connectorID;
    }
    
    public void setConnectorID(ConnectorID connectorID) {
        this.connectorID = connectorID;
    }    

    public void setConnectorBindingID(String string) {
        connectorBindingID = string;
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

    public boolean isTransactional(){
        return this.txnContext != null && this.txnContext.isInTransaction();
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
			throw new IllegalArgumentException("fetch size must be positive");
		}
		this.fetchSize = fetchSize;
	}   

    /**
     * Get time that the time when the command was created by the client.
     * @return timestamp in millis
     */
    public Date getSubmittedTimestamp() {
        return submittedTimestamp;
    }
    
    /**
     * Set time that the time when the command was created by the client.
     * NOTE: By default, this gets set to the current time by the constructor.
     * @param submittedTimestamp Time submitted to server.
     */
    public void setSubmittedTimestamp(Date submittedTimestamp) {
        this.submittedTimestamp = submittedTimestamp;
    }    
    
    /**
     * Start the clock on submission start - this should be called when the request is originally created.
     */
    public void markSubmissionStart() {
        setSubmittedTimestamp(new Date());
    }
    
    
    /**
     * Get time that the request was assigned a unique ID by the server.
     * @return timestamp in millis
     */
    public Date getProcessingTimestamp() {
        return processingTimestamp;
    }

    /**
     * Set time that the request is submitted on the server.
     * @param processingTimestamp Time submitted to server.
     */
    public void setProcessingTimestamp(Date processingTimestamp) {
        this.processingTimestamp = processingTimestamp;
    }

    /**
     * Start the clock on processing times - this should be called when the query
     * hits the QueryService or SubscriptionService.
     */
    public void markProcessingStart() {
        setProcessingTimestamp(new Date());
    }	
	
	public boolean useResultSetCache() {
		//not use caching when there is a txn 
		return useResultSetCache 
			&& !isTransactional();
	}

	public void setUseResultSetCache(boolean useResultSetCacse) {
		this.useResultSetCache = useResultSetCacse;
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
