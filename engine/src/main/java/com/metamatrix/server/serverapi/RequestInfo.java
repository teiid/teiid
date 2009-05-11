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

package com.metamatrix.server.serverapi;

import java.io.Serializable;
import java.util.Date;

import com.metamatrix.dqp.message.RequestID;
import com.metamatrix.platform.security.api.SessionToken;

/**
 * This class is used by <code>RequestManager</code>'s <code>getRequests</code>
 * methods to return request information useful for the console.
 */

public class RequestInfo implements Serializable {

	private RequestID requestID;
    private String command;
    private SessionToken token;
    private Date submittedTimestamp;
    private Date processingTimestamp;
    private boolean isSubscription;
    private int nodeID = Integer.MIN_VALUE;
    private String connectorBindingUUID;
    private String transactionId;
    private int executionID;
    
    public RequestInfo(RequestID requestId, String originalCommand, Date submittedTime, Date processingTime) {
        this.requestID = requestId;
        this.command = originalCommand;     // request command was modified
        this.submittedTimestamp = submittedTime;
        this.processingTimestamp = processingTime;
    }
    

    /**
     * Return RequestID for rquest this object represents
     * @return Request ID
     */
    public RequestID getRequestID() {
        return this.requestID;
    }

    /**
     * Returns the session token that the request was submitted under.
     * @return The session token
     */
    public SessionToken getSessionToken() {
        return this.token;
    }
    
    public String getUserName() {
    	return this.token.getUsername();
    }

    /**
     * Get time that the request was created by the user.
     * @return Submitted time in millis.
     */
    public Date getSubmittedTimestamp() {
        return this.submittedTimestamp;
    }

    /**
     * Get time that the request was assigned a unique ID by the server.
     * @return Server processing timestamp in millis
     */
    public Date getProcessingTimestamp() {
        return this.processingTimestamp;
    }

    /**
     * Return true if the request is for a subsription
     * @return tru for susription else false
     */
    public boolean isSubscription() {
        return this.isSubscription;
    }
    
    /**
     * Return true if the request represents the sub atomic query
     * for the specified request ID.
     * @return tru for susription else false
     */    
    public boolean isAtomicQuery() {
        return (this.connectorBindingUUID!=null); 
    }
    
    public int getNodeID() {
        return this.nodeID;
    }
    
    public void setNodeID(int nodeID) {
        this.nodeID = nodeID;
    }
    
    public int getExecutionID() {
		return executionID;
	}
    
    public void setExecutionID(int executionID) {
		this.executionID = executionID;
	}

    /**
     * Get the actual command to perform.
     * @return Command to perform
     */
    public String getCommand() {
        return this.command;
    }

    /**
     * Set the session token that the request was submitted under.
     */
    public void setSessionToken(SessionToken token) {
        this.token = token;
    }
    
    /**
     * Set the connector binding UUID.
     */
    public void setConnectorBindingUUID(String connBindUUID) {
        this.connectorBindingUUID = connBindUUID;
}    
    /**
     * Return the connector binding UUID;
     */
    public String getConnectorBindingUUID() {
        return this.connectorBindingUUID;
    }


	public String getTransactionId() {
		return transactionId;
	}


	public void setTransactionId(String transactionId) {
		this.transactionId = transactionId;
	}
	
    @Override
	public boolean equals(Object obj) {
    	if (!(obj instanceof RequestInfo)) {
    		return false;
    	}
    	RequestInfo value = (RequestInfo)obj;
    	if (connectorBindingUUID == null) {
    		return requestID.equals(value.getRequestID());
    	}
		return requestID.equals(value.getRequestID()) && connectorBindingUUID.equals(value.getConnectorBindingUUID()) && nodeID == value.nodeID;
	}
    
    @Override
    public int hashCode() {
    	return requestID.hashCode();
    }
}
