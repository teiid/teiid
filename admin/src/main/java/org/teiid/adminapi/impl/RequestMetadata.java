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

package org.teiid.adminapi.impl;

import java.util.Date;

import org.teiid.adminapi.Request;
import org.teiid.core.util.HashCodeUtil;



public class RequestMetadata extends AdminObjectImpl implements Request {

	private static final long serialVersionUID = -2779106368517784259L;
	
	private long executionId;
	private String sessionId;
    private String command;
    private long startTime;
    private boolean sourceRequest;
	private Integer nodeID;
    private String transactionId;
    private ProcessingState processingState = ProcessingState.PROCESSING;
    private ThreadState threadState = ThreadState.RUNNING;
    
    @Override
    public long getExecutionId() {
		return executionId;
	}
    
    public void setExecutionId(long id) {
		this.executionId = id;
	}
    
    @Override
    public ProcessingState getState() {
		return processingState;
	}
    
    public void setState(ProcessingState state) {
		this.processingState = state;
	}
    
	@Override
	public ThreadState getThreadState() {
		return threadState;
	}
	
	public void setThreadState(ThreadState threadState) {
		this.threadState = threadState;
	}
    
    @Override
    public String getSessionId() {
        return this.sessionId;
    }
    
    public void setSessionId(String session) {
        this.sessionId = session;
    }
    
    @Override
    public long getStartTime() {
        return this.startTime;
    }
    
    public void setStartTime(long time) {
        this.startTime = time;
    }    

    @Override
    public String getCommand() {
        return this.command;
    }
    
    public void setCommand(String cmd) {
        this.command = cmd;
    }    
    
    @Override
    public boolean sourceRequest() {
		return sourceRequest;
	}

	public void setSourceRequest(boolean sourceRequest) {
		this.sourceRequest = sourceRequest;
	}    
        
	@Override
    public Integer getNodeId() {
        return this.nodeID;
    }
    
    public void setNodeId(Integer nodeID) {
        this.nodeID = nodeID;
    }
    
	@Override
	public String getTransactionId() {
		return this.transactionId;
	}

	public void setTransactionId(String id) {
		this.transactionId = id;
	}
	
    @Override
	public boolean equals(Object obj) {
    	if (!(obj instanceof RequestMetadata)) {
    		return false;
    	}
    	RequestMetadata value = (RequestMetadata)obj;
    	if (!sourceRequest()) {
    		return sessionId == value.sessionId && executionId == value.executionId;
    	}
		return sessionId == value.sessionId && executionId == value.executionId && nodeID.equals(value.nodeID);
	}
    
    public int hashCode() {
    	return HashCodeUtil.hashCode((int)executionId, sessionId);
    }    
    
    @SuppressWarnings("nls")
	public String toString() {
    	StringBuilder str = new StringBuilder();
    	str.append("Request: sessionid=").append(sessionId);
    	str.append("; executionId=").append(executionId);
    	if (nodeID != null) {
    		str.append("; nodeId=").append(nodeID);
    	}
    	if (transactionId != null) {
    		str.append("; transactionId=").append(transactionId);
    	}
    	str.append("; sourceRequest=").append(sourceRequest);
    	str.append("; processingTime=").append(new Date(startTime));
    	str.append("; command=").append(command); 
    	
    	return str.toString();
    }
}
