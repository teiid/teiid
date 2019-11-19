/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

    @Override
    public boolean isSourceRequest() {
        return sourceRequest;
    }
}
