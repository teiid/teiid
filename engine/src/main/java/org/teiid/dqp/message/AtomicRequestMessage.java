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

/*
 */
package org.teiid.dqp.message;

import java.util.concurrent.atomic.AtomicInteger;

import org.teiid.client.RequestMessage;
import org.teiid.common.buffer.BufferManager;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.dqp.service.TransactionContext;
import org.teiid.dqp.service.TransactionContext.Scope;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.util.CommandContext;


/**
 * This class defines a request message for the Connector layer. This is uniquely identified
 * by AtomicRequestID.
 */
public class AtomicRequestMessage {
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
    private int fetchSize = BufferManager.DEFAULT_PROCESSOR_BATCH_SIZE;

    // The time when command begins processing on the server.
    private long processingTimestamp = System.currentTimeMillis();

    private boolean partialResultsFlag;

    private RequestID requestID;

    private boolean exceptionOnMaxRows;
    private int maxRows;

    private boolean serial;

    private boolean copyStreamingLobs;

    private DQPWorkContext workContext;
    private CommandContext commandContext;
    private BufferManager bufferManager;

    public AtomicRequestMessage(RequestMessage requestMessage, DQPWorkContext parent, int nodeId){
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
        return serial;
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

    public CommandContext getCommandContext() {
        return commandContext;
    }

    public void setCommandContext(CommandContext commandContext) {
        this.commandContext = commandContext;
    }

    public BufferManager getBufferManager() {
        return bufferManager;
    }

    public void setBufferManager(BufferManager bufferManager) {
        this.bufferManager = bufferManager;
    }

    public boolean isCopyStreamingLobs() {
        return copyStreamingLobs;
    }

    public void setCopyStreamingLobs(boolean copyStreamingLobs) {
        this.copyStreamingLobs = copyStreamingLobs;
    }

}
