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

package com.metamatrix.dqp.internal.datamgr.impl;

import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import com.metamatrix.common.xa.TransactionContext;
import com.metamatrix.data.api.Batch;
import com.metamatrix.data.api.Connection;
import com.metamatrix.data.api.Connector;
import com.metamatrix.data.api.ConnectorCapabilities;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ConnectorMetadata;
import com.metamatrix.data.api.Execution;
import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.data.api.SecurityContext;
import com.metamatrix.data.api.SynchQueryCommandExecution;
import com.metamatrix.data.basic.BasicBatch;
import com.metamatrix.data.basic.BasicConnectorCapabilities;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.IQueryCommand;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;
import com.metamatrix.data.xa.api.XAConnector;

public class FakeConnector implements Connector, XAConnector {
	private static final int BATCH_SIZE = 5;
	
	private class QueryCommandBasicCapabilities extends
			BasicConnectorCapabilities {
		@Override
		public boolean supportsExecutionMode(int executionMode) {
			return executionMode == ConnectorCapabilities.EXECUTION_MODE.SYNCH_QUERYCOMMAND;
		}
	}
	
	private boolean executeBlocks;
    private boolean nextBatchBlocks;
    private boolean returnsFinalBatch;
    private boolean driverThrowsExceptionOnCancel;
    private long simulatedBatchRetrievalTime = 1000L;
	
    public Connection getConnection(com.metamatrix.data.api.SecurityContext context) throws ConnectorException {
        return new FakeConnection();
    }
    public void initialize(ConnectorEnvironment environment) throws ConnectorException {}
    public void start() throws ConnectorException {}
    public void stop() {}
	public Connection getXAConnection(SecurityContext securityContext,
			TransactionContext transactionContext) throws ConnectorException {
		return new FakeXAConnection();
	}
	
    private final class FakeConnection implements Connection {
        public boolean released = false;
        public Execution createExecution(int executionMode, ExecutionContext executionContext, RuntimeMetadata metadata) throws ConnectorException {
            return new FakeBlockingExecution(executionContext);
        }
        public ConnectorCapabilities getCapabilities() {return new QueryCommandBasicCapabilities();}
        public ConnectorMetadata getMetadata() {return null;}
        public void release() {
            Assert.assertFalse("The connection should not be released more than once", released); //$NON-NLS-1$
            released = true;
        }
    }
    
    private final class FakeXAConnection implements Connection {
        public boolean released = false;
        public Execution createExecution(int executionMode, ExecutionContext executionContext, RuntimeMetadata metadata) throws ConnectorException {
            return new FakeBlockingExecution(executionContext);
        }
        public ConnectorCapabilities getCapabilities() {
            return new QueryCommandBasicCapabilities() {
                public boolean supportsXATransactions() {
                    return true;
                }
            };
        }
        public ConnectorMetadata getMetadata() {return null;}
        public void release() {
            Assert.assertFalse("The connection should not be released more than once", released); //$NON-NLS-1$
            released = true;
        }
    }   
    
    private final class FakeBlockingExecution implements SynchQueryCommandExecution {
        private boolean closed = false;
        private boolean cancelled = false;
        ExecutionContext ec;
        public FakeBlockingExecution(ExecutionContext ec) {
            this.ec = ec;
        }
        public void execute(IQueryCommand query, int maxBatchSize) throws ConnectorException {
            if (executeBlocks) {
                waitForCancel();
            }
        }
        public synchronized void cancel() throws ConnectorException {
            cancelled = true;
            this.notify();
        }
        public void close() throws ConnectorException {
            Assert.assertFalse("The execution should not be closed more than once", closed); //$NON-NLS-1$
            closed = true;
        }
        public Batch nextBatch() throws ConnectorException {
            if (nextBatchBlocks) {
                waitForCancel();
            }
            List[] rows = new List[BATCH_SIZE];
            for (int i = 0; i < rows.length; i++) {
                Integer[] row = new Integer[1];
                row[0] = new Integer(i);
                rows[i] = Arrays.asList(row);
            }
            BasicBatch batch = new BasicBatch(Arrays.asList(rows));
            if (returnsFinalBatch) {
                batch.setLast();
            }
            return batch;
        }
        
        private synchronized void waitForCancel() throws ConnectorException {
            try {
                this.wait(simulatedBatchRetrievalTime);
                if (cancelled && driverThrowsExceptionOnCancel) {
                    throw new ConnectorException("Request cancelled"); //$NON-NLS-1$
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

	public boolean isExecuteBlocks() {
		return executeBlocks;
	}
	public void setExecuteBlocks(boolean executeBlocks) {
		this.executeBlocks = executeBlocks;
	}
	public boolean isNextBatchBlocks() {
		return nextBatchBlocks;
	}
	public void setNextBatchBlocks(boolean nextBatchBlocks) {
		this.nextBatchBlocks = nextBatchBlocks;
	}
	public boolean isReturnsFinalBatch() {
		return returnsFinalBatch;
	}
	public void setReturnsFinalBatch(boolean returnsFinalBatch) {
		this.returnsFinalBatch = returnsFinalBatch;
	}
	public boolean isDriverThrowsExceptionOnCancel() {
		return driverThrowsExceptionOnCancel;
	}
	public void setDriverThrowsExceptionOnCancel(
			boolean driverThrowsExceptionOnCancel) {
		this.driverThrowsExceptionOnCancel = driverThrowsExceptionOnCancel;
	}
	public long getSimulatedBatchRetrievalTime() {
		return simulatedBatchRetrievalTime;
	}
	public void setSimulatedBatchRetrievalTime(long simulatedBatchRetrievalTime) {
		this.simulatedBatchRetrievalTime = simulatedBatchRetrievalTime;
	}

}
