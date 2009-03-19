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

package org.teiid.dqp.internal.datamgr.impl;

import java.util.Arrays;
import java.util.List;

import javax.transaction.xa.XAResource;

import org.teiid.connector.api.Connection;
import org.teiid.connector.api.ConnectorCapabilities;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.DataNotAvailableException;
import org.teiid.connector.api.Execution;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.ResultSetExecution;
import org.teiid.connector.api.UpdateExecution;
import org.teiid.connector.basic.BasicConnection;
import org.teiid.connector.basic.BasicConnector;
import org.teiid.connector.basic.BasicConnectorCapabilities;
import org.teiid.connector.basic.BasicExecution;
import org.teiid.connector.language.ICommand;
import org.teiid.connector.language.IQueryCommand;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;
import org.teiid.connector.xa.api.TransactionContext;
import org.teiid.connector.xa.api.XAConnection;
import org.teiid.connector.xa.api.XAConnector;

import junit.framework.Assert;


public class FakeConnector extends BasicConnector implements XAConnector {
	private static final int RESULT_SIZE = 5;
	
	private boolean executeBlocks;
    private boolean nextBatchBlocks;
    private boolean returnsFinalBatch;
    private boolean driverThrowsExceptionOnCancel;
    private long simulatedBatchRetrievalTime = 1000L;
    private ClassLoader classloader;
    
    private int connectionCount;
    private int executionCount;
    
    public int getConnectionCount() {
		return connectionCount;
	}
    
    public int getExecutionCount() {
		return executionCount;
	}
    
    @Override
    public Connection getConnection(org.teiid.connector.api.ExecutionContext context) throws ConnectorException {
        return new FakeConnection();
    }
    @Override
    public void start(ConnectorEnvironment environment)
    		throws ConnectorException {
    	
    }
    @Override
    public void stop() {}
    @Override
	public XAConnection getXAConnection(ExecutionContext executionContext,
			TransactionContext transactionContext) throws ConnectorException {
		return new FakeConnection();
	}
	
    private class FakeConnection extends BasicConnection implements XAConnection {
    	
    	public FakeConnection() {
			connectionCount++;
		}
    	
        public boolean released = false;
        public Execution createExecution(ICommand command, ExecutionContext executionContext, RuntimeMetadata metadata) throws ConnectorException {
        	executionCount++;
            return new FakeBlockingExecution(executionContext);
        }
        public ConnectorCapabilities getCapabilities() {
        	return new BasicConnectorCapabilities();
        }
        public void close() {
            Assert.assertFalse("The connection should not be released more than once", released); //$NON-NLS-1$
            released = true;
        }
		@Override
		public XAResource getXAResource() throws ConnectorException {
			return null;
		}
    }   
    
    private final class FakeBlockingExecution extends BasicExecution implements ResultSetExecution, UpdateExecution {
        private boolean closed = false;
        private boolean cancelled = false;
        private int rowCount;
        ExecutionContext ec;
        public FakeBlockingExecution(ExecutionContext ec) {
            this.ec = ec;
        }
        public void execute(IQueryCommand query, int maxBatchSize) throws ConnectorException {
            if (executeBlocks) {
                waitForCancel();
            }
            if (classloader != null) {
            	Assert.assertSame(classloader, Thread.currentThread().getContextClassLoader());
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
        @Override
        public void execute() throws ConnectorException {
            ec.addWarning(new Exception("Some warning")); //$NON-NLS-1$
        }
        @Override
        public List next() throws ConnectorException, DataNotAvailableException {
        	if (nextBatchBlocks) {
                waitForCancel();
            }
            if (this.rowCount >= RESULT_SIZE || returnsFinalBatch) {
            	return null;
            }
            this.rowCount++;
            return Arrays.asList(this.rowCount - 1);
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
		@Override
		public int[] getUpdateCounts() throws DataNotAvailableException,
				ConnectorException {
			return new int[] {1};
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
	
	public void setClassloader(ClassLoader classloader) {
		this.classloader = classloader;
	}
	
	@Override
	public ConnectorCapabilities getCapabilities() {
		return null;
	}
}
