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

package com.metamatrix.dqp.internal.pooling.connector;

import javax.transaction.xa.XAResource;

import com.metamatrix.connector.api.Connection;
import com.metamatrix.connector.api.ConnectorCapabilities;
import com.metamatrix.connector.api.Execution;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.language.ICommand;
import com.metamatrix.connector.metadata.runtime.RuntimeMetadata;
import com.metamatrix.connector.pool.PoolAwareConnection;
import com.metamatrix.connector.xa.api.XAConnection;

public class ConnectionWrapper implements PoolAwareConnection, XAConnection {

	private Connection connection;
    private long timeReturnedToPool = System.currentTimeMillis();
    private long lastTest = System.currentTimeMillis();
    private boolean isDead;
    private long testInterval;
    private ConnectionPool connectionPool;
    private int leaseCount;
    private boolean isInTxn;
    
    public ConnectionWrapper(Connection connection,
			ConnectionPool connectionPool, long testInterval) {
		this.connection = connection;
		this.connectionPool = connectionPool;
		this.testInterval = testInterval;
	}
    
    public Connection getConnection() {
		return connection;
	}

	public long getTimeReturnedToPool() {
		return timeReturnedToPool;
	}
    
	@Override
	public boolean isAlive() {
		if (isDead) {
			return false;
		}
		if (connection instanceof PoolAwareConnection) {
			long now = System.currentTimeMillis();
			if (now - lastTest > testInterval) {
				boolean result = ((PoolAwareConnection)connection).isAlive();
				lastTest = now; 
				this.isDead = !result;
			}
		}
		return !isDead;
	}
	
	@Override
	public Execution createExecution(ICommand command,
			ExecutionContext executionContext, RuntimeMetadata metadata)
			throws ConnectorException {
		return connection.createExecution(command, executionContext, metadata);
	}

	@Override
	public ConnectorCapabilities getCapabilities() {
		return connection.getCapabilities();
	}

	@Override
	public void close() {
		synchronized (this) {
			if (this.leaseCount > 0) {
				this.leaseCount--;
			}
			if (isInTxn() || this.isLeased()) {
				return;
			}
		}
		this.closeCalled();
		this.timeReturnedToPool = System.currentTimeMillis();
		this.connectionPool.release(this, false);
	}

	@Override
	public XAResource getXAResource() throws ConnectorException {
		if (this.connection instanceof XAConnection) {
			return ((XAConnection)this.connection).getXAResource();
		}
		return null;
	}
	
    public synchronized boolean isLeased() {
        return this.leaseCount > 0;
    }    
    
    public synchronized void lease() throws ConnectorException {
        this.leaseCount++;
    }
    
    public synchronized boolean isInTxn() {
		return isInTxn;
	}
    
    public synchronized void setInTxn(boolean isInTxn) {
		this.isInTxn = isInTxn;
	}

	@Override
	public void closeCalled() {
		if (this.connection instanceof PoolAwareConnection) {
			((PoolAwareConnection)this.connection).closeCalled();
		}
	}
	
	public void setTestInterval(long testInterval) {
		this.testInterval = testInterval;
	}

}
