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

package com.metamatrix.dqp.internal.pooling.connector;

import javax.transaction.xa.XAResource;

import com.metamatrix.data.api.Connection;
import com.metamatrix.data.api.ConnectorCapabilities;
import com.metamatrix.data.api.Execution;
import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;
import com.metamatrix.data.pool.PoolAwareConnection;
import com.metamatrix.data.xa.api.XAConnection;

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
	public Execution createExecution(int executionMode,
			ExecutionContext executionContext, RuntimeMetadata metadata)
			throws ConnectorException {
		return connection.createExecution(executionMode, executionContext, metadata);
	}

	@Override
	public ConnectorCapabilities getCapabilities() {
		return connection.getCapabilities();
	}

	@Override
	public void release() {
		synchronized (this) {
			if (this.leaseCount > 0) {
				this.leaseCount--;
			}
			if (isInTxn() || this.isLeased()) {
				return;
			}
		}
		this.connectionReleased();
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
	
	public void close() {
		this.connection.release();
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
	public void connectionReleased() {
		if (this.connection instanceof PoolAwareConnection) {
			((PoolAwareConnection)this.connection).connectionReleased();
		}
	}
	
	public void setTestInterval(long testInterval) {
		this.testInterval = testInterval;
	}

}
