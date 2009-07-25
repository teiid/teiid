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

import java.util.concurrent.TimeUnit;

import org.teiid.connector.api.Connection;
import org.teiid.connector.api.Connector;
import org.teiid.connector.api.ConnectorCapabilities;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorIdentity;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.MetadataProvider;
import org.teiid.connector.metadata.runtime.MetadataFactory;
import org.teiid.connector.xa.api.TransactionContext;
import org.teiid.connector.xa.api.XAConnection;
import org.teiid.connector.xa.api.XAConnector;
import org.teiid.dqp.internal.pooling.connector.ConnectionPool;

import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.dqp.service.ConnectorStatus;

/**
 * ConnectorWrapper adds default behavior to the wrapped connector.
 */
public class ConnectorWrapper implements XAConnector, MetadataProvider {
	
	private Connector actualConnector;
	private String name;
	private volatile ConnectorStatus status = ConnectorStatus.UNABLE_TO_CHECK;
	
	public ConnectorWrapper(Connector actualConnector){
		this.actualConnector = actualConnector;
	}

	public void start(ConnectorEnvironment environment) throws ConnectorException {
		name = environment.getConnectorName();
		actualConnector.start(environment);
		int interval = PropertiesUtils.getIntProperty(environment.getProperties(), ConnectionPool.SOURCE_CONNECTION_TEST_INTERVAL, ConnectionPool.DEFAULT_SOURCE_CONNECTION_TEST_INTERVAL);
		if (interval > 0 && isConnectionTestable()) {
			environment.scheduleAtFixedRate(new Runnable() {
				@Override
				public void run() {
					updateStatus();
				}
			}, 0, interval, TimeUnit.SECONDS);
		}
	}
	
	protected boolean isConnectionTestable() {
		return supportsSingleIdentity();
	}

	public void stop() {
		actualConnector.stop();
	}
	
	@Override
	public final Connection getConnection(ExecutionContext context)
			throws ConnectorException {
    	setIdentity(context);
		return getConnectionDirect(context);
	}

	protected Connection getConnectionDirect(ExecutionContext context)
			throws ConnectorException {
		return actualConnector.getConnection(context);
	}
	
	@Override
    public final XAConnection getXAConnection( ExecutionContext executionContext, TransactionContext transactionContext) throws ConnectorException {
    	setIdentity(executionContext);
		return getXAConnectionDirect(executionContext, transactionContext);
    }

	protected XAConnection getXAConnectionDirect(ExecutionContext executionContext,
			TransactionContext transactionContext) throws ConnectorException {
		if (actualConnector instanceof XAConnector) {
    		return ((XAConnector)actualConnector).getXAConnection(executionContext, transactionContext);
    	}
    	return null;
	}

	private void setIdentity(ExecutionContext executionContext)
			throws ConnectorException {
		if (executionContext instanceof ExecutionContextImpl && executionContext.getConnectorIdentity() == null) {
    		((ExecutionContextImpl)executionContext).setConnectorIdentity(createIdentity(executionContext));
    	}
	}
	
	public ConnectorCapabilities getCapabilities() {
	    return actualConnector.getCapabilities();
	}
	
	public final ConnectorStatus getStatus() {
		return status;
	}
	
	protected void updateStatus() {
		this.status = testConnection();
	}

	protected ConnectorStatus testConnection() {
		if (supportsSingleIdentity()) {
			Connection conn = null;
			try {
				conn = this.getConnection(null);
				return conn.isAlive()?ConnectorStatus.OPEN:ConnectorStatus.DATA_SOURCE_UNAVAILABLE;
			} catch (ConnectorException e) {
				return ConnectorStatus.DATA_SOURCE_UNAVAILABLE;
			} finally {
				if (conn != null) {
					conn.close();
				}
			}
		}
		return ConnectorStatus.UNABLE_TO_CHECK;
	}
	
	public Connector getActualConnector() {
		return actualConnector;
	}
	
	@Override
	public ConnectorIdentity createIdentity(ExecutionContext context)
			throws ConnectorException {
		return actualConnector.createIdentity(context);
	}
	
	public boolean supportsSingleIdentity() {
		try {
			return createIdentity(null) != null;
		} catch (ConnectorException e) {
			return false;
		}
	}
	
	public String getConnectorBindingName() {
		return this.name;
	}
	
	@Override
	public void getConnectorMetadata(MetadataFactory metadataFactory)
			throws ConnectorException {
		if (this.actualConnector instanceof MetadataProvider) {
			((MetadataProvider)this.actualConnector).getConnectorMetadata(metadataFactory);
		}
	}
}
