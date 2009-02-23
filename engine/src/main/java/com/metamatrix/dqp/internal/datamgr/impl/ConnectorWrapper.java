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

package com.metamatrix.dqp.internal.datamgr.impl;

import com.metamatrix.connector.api.Connection;
import com.metamatrix.connector.api.Connector;
import com.metamatrix.connector.api.ConnectorCapabilities;
import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.api.ConnectorException;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.identity.ConnectorIdentity;
import com.metamatrix.connector.identity.ConnectorIdentityFactory;
import com.metamatrix.connector.identity.SingleIdentity;
import com.metamatrix.connector.xa.api.TransactionContext;
import com.metamatrix.connector.xa.api.XAConnection;
import com.metamatrix.connector.xa.api.XAConnector;

/**
 * ConnectorWrapper adds default behavior to the wrapped connector.
 */
public class ConnectorWrapper implements XAConnector, ConnectorIdentityFactory {
	
	private Connector actualConnector;
	
	public ConnectorWrapper(Connector actualConnector){
		this.actualConnector = actualConnector;
	}

	public void start(ConnectorEnvironment environment) throws ConnectorException {
		actualConnector.start(environment);
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
	
	public final Boolean getStatus() {
		if (supportsSingleIdentity()) {
			Connection conn = null;
			try {
				conn = this.getConnection(null);
				return conn.isAlive();
			} catch (ConnectorException e) {
				return Boolean.FALSE;
			} finally {
				conn.close();
			}
		}
		return null;
	}
	
	public Connector getActualConnector() {
		return actualConnector;
	}
	
	@Override
	public ConnectorIdentity createIdentity(ExecutionContext context)
			throws ConnectorException {
		if (actualConnector instanceof ConnectorIdentityFactory) {
			return ((ConnectorIdentityFactory)actualConnector).createIdentity(context);
		}
		return new SingleIdentity();
	}
	
	public boolean supportsSingleIdentity() {
		try {
			return createIdentity(null) != null;
		} catch (ConnectorException e) {
			return false;
		}
	}
}
