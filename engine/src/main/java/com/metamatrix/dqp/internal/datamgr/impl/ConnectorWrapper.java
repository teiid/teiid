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

import com.metamatrix.connector.api.Connection;
import com.metamatrix.connector.api.Connector;
import com.metamatrix.connector.api.ConnectorCapabilities;
import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.monitor.AliveStatus;
import com.metamatrix.connector.monitor.ConnectionStatus;
import com.metamatrix.connector.monitor.MonitoredConnector;
import com.metamatrix.connector.pool.ConnectorIdentity;
import com.metamatrix.connector.pool.ConnectorIdentityFactory;
import com.metamatrix.connector.pool.SingleIdentity;
import com.metamatrix.connector.xa.api.TransactionContext;
import com.metamatrix.connector.xa.api.XAConnection;
import com.metamatrix.connector.xa.api.XAConnector;

/**
 * ConnectorWrapper adds default behavior to the wrapped connector.
 */
public class ConnectorWrapper implements XAConnector, MonitoredConnector, ConnectorIdentityFactory {
	
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
	
	@Override
	public final ConnectionStatus getStatus() {
		if(actualConnector instanceof MonitoredConnector){
            return ((MonitoredConnector)actualConnector).getStatus();
		}
		return getStatusDirect();
	}

	protected ConnectionStatus getStatusDirect() {
		return new ConnectionStatus(AliveStatus.UNKNOWN);
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
		return new SingleIdentity(context);
	}
	
	public boolean supportsSingleIdentity() {
		try {
			return createIdentity(null) != null;
		} catch (ConnectorException e) {
			return false;
		}
	}
}
