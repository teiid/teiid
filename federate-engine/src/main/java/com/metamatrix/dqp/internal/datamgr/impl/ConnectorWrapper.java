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

import com.metamatrix.data.api.Connection;
import com.metamatrix.data.api.Connector;
import com.metamatrix.data.api.ConnectorCapabilities;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.GlobalCapabilitiesProvider;
import com.metamatrix.data.api.SecurityContext;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.monitor.AliveStatus;
import com.metamatrix.data.monitor.ConnectionStatus;
import com.metamatrix.data.monitor.MonitoredConnector;
import com.metamatrix.data.pool.ConnectorIdentity;
import com.metamatrix.data.pool.ConnectorIdentityFactory;
import com.metamatrix.data.pool.SingleIdentity;
import com.metamatrix.data.xa.api.TransactionContext;
import com.metamatrix.data.xa.api.XAConnection;
import com.metamatrix.data.xa.api.XAConnector;

/**
 * ConnectorWrapper adds default behavior to the wrapped connector.
 */
public class ConnectorWrapper implements XAConnector, GlobalCapabilitiesProvider, MonitoredConnector, ConnectorIdentityFactory {
	
	private Connector actualConnector;
	
	public ConnectorWrapper(Connector actualConnector){
		this.actualConnector = actualConnector;
	}

	public void initialize(ConnectorEnvironment environment) throws ConnectorException {
		actualConnector.initialize(environment);
	}

	public void start() throws ConnectorException {
		actualConnector.start();
	}

	public void stop() {
		actualConnector.stop();
	}
	
	@Override
	public final Connection getConnection(SecurityContext context)
			throws ConnectorException {
    	setIdentity(context);
		return getConnectionDirect(context);
	}

	protected Connection getConnectionDirect(SecurityContext context)
			throws ConnectorException {
		return actualConnector.getConnection(context);
	}
	
	@Override
    public final XAConnection getXAConnection( SecurityContext securityContext, TransactionContext transactionContext) throws ConnectorException {
    	setIdentity(securityContext);
		return getXAConnectionDirect(securityContext, transactionContext);
    }

	protected XAConnection getXAConnectionDirect(SecurityContext securityContext,
			TransactionContext transactionContext) throws ConnectorException {
		if (actualConnector instanceof XAConnector) {
    		return ((XAConnector)actualConnector).getXAConnection(securityContext, transactionContext);
    	}
    	return null;
	}

	private void setIdentity(SecurityContext securityContext)
			throws ConnectorException {
		if (securityContext.getConnectorIdentity() == null && securityContext instanceof ExecutionContextImpl) {
    		((ExecutionContextImpl)securityContext).setConnectorIdentity(createIdentity(securityContext));
    	}
	}
	
	public ConnectorCapabilities getCapabilities() {
		if(actualConnector instanceof GlobalCapabilitiesProvider){
            return ((GlobalCapabilitiesProvider)actualConnector).getCapabilities();
		}
		return null;
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
	public ConnectorIdentity createIdentity(SecurityContext context)
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
