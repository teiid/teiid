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
package org.teiid.connector.basic;

import java.lang.reflect.Proxy;

import javax.resource.spi.LocalTransaction;
import javax.transaction.xa.XAResource;

import org.teiid.connector.DataPlugin;
import org.teiid.connector.api.Connection;
import org.teiid.connector.api.ConnectorCapabilities;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.Execution;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.MetadataProvider;
import org.teiid.connector.basic.WrappedConnector.CapabilitesOverloader;
import org.teiid.connector.language.Command;
import org.teiid.connector.metadata.runtime.MetadataFactory;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

public class WrappedConnection implements Connection, MetadataProvider {

	private ConnectorEnvironment env;
	private ConnectorCapabilities caps;
	private BasicManagedConnection mc;
	boolean closed = false;
	
	public WrappedConnection(BasicManagedConnection mc, ConnectorEnvironment env) {
		this.mc = mc;
		this.env = env;
	}
	
	@Override
	public void close() throws ConnectorException {
		if (!this.closed && this.mc != null) {
			this.closed = true;
			this.mc.connectionClosed(this);
			this.mc = null;
		}
	}

	@Override
	public Execution createExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata)
			throws ConnectorException {
		return this.mc.getConnection().createExecution(command, executionContext, metadata);
	}

	@Override
	public ConnectorCapabilities getCapabilities() throws ConnectorException {
		if (this.caps == null) {
			this.caps = this.mc.getConnection().getCapabilities();
			if (caps != null && this.env.getOverrideCapabilities() != null) {
				caps = (ConnectorCapabilities) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[] {ConnectorCapabilities.class}, new CapabilitesOverloader(caps, this.env.getOverrideCapabilities()));
			}
		}
		return this.caps;
	}

	@Override
	public LocalTransaction getLocalTransaction() throws ConnectorException {
		return this.mc.getConnection().getLocalTransaction();
	}

	@Override
	public boolean isAlive() throws ConnectorException {
		return this.mc.getConnection().isAlive();
	}

	@Override
	public XAResource getXAResource() throws ConnectorException {
		return this.mc.getConnection().getXAResource();
	}
	
	@Override
	public void getConnectorMetadata(MetadataFactory metadataFactory) throws ConnectorException {
		if (this.mc.getConnection() instanceof MetadataProvider) {
			((MetadataProvider) this.mc.getConnection()).getConnectorMetadata(metadataFactory);
		} else {
			throw new ConnectorException(DataPlugin.Util.getString("WrappedConnection.no_metadata"));	//$NON-NLS-1$
		}
	}
	
	// Called by managed connection for the connection management
	void setManagedConnection(BasicManagedConnection mc) {
		this.mc = mc;
	}

}
