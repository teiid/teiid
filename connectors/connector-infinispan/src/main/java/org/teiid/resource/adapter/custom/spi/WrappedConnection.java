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
package org.teiid.resource.adapter.custom.spi;

import javax.resource.ResourceException;
import javax.resource.cci.Connection;
import javax.resource.cci.ConnectionMetaData;
import javax.resource.cci.Interaction;
import javax.resource.cci.ResultSetInfo;


public class WrappedConnection implements Connection {

	private BasicManagedConnection mc;
	boolean closed = false;
	
	public WrappedConnection(BasicManagedConnection mc) {
		this.mc = mc;
	}
	
	@Override
	public void close() throws ResourceException {
		if (!this.closed && this.mc != null) {
			this.closed = true;
			this.mc.connectionClosed(this);
			this.mc = null;
		}
	}

	// Called by managed connection for the connection management
	void setManagedConnection(BasicManagedConnection mc) {
		this.mc = mc;
	}

	@Override
	public Interaction createInteraction() throws ResourceException {
		return this.mc.getConnection().createInteraction();
	}

	@Override
	public javax.resource.cci.LocalTransaction getLocalTransaction() throws ResourceException {
		return this.mc.getConnection().getLocalTransaction();
	}

	@Override
	public ConnectionMetaData getMetaData() throws ResourceException {
		return this.mc.getConnection().getMetaData();
	}

	@Override
	public ResultSetInfo getResultSetInfo() throws ResourceException {
		return this.mc.getConnection().getResultSetInfo();
	}
	
	public Connection unwrap() throws ResourceException {
		return this.mc.getConnection();
	}

}
