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
import javax.resource.cci.LocalTransaction;
import javax.resource.cci.ResultSetInfo;
import javax.resource.spi.ManagedConnection;
import javax.transaction.xa.XAResource;

public abstract class BasicConnection implements Connection {

	@Override
	public Interaction createInteraction() throws ResourceException {
		throw new ResourceException("This operation not supported"); //$NON-NLS-1$
	}

	@Override
	public LocalTransaction getLocalTransaction() throws ResourceException {
		return null;
	}

	@Override
	public ConnectionMetaData getMetaData() throws ResourceException {
		throw new ResourceException("This operation not supported"); //$NON-NLS-1$
	}

	@Override
	public ResultSetInfo getResultSetInfo() throws ResourceException {
		throw new ResourceException("This operation not supported"); //$NON-NLS-1$
	}
	
	public XAResource getXAResource() throws ResourceException {
		return null;
	}
	
	/**
	 * Tests the connection to see if it is still valid.
	 * @return
	 */
	public boolean isAlive() {
		return true;
	}
	
	/**
	 * Called by the {@link ManagedConnection} to indicate the physical connection
	 * should be cleaned up for reuse.
	 */
	public void cleanUp() {
		
	}

}
