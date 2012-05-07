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

import java.io.Serializable;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.resource.Referenceable;
import javax.resource.ResourceException;
import javax.resource.cci.Connection;
import javax.resource.cci.ConnectionFactory;
import javax.resource.cci.ConnectionSpec;
import javax.resource.cci.RecordFactory;
import javax.resource.cci.ResourceAdapterMetaData;
import javax.resource.spi.ConnectionManager;


public class WrappedConnectionFactory implements ConnectionFactory, Referenceable, Serializable  {

	private static final long serialVersionUID = 5499157394014613035L;
	private BasicConnectionFactory delegate;
	private ConnectionManager cm;	
	private BasicManagedConnectionFactory mcf;
	private Reference reference;
	
	public WrappedConnectionFactory(BasicConnectionFactory delegate, ConnectionManager cm, BasicManagedConnectionFactory mcf) {
		this.delegate = delegate;
		this.cm = cm;
		this.mcf = mcf;
	}
	
	@Override
	public Connection getConnection() throws ResourceException {
		return (Connection)cm.allocateConnection(mcf, null);
	}


	@Override
	public void setReference(Reference arg0) {
		this.reference = arg0;
	}

	@Override
	public Reference getReference() throws NamingException {
		return this.reference;
	}

	@Override
	public Connection getConnection(ConnectionSpec arg0) throws ResourceException {
		return (Connection)cm.allocateConnection(mcf, new ConnectionRequestInfoWrapper(arg0));
	}

	@Override
	public ResourceAdapterMetaData getMetaData() throws ResourceException {
		return this.delegate.getMetaData();
	}

	@Override
	public RecordFactory getRecordFactory() throws ResourceException {
		return this.delegate.getRecordFactory();
	}
}
