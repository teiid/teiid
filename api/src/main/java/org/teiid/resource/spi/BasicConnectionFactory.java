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

package org.teiid.resource.spi;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.resource.ResourceException;
import javax.resource.cci.ConnectionFactory;
import javax.resource.cci.ConnectionSpec;
import javax.resource.cci.RecordFactory;
import javax.resource.cci.ResourceAdapterMetaData;

public abstract class BasicConnectionFactory<T extends BasicConnection> implements ConnectionFactory {
	private static final long serialVersionUID = 2900581028589520388L;
	private Reference reference;
	
	@Override
	public abstract T getConnection() throws ResourceException;
	
	@Override
	public BasicConnection getConnection(ConnectionSpec arg0) throws ResourceException {
		throw new ResourceException("This operation not supported"); //$NON-NLS-1$;
	}

	@Override
	public ResourceAdapterMetaData getMetaData() throws ResourceException {
		throw new ResourceException("This operation not supported"); //$NON-NLS-1$;
	}

	@Override
	public RecordFactory getRecordFactory() throws ResourceException {
		throw new ResourceException("This operation not supported"); //$NON-NLS-1$
	}

	@Override
	public void setReference(Reference arg0) {
		this.reference = arg0; 
	}

	@Override
	public Reference getReference() throws NamingException {
		return this.reference;
	}
}
