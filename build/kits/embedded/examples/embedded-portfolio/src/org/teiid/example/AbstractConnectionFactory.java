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
package org.teiid.example;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.resource.ResourceException;
import javax.resource.cci.*;

public abstract class AbstractConnectionFactory<C extends javax.resource.cci.Connection> implements ConnectionFactory {
	private static final long serialVersionUID = 4906165501503764939L;

	@Override
	public void setReference(Reference reference) {
	}

	@Override
	public Reference getReference() throws NamingException {
		return null;
	}

	@Override
	public abstract C getConnection() throws ResourceException;

	@Override
	public C getConnection(ConnectionSpec properties)
			throws ResourceException {
		return null;
	}

	@Override
	public RecordFactory getRecordFactory() throws ResourceException {
		return null;
	}

	@Override
	public ResourceAdapterMetaData getMetaData() throws ResourceException {
		return null;
	}
}
