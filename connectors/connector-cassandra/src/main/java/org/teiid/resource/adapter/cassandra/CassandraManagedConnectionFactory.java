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

package org.teiid.resource.adapter.cassandra;

import javax.resource.ResourceException;

import org.teiid.core.BundleUtil;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;

public class CassandraManagedConnectionFactory extends BasicManagedConnectionFactory{

	private static final long serialVersionUID = 6467964324032304311L;
	private String address;
	private String keyspace;
	
	public static final BundleUtil UTIL = BundleUtil.getBundleUtil(CassandraManagedConnectionFactory.class);
	
	@Override
	@SuppressWarnings("serial")
	public BasicConnectionFactory<CassandraConnectionImpl> createConnectionFactory() throws ResourceException {
		return new BasicConnectionFactory<CassandraConnectionImpl>() {
			@Override
			public CassandraConnectionImpl getConnection() throws ResourceException {
				return new CassandraConnectionImpl(CassandraManagedConnectionFactory.this);
			}
		};
	}

	public String getKeyspace() {
		return keyspace;
	}

	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((address == null) ? 0 : address.hashCode());
		result = prime * result
				+ ((keyspace == null) ? 0 : keyspace.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CassandraManagedConnectionFactory other = (CassandraManagedConnectionFactory) obj;
		if (address == null) {
			if (other.address != null)
				return false;
		} else if (!address.equals(other.address))
			return false;
		if (keyspace == null) {
			if (other.keyspace != null)
				return false;
		} else if (!keyspace.equals(other.keyspace))
			return false;
		return true;
	}

	
}
