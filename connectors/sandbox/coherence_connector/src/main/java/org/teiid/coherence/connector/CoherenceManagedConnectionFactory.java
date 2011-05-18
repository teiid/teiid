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
package org.teiid.coherence.connector;

import javax.resource.ResourceException;

import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;

public class CoherenceManagedConnectionFactory extends BasicManagedConnectionFactory {
	
	private static final long serialVersionUID = -1832915223199053471L;

	private String cacheName = "Trades";
	
	@Override
	public BasicConnectionFactory createConnectionFactory() throws ResourceException {
		return new BasicConnectionFactory() {

			private static final long serialVersionUID = 1L;

			@Override
			public CoherenceConnectionImpl getConnection() throws ResourceException {
				return new CoherenceConnectionImpl(CoherenceManagedConnectionFactory.this);
			}
		};
	}	
	
	public String getCacheName() {
		return cacheName;
	}
	
	public void setCacheName(String cachename) {
		this.cacheName = cachename;
	}
	
}
