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
package org.teiid.resource.adapter.hbase;

import javax.resource.ResourceException;
import javax.resource.spi.InvalidPropertyException;

import org.teiid.core.BundleUtil;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;

public class HBaseManagedConnectionFactory extends BasicManagedConnectionFactory{
	private static final long serialVersionUID = -4945630936957298180L;

	public static final BundleUtil UTIL = BundleUtil.getBundleUtil(HBaseManagedConnectionFactory.class);
	
	private String zkQuorum;

	@Override
	@SuppressWarnings("serial")
	public BasicConnectionFactory<HBaseConnectionImpl> createConnectionFactory() throws ResourceException {
		
		if(this.zkQuorum == null) {
			throw new InvalidPropertyException(UTIL.getString("no_quorum")); 
		}
		
		return new BasicConnectionFactory<HBaseConnectionImpl>() {

			@Override
			public HBaseConnectionImpl getConnection() throws ResourceException {
				return new HBaseConnectionImpl(zkQuorum);
			}
			
		};
	}

	public String getZkQuorum() {
		return zkQuorum;
	}

	public void setZkQuorum(String zkQuorum) {
		this.zkQuorum = zkQuorum;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((this.zkQuorum == null) ? 0 : this.zkQuorum.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		HBaseManagedConnectionFactory other = (HBaseManagedConnectionFactory) obj;
		if (!checkEquals(this.zkQuorum, other.zkQuorum)) {
			return false;
		}
		return true;
	}
}
