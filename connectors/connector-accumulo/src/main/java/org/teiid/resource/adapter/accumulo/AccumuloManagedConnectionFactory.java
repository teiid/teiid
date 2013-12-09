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

package org.teiid.resource.adapter.accumulo;

import javax.resource.ResourceException;

import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.teiid.core.BundleUtil;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;

public class AccumuloManagedConnectionFactory extends BasicManagedConnectionFactory{

	private static final long serialVersionUID = 1608787576847881344L;

	public static final BundleUtil UTIL = BundleUtil.getBundleUtil(AccumuloManagedConnectionFactory.class);
	
	private String instanceName;
	private String zooKeeperServerList;
	private String username;
	private String password;
	private String roles;
	
	@Override
	public BasicConnectionFactory<AccumuloConnectionImpl> createConnectionFactory() throws ResourceException {
		return new AccumuloConnectionFactory(this);
	}

	class AccumuloConnectionFactory extends BasicConnectionFactory<AccumuloConnectionImpl>{
		private static final long serialVersionUID = 831361159531236916L;
		private ZooKeeperInstance instance;
		private AccumuloManagedConnectionFactory mcf;
		
		public AccumuloConnectionFactory(AccumuloManagedConnectionFactory mcf) {
			this.mcf = mcf;
			this.instance = new ZooKeeperInstance(mcf.getInstanceName(), mcf.getZooKeeperServerList());		
		}
		@Override
		public AccumuloConnectionImpl getConnection() throws ResourceException {
			return new AccumuloConnectionImpl(this.mcf, this.instance);
		}
	}
	
	public String getInstanceName() {
		return instanceName;
	}

	public void setInstanceName(String instanceName) {
		this.instanceName = instanceName;
	}

	public String getZooKeeperServerList() {
		return zooKeeperServerList;
	}

	public void setZooKeeperServerList(String zooKeeperServerList) {
		this.zooKeeperServerList = zooKeeperServerList;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getRoles() {
		return roles;
	}

	public void setRoles(String roles) {
		this.roles = roles;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((instanceName == null) ? 0 : instanceName.hashCode());
		result = prime * result
				+ ((username == null) ? 0 : username.hashCode());
		result = prime
				* result
				+ ((zooKeeperServerList == null) ? 0 : zooKeeperServerList
						.hashCode());
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
		AccumuloManagedConnectionFactory other = (AccumuloManagedConnectionFactory) obj;
		if (instanceName == null) {
			if (other.instanceName != null)
				return false;
		} else if (!instanceName.equals(other.instanceName))
			return false;
		if (username == null) {
			if (other.username != null)
				return false;
		} else if (!username.equals(other.username))
			return false;
		if (zooKeeperServerList == null) {
			if (other.zooKeeperServerList != null)
				return false;
		} else if (!zooKeeperServerList.equals(other.zooKeeperServerList))
			return false;
		return true;
	}
}
