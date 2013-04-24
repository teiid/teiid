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
package org.teiid.resource.adapter.mongodb;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import javax.resource.ResourceException;
import javax.resource.spi.InvalidPropertyException;

import org.teiid.core.BundleUtil;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;

import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

public class MongoDBManagedConnectionFactory extends BasicManagedConnectionFactory{
	private static final long serialVersionUID = -4945630936957298180L;

	public static final BundleUtil UTIL = BundleUtil.getBundleUtil(MongoDBManagedConnectionFactory.class);

	private String remoteServerList=null;
	private String username;
	private String password;
	private String database;

	@Override
	@SuppressWarnings("serial")
	public BasicConnectionFactory<MongoDBConnectionImpl> createConnectionFactory() throws ResourceException {
		if (this.remoteServerList == null) {
			throw new InvalidPropertyException(UTIL.getString("no_server")); //$NON-NLS-1$
		}
		if (this.database == null) {
			throw new InvalidPropertyException(UTIL.getString("no_database")); //$NON-NLS-1$
		}

		final List<ServerAddress> servers = getServers();

		//TODO: need to define all the properties on the ra.xml and build this correctly
		final MongoClientOptions options = MongoClientOptions.builder().build();

		return new BasicConnectionFactory<MongoDBConnectionImpl>() {
			@Override
			public MongoDBConnectionImpl getConnection() throws ResourceException {
				MongoCredential credential = null;
				if (MongoDBManagedConnectionFactory.this.username != null && MongoDBManagedConnectionFactory.this.password != null) {
					credential = MongoCredential.createMongoCRCredential(MongoDBManagedConnectionFactory.this.username, MongoDBManagedConnectionFactory.this.database, MongoDBManagedConnectionFactory.this.password.toCharArray());
				}
				return new MongoDBConnectionImpl(MongoDBManagedConnectionFactory.this.database, servers, credential, options);
			}
		};
	}

	/**
	 * Returns the <code>host:port[;host:port...]</code> list that identifies the remote servers
	 * to include in this cluster;
	 * @return <code>host:port[;host:port...]</code> list
	 */
   public String getRemoteServerList() {
        return this.remoteServerList;
    }

    /**
     * Set the list of remote servers that make up the MongoDB cluster.
     * @param remoteServerList the server list in appropriate <code>server:port;server2:port2</code> format.
     */
    public void setRemoteServerList( String remoteServerList ) {
        this.remoteServerList = remoteServerList;
    }

	public String getUsername() {
		return this.username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return this.password;
	}

	public void setPassword(String googlePassword) {
		this.password = googlePassword;
	}

	public String getDatabase() {
		return this.database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}


	protected List<ServerAddress> getServers() throws ResourceException {
		List<ServerAddress> addresses = new ArrayList<ServerAddress>();
		StringTokenizer st = new StringTokenizer(getRemoteServerList(), ";"); //$NON-NLS-1$
		while (st.hasMoreTokens()) {
			String token = st.nextToken();
			int idx = token.indexOf(':');
			if (idx < 0) {
				throw new InvalidPropertyException(UTIL.getString("no_database")); //$NON-NLS-1$
			}
			try {
				addresses.add(new ServerAddress(token.substring(0, idx), Integer.valueOf(token.substring(idx+1))));
			} catch(UnknownHostException e) {
				throw new ResourceException(e);
			}
		}
		return addresses;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((this.remoteServerList == null) ? 0 : this.remoteServerList.hashCode());
		result = prime * result + ((this.database == null) ? 0 : this.database.hashCode());
		result = prime * result + ((this.username == null) ? 0 : this.username.hashCode());
		result = prime * result + ((this.password == null) ? 0 : this.password.hashCode());
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
		MongoDBManagedConnectionFactory other = (MongoDBManagedConnectionFactory) obj;
		if (!checkEquals(this.remoteServerList, other.remoteServerList)) {
			return false;
		}
		if (!checkEquals(this.database, other.database)) {
			return false;
		}
		if (!checkEquals(this.username, other.username)) {
			return false;
		}
		if (!checkEquals(this.password, other.password)) {
			return false;
		}
		return true;
	}
}
