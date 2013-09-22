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

import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.resource.spi.BasicConnection;
import org.teiid.translator.cassandra.CassandraConnection;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

/**
 * Represents a connection to Cassandra database.
 * */
public class CassandraConnectionImpl extends BasicConnection implements CassandraConnection{
	private CassandraManagedConnectionFactory config;
	private Cluster cluster = null;
	private Session session = null;
	private Metadata metadata = null;

	public CassandraConnectionImpl(CassandraManagedConnectionFactory config) {
		this.config = config;
		
		cluster = Cluster.builder()
	            .addContactPoint(config.getAddress()).build();
		
		metadata = cluster.getMetadata();
		
		session = cluster.connect(config.getKeyspace());
	}

	@Override
	public void close() throws ResourceException {
		if(cluster != null){
			cluster.shutdown();
		}
		LogManager.logInfo(LogConstants.CTX_CONNECTOR, CassandraManagedConnectionFactory.UTIL.
				getString("shutting_down")); //$NON-NLS-1$
	}
	
	@Override
	public boolean isAlive() {
		LogManager.logInfo(LogConstants.CTX_CONNECTOR, CassandraManagedConnectionFactory.UTIL.
				getString("alive")); //$NON-NLS-1$
		return true;
	}
	
	@Override
	public ResultSet executeQuery(String query){
		return session.execute(query);
	}

	@Override
	public KeyspaceMetadata keyspaceInfo() {
		return metadata.getKeyspace(config.getKeyspace());
	}

}
