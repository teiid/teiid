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

import java.util.List;

import javax.resource.ResourceException;

import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.resource.spi.BasicConnection;
import org.teiid.translator.cassandra.CassandraConnection;

import com.datastax.driver.core.*;

/**
 * Represents a connection to Cassandra database.
 * */
public class CassandraConnectionImpl extends BasicConnection implements CassandraConnection{
	private CassandraManagedConnectionFactory config;
	private Cluster cluster = null;
	private Session session = null;
	private Metadata metadata = null;
	private ProtocolVersion version;
	
	public CassandraConnectionImpl(CassandraManagedConnectionFactory config, Metadata metadata) {
		this.config = config;
		this.metadata = metadata;
	}

	public CassandraConnectionImpl(CassandraManagedConnectionFactory config) {
		this.config = config;
		
		Cluster.Builder builder  = Cluster.builder().addContactPoint(config.getAddress());
		
		if (this.config.getUsername() != null) {
		    builder.withCredentials(this.config.getUsername(), this.config.getPassword());
		}
		
		if (this.config.getPort() != null) {
		    builder.withPort(this.config.getPort());
		}
		
		this.cluster = builder.build();
		
		this.metadata = cluster.getMetadata();
		
		this.session = cluster.connect(config.getKeyspace());
		
		this.version = this.session.getCluster().getConfiguration().getProtocolOptions().getProtocolVersionEnum();
	}

	@Override
	public void close() throws ResourceException {
		if(cluster != null){
			cluster.close();
		}
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, CassandraManagedConnectionFactory.UTIL.getString("shutting_down")); //$NON-NLS-1$
	}
	
	@Override
	public boolean isAlive() {
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, CassandraManagedConnectionFactory.UTIL.getString("alive")); //$NON-NLS-1$
		return true;
	}
	
	@Override
	public ResultSetFuture executeQuery(String query){
		return session.executeAsync(query);
	}
	
	@Override
	public ResultSetFuture executeBatch(List<String> updates){
		BatchStatement bs = new BatchStatement();
		for (String update : updates) {
			bs.add(new SimpleStatement(update));
		}
		return session.executeAsync(bs);
	}
	
	@Override
	public ResultSetFuture executeBatch(String update, List<Object[]> values) {
		PreparedStatement ps = session.prepare(update);
		BatchStatement bs = new BatchStatement();
		for (Object[] bindValues : values) {
			BoundStatement bound = ps.bind(bindValues);
			bs.add(bound);
		}
		return session.executeAsync(bs);
	}

	@Override
	public KeyspaceMetadata keyspaceInfo() throws ResourceException {
		String keyspace = config.getKeyspace();
		KeyspaceMetadata result = metadata.getKeyspace(keyspace);
		if (result == null && keyspace.length() > 2 && keyspace.charAt(0) == '"' && keyspace.charAt(keyspace.length() - 1) == '"') {
			//try unquoted
			keyspace = keyspace.substring(1, keyspace.length() - 1);
			result = metadata.getKeyspace(keyspace);
		}
		if (result == null) {
			throw new ResourceException(keyspace);
		}
		return result;
	}
	
	@Override
	public ProtocolVersion getVersion() {
		return version;
	}
	
}
