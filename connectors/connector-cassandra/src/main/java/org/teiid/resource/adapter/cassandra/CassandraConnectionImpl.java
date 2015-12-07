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

import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

import javax.resource.ResourceException;

import org.teiid.core.types.BinaryType;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.resource.spi.BasicConnection;
import org.teiid.translator.cassandra.CassandraConnection;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;

/**
 * Represents a connection to Cassandra database.
 * */
public class CassandraConnectionImpl extends BasicConnection implements CassandraConnection{
	private CassandraManagedConnectionFactory config;
	private Cluster cluster = null;
	private Session session = null;
	private Metadata metadata = null;
	
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
	public ResultSet executeQuery(String query){
		return session.execute(query);
	}
	
	@Override
	public void executeBatch(List<String> updates){
		BatchStatement bs = new BatchStatement();
		for (String update : updates) {
			bs.add(new SimpleStatement(update));
		}
		session.execute(bs);
	}
	
	@Override
	public int executeBatch(String update, Iterator<? extends List<?>> values) throws ResourceException {
		PreparedStatement ps = session.prepare(update);
		BatchStatement bs = new BatchStatement();
		int count = 0;
		while (values.hasNext()) {
			Object[] bindValues = values.next().toArray();
			for (int i = 0; i < bindValues.length; i++) {
				if (bindValues[i] instanceof Blob) {
					Blob blob = (Blob)bindValues[i];
					try {
						if (blob.length() > Integer.MAX_VALUE) {
							throw new AssertionError("Blob is too large"); //$NON-NLS-1$
						}
						byte[] bytes = ((Blob)bindValues[i]).getBytes(0, (int) blob.length());
						bindValues[i] = ByteBuffer.wrap(bytes);
					} catch (SQLException e) {
						throw new ResourceException(e);
					}
				} else if (bindValues[i] instanceof BinaryType) {
					bindValues[i] = ByteBuffer.wrap(((BinaryType)bindValues[i]).getBytesDirect());
				}
			}
			BoundStatement bound = ps.bind(bindValues);
			bs.add(bound);
			count++;
		}
		session.execute(bs);
		return count;
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
	
}
