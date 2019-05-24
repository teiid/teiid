/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.resource.adapter.cassandra;

import java.util.List;
import java.util.Set;

import javax.resource.ResourceException;

import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.resource.spi.BasicConnection;
import org.teiid.translator.TranslatorException;
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
    private VersionNumber version;

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

        Set<Host> allHosts = cluster.getMetadata().getAllHosts();
        if (!allHosts.isEmpty()) {
            Host host = allHosts.iterator().next();
            this.version = host.getCassandraVersion();
        }
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
    public KeyspaceMetadata keyspaceInfo() throws TranslatorException {
        String keyspace = config.getKeyspace();
        KeyspaceMetadata result = metadata.getKeyspace(keyspace);
        if (result == null && keyspace.length() > 2 && keyspace.charAt(0) == '"' && keyspace.charAt(keyspace.length() - 1) == '"') {
            //try unquoted
            keyspace = keyspace.substring(1, keyspace.length() - 1);
            result = metadata.getKeyspace(keyspace);
        }
        if (result == null) {
            throw new TranslatorException(keyspace);
        }
        return result;
    }

    @Override
    public VersionNumber getVersion() {
        return version;
    }

}
