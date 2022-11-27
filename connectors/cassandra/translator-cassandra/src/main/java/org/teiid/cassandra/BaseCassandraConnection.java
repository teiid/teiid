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

package org.teiid.cassandra;

import java.util.List;

import org.teiid.translator.TranslatorException;
import org.teiid.translator.cassandra.CassandraConnection;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.VersionNumber;

public class BaseCassandraConnection implements CassandraConnection {

    private CassandraConfiguration config;
    private Session session = null;
    private Metadata metadata = null;
    private VersionNumber version;

    public BaseCassandraConnection(CassandraConnectionFactory connectionFactory) {
        this.config = connectionFactory.getConfig();
        Cluster cluster = connectionFactory.getCluster();
        this.metadata = cluster.getMetadata();
        this.version = connectionFactory.getVersion();
        this.session = cluster.connect(config.getKeyspace());
    }

    protected BaseCassandraConnection(CassandraConfiguration config, Metadata metadata) {
        this.config = config;

        this.metadata = metadata;
    }

    @Override
    public ResultSetFuture executeQuery(String query) {
        return session.executeAsync(query);
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
    public ResultSetFuture executeBatch(List<String> updates) {
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
    public VersionNumber getVersion() {
        return version;
    }

    @Override
    public void close() {
        if(session != null){
            session.close();
        }
    }
}
