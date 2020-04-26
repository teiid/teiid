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

import javax.resource.ResourceException;

import org.teiid.cassandra.BaseCassandraConnection;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;

import com.datastax.driver.core.*;
import org.teiid.translator.cassandra.CassandraConnection;

/**
 * Represents a connection to Cassandra database.
 * */
public class CassandraConnectionImpl extends BaseCassandraConnection implements CassandraConnection {
    private CassandraManagedConnectionFactory config;

    public CassandraConnectionImpl(String keyspace) {
        super(keyspace);
    }

    public CassandraConnectionImpl(CassandraManagedConnectionFactory config, Metadata metadata) {
        super(getCluster(config),config.getKeyspace(), metadata);
        this.config = config;
    }

    public CassandraConnectionImpl(CassandraManagedConnectionFactory config) {
        super(getCluster(config), config.getKeyspace());
        this.config = config;
    }

    private static Cluster getCluster(CassandraManagedConnectionFactory config) {
        Cluster.Builder builder  = Cluster.builder().addContactPoint(config.getAddress());

        if (config.getUsername() != null) {
            builder.withCredentials(config.getUsername(), config.getPassword());
        }

        if (config.getPort() != null) {
            builder.withPort(config.getPort());
        }

        return builder.build();
    }

    @Override
    public void close() throws ResourceException, Exception {
        super.close();
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, CassandraManagedConnectionFactory.UTIL.getString("shutting_down")); //$NON-NLS-1$
    }


    public boolean isAlive() {
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, CassandraManagedConnectionFactory.UTIL.getString("alive")); //$NON-NLS-1$
        return true;
    }

}
