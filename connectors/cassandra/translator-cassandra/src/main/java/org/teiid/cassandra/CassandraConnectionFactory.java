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

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.VersionNumber;

public class CassandraConnectionFactory implements Closeable {

    private final CassandraConfiguration config;
    private Cluster cluster;
    private volatile VersionNumber version;

    public CassandraConnectionFactory(CassandraConfiguration config) {
        this.config = config;
        Cluster.Builder builder = Cluster.builder().addContactPoint(config.getAddress());

        if (this.config.getUsername() != null) {
            builder.withCredentials(this.config.getUsername(), this.config.getPassword());
        }

        if (this.config.getPort() != null) {
            builder.withPort(this.config.getPort());
        }

        this.cluster = builder.build();
    }

    public Cluster getCluster() {
        return cluster;
    }

    public VersionNumber getVersion() {
        if (version == null) {
            Set<Host> allHosts = cluster.getMetadata().getAllHosts();
            if (!allHosts.isEmpty()) {
                Host host = allHosts.iterator().next();
                this.version = host.getCassandraVersion();
            }
        }
        return version;
    }

    public CassandraConfiguration getConfig() {
        return config;
    }

    @Override
    public void close() throws IOException {
        cluster.close();
    }

}
