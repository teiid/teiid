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

import org.teiid.cassandra.CassandraConfiguration;
import org.teiid.cassandra.CassandraConnectionFactory;
import org.teiid.core.BundleUtil;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;
import org.teiid.resource.spi.ResourceConnection;

public class CassandraManagedConnectionFactory extends BasicManagedConnectionFactory implements CassandraConfiguration {

    private static final long serialVersionUID = 6467964324032304311L;
    private String address;
    private String keyspace;
    private String username;
    private String password;
    private Integer port;

    public static final BundleUtil UTIL = BundleUtil.getBundleUtil(CassandraManagedConnectionFactory.class);

    @Override
    @SuppressWarnings("serial")
    public BasicConnectionFactory<ResourceConnection> createConnectionFactory() throws ResourceException {
        CassandraConnectionFactory connectionFactory = new CassandraConnectionFactory(this);

        return new BasicConnectionFactory<ResourceConnection>() {
            public ResourceConnection getConnection() throws ResourceException {
                return new CassandraConnectionImpl(connectionFactory);
            }
        };
    }

    @Override
    public String getKeyspace() {
        return keyspace;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    @Override
    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    @Override
    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    @Override
    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    @Override
    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((address == null) ? 0 : address.hashCode());
        result = prime * result
                + ((keyspace == null) ? 0 : keyspace.hashCode());
        result = prime * result
                + ((password == null) ? 0 : password.hashCode());
        result = prime * result + ((port == null) ? 0 : port.hashCode());
        result = prime * result
                + ((username == null) ? 0 : username.hashCode());
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
        CassandraManagedConnectionFactory other = (CassandraManagedConnectionFactory) obj;
        if (address == null) {
            if (other.address != null)
                return false;
        } else if (!address.equals(other.address))
            return false;
        if (keyspace == null) {
            if (other.keyspace != null)
                return false;
        } else if (!keyspace.equals(other.keyspace))
            return false;
        if (password == null) {
            if (other.password != null)
                return false;
        } else if (!password.equals(other.password))
            return false;
        if (port == null) {
            if (other.port != null)
                return false;
        } else if (!port.equals(other.port))
            return false;
        if (username == null) {
            if (other.username != null)
                return false;
        } else if (!username.equals(other.username))
            return false;
        return true;
    }
}
