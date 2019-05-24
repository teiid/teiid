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
package org.teiid.resource.adapter.couchbase;

import java.util.concurrent.TimeUnit;

import javax.resource.ResourceException;
import javax.resource.spi.InvalidPropertyException;

import org.teiid.core.BundleUtil;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;

import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.consistency.ScanConsistency;

/**
 * Represents a managed connection factory instance for create {@code CouchbaseConnection}.
 *
 * @author kylin
 *
 */
public class CouchbaseManagedConnectionFactory extends BasicManagedConnectionFactory{

    private static final long serialVersionUID = 8822399069779170119L;

    public static final BundleUtil UTIL = BundleUtil.getBundleUtil(CouchbaseManagedConnectionFactory.class);

    private Long managementTimeout = TimeUnit.SECONDS.toMillis(75);

    private Long queryTimeout = TimeUnit.SECONDS.toMillis(75);

    private Long viewTimeout = TimeUnit.SECONDS.toMillis(75);

    private Long kvTimeout = Long.valueOf(2500);

    private Long searchTimeout = TimeUnit.SECONDS.toMillis(75);

    private Long connectTimeout = TimeUnit.SECONDS.toMillis(5);

    private String scanConsistency = ScanConsistency.NOT_BOUNDED.name();

    private Boolean dnsSrvEnabled = false;

    /**
     * The connection string to identify the remote cluster
     */
    private String connectionString = null;

    /**
     * The Keyspace/Bucket in Couchbase Server
     */
    private String keyspace = null;

    private String namespace = null;

    /**
     * The Keyspace/Bucket password in Couchbase Server
     */
    private String password = null;

    /**
     * Pair with connectTimeout, allowed value including MILLISECONDS, SECONDS, etc.
     */
    private String timeUnit = null;


    public Long getManagementTimeout() {
        return managementTimeout;
    }

    public void setManagementTimeout(Long managementTimeout) {
        this.managementTimeout = managementTimeout;
    }

    public Long getQueryTimeout() {
        return queryTimeout;
    }

    public void setQueryTimeout(Long queryTimeout) {
        this.queryTimeout = queryTimeout;
    }

    public Long getViewTimeout() {
        return viewTimeout;
    }

    public void setViewTimeout(Long viewTimeout) {
        this.viewTimeout = viewTimeout;
    }

    public Long getKvTimeout() {
        return kvTimeout;
    }

    public void setKvTimeout(Long kvTimeout) {
        this.kvTimeout = kvTimeout;
    }

    public Long getSearchTimeout() {
        return searchTimeout;
    }

    public void setSearchTimeout(Long searchTimeout) {
        this.searchTimeout = searchTimeout;
    }

    public Long getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(Long connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public Boolean getDnsSrvEnabled() {
        return dnsSrvEnabled;
    }

    public void setDnsSrvEnabled(Boolean dnsSrvEnabled) {
        this.dnsSrvEnabled = dnsSrvEnabled;
    }

    public String getConnectionString() {
        return connectionString;
    }

    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getTimeUnit() {
        return timeUnit;
    }

    public void setTimeUnit(String timeUnit) {
        this.timeUnit = timeUnit;
    }

    public String getScanConsistency() {
        return scanConsistency;
    }

    public void setScanConsistency(String scanConsistency) {
        this.scanConsistency = scanConsistency;
    }

    @SuppressWarnings("serial")
    @Override
    public BasicConnectionFactory<CouchbaseConnectionImpl> createConnectionFactory() throws ResourceException {

        final CouchbaseEnvironment environment = DefaultCouchbaseEnvironment.builder()
                .managementTimeout(managementTimeout)
                .queryTimeout(queryTimeout)
                .viewTimeout(viewTimeout)
                .kvTimeout(kvTimeout)
                .searchTimeout(searchTimeout)
                .connectTimeout(connectTimeout)
                .dnsSrvEnabled(dnsSrvEnabled)
                .build();

        if (this.connectionString == null) {
            throw new InvalidPropertyException(UTIL.getString("no_server")); //$NON-NLS-1$
        }

        if (this.keyspace == null) {
            throw new InvalidPropertyException(UTIL.getString("no_keyspace")); //$NON-NLS-1$
        }

        if (this.namespace == null) {
            throw new InvalidPropertyException(UTIL.getString("no_namespace")); //$NON-NLS-1$
        }

        final ScanConsistency consistency = ScanConsistency.valueOf(scanConsistency);

        TimeUnit unit = TimeUnit.MILLISECONDS;
        if(this.timeUnit != null) {
            try {
                unit = TimeUnit.valueOf(timeUnit);
            } catch (IllegalArgumentException e) {
                throw new InvalidPropertyException(UTIL.getString("invalid_timeUnit", timeUnit)); //$NON-NLS-1$
            }
        }
        final TimeUnit timeoutUnit = unit;

        return new BasicConnectionFactory<CouchbaseConnectionImpl>(){

            @Override
            public CouchbaseConnectionImpl getConnection() throws ResourceException {
                return new CouchbaseConnectionImpl(environment, connectionString, keyspace, password, timeoutUnit, namespace, consistency);
            }};

    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((connectTimeout == null) ? 0 : connectTimeout.hashCode());
        result = prime * result + ((connectionString == null) ? 0
                : connectionString.hashCode());
        result = prime * result
                + ((dnsSrvEnabled == null) ? 0 : dnsSrvEnabled.hashCode());
        result = prime * result
                + ((keyspace == null) ? 0 : keyspace.hashCode());
        result = prime * result
                + ((kvTimeout == null) ? 0 : kvTimeout.hashCode());
        result = prime * result + ((managementTimeout == null) ? 0
                : managementTimeout.hashCode());
        result = prime * result
                + ((namespace == null) ? 0 : namespace.hashCode());
        result = prime * result
                + ((password == null) ? 0 : password.hashCode());
        result = prime * result
                + ((queryTimeout == null) ? 0 : queryTimeout.hashCode());
        result = prime * result
                + ((scanConsistency == null) ? 0 : scanConsistency.hashCode());
        result = prime * result
                + ((searchTimeout == null) ? 0 : searchTimeout.hashCode());
        result = prime * result
                + ((timeUnit == null) ? 0 : timeUnit.hashCode());
        result = prime * result
                + ((viewTimeout == null) ? 0 : viewTimeout.hashCode());
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
        CouchbaseManagedConnectionFactory other = (CouchbaseManagedConnectionFactory) obj;
        if (connectTimeout == null) {
            if (other.connectTimeout != null)
                return false;
        } else if (!connectTimeout.equals(other.connectTimeout))
            return false;
        if (connectionString == null) {
            if (other.connectionString != null)
                return false;
        } else if (!connectionString.equals(other.connectionString))
            return false;
        if (dnsSrvEnabled == null) {
            if (other.dnsSrvEnabled != null)
                return false;
        } else if (!dnsSrvEnabled.equals(other.dnsSrvEnabled))
            return false;
        if (keyspace == null) {
            if (other.keyspace != null)
                return false;
        } else if (!keyspace.equals(other.keyspace))
            return false;
        if (kvTimeout == null) {
            if (other.kvTimeout != null)
                return false;
        } else if (!kvTimeout.equals(other.kvTimeout))
            return false;
        if (managementTimeout == null) {
            if (other.managementTimeout != null)
                return false;
        } else if (!managementTimeout.equals(other.managementTimeout))
            return false;
        if (namespace == null) {
            if (other.namespace != null)
                return false;
        } else if (!namespace.equals(other.namespace))
            return false;
        if (password == null) {
            if (other.password != null)
                return false;
        } else if (!password.equals(other.password))
            return false;
        if (queryTimeout == null) {
            if (other.queryTimeout != null)
                return false;
        } else if (!queryTimeout.equals(other.queryTimeout))
            return false;
        if (scanConsistency == null) {
            if (other.scanConsistency != null)
                return false;
        } else if (!scanConsistency.equals(other.scanConsistency))
            return false;
        if (searchTimeout == null) {
            if (other.searchTimeout != null)
                return false;
        } else if (!searchTimeout.equals(other.searchTimeout))
            return false;
        if (timeUnit == null) {
            if (other.timeUnit != null)
                return false;
        } else if (!timeUnit.equals(other.timeUnit))
            return false;
        if (viewTimeout == null) {
            if (other.viewTimeout != null)
                return false;
        } else if (!viewTimeout.equals(other.viewTimeout))
            return false;
        return true;
    }

}
