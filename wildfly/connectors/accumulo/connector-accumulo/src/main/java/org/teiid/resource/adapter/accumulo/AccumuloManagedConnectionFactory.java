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

package org.teiid.resource.adapter.accumulo;

import javax.resource.ResourceException;

import org.teiid.core.BundleUtil;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;

public class AccumuloManagedConnectionFactory extends BasicManagedConnectionFactory{

    private static final long serialVersionUID = 1608787576847881344L;

    public static final BundleUtil UTIL = BundleUtil.getBundleUtil(AccumuloManagedConnectionFactory.class);

    private String instanceName;
    private String zooKeeperServerList;
    private String username;
    private String password;
    private String roles;

    @Override
    public BasicConnectionFactory<AccumuloConnectionImpl> createConnectionFactory() throws ResourceException {
        return new AccumuloConnectionFactory(this);
    }

    class AccumuloConnectionFactory extends BasicConnectionFactory<AccumuloConnectionImpl>{
        private static final long serialVersionUID = 831361159531236916L;
        private AccumuloManagedConnectionFactory mcf;

        public AccumuloConnectionFactory(AccumuloManagedConnectionFactory mcf) {
            this.mcf = mcf;
        }
        @Override
        public AccumuloConnectionImpl getConnection() throws ResourceException {
            return new AccumuloConnectionImpl(this.mcf);
        }
    }

    public String getInstanceName() {
        return instanceName;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    public String getZooKeeperServerList() {
        return zooKeeperServerList;
    }

    public void setZooKeeperServerList(String zooKeeperServerList) {
        this.zooKeeperServerList = zooKeeperServerList;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getRoles() {
        return roles;
    }

    public void setRoles(String roles) {
        this.roles = roles;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((instanceName == null) ? 0 : instanceName.hashCode());
        result = prime * result
                + ((password == null) ? 0 : password.hashCode());
        result = prime * result + ((roles == null) ? 0 : roles.hashCode());
        result = prime * result
                + ((username == null) ? 0 : username.hashCode());
        result = prime * result + ((zooKeeperServerList == null) ? 0
                : zooKeeperServerList.hashCode());
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
        AccumuloManagedConnectionFactory other = (AccumuloManagedConnectionFactory) obj;
        if (instanceName == null) {
            if (other.instanceName != null)
                return false;
        } else if (!instanceName.equals(other.instanceName))
            return false;
        if (password == null) {
            if (other.password != null)
                return false;
        } else if (!password.equals(other.password))
            return false;
        if (roles == null) {
            if (other.roles != null)
                return false;
        } else if (!roles.equals(other.roles))
            return false;
        if (username == null) {
            if (other.username != null)
                return false;
        } else if (!username.equals(other.username))
            return false;
        if (zooKeeperServerList == null) {
            if (other.zooKeeperServerList != null)
                return false;
        } else if (!zooKeeperServerList.equals(other.zooKeeperServerList))
            return false;
        return true;
    }

}
