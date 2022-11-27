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
package org.teiid.resource.adapter.solr;

import javax.resource.ResourceException;

import org.teiid.resource.spi.BasicManagedConnectionFactory;

public class SolrManagedConnectionFactory extends BasicManagedConnectionFactory {

    private static final long serialVersionUID = -2751565394772750705L;
    private String url;
    private Integer soTimeout;
    private Boolean allowCompression;
    private Integer connTimeout; // min 5 seconds to establish TCP
    private Integer maxConns;
    private Integer maxRetries;
    private String coreName;
    private String authPassword; // httpbasic - password
    private String authUserName; // httpbasic - username

    @Override
    public SolrConnectionFactory createConnectionFactory()
            throws ResourceException {
        return new SolrConnectionFactory(SolrManagedConnectionFactory.this);
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Integer getSoTimeout() {
        return soTimeout;
    }

    public void setSoTimeout(Integer soTimeout) {
        this.soTimeout = soTimeout;
    }

    public Boolean getAllowCompression() {
        return allowCompression;
    }

    public void setAllowCompression(Boolean allowCompression) {
        this.allowCompression = allowCompression;
    }

    public Integer getConnTimeout() {
        return connTimeout;
    }

    public void setConnTimeout(Integer connTimeout) {
        this.connTimeout = connTimeout;
    }

    public Integer getMaxConns() {
        return maxConns;
    }

    public void setMaxConns(Integer maxConns) {
        this.maxConns = maxConns;
    }

    public Integer getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(Integer maxRetries) {
        this.maxRetries = maxRetries;
    }

    public String getCoreName() {
        return coreName;
    }

    public void setCoreName(String coreName) {
        this.coreName = coreName;
    }

    public String getAuthPassword() {
        return authPassword;
    }

    public String getAuthUserName() {
        return authUserName;
    }

    public void setAuthPassword(String authPassword) {
        this.authPassword = authPassword;
    }

    public void setAuthUserName(String authUserName) {
        this.authUserName = authUserName;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((allowCompression == null) ? 0
                : allowCompression.hashCode());
        result = prime * result
                + ((authPassword == null) ? 0 : authPassword.hashCode());
        result = prime * result
                + ((authUserName == null) ? 0 : authUserName.hashCode());
        result = prime * result
                + ((connTimeout == null) ? 0 : connTimeout.hashCode());
        result = prime * result
                + ((coreName == null) ? 0 : coreName.hashCode());
        result = prime * result
                + ((maxConns == null) ? 0 : maxConns.hashCode());
        result = prime * result
                + ((maxRetries == null) ? 0 : maxRetries.hashCode());
        result = prime * result
                + ((soTimeout == null) ? 0 : soTimeout.hashCode());
        result = prime * result + ((url == null) ? 0 : url.hashCode());
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
        SolrManagedConnectionFactory other = (SolrManagedConnectionFactory) obj;
        if (allowCompression == null) {
            if (other.allowCompression != null)
                return false;
        } else if (!allowCompression.equals(other.allowCompression))
            return false;
        if (authPassword == null) {
            if (other.authPassword != null)
                return false;
        } else if (!authPassword.equals(other.authPassword))
            return false;
        if (authUserName == null) {
            if (other.authUserName != null)
                return false;
        } else if (!authUserName.equals(other.authUserName))
            return false;
        if (connTimeout == null) {
            if (other.connTimeout != null)
                return false;
        } else if (!connTimeout.equals(other.connTimeout))
            return false;
        if (coreName == null) {
            if (other.coreName != null)
                return false;
        } else if (!coreName.equals(other.coreName))
            return false;
        if (maxConns == null) {
            if (other.maxConns != null)
                return false;
        } else if (!maxConns.equals(other.maxConns))
            return false;
        if (maxRetries == null) {
            if (other.maxRetries != null)
                return false;
        } else if (!maxRetries.equals(other.maxRetries))
            return false;
        if (soTimeout == null) {
            if (other.soTimeout != null)
                return false;
        } else if (!soTimeout.equals(other.soTimeout))
            return false;
        if (url == null) {
            if (other.url != null)
                return false;
        } else if (!url.equals(other.url))
            return false;
        return true;
    }


}