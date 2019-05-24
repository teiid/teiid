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
package org.teiid.resource.adapter.ldap;

import javax.resource.ResourceException;

import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;

public class LDAPManagedConnectionFactory extends BasicManagedConnectionFactory {

    private static final long serialVersionUID = -1832915223199053471L;

    private String ldapAdminUserDN;
    private String ldapAdminUserPassword;
    private String ldapUrl;
    private Long ldapTxnTimeoutInMillis;
    private String ldapContextFactory = "com.sun.jndi.ldap.LdapCtxFactory"; //$NON-NLS-1$
    private String ldapAuthType = "simple"; //$NON-NLS-1$

    @Override
    @SuppressWarnings("serial")
    public BasicConnectionFactory<LDAPConnectionImpl> createConnectionFactory() throws ResourceException {
        return new BasicConnectionFactory<LDAPConnectionImpl>() {
            @Override
            public LDAPConnectionImpl getConnection() throws ResourceException {
                ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
                try {
                    Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
                    return new LDAPConnectionImpl(LDAPManagedConnectionFactory.this);
                }
                finally {
                    Thread.currentThread().setContextClassLoader(contextClassLoader);
                }
            }
        };
    }

    public String getLdapAdminUserDN() {
        return ldapAdminUserDN;
    }

    public void setLdapAdminUserDN(String ldapAdminUserDN) {
        this.ldapAdminUserDN = ldapAdminUserDN;
    }

    public String getLdapAdminUserPassword() {
        return ldapAdminUserPassword;
    }

    public void setLdapAdminUserPassword(String ldapAdminUserPassword) {
        this.ldapAdminUserPassword = ldapAdminUserPassword;
    }

    public Long getLdapTxnTimeoutInMillis() {
        return ldapTxnTimeoutInMillis;
    }

    public void setLdapTxnTimeoutInMillis(Long ldapTxnTimeoutInMillis) {
        this.ldapTxnTimeoutInMillis = ldapTxnTimeoutInMillis.longValue();
    }

    public String getLdapUrl() {
        return ldapUrl;
    }

    public void setLdapUrl(String ldapUrl) {
        this.ldapUrl = ldapUrl;
    }

    public String getLdapContextFactory() {
        return ldapContextFactory;
    }

    public void setLdapContextFactory(String ldapContextFactory) {
        this.ldapContextFactory = ldapContextFactory;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((ldapAdminUserDN == null) ? 0 : ldapAdminUserDN.hashCode());
        result = prime * result    + ((ldapAdminUserPassword == null) ? 0 : ldapAdminUserPassword.hashCode());
        result = prime * result    + ((ldapContextFactory == null) ? 0 : ldapContextFactory.hashCode());
        result = prime * result    + (ldapTxnTimeoutInMillis == null ? 0 : (int) (ldapTxnTimeoutInMillis ^ (ldapTxnTimeoutInMillis >>> 32)));
        result = prime * result + ((ldapUrl == null) ? 0 : ldapUrl.hashCode());
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
        LDAPManagedConnectionFactory other = (LDAPManagedConnectionFactory) obj;
        if (!checkEquals(this.ldapAdminUserDN, other.ldapAdminUserDN)) {
            return false;
        }
        if (!checkEquals(this.ldapAdminUserPassword, other.ldapAdminUserPassword)) {
            return false;
        }
        if (!checkEquals(this.ldapContextFactory, other.ldapContextFactory)) {
            return false;
        }
        if (!checkEquals(this.ldapTxnTimeoutInMillis, other.ldapTxnTimeoutInMillis)) {
            return false;
        }
        if (!checkEquals(this.ldapUrl, other.ldapUrl)) {
            return false;
        }
        if (!checkEquals(this.ldapAuthType, other.ldapAuthType)) {
            return false;
        }
        return true;
    }

    public String getLdapAuthType() {
        return ldapAuthType;
    }

    public void setLdapAuthType(String ldapAuthType) {
        this.ldapAuthType = ldapAuthType;
    }

}
