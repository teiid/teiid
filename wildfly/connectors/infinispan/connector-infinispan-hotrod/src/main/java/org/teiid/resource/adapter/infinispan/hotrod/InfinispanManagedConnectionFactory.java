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
package org.teiid.resource.adapter.infinispan.hotrod;

import javax.resource.ResourceException;

import org.infinispan.transaction.lookup.WildflyTransactionManagerLookup;
import org.teiid.infinispan.api.BaseInfinispanConnection;
import org.teiid.infinispan.api.InfinispanConfiguration;
import org.teiid.infinispan.api.InfinispanConnectionFactory;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;
import org.teiid.resource.spi.ResourceConnection;
import org.teiid.translator.TranslatorException;


public class InfinispanManagedConnectionFactory extends BasicManagedConnectionFactory implements InfinispanConfiguration {
    private static final long serialVersionUID = -4791974803005018658L;

    public static class InfinispanResourceConnection extends BaseInfinispanConnection implements ResourceConnection {

        public InfinispanResourceConnection(InfinispanConnectionFactory icf)
                throws TranslatorException {
            super(icf);
        }

    }

    private String remoteServerList;
    private String cacheName;
    private String transactionMode;

    // security
    private String saslMechanism;
    private String userName;
    private String password;
    private String authenticationRealm = "default";
    private String authenticationServerName = "infinispan";
    private String cacheTemplate;

    private String trustStoreFileName = System.getProperty("javax.net.ssl.trustStore");
    private String trustStorePassword = System.getProperty("javax.net.ssl.trustStorePassword");
    private String keyStoreFileName = System.getProperty("javax.net.ssl.keyStore");
    private String keyStorePassword = System.getProperty("javax.net.ssl.keyStorePassword");

    public String getRemoteServerList() {
        return remoteServerList;
    }

    public void setRemoteServerList(String remoteServerList) {
        this.remoteServerList = remoteServerList;
    }

    public String getCacheName() {
        return cacheName;
    }

    public void setCacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    public String getTransactionMode() {
        return transactionMode;
    }

    public void setTransactionMode(String transactionMode) {
        this.transactionMode = transactionMode;
    }

    @Override
    public BasicConnectionFactory<InfinispanResourceConnection> createConnectionFactory()
            throws ResourceException {
        try {
            return new BasicConnectionFactory<InfinispanResourceConnection>() {
                private static final long serialVersionUID = 273357440758948926L;
                InfinispanConnectionFactory icf = new InfinispanConnectionFactory(InfinispanManagedConnectionFactory.this, new WildflyTransactionManagerLookup());

                @Override
                public InfinispanResourceConnection getConnection()
                        throws ResourceException {
                    try {
                        return new InfinispanResourceConnection(icf);
                    } catch (TranslatorException e) {
                        throw new ResourceException(e);
                    }
                }
            };
        } catch (TranslatorException e) {
            throw new ResourceException(e);
        }
    }

    public String getSaslMechanism() {
        return saslMechanism;
    }

    public void setSaslMechanism(String saslMechanism) {
        this.saslMechanism = saslMechanism;
    }

    public String getUserName() {
        return userName;
    }

    public String getUsername() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getAuthenticationRealm() {
        return authenticationRealm;
    }

    public void setAuthenticationRealm(String authenticationRealm) {
        this.authenticationRealm = authenticationRealm;
    }

    public String getAuthenticationServerName() {
        return authenticationServerName;
    }

    public void setAuthenticationServerName(String authenticationServerName) {
        this.authenticationServerName = authenticationServerName;
    }

    public String getTrustStoreFileName() {
        return trustStoreFileName;
    }

    public void setTrustStoreFileName(String trustStoreFileName) {
        this.trustStoreFileName = trustStoreFileName;
    }

    public String getTrustStorePassword() {
        return trustStorePassword;
    }

    public void setTrustStorePassword(String trustStorePassword) {
        this.trustStorePassword = trustStorePassword;
    }

    public String getKeyStoreFileName() {
        return keyStoreFileName;
    }

    public void setKeyStoreFileName(String keyStoreFileName) {
        this.keyStoreFileName = keyStoreFileName;
    }

    public String getKeyStorePassword() {
        return keyStorePassword;
    }

    public void setKeyStorePassword(String keyStorePassword) {
        this.keyStorePassword = keyStorePassword;
    }

    public String getCacheTemplate() {
        return cacheTemplate;
    }

    public void setCacheTemplate(String cacheTemplate) {
        this.cacheTemplate = cacheTemplate;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((authenticationRealm == null) ? 0
                : authenticationRealm.hashCode());
        result = prime * result + ((authenticationServerName == null) ? 0
                : authenticationServerName.hashCode());
        result = prime * result
                + ((cacheName == null) ? 0 : cacheName.hashCode());
        result = prime * result + ((keyStoreFileName == null) ? 0
                : keyStoreFileName.hashCode());
        result = prime * result + ((keyStorePassword == null) ? 0
                : keyStorePassword.hashCode());
        result = prime * result
                + ((password == null) ? 0 : password.hashCode());
        result = prime * result + ((remoteServerList == null) ? 0
                : remoteServerList.hashCode());
        result = prime * result
                + ((saslMechanism == null) ? 0 : saslMechanism.hashCode());
        result = prime * result + ((trustStoreFileName == null) ? 0
                : trustStoreFileName.hashCode());
        result = prime * result + ((trustStorePassword == null) ? 0
                : trustStorePassword.hashCode());
        result = prime * result
                + ((userName == null) ? 0 : userName.hashCode());
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
        InfinispanManagedConnectionFactory other = (InfinispanManagedConnectionFactory) obj;
        if (authenticationRealm == null) {
            if (other.authenticationRealm != null)
                return false;
        } else if (!authenticationRealm.equals(other.authenticationRealm))
            return false;
        if (authenticationServerName == null) {
            if (other.authenticationServerName != null)
                return false;
        } else if (!authenticationServerName
                .equals(other.authenticationServerName))
            return false;
        if (cacheName == null) {
            if (other.cacheName != null)
                return false;
        } else if (!cacheName.equals(other.cacheName))
            return false;
        if (keyStoreFileName == null) {
            if (other.keyStoreFileName != null)
                return false;
        } else if (!keyStoreFileName.equals(other.keyStoreFileName))
            return false;
        if (keyStorePassword == null) {
            if (other.keyStorePassword != null)
                return false;
        } else if (!keyStorePassword.equals(other.keyStorePassword))
            return false;
        if (password == null) {
            if (other.password != null)
                return false;
        } else if (!password.equals(other.password))
            return false;
        if (remoteServerList == null) {
            if (other.remoteServerList != null)
                return false;
        } else if (!remoteServerList.equals(other.remoteServerList))
            return false;
        if (saslMechanism == null) {
            if (other.saslMechanism != null)
                return false;
        } else if (!saslMechanism.equals(other.saslMechanism))
            return false;
        if (trustStoreFileName == null) {
            if (other.trustStoreFileName != null)
                return false;
        } else if (!trustStoreFileName.equals(other.trustStoreFileName))
            return false;
        if (trustStorePassword == null) {
            if (other.trustStorePassword != null)
                return false;
        } else if (!trustStorePassword.equals(other.trustStorePassword))
            return false;
        if (userName == null) {
            if (other.userName != null)
                return false;
        } else if (!userName.equals(other.userName))
            return false;
        if (transactionMode == null) {
            if (other.transactionMode != null)
                return false;
        } else if (!transactionMode.equals(other.transactionMode))
            return false;
        return true;
    }
}
