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

package org.teiid.transport;

import java.io.IOException;
import java.security.GeneralSecurityException;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;

import org.teiid.net.socket.SocketUtil;



public class SSLConfiguration {

    public static final String ONEWAY = "1-way"; //$NON-NLS-1$ - one way is the default
    public static final String TWOWAY = "2-way"; //$NON-NLS-1$
    @Deprecated
    public static final String ANONYMOUS = "anonymous"; //$NON-NLS-1$

    public enum ClientAuth {
        NONE(ONEWAY),
        WANT(null),
        NEED(TWOWAY);

        private String oldValue;

        private ClientAuth(String oldValue) {
            this.oldValue = oldValue;
        }

        public String getOldValue() {
            return oldValue;
        }
    }

    public static final String LOGIN = "login"; //$NON-NLS-1$
    public static final String DISABLED = "disabled"; //$NON-NLS-1$
    public static final String ENABLED = "enabled"; //$NON-NLS-1$

    private static final String DEFAULT_KEYSTORE_TYPE = "JKS"; //$NON-NLS-1$

    /*
     * External SSL resource settings
     */
    private String mode = LOGIN;
    private String sslProtocol = SocketUtil.DEFAULT_PROTOCOL;
    private String keyManagerFactoryAlgorithm = KeyManagerFactory.getDefaultAlgorithm();
    private String keyStoreType = DEFAULT_KEYSTORE_TYPE;
    private String keyStoreFileName;
    private String keyStorePassword = ""; //$NON-NLS-1$
    private String trustStoreFileName;
    private String trustStorePassword = ""; //$NON-NLS-1$
    private ClientAuth authenticationMode = ClientAuth.NONE;
    private String[] enabledCipherSuites;
    private String keyAlias;
    private String keyPassword;
    private boolean truststoreCheckExpired;
    private boolean disableTrustManager;

    public SSLEngine getServerSSLEngine() throws IOException, GeneralSecurityException {
        if (!isSslEnabled()) {
            return null;
        }

        // Use the SSLContext to create an SSLServerSocketFactory.
        SSLContext context = null;

        context = SocketUtil.getSSLContext(getKeyManagers(),
                this.disableTrustManager ? getTrustAllManagers() : getTrustManagers(), this.sslProtocol);

        SSLEngine result = context.createSSLEngine();
        result.setUseClientMode(false);
        if (this.enabledCipherSuites != null) {
            result.setEnabledCipherSuites(this.enabledCipherSuites);
        }
        switch (authenticationMode) {
        case NEED:
            result.setNeedClientAuth(true);
            break;
        case WANT:
            result.setWantClientAuth(true);
            break;
        default:
            break;
        }

        return result;
    }

    public boolean isClientEncryptionEnabled() {
        return LOGIN.equalsIgnoreCase(mode);
    }

    public boolean isSslEnabled() {
        return ENABLED.equalsIgnoreCase(mode);
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public void setKeystoreFilename(String value) {
        this.keyStoreFileName = value;
    }

    public void setKeystorePassword(String value) {
        this.keyStorePassword = value;
    }

    public void setKeystoreType(String value) {
        this.keyStoreType = value;
    }

    public void setSslProtocol(String value) {
        this.sslProtocol = value;
    }

    public void setKeymanagementAlgorithm(String value) {
        this.keyManagerFactoryAlgorithm = value;
    }

    public void setTruststoreFilename(String value) {
        this.trustStoreFileName = value;
    }

    public void setTruststorePassword(String value) {
        this.trustStorePassword = value;
    }

    public void setAuthenticationMode(ClientAuth value) {
        this.authenticationMode = value;
    }

    public void setAuthenticationMode(String value) {
        for (ClientAuth auth : ClientAuth.values()) {
            if (auth.name().equals(value) || value.equals(auth.oldValue)) {
                this.authenticationMode = auth;
                return;
            }
        }
        if (value.equals(ANONYMOUS)) {
            this.authenticationMode = ClientAuth.NONE;
            this.enabledCipherSuites = new String[] {SocketUtil.ANON_CIPHER_SUITE};
            return;
        }
        throw new IllegalArgumentException("Unknown authentication mode"); //$NON-NLS-1$
    }

    public ClientAuth getAuthenticationMode() {
        return authenticationMode;
    }

    public void setEnabledCipherSuites(String enabledCipherSuites) {
        this.enabledCipherSuites = enabledCipherSuites.split(","); //$NON-NLS-1$
        for (int i = 0; i < this.enabledCipherSuites.length; i++) {
            this.enabledCipherSuites[i] = this.enabledCipherSuites[i].trim();
        }
    }

    public String[] getEnabledCipherSuitesAsArray() {
        return enabledCipherSuites;
    }

    public void setKeystoreKeyAlias(String alias) {
        this.keyAlias = alias;
    }

    public void setKeystoreKeyPassword(String keyPassword) {
        this.keyPassword = keyPassword;
    }

    public boolean isTruststoreCheckExpired() {
        return truststoreCheckExpired;
    }

    public void setTruststoreCheckExpired(boolean checkExpired) {
        this.truststoreCheckExpired = checkExpired;
    }

    public KeyManager[] getKeyManagers() throws IOException, GeneralSecurityException {
        return SocketUtil.getKeyManagers(keyStoreFileName, this.keyStorePassword, this.keyManagerFactoryAlgorithm,
                this.keyStoreType, this.keyAlias, this.keyPassword);
    }

    public TrustManager[] getTrustManagers() throws IOException, GeneralSecurityException {
        return SocketUtil.getTrustManagers(this.trustStoreFileName, this.trustStorePassword,
                this.keyManagerFactoryAlgorithm, this.keyStoreType, this.truststoreCheckExpired);
    }

    public static TrustManager[] getTrustAllManagers() {
        return SocketUtil.getTrustAllManagers();
    }

    public boolean isDisableTrustManager() {
        return disableTrustManager;
    }

    public void setDisableTrustManager(boolean disableTrustManager) {
        this.disableTrustManager = disableTrustManager;
    }
}
