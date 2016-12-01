/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */
package org.teiid.resource.adapter.ftp;

import static org.teiid.core.util.Assertion.*;

import java.io.IOException;
import java.util.Arrays;

import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPSClient;

public class FtpsManagedConnectionFactory extends FtpManagedConnectionFactory {

    private static final long serialVersionUID = -1235746727896683420L;
    
    private Boolean useClientMode;

    private Boolean sessionCreation;

    private String authValue;

    private TrustManager trustManager;

    private String[] cipherSuites;

    private String[] protocols;

    private KeyManager keyManager;

    private Boolean needClientAuth;

    private Boolean wantsClientAuth;

    private boolean implicit = false;

    private String execProt = "P"; //$NON-NLS-1$

    private String protocol;

    public Boolean getUseClientMode() {
        return useClientMode;
    }

    public void setUseClientMode(Boolean useClientMode) {
        this.useClientMode = useClientMode;
    }

    public Boolean getSessionCreation() {
        return sessionCreation;
    }

    public void setSessionCreation(Boolean sessionCreation) {
        this.sessionCreation = sessionCreation;
    }

    public String getAuthValue() {
        return authValue;
    }

    public void setAuthValue(String authValue) {
        isNotNull(authValue);
        this.authValue = authValue;
    }

    public TrustManager getTrustManager() {
        return trustManager;
    }

    public void setTrustManager(TrustManager trustManager) {
        this.trustManager = trustManager;
    }

    public String[] getCipherSuites() {
        return cipherSuites;
    }

    public void setCipherSuites(String[] cipherSuites) {
        this.cipherSuites = cipherSuites;
    }

    public String[] getProtocols() {
        return protocols;
    }

    public void setProtocols(String[] protocols) {
        this.protocols = protocols;
    }

    public KeyManager getKeyManager() {
        return keyManager;
    }

    public void setKeyManager(KeyManager keyManager) {
        this.keyManager = keyManager;
    }

    public Boolean getNeedClientAuth() {
        return needClientAuth;
    }

    public void setNeedClientAuth(Boolean needClientAuth) {
        this.needClientAuth = needClientAuth;
    }

    public Boolean getWantsClientAuth() {
        return wantsClientAuth;
    }

    public void setWantsClientAuth(Boolean wantsClientAuth) {
        this.wantsClientAuth = wantsClientAuth;
    }

    public boolean isImplicit() {
        return implicit;
    }

    public void setImplicit(boolean implicit) {
        this.implicit = implicit;
    }

    public String getExecProt() {
        return execProt;
    }

    public void setExecProt(String execProt) {
        this.execProt = execProt;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        // TODO: add more validation for a valid protocol
        assertTrue(protocol != null && protocol.length() > 0 && !protocol.contains(" ")); //$NON-NLS-1$
        this.protocol = protocol;
    }

    @Override
    protected FTPClient createClientInstance() {
        if(this.getProtocol() != null) {
            return new FTPSClient(this.protocol, this.implicit);
        }
        return new FTPSClient(this.implicit);
    }

    @Override
    protected void postProcessClientBeforeConnect(FTPClient client) throws IOException {
        FTPSClient ftpsClient  = (FTPSClient) client;
        ftpsClient.execPBSZ(0);
        ftpsClient.execPROT(this.execProt);
        super.postProcessClientBeforeConnect(ftpsClient);
    }

    @Override
    protected void postProcessClientAfterConnect(FTPClient client) throws IOException {
        FTPSClient ftpsClient  = (FTPSClient) client;
        if(this.getAuthValue() != null) {
            ftpsClient.setAuthValue(this.authValue);
        }
        if (this.trustManager != null) {
            ftpsClient.setTrustManager(this.trustManager);
        }
        if (this.cipherSuites != null) {
            ftpsClient.setEnabledCipherSuites(this.cipherSuites);
        }
        if (this.protocols != null) {
            ftpsClient.setEnabledProtocols(this.protocols);
        }
        if (this.sessionCreation != null) {
            ftpsClient.setEnabledSessionCreation(this.sessionCreation);
        }
        if (this.useClientMode != null) {
            ftpsClient.setUseClientMode(this.useClientMode);
        }
        if (this.sessionCreation != null) {
            ftpsClient.setEnabledSessionCreation(this.sessionCreation);
        }
        if (this.keyManager != null) {
            ftpsClient.setKeyManager(this.keyManager);
        }
        if (this.needClientAuth != null) {
            ftpsClient.setNeedClientAuth(this.needClientAuth);
        }
        if (this.wantsClientAuth != null) {
            ftpsClient.setWantClientAuth(this.wantsClientAuth);
        }
        super.postProcessClientAfterConnect(ftpsClient);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((this.authValue == null) ? 0 : this.authValue.hashCode());
        result = prime * result + ((this.protocol == null) ? 0 : this.protocol.hashCode());
        result = prime * result + ((this.execProt == null) ? 0 : this.execProt.hashCode());
        result = prime * result + ((this.useClientMode == null) ? 0 : this.useClientMode.hashCode());
        result = prime * result + ((this.sessionCreation == null) ? 0 : this.sessionCreation.hashCode());
        result = prime * result + ((this.needClientAuth == null) ? 0 : this.needClientAuth.hashCode());
        result = prime * result + ((this.wantsClientAuth == null) ? 0 : this.wantsClientAuth.hashCode());
        result = prime * result + ((this.implicit) ? 1231 : 1237);
        result = prime * result + ((this.cipherSuites == null) ? 0 : Arrays.hashCode(this.cipherSuites));
        result = prime * result + ((this.protocols == null) ? 0 : Arrays.hashCode(this.protocols));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if(!super.equals(obj)){
            return false;
        }
        FtpsManagedConnectionFactory other = (FtpsManagedConnectionFactory) obj;
        if (!checkEquals(this.authValue, other.authValue)) {
            return false;
        }
        if (!checkEquals(this.protocol, other.protocol)) {
            return false;
        }
        if (!checkEquals(this.execProt, other.execProt)) {
            return false;
        }
        if(!this.useClientMode.equals(other.useClientMode)) {
            return false;
        }
        if(!this.sessionCreation.equals(other.sessionCreation)) {
            return false;
        }
        if(!this.needClientAuth.equals(other.needClientAuth)) {
            return false;
        }
        if(!this.wantsClientAuth.equals(other.wantsClientAuth)) {
            return false;
        }
        if(this.implicit != other.implicit) {
            return false;
        }
        if(!Arrays.equals(this.cipherSuites, other.cipherSuites)) {
            return false;
        }
        if(!Arrays.equals(this.protocols, other.protocols)) {
            return false;
        }
        return true;
    }
}
