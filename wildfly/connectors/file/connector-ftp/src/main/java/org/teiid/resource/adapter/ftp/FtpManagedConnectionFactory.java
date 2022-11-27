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
package org.teiid.resource.adapter.ftp;

import static org.apache.commons.net.ftp.FTP.ASCII_FILE_TYPE;
import static org.apache.commons.net.ftp.FTP.BINARY_FILE_TYPE;
import static org.apache.commons.net.ftp.FTP.EBCDIC_FILE_TYPE;
import static org.apache.commons.net.ftp.FTP.LOCAL_FILE_TYPE;
import static org.apache.commons.net.ftp.FTPClient.ACTIVE_LOCAL_DATA_CONNECTION_MODE;
import static org.apache.commons.net.ftp.FTPClient.PASSIVE_LOCAL_DATA_CONNECTION_MODE;
import static org.teiid.core.util.Assertion.assertTrue;
import static org.teiid.core.util.Assertion.isNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Arrays;

import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;
import javax.resource.ResourceException;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.util.KeyManagerUtils;
import org.apache.commons.net.util.TrustManagerUtils;
import org.teiid.core.BundleUtil;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.file.ftp.FtpConfiguration;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;
import org.teiid.translator.TranslatorException;

public class FtpManagedConnectionFactory extends BasicManagedConnectionFactory implements FtpConfiguration {

    private static final long serialVersionUID = -687763504336137294L;

    public static final BundleUtil UTIL = BundleUtil.getBundleUtil(FtpManagedConnectionFactory.class);

    private String parentDirectory;

    private String fileMapping;

    protected FTPClientConfig config;

    protected String username;

    protected String host;

    protected String password;

    protected Integer port = FTP.DEFAULT_PORT;

    protected Integer bufferSize = 2048;

    protected Integer clientMode = ACTIVE_LOCAL_DATA_CONNECTION_MODE;

    protected Integer fileType = BINARY_FILE_TYPE;

    protected String controlEncoding = FTP.DEFAULT_CONTROL_ENCODING;

    private Integer connectTimeout;

    private Integer defaultTimeout;

    private Integer dataTimeout;

    private Boolean isFtps = false;

    private Boolean useClientMode;

    private Boolean sessionCreation;

    private String authValue;

    private String certificate;

    private TrustManager trustManager;

    private String[] cipherSuites;

    private String[] protocols;

    private String keyPath;

    private String keyPassword;

    private KeyManager keyManager;

    private Boolean needClientAuth;

    private Boolean wantsClientAuth;

    private Boolean implicit = false;

    private String execProt = "P"; //$NON-NLS-1$

    private String protocol;

    public FTPClientConfig getConfig() {
        return config;
    }

    @Override
    public String getParentDirectory() {
        return parentDirectory;
    }

    public void setParentDirectory(String parentDirectory) {
        isNotNull(parentDirectory, UTIL.getString("parentdirectory_not_null"));//$NON-NLS-1$
        this.parentDirectory = parentDirectory;
    }

    @Override
    public String getFileMapping() {
        return fileMapping;
    }

    public void setFileMapping(String fileMapping) {
        this.fileMapping = fileMapping;
    }

    public void setConfig(FTPClientConfig config) {
        isNotNull(config, UTIL.getString("ftp_client_config"));//$NON-NLS-1$
        this.config = config;
    }

    @Override
    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        isNotNull(username, UTIL.getString("ftp_client_username"));//$NON-NLS-1$
        this.username = username;
    }

    @Override
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        assertTrue(host!= null && host.length() > 0, UTIL.getString("ftp_client_host"));//$NON-NLS-1$
        this.host = host;
    }

    @Override
    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        isNotNull(password, UTIL.getString("ftp_client_password"));//$NON-NLS-1$
        this.password = password;
    }

    @Override
    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        assertTrue(port > 0, UTIL.getString("ftp_client_port"));//$NON-NLS-1$
        this.port = port;
    }

    @Override
    public Integer getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(Integer bufferSize) {
        this.bufferSize = bufferSize;
    }

    @Override
    public Integer getClientMode() {
        return clientMode;
    }

    public void setClientMode(Integer clientMode) {
        assertTrue(clientMode == ACTIVE_LOCAL_DATA_CONNECTION_MODE || clientMode == PASSIVE_LOCAL_DATA_CONNECTION_MODE, UTIL.getString("ftp_client_clientMode", clientMode));//$NON-NLS-1$
        this.clientMode = clientMode;
    }

    @Override
    public Integer getFileType() {
        return fileType;
    }

    /**
     * File types defined by {@link org.apache.commons.net.ftp.FTP} constants:
     * <ul>
     * <li>{@link org.apache.commons.net.ftp.FTP#ASCII_FILE_TYPE}</li>
     * <li>{@link org.apache.commons.net.ftp.FTP#EBCDIC_FILE_TYPE}</li>
     * <li>{@link org.apache.commons.net.ftp.FTP#BINARY_FILE_TYPE}</li>
     * <li>{@link org.apache.commons.net.ftp.FTP#LOCAL_FILE_TYPE}</li>
     * </ul>
     * @param fileType The file type.
     */
    public void setFileType(Integer fileType) {
        assertTrue(fileType == ASCII_FILE_TYPE || fileType == EBCDIC_FILE_TYPE || fileType == BINARY_FILE_TYPE || fileType == LOCAL_FILE_TYPE);
        this.fileType = fileType;
    }

    @Override
    public String getControlEncoding() {
        return controlEncoding;
    }

    public void setControlEncoding(String controlEncoding) {
        isNotNull(controlEncoding);
        this.controlEncoding = controlEncoding;
    }

    @Override
    public Integer getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(Integer connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    @Override
    public Integer getDefaultTimeout() {
        return defaultTimeout;
    }

    public void setDefaultTimeout(Integer defaultTimeout) {
        this.defaultTimeout = defaultTimeout;
    }

    @Override
    public Integer getDataTimeout() {
        return dataTimeout;
    }

    public void setDataTimeout(Integer dataTimeout) {
        this.dataTimeout = dataTimeout;
    }

    @Override
    public Boolean getIsFtps() {
        return isFtps;
    }

    public void setIsFtps(Boolean isFtps) {
        this.isFtps = isFtps;
    }

    @Override
    public Boolean getUseClientMode() {
        return useClientMode;
    }

    public void setUseClientMode(Boolean useClientMode) {
        this.useClientMode = useClientMode;
    }

    @Override
    public Boolean getSessionCreation() {
        return sessionCreation;
    }

    public void setSessionCreation(Boolean sessionCreation) {
        this.sessionCreation = sessionCreation;
    }

    @Override
    public String getAuthValue() {
        return authValue;
    }

    public void setAuthValue(String authValue) {
        isNotNull(authValue);
        this.authValue = authValue;
    }

    public String getCertificate() {
        return certificate;
    }

    public void setCertificate(String certificate) {
        this.certificate = certificate;
        if(this.certificate != null && Files.exists(Paths.get(this.certificate))) {
            try {
                CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509"); //$NON-NLS-1$
                InputStream in = Files.newInputStream(Paths.get(this.certificate));
                Certificate cert = certificateFactory.generateCertificate(in);
                KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
                keyStore.load(null);
                keyStore.setCertificateEntry("alias", cert); //$NON-NLS-1$
                this.trustManager = TrustManagerUtils.getDefaultTrustManager(keyStore);
            } catch (IOException | GeneralSecurityException e) {
                throw new TeiidRuntimeException(UTIL.getString("ftp_certificate_path", certificate, e));  //$NON-NLS-1$
            }
        }
    }

    public String getCipherSuites() {
        return formStringFromArray(cipherSuites);
    }

    private String formStringFromArray(String[] cipherSuites) {
        String result = ""; //$NON-NLS-1$
        for(String str : cipherSuites) {
            result = result + str + ","; //$NON-NLS-1$
        }
        return result.substring(0, result.length() - 1);
    }

    public void setCipherSuites(String cipherSuites) {
        assertTrue(cipherSuites != null && cipherSuites.length() > 0, UTIL.getString("ftp_client_invalid_array", cipherSuites, "cipherSuites"));//$NON-NLS-1$ //$NON-NLS-12$
        this.cipherSuites = cipherSuites.split(","); //$NON-NLS-1$
    }

    public String getProtocols() {
        return formStringFromArray(protocols);
    }

    public void setProtocols(String protocols) {
        assertTrue(protocols != null && protocols.length() > 0, UTIL.getString("ftp_client_invalid_array", protocols, "protocols"));//$NON-NLS-1$ //$NON-NLS-12$
        this.protocols = protocols.split(","); //$NON-NLS-1$
    }

    public String getKeyPath() {
        return keyPath;
    }

    public void setKeyPath(String keyPath) {
        this.keyPath = keyPath;
        if(this.keyPath != null && Files.exists(Paths.get(this.keyPath))) {
            if(this.keyPassword == null){
                this.keyPassword = ""; //$NON-NLS-1$
            }
            try {
                this.keyManager = KeyManagerUtils.createClientKeyManager(Paths.get(this.keyPath).toFile(), this.keyPassword);
            } catch (IOException | GeneralSecurityException e) {
                throw new TeiidRuntimeException(UTIL.getString("ftp_ketstore_path", this.keyPath, e));  //$NON-NLS-1$
            }
        }
    }

    public String getKeyPassword() {
        return keyPassword;
    }

    public void setKeyPassword(String keyPassword) {
        this.keyPassword = keyPassword;
    }

    @Override
    public Boolean getNeedClientAuth() {
        return needClientAuth;
    }

    public void setNeedClientAuth(Boolean needClientAuth) {
        this.needClientAuth = needClientAuth;
    }

    @Override
    public Boolean getWantsClientAuth() {
        return wantsClientAuth;
    }

    public void setWantsClientAuth(Boolean wantsClientAuth) {
        this.wantsClientAuth = wantsClientAuth;
    }

    @Override
    public Boolean isImplicit() {
        return implicit;
    }

    public void setImplicit(Boolean implicit) {
        this.implicit = implicit;
    }

    @Override
    public String getExecProt() {
        return execProt;
    }

    public void setExecProt(String execProt) {
        this.execProt = execProt;
    }

    @Override
    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        // TODO: add more validation for a valid protocol
        assertTrue(protocol != null && protocol.length() > 0 && !protocol.contains(" ")); //$NON-NLS-1$
        this.protocol = protocol;
    }

    @SuppressWarnings("serial")
    @Override
    public BasicConnectionFactory<FtpFileConnectionImpl> createConnectionFactory()throws ResourceException {
        return new BasicConnectionFactory<FtpFileConnectionImpl>() {
            @Override
            public FtpFileConnectionImpl getConnection() throws ResourceException {
                try {
                    return new FtpFileConnectionImpl(configuration());
                } catch (TranslatorException e) {
                    throw new ResourceException(e);
                }
            }};
    }

    FtpConfiguration configuration() {
        return this;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((this.parentDirectory == null) ? 0 : this.parentDirectory.hashCode());
        result = prime * result + ((this.username == null) ? 0 : this.username.hashCode());
        result = prime * result + ((this.password == null) ? 0 : this.password.hashCode());
        result = prime * result + ((this.host == null) ? 0 : this.host.hashCode());
        result = prime * result + ((this.controlEncoding == null) ? 0 : this.controlEncoding.hashCode());
        result = prime * result + ((this.connectTimeout == null) ? 0 : this.connectTimeout.hashCode());
        result = prime * result + ((this.dataTimeout == null) ? 0 : this.dataTimeout.hashCode());
        result = prime * result + ((this.defaultTimeout == null) ? 0 : this.defaultTimeout.hashCode());
        result = prime * result + this.port;
        result = prime * result + this.fileType;
        result = prime * result + this.clientMode;
        result = prime * result + this.bufferSize;
        if(this.isFtps) {
            result = prime * result + ((this.certificate == null) ? 0 : this.certificate.hashCode());
            result = prime * result + ((this.keyPath == null) ? 0 : this.keyPath.hashCode());
            result = prime * result + ((this.keyPassword == null) ? 0 : this.keyPassword.hashCode());
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
        }
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj) {
            return true;
        }
        if(null == obj) {
            return false;
        }
        if(getClass() != obj.getClass()) {
            return false;
        }
        FtpManagedConnectionFactory other = (FtpManagedConnectionFactory) obj;
        if (!checkEquals(this.parentDirectory, other.parentDirectory)) {
            return false;
        }
        if (!checkEquals(this.username, other.username)) {
            return false;
        }
        if (!checkEquals(this.password, other.password)) {
            return false;
        }
        if (!checkEquals(this.host, other.host)) {
            return false;
        }
        if (!checkEquals(this.controlEncoding, other.controlEncoding)) {
            return false;
        }
        if(this.connectTimeout != other.connectTimeout) {
            return false;
        }
        if(this.dataTimeout != other.dataTimeout) {
            return false;
        }
        if(this.defaultTimeout != other.defaultTimeout) {
            return false;
        }
        if(this.port != other.port) {
            return false;
        }
        if(this.fileType != other.fileType) {
            return false;
        }
        if(this.clientMode != other.clientMode) {
            return false;
        }
        if(this.bufferSize != other.bufferSize) {
            return false;
        }
        if(this.isFtps) {
            if (!checkEquals(this.certificate, other.certificate)) {
                return false;
            }
            if (!checkEquals(this.keyPath, other.keyPath)) {
                return false;
            }
            if (!checkEquals(this.keyPassword, other.keyPassword)) {
                return false;
            }
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
        }
        return true;
    }

    @Override
    public TrustManager getTrustManager() {
        return this.trustManager;
    }

    @Override
    public KeyManager getKeyManager() {
        return this.keyManager;
    }

    @Override
    public String[] getSupportedCipherSuites() {
        return this.cipherSuites;
    }

    @Override
    public String[] getSupportedProtocols() {
        return this.protocols;
    }
}
