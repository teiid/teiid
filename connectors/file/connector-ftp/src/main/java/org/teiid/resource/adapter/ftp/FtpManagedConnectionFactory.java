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
import static org.apache.commons.net.ftp.FTPClient.ACTIVE_LOCAL_DATA_CONNECTION_MODE;
import static org.apache.commons.net.ftp.FTPClient.PASSIVE_LOCAL_DATA_CONNECTION_MODE;
import static org.apache.commons.net.ftp.FTP.ASCII_FILE_TYPE;
import static org.apache.commons.net.ftp.FTP.EBCDIC_FILE_TYPE;
import static org.apache.commons.net.ftp.FTP.BINARY_FILE_TYPE;
import static org.apache.commons.net.ftp.FTP.LOCAL_FILE_TYPE;

import java.io.IOException;
import java.util.Arrays;

import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;
import javax.resource.ResourceException;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.commons.net.ftp.FTPSClient;
import org.teiid.core.BundleUtil;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;

public class FtpManagedConnectionFactory extends BasicManagedConnectionFactory {

    private static final long serialVersionUID = -687763504336137294L;
    
    public static final BundleUtil UTIL = BundleUtil.getBundleUtil(FtpManagedConnectionFactory.class);
    
    private String parentDirectory;
    
    protected FTPClientConfig config;

    protected String username;

    protected String host;

    protected String password;

    protected int port = FTP.DEFAULT_PORT;

    protected int bufferSize = 2048;

    protected int clientMode = ACTIVE_LOCAL_DATA_CONNECTION_MODE;

    protected int fileType = BINARY_FILE_TYPE;

    protected String controlEncoding = FTP.DEFAULT_CONTROL_ENCODING;

    private Integer connectTimeout;

    private Integer defaultTimeout;

    private Integer dataTimeout;
    
    private Boolean isFtps = false;
    
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

    public FTPClientConfig getConfig() {
        return config;
    }

    public String getParentDirectory() {
        return parentDirectory;
    }

    public void setParentDirectory(String parentDirectory) {
        isNotNull(parentDirectory, UTIL.getString("parentdirectory_not_null"));//$NON-NLS-1$
        this.parentDirectory = parentDirectory;
    }

    public void setConfig(FTPClientConfig config) {
        isNotNull(config, UTIL.getString("ftp_client_config"));//$NON-NLS-1$
        this.config = config;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        isNotNull(username, UTIL.getString("ftp_client_username"));//$NON-NLS-1$
        this.username = username;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        assertTrue(host!= null && host.length() > 0, UTIL.getString("ftp_client_host"));//$NON-NLS-1$
        this.host = host;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        isNotNull(password, UTIL.getString("ftp_client_password"));//$NON-NLS-1$
        this.password = password;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        assertTrue(port > 0, UTIL.getString("ftp_client_port"));//$NON-NLS-1$
        this.port = port;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public int getClientMode() {
        return clientMode;
    }

    public void setClientMode(int clientMode) {
        assertTrue(clientMode == ACTIVE_LOCAL_DATA_CONNECTION_MODE || clientMode == PASSIVE_LOCAL_DATA_CONNECTION_MODE, UTIL.getString("ftp_client_clientMode", clientMode));//$NON-NLS-1$
        this.clientMode = clientMode;
    }

    public int getFileType() {
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
    public void setFileType(int fileType) {
        assertTrue(fileType == ASCII_FILE_TYPE || fileType == EBCDIC_FILE_TYPE || fileType == BINARY_FILE_TYPE || fileType == LOCAL_FILE_TYPE);
        this.fileType = fileType;
    }

    public String getControlEncoding() {
        return controlEncoding;
    }

    public void setControlEncoding(String controlEncoding) {
        isNotNull(controlEncoding);
        this.controlEncoding = controlEncoding;
    }

    public Integer getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(Integer connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public Integer getDefaultTimeout() {
        return defaultTimeout;
    }

    public void setDefaultTimeout(Integer defaultTimeout) {
        this.defaultTimeout = defaultTimeout;
    }

    public Integer getDataTimeout() {
        return dataTimeout;
    }

    public void setDataTimeout(Integer dataTimeout) {
        this.dataTimeout = dataTimeout;
    }
    
    public Boolean getIsFtps() {
        return isFtps;
    }

    public void setIsFtps(Boolean isFtps) {
        this.isFtps = isFtps;
    }

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

    @SuppressWarnings("serial")
    @Override
    public BasicConnectionFactory<FtpFileConnectionImpl> createConnectionFactory()throws ResourceException {
                
        return new BasicConnectionFactory<FtpFileConnectionImpl>() {

            @Override
            public FtpFileConnectionImpl getConnection() throws ResourceException {
                try {
                    return new FtpFileConnectionImpl(createClient(), parentDirectory);
                } catch (IOException e) {
                    throw new ResourceException(e);
                }
            }};
    }
    
    private FTPClient createClient() throws IOException, ResourceException {
        
        FTPClient client = createClientInstance();
           
        beforeConnectProcessing(client);
        
        client.connect(this.host, this.port);
        
        if (!FTPReply.isPositiveCompletion(client.getReplyCode())) {
            throw new ResourceException(UTIL.getString("ftp_connect_failed", this.host, this.port)); //$NON-NLS-1$
        }
        
        if (!client.login(this.username, this.password)) {
            throw new IllegalStateException(UTIL.getString("ftp_login_failed", client.getReplyString())); //$NON-NLS-1$
        }
        
        afterConnectProcessing(client);
        
        return client;
        
    }
    
    private FTPClient createClientInstance() {
        if(this.isFtps) {
            if(this.getProtocol() != null) {
                return new FTPSClient(this.protocol, this.implicit);
            }
            return new FTPSClient(this.implicit);
        }
        return new FTPClient();
    }

    private void beforeConnectProcessing(FTPClient client) throws IOException {
        
        client.configure(this.config);
        if (this.connectTimeout != null) {
            client.setConnectTimeout(this.connectTimeout);
        }
        if (this.defaultTimeout != null) {
            client.setDefaultTimeout(this.defaultTimeout);
        }
        if (this.dataTimeout != null) {
            client.setDataTimeout(this.dataTimeout);
        }
        client.setControlEncoding(this.controlEncoding);
        
        if(this.isFtps){
            FTPSClient ftpsClient  = (FTPSClient) client;
            ftpsClient.execPBSZ(0);
            ftpsClient.execPROT(this.execProt);
        }    
    }
    

    private void afterConnectProcessing(FTPClient client) throws IOException {
        
        if (this.parentDirectory == null) {
            throw new IOException(UTIL.getString("parentdirectory_not_set")); //$NON-NLS-1$
        }
        
        if(!client.changeWorkingDirectory(this.getParentDirectory())){
            throw new IOException(UTIL.getString("ftp_dir_not_exist", this.getParentDirectory())); //$NON-NLS-1$
        }
        
        updateClientMode(client);
        
        client.setFileType(this.fileType);
        client.setBufferSize(this.bufferSize);
        
        if(this.isFtps) {
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
        }
    }
    
    private void updateClientMode(FTPClient client) {
        switch (this.clientMode) {
            case ACTIVE_LOCAL_DATA_CONNECTION_MODE:
                client.enterLocalActiveMode();
                break;
            case PASSIVE_LOCAL_DATA_CONNECTION_MODE:
                client.enterLocalPassiveMode();
                break;
            default:
                break;
        }
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
    
}
