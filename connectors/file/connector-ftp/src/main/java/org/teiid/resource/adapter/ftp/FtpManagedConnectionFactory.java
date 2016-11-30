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

import javax.resource.ResourceException;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPReply;
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
        
        this.postProcessClientBeforeConnect(client);
        
        client.connect(this.host, this.port);
        
        if (!FTPReply.isPositiveCompletion(client.getReplyCode())) {
            throw new ResourceException(UTIL.getString("ftp_connect_failed", this.host, this.port)); //$NON-NLS-1$
        }
        
        if (!client.login(this.username, this.password)) {
            throw new IllegalStateException(UTIL.getString("ftp_login_failed", client.getReplyString())); //$NON-NLS-1$
        }
        
        this.postProcessClientAfterConnect(client);
        
        this.updateClientMode(client);
        client.setFileType(this.fileType);
        client.setBufferSize(this.bufferSize);
        
        return client;
        
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

    protected FTPClient createClientInstance() {
        return new FTPClient();
    }
    
    /**
     * Will handle additional initialization before client.connect() method was invoked.
     *
     * @param client The client.
     * @throws IOException Any IOException.
     */
    protected void postProcessClientBeforeConnect(FTPClient client) throws IOException {
    }
    
    /**
     * Will handle additional initialization after client.connect() method was invoked,
     * but before any action on the client has been taken
     *
     * @param t The client.
     * @throws IOException Any IOException
     */
    protected void postProcessClientAfterConnect(FTPClient client) throws IOException {
        if (this.parentDirectory == null) {
            throw new IOException(UTIL.getString("parentdirectory_not_set")); //$NON-NLS-1$
        }
        
        if(!client.changeWorkingDirectory(this.getParentDirectory())){
            throw new IOException(UTIL.getString("ftp_dir_not_exist", this.getParentDirectory())); //$NON-NLS-1$
        }
    }
    
}
