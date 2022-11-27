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
package org.teiid.file.ftp;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.commons.net.ftp.FTPSClient;
import org.jboss.vfs.VFS;
import org.jboss.vfs.VirtualFile;
import org.teiid.core.BundleUtil;
import org.teiid.core.util.StringUtil;
import org.teiid.file.VirtualFileConnection;
import org.teiid.translator.TranslatorException;

public class FtpFileConnection implements VirtualFileConnection {

    static class JBossVirtualFile implements org.teiid.file.VirtualFile {

        private VirtualFile file;

        public JBossVirtualFile(VirtualFile file) {
            this.file = file;
        }

        @Override
        public String getName() {
            return file.getName();
        }

        @Override
        public String getPath() {
            return file.getPathName();
        }

        @Override
        public boolean isDirectory() {
            return file.isDirectory();
        }

        @Override
        public InputStream openInputStream(boolean lock) throws IOException {
            //locking not supported
            return file.openStream();
        }

        @Override
        public OutputStream openOutputStream(boolean lock) throws IOException {
            throw new IOException("not supported"); //$NON-NLS-1$
        }

        @Override
        public long getLastModified() {
            return file.getLastModified();
        }

        @Override
        public long getCreationTime() {
            //not supported through vfs
            return file.getLastModified();
        }

        @Override
        public long getSize() {
            return file.getSize();
        }

    }

    public static final BundleUtil UTIL = BundleUtil.getBundleUtil(FtpFileConnection.class);

    private VirtualFile mountPoint;
    private Map<String, String> fileMapping;
    private Closeable closeable;
    private final FTPClient client;

    public FtpFileConnection(FtpConfiguration config) throws TranslatorException {
        this.client = createFtpClient(config);

        final Map<String, String> map = StringUtil.valueOf(config.getFileMapping(), Map.class);
        if(map == null) {
            this.fileMapping = Collections.emptyMap();
        } else {
            this.fileMapping = map;
        }

        String pathname = config.getParentDirectory();
        try {
            if(this.client.cwd(pathname) != 250) {
                throw new TranslatorException(UTIL.getString("parentdirectory_not_set")); //$NON-NLS-1$
            }
            this.client.changeWorkingDirectory(pathname);
            this.mountPoint = VFS.getChild(pathname);
            this.closeable = VFS.mount(mountPoint, new FtpFileSystem(this.client));
        } catch (IOException e) {
            throw new TranslatorException(e, UTIL.getString("vfs_mount_error", pathname)); //$NON-NLS-1$
        }
    }

    FTPClient getClient() {
        return this.client;
    }

    @Override
    public void close() throws Exception {
        this.closeable.close();
    }

    @Override
    public org.teiid.file.VirtualFile[] getFiles(String pattern) {
        pattern = checkPattern(pattern);
        VirtualFile file = this.getFile(pattern);
        if (file.isDirectory()) {
            List<VirtualFile> children = file.getChildren();
            org.teiid.file.VirtualFile[] result = new org.teiid.file.VirtualFile[children.size()];
            for (int i = 0; i < result.length; i++) {
                result[i] = new JBossVirtualFile(children.get(i));
            }
            return result;
        }
        if(file.exists()) {
            return new org.teiid.file.VirtualFile[]{new JBossVirtualFile(file)};
        }

        return null;
    }

    static String checkPattern(String pattern) {
        long count = pattern.chars().filter((c)->{return c == '*';}).count();
        if (count > 0) {
            pattern = pattern.replaceAll("[*]{2}", "[*]"); //$NON-NLS-1$ //$NON-NLS-2$
            long newCount = pattern.chars().filter((c)->{return c == '*';}).count();
            //check for unescaped star
            if (newCount != count/2) {
                //could be implemented with a recursive search from the root
                throw new IllegalArgumentException("glob searches are not yet supported with ftp"); //$NON-NLS-1$
            }
        }
        return pattern;
    }

    public VirtualFile getFile(String path) {
        if(path == null) {
            return this.mountPoint;
        }
        String altPath = fileMapping.get(path);
        if (altPath != null) {
            path = altPath;
        }
        return this.mountPoint.getChild(path);
    }

    @Override
    public void add(InputStream in, String path) throws TranslatorException {
        try {
            this.client.storeFile(path, in);
        } catch (IOException e) {
            throw new TranslatorException(e, UTIL.getString("ftp_failed_write", path, this.client.getReplyString())); //$NON-NLS-1$
        }
    }

    @Override
    public boolean remove(String path) {
        return this.mountPoint.getChild(path).delete();
    }

    private FTPClient createFtpClient(FtpConfiguration config) throws TranslatorException {
        FTPClient client = createClientInstance(config);
        try {
            beforeConnect(client, config);

            client.connect(config.getHost(), config.getPort());

            if (!FTPReply.isPositiveCompletion(client.getReplyCode())) {
                throw new TranslatorException(UTIL.getString("ftp_connect_failed", config.getHost(), config.getPort())); //$NON-NLS-1$
            }

            if (!client.login(config.getUsername(), config.getPassword())) {
                throw new IllegalStateException(UTIL.getString("ftp_login_failed", client.getReplyString())); //$NON-NLS-1$
            }

            afterConnect(client, config);
        } catch (IOException e) {
            throw new TranslatorException(e);
        }
        return client;
    }

    private FTPClient createClientInstance(FtpConfiguration config) {
        if(config.getIsFtps()) {
            if(config.getProtocol() != null) {
                return new FTPSClient(config.getProtocol(), config.isImplicit());
            }
            return new FTPSClient(config.isImplicit());
        }
        return new FTPClient();
    }

    private void beforeConnect(FTPClient client, FtpConfiguration config) throws IOException {
        if (config.getConnectTimeout() != null) {
            client.setConnectTimeout(config.getConnectTimeout());
        }
        if (config.getDefaultTimeout() != null) {
            client.setDefaultTimeout(config.getDefaultTimeout());
        }
        if (config.getDataTimeout() != null) {
            client.setDataTimeout(config.getDataTimeout());
        }
        client.setControlEncoding(config.getControlEncoding());

        if(config.getIsFtps()){
            FTPSClient ftpsClient  = (FTPSClient) client;
            ftpsClient.execPBSZ(0);
            ftpsClient.execPROT(config.getExecProt());
        }
    }

    private void afterConnect(FTPClient client, FtpConfiguration config) throws IOException {

        if (config.getParentDirectory() == null) {
            throw new IOException(UTIL.getString("parentdirectory_not_set")); //$NON-NLS-1$
        }

        if(!client.changeWorkingDirectory(config.getParentDirectory())){
            throw new IOException(UTIL.getString("ftp_dir_not_exist", config.getParentDirectory())); //$NON-NLS-1$
        }

        switch (config.getClientMode()) {
        case FTPClient.ACTIVE_LOCAL_DATA_CONNECTION_MODE:
            client.enterLocalActiveMode();
            break;
        case FTPClient.PASSIVE_LOCAL_DATA_CONNECTION_MODE:
            client.enterLocalPassiveMode();
            break;
        default:
            break;
        }

        client.setFileType(config.getFileType());
        client.setBufferSize(config.getBufferSize());

        if(config.getIsFtps()) {
            FTPSClient ftpsClient  = (FTPSClient) client;
            if(config.getAuthValue() != null) {
                ftpsClient.setAuthValue(config.getAuthValue());
            }
            if (config.getTrustManager() != null) {
                ftpsClient.setTrustManager(config.getTrustManager());
            }
            if (config.getSupportedCipherSuites() != null) {
                ftpsClient.setEnabledCipherSuites(config.getSupportedCipherSuites());
            }
            if (config.getSupportedProtocols() != null) {
                ftpsClient.setEnabledProtocols(config.getSupportedProtocols());
            }
            if (config.getSessionCreation() != null) {
                ftpsClient.setEnabledSessionCreation(config.getSessionCreation() );
            }
            if (config.getUseClientMode() != null) {
                ftpsClient.setUseClientMode(config.getUseClientMode());
            }
            if (config.getKeyManager() != null) {
                ftpsClient.setKeyManager(config.getKeyManager());
            }
            if (config.getNeedClientAuth() != null) {
                ftpsClient.setNeedClientAuth(config.getNeedClientAuth());
            }
            if (config.getWantsClientAuth() != null) {
                ftpsClient.setWantClientAuth(config.getWantsClientAuth());
            }
        }
    }

    @Override
    public boolean areFilesUsableAfterClose() {
        return false;
    }

}
