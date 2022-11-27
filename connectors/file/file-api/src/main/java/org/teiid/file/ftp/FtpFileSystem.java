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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.CodeSigner;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.jboss.vfs.VirtualFile;
import org.jboss.vfs.spi.FileSystem;

public class FtpFileSystem implements FileSystem {

    private FTPClient client;


    public FtpFileSystem(FTPClient ftpClient) {
        this.client = ftpClient;
    }

    @Override
    public File getFile(VirtualFile mountPoint, VirtualFile target) throws IOException {
        throw new IOException();
    }

    @Override
    public InputStream openInputStream(VirtualFile mountPoint, VirtualFile target) throws IOException {
        return this.client.retrieveFileStream(target.getName());
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public boolean delete(VirtualFile mountPoint, VirtualFile target) {
        try {
            return this.client.deleteFile(target.getName());
        } catch (IOException e) {
            throw new FtpOperationException(e);
        }
    }

    @Override
    public long getSize(VirtualFile mountPoint, VirtualFile target) {
        try {
            FTPFile file = this.client.mlistFile(target.getName());
            return file != null ? file.getSize() : -1;
        } catch (IOException e) {
            throw new FtpOperationException(e);
        }
    }

    @Override
    public long getLastModified(VirtualFile mountPoint, VirtualFile target) {
        try {
            FTPFile file = this.client.mdtmFile(target.getName());
            return file != null ? file.getTimestamp().getTimeInMillis() : -1;
        } catch (IOException e) {
            throw new FtpOperationException(e);
        }
    }

    @Override
    public boolean exists(VirtualFile mountPoint, VirtualFile target) {
        return isFile(mountPoint, target) || isDirectory(mountPoint, target);
    }

    @Override
    public boolean isFile(VirtualFile mountPoint, VirtualFile target) {
        InputStream in = null;
        try {
            in = this.openInputStream(mountPoint, target);
            int returnCode = this.client.getReplyCode();
            if (in == null || returnCode == 550){
                return false;
            }
            return true;
        } catch (IOException e) {
            throw new FtpOperationException(e);
        } finally {
            if(null != in) {
                try {
                    in.close();
                    this.client.completePendingCommand();
                } catch (IOException e) {
                    throw new FtpOperationException(e);
                }
            }
        }
    }

    @Override
    public boolean isDirectory(VirtualFile mountPoint, VirtualFile target) {
        try {
            if(this.client.cwd(target.getPathName()) == 250) {
                return true;
            }
        } catch (IOException e) {
            throw new FtpOperationException(e);
        }
        return false;
    }

    @Override
    public List<String> getDirectoryEntries(VirtualFile mountPoint, VirtualFile target) {
        try {
            return Arrays.asList(this.client.listNames(target.getPathName()));
        } catch (IOException e) {
            throw new FtpOperationException(e);
        }
    }

    @Override
    public CodeSigner[] getCodeSigners(VirtualFile mountPoint, VirtualFile target) {
        return null;
    }

    @Override
    public void close() throws IOException {
        this.client.disconnect();
    }

    @Override
    public File getMountSource() {
        return null;
    }

    @Override
    public URI getRootURI() throws URISyntaxException {
        try {
            return new URI("ftp", this.client.printWorkingDirectory() + "!/", null);
        } catch (IOException e) {
            throw new FtpOperationException(e);
        }
    }

    private static class FtpOperationException extends RuntimeException {

        private static final long serialVersionUID = 2112491370833353846L;

        FtpOperationException(Throwable cause){
            super(cause);
        }
    }

}
