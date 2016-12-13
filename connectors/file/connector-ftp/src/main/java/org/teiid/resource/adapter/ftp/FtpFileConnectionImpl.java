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

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.resource.ResourceException;
import javax.resource.spi.InvalidPropertyException;

import org.apache.commons.net.ftp.FTPClient;
import org.jboss.vfs.VFS;
import org.jboss.vfs.VirtualFile;
import org.jboss.vfs.VirtualFileFilter;
import org.teiid.core.BundleUtil;
import org.teiid.file.VirtualFileConnection;
import org.teiid.resource.spi.BasicConnection;

public class FtpFileConnectionImpl extends BasicConnection implements VirtualFileConnection {
    
    public static final BundleUtil UTIL = BundleUtil.getBundleUtil(FtpFileConnectionImpl.class);
    
    private VirtualFile mountPoint;
    private Map<String, String> fileMapping;
    private Closeable closeable;
    private final FTPClient client;
    
    public FtpFileConnectionImpl(FTPClient client, String pathname, Map<String, String> fileMapping) throws ResourceException {
        this.client = client;
        if(fileMapping == null) {
            this.fileMapping = Collections.emptyMap();
        } else {
            this.fileMapping = fileMapping;
        }
        
        try {
            if(this.client.cwd(pathname) != 250) {
                throw new InvalidPropertyException(UTIL.getString("parentdirectory_not_set")); //$NON-NLS-1$
            }
            this.client.changeWorkingDirectory(pathname);
            this.mountPoint = VFS.getChild(pathname);
            this.closeable = VFS.mount(mountPoint, new FtpFileSystem(this.client));
        } catch (IOException e) {
            throw new ResourceException(UTIL.getString("vfs_mount_error", pathname), e); //$NON-NLS-1$
        }

    }

    @Override
    public void close() throws ResourceException {
        try {
            this.closeable.close();
        } catch (IOException e) {
            throw new ResourceException(e);
        }
    }

    @Override
    public VirtualFile[] getFiles(String pattern) throws ResourceException {
        if(this.getFile(pattern).exists()) {
            return new VirtualFile[]{this.getFile(pattern)};
        }
        
        if (pattern.contains("*")){ //$NON-NLS-1$ 
            pattern = pattern.replaceAll("\\\\", "\\\\\\\\"); //$NON-NLS-1$ //$NON-NLS-2$ 
            pattern = pattern.replaceAll("\\?", "\\\\?"); //$NON-NLS-1$ //$NON-NLS-2$
            pattern = pattern.replaceAll("\\[", "\\\\["); //$NON-NLS-1$ //$NON-NLS-2$
            pattern = pattern.replaceAll("\\{", "\\\\{"); //$NON-NLS-1$ //$NON-NLS-2$
            final PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:*" + pattern); //$NON-NLS-1$
            try {
                List<VirtualFile> list = this.mountPoint.getChildren(new VirtualFileFilter(){

                    @Override
                    public boolean accepts(VirtualFile file) {
                        return matcher.matches(Paths.get(file.getName()));
                    }});
                return list.toArray(new VirtualFile[list.size()]);
            } catch (IOException e) {
                throw new ResourceException(e);
            }
        }

        return new VirtualFile[]{};
    }

    @Override
    public VirtualFile getFile(String path) throws ResourceException {
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
    public boolean add(VirtualFile file) throws ResourceException {
        try {
            return this.client.storeFile(file.getName(), file.openStream());
        } catch (IOException e) {
            throw new ResourceException(UTIL.getString("ftp_failed_write", file.getName(), this.client.getReplyString()), e); //$NON-NLS-1$
        }
    }

    @Override
    public boolean remove(String path) throws ResourceException {
        return VFS.getChild(path).delete();
    }
    
}
