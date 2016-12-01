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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import javax.resource.ResourceException;

import org.apache.commons.net.ftp.FTPClient;
import org.teiid.core.BundleUtil;
import org.teiid.file.FtpFileConnection;
import org.teiid.resource.spi.BasicConnection;

public class FtpFileConnectionImpl extends BasicConnection implements FtpFileConnection {
    
    public static final BundleUtil UTIL = BundleUtil.getBundleUtil(FtpFileConnectionImpl.class);
    
    private final FTPClient client;
		
	public FtpFileConnectionImpl(FTPClient client, String parentDirectory) {
        this.client = client;
    }

    @Override
	public File getFile(String name) throws ResourceException {
        try {
            if(this.exists(name)) {
                File tmpFile = File.createTempFile(String.valueOf(System.currentTimeMillis()), ".tmp"); //$NON-NLS-1$
                tmpFile.deleteOnExit(); 
                this.read(name, new FileOutputStream(tmpFile));
                return tmpFile;
            }
        } catch (IOException e) {
            throw new ResourceException(e);
        }
    	return null;
    }

	@Override
    public File[] getFiles(String pattern) throws ResourceException {
	    
	    File datafile = this.getFile(pattern);
        if(datafile != null) {
            return new File[] {datafile};
        }
        
        if (pattern.contains("*"))  { //$NON-NLS-1$ 
            pattern = pattern.replaceAll("\\\\", "\\\\\\\\"); //$NON-NLS-1$ //$NON-NLS-2$ 
            pattern = pattern.replaceAll("\\?", "\\\\?"); //$NON-NLS-1$ //$NON-NLS-2$
            pattern = pattern.replaceAll("\\[", "\\\\["); //$NON-NLS-1$ //$NON-NLS-2$
            pattern = pattern.replaceAll("\\{", "\\\\{"); //$NON-NLS-1$ //$NON-NLS-2$
            
            try {
                PathMatcher matcher =  FileSystems.getDefault().getPathMatcher("glob:" + pattern); //$NON-NLS-1$
                String[] allFiles = this.listNames();
                List<File> files = new ArrayList<>(allFiles.length);
                for(String name : allFiles) {
                    if(matcher.matches(Paths.get(name))) {
                        File remoteFile = File.createTempFile(String.valueOf(System.currentTimeMillis()), ".tmp"); //$NON-NLS-1$
                        remoteFile.deleteOnExit();
                        FileOutputStream fos = new FileOutputStream(remoteFile);
                        this.read(name, fos);
                        files.add(remoteFile);
                    }
                }
                return files.toArray(new File[files.size()]);
            } catch (IOException e) {
                throw new ResourceException(e);
            } 
        }
        return null;
    }

    @Override
	public void close() throws ResourceException {
		try {
            this.client.disconnect();
        } catch (IOException e) {
            throw new ResourceException(e);
        }
	}

    @Override
    public boolean remove(String name) throws ResourceException {
        try {
            return this.client.deleteFile(name);
        } catch (IOException e) {
            throw new ResourceException(e);
        }
    }

    @Override
    public void read(String name, OutputStream out) throws ResourceException {
        try {
            boolean completed = this.client.retrieveFile(name, out);
            if(!completed) {
                throw new IOException(UTIL.getString("ftp_failed_read", name, this.client.getReplyString())); //$NON-NLS-1$
            }
        } catch (IOException e) {
            throw new ResourceException(e);
        }
    }

    @Override
    public void write(InputStream in, String name) throws ResourceException {
        try {
            boolean completed = this.client.storeFile(name, in);
            if(!completed) {
                throw new IOException(UTIL.getString("ftp_failed_write", name, this.client.getReplyString())); //$NON-NLS-1$
            }
        } catch (IOException e) {
            throw new ResourceException(e);
        }
    }

    @Override
    public void append(InputStream in, String name) throws ResourceException {
        try {
            boolean completed = this.client.appendFile(name, in);
            if(!completed) {
                throw new IOException(UTIL.getString("ftp_failed_append", name, this.client.getReplyString())); //$NON-NLS-1$
            }
        } catch (IOException e) {
            throw new ResourceException(e);
        }
    }

    @Override
    public void rename(String oldName, String newName) throws ResourceException {
        try {
            boolean completed = this.client.rename(oldName, newName);
            if(!completed) {
                throw new IOException(UTIL.getString("ftp_failed_rename", oldName, newName, this.client.getReplyString())); //$NON-NLS-1$
            }
        } catch (IOException e) {
            throw new ResourceException(e);
        }
    }

    @Override
    public boolean exists(String name) throws ResourceException {
        try {
            String[] names = this.client.listNames();
            boolean exists = false;
            if (names != null && names.length > 0){
                for(String n : names) {
                    if(n.equals(name)) {
                        exists = true;
                        break;
                    }
                }
            }
            return exists;
        } catch (IOException e) {
            throw new ResourceException(e);
        }
    }

    @Override
    public String[] listNames() throws ResourceException {
        try {
            return this.client.listNames();
        } catch (IOException e) {
            throw new ResourceException(e);
        }
    }
	
}
