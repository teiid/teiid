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

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;

import javax.resource.ResourceException;

import org.junit.Ignore;
import org.junit.Test;
import org.teiid.file.FtpFileConnection;

public class TestFtpManagedConnectionFactory {
    
    
    private FtpManagedConnectionFactory sample(){
        FtpManagedConnectionFactory mcf = new FtpManagedConnectionFactory();
        mcf.setParentDirectory("/home/kylin/vsftpd"); //$NON-NLS-1$
        mcf.setHost("10.66.192.120"); //$NON-NLS-1$
        mcf.setPort(21); 
        mcf.setUsername("kylin"); //$NON-NLS-1$
        mcf.setPassword("redhat"); //$NON-NLS-1$
        return mcf;
    }
    
    @Ignore
    @Test
    public void testListNames() throws ResourceException {
        FtpFileConnection conn = sample().createConnectionFactory().getConnection();
        String[] names = conn.listNames();
        assertTrue(names != null && names.length > 0);
        conn.close();
    }
    
    @Ignore
    @Test
    public void testExist() throws ResourceException {
        FtpFileConnection conn = sample().createConnectionFactory().getConnection();
        assertTrue(conn.exists("marketdata-price.txt")); //$NON-NLS-1$
        assertFalse(conn.exists("marketdata-price.txt-")); //$NON-NLS-1$
        conn.close();
    }
    
    @Ignore
    @Test
    public void testAddRemove() throws ResourceException, FileNotFoundException {
        FtpFileConnection conn = sample().createConnectionFactory().getConnection();
        String file = "pom.xml"; //$NON-NLS-1$ 
        assertFalse(conn.exists(file)); 
        conn.write(new FileInputStream(file), file); 
        assertTrue(conn.exists(file)); 
        conn.remove(file); 
        assertFalse(conn.exists(file)); 
        conn.close();
    }
    
    @Ignore
    @Test
    public void testRead() throws ResourceException, IOException {
        FtpFileConnection conn = sample().createConnectionFactory().getConnection();
        String file = "pom.xml"; //$NON-NLS-1$
        File localFile = new File(file);
        conn.write(new FileInputStream(localFile), file);
        assertTrue(conn.exists(file)); 
        File remoteFile = File.createTempFile(String.valueOf(System.currentTimeMillis()), ".tmp"); //$NON-NLS-1$
        remoteFile.deleteOnExit();
        FileOutputStream fos = new FileOutputStream(remoteFile);
        conn.read(file, fos);
        assertEquals(localFile.length(), remoteFile.length());
        conn.remove(file);
        conn.close();
    }
    
    @Ignore
    @Test(expected = IOException.class)
    public void testReadNotExist() throws ResourceException, IOException {
        FtpFileConnection conn = sample().createConnectionFactory().getConnection();
        File remoteFile = File.createTempFile(String.valueOf(System.currentTimeMillis()), ".tmp"); //$NON-NLS-1$
        remoteFile.deleteOnExit(); 
        FileOutputStream fos = new FileOutputStream(remoteFile);
        conn.read("no-exist", fos); //$NON-NLS-1$
    }
    
    @Ignore
    @Test
    public void testAppend() throws ResourceException, IOException {
        FtpFileConnection conn = sample().createConnectionFactory().getConnection();
        String file = "pom.xml"; //$NON-NLS-1$
        File localFile = new File(file);
        InputStream in= new FileInputStream(localFile);
        conn.write(in, file);
        conn.append(in, file);
        File remoteFile = File.createTempFile(String.valueOf(System.currentTimeMillis()), ".tmp"); //$NON-NLS-1$
        remoteFile.deleteOnExit();
        FileOutputStream fos = new FileOutputStream(remoteFile);
        conn.read(file, fos);
        assertEquals(localFile.length(), remoteFile.length());
        conn.remove(file);
        conn.close();
    }
    
    @Ignore
    @Test
    public void testRename() throws ResourceException, FileNotFoundException {
        FtpFileConnection conn = sample().createConnectionFactory().getConnection();
        String file = "pom.xml"; //$NON-NLS-1$
        String fileName = "pom-new.xml"; //$NON-NLS-1$
        File localFile = new File(file);
        InputStream in= new FileInputStream(localFile);
        conn.write(in, file);
        assertTrue(conn.exists(file)); 
        assertFalse(conn.exists(fileName)); 
        conn.rename(file, fileName);
        assertFalse(conn.exists(file)); 
        assertTrue(conn.exists(fileName)); 
        conn.remove(fileName);
        conn.close();
    }
    
    @Ignore
    @Test
    public void testGetFile() throws ResourceException {
        FtpFileConnection conn = sample().createConnectionFactory().getConnection();
        assertTrue(conn.getFile("marketdata-price.txt").exists()); //$NON-NLS-1$
        assertNull(conn.getFile("*.txt"));  //$NON-NLS-1$
    }
    
    @Test
    public void testPattern() {
        String fileName = "*.txt"; //$NON-NLS-1$
        PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + fileName); //$NON-NLS-1$ 
        String[] array = new String[]{"marketdata-price.txt", "marketdata-price1.txt"}; //$NON-NLS-1$ //$NON-NLS-2$
        for(String name : array) {
            assertTrue(matcher.matches(Paths.get(name)));
        }
        
        fileName = "*data*"; //$NON-NLS-1$
        matcher = FileSystems.getDefault().getPathMatcher("glob:" + fileName);  //$NON-NLS-1$
        for(String name : array) {
            assertTrue(matcher.matches(Paths.get(name)));
        }
    }
    
    @Ignore
    @Test
    public void testGetFiles() throws ResourceException {
        FtpFileConnection conn = sample().createConnectionFactory().getConnection();
        File[] files = FtpFileConnection.Util.getFiles("*.txt", conn, true); //$NON-NLS-1$
        assertEquals(2, files.length);
    }

}
