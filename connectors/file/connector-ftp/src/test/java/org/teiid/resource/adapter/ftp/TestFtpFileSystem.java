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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import javax.resource.ResourceException;

import org.apache.commons.net.ftp.FTPClient;
import org.jboss.vfs.VFS;
import org.jboss.vfs.VirtualFile;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

@Ignore("Ignore due to this test depend on remote ftp server and reference configuration")
public class TestFtpFileSystem {

    @BeforeClass
    public static void sample() throws IOException, ResourceException {
        FtpManagedConnectionFactory mcf = new FtpManagedConnectionFactory();
        mcf.setParentDirectory("/home/kylin/vsftpd"); //$NON-NLS-1$
        mcf.setHost("10.66.192.120"); //$NON-NLS-1$
        mcf.setPort(21); 
        mcf.setUsername("kylin"); //$NON-NLS-1$
        mcf.setPassword("redhat"); //$NON-NLS-1$
        VirtualFile mountPoint = VFS.getChild("/home/kylin/vsftpd"); //$NON-NLS-1$
        VFS.mount(mountPoint, new FtpFileSystem(mcf.createClient()));
    }
    
    @Test
    public void testGetFile() throws IOException, ResourceException {
        File file = VFS.getChild("/home/kylin/vsftpd/not-exist.txt").getPhysicalFile(); //$NON-NLS-1$
        assertNull(file);
    }
    
    @Test
    public void testOpenStream() throws IOException, ResourceException {
        InputStream in = VFS.getChild("/home/kylin/vsftpd/not-exist.txt").openStream(); //$NON-NLS-1$
        assertNull(in);
        in = VFS.getChild("/home/kylin/vsftpd/marketdata-price.txt").openStream(); //$NON-NLS-1$
        assertNotNull(in);
        in.close();
    }
    
    @Test
    public void testDelete() throws IOException, ResourceException {
        writeFile(new File("pom.xml").getAbsolutePath()); //$NON-NLS-1$
        VirtualFile file = VFS.getChild("/home/kylin/vsftpd/pom.xml"); //$NON-NLS-1$
        assertTrue(file.exists());
        assertTrue(file.delete());
        assertFalse(file.exists());
    }
    
    private void writeFile(String path) throws ResourceException, IOException {
        VirtualFile file = VFS.getChild(path);
        FtpManagedConnectionFactory mcf = new FtpManagedConnectionFactory();
        mcf.setParentDirectory("/home/kylin/vsftpd"); //$NON-NLS-1$
        mcf.setHost("10.66.192.120"); //$NON-NLS-1$
        mcf.setPort(21); 
        mcf.setUsername("kylin"); //$NON-NLS-1$
        mcf.setPassword("redhat"); //$NON-NLS-1$
        FTPClient client = mcf.createClient();  
        client.storeFile(file.getName(), file.openStream());
        client.disconnect();
    }
    
    @Test
    public void testGetSize() {
        assertEquals(-1, VFS.getChild("/home/kylin/vsftpd/marketdata-price.txt").getSize()); //$NON-NLS-1$
        assertEquals(-1, VFS.getChild("/home/kylin/vsftpd/sub").getSize()); //$NON-NLS-1$
 
    }
    
    @Test
    public void testGetLastModified() {
        assertEquals(1480314470000l, VFS.getChild("/home/kylin/vsftpd/marketdata-price.txt").getLastModified()); //$NON-NLS-1$
        assertEquals(-1, VFS.getChild("/home/kylin/vsftpd/sub").getLastModified()); //$NON-NLS-1$
    }
    
    @Test
    public void testExists(){
        assertTrue(VFS.getChild("/home/kylin/vsftpd/marketdata-price.txt").exists()); //$NON-NLS-1$
        assertTrue(VFS.getChild("/home/kylin/vsftpd/marketdata-price1.txt").exists()); //$NON-NLS-1$
        assertTrue(VFS.getChild("/home/kylin/vsftpd/sub").exists()); //$NON-NLS-1$
    }
    
    @Test
    public void testIsFile() {
        assertTrue(VFS.getChild("/home/kylin/vsftpd/marketdata-price.txt").isFile()); //$NON-NLS-1$
        assertTrue(VFS.getChild("/home/kylin/vsftpd/marketdata-price1.txt").isFile()); //$NON-NLS-1$
        assertFalse(VFS.getChild("/home/kylin/vsftpd/sub").isFile()); //$NON-NLS-1$
    }
    
    @Test
    public void testIsDirectory() {
        assertFalse(VFS.getChild("/home/kylin/vsftpd/marketdata-price.txt").isDirectory()); //$NON-NLS-1$
        assertFalse(VFS.getChild("/home/kylin/vsftpd/marketdata-price1.txt").isDirectory()); //$NON-NLS-1$
        assertTrue(VFS.getChild("/home/kylin/vsftpd/sub").isDirectory()); //$NON-NLS-1$
    }
    
    @Test
    public void testNamelist() {
        assertTrue(VFS.getChild("/home/kylin/vsftpd").getChildren().size() > 0); //$NON-NLS-1$
    }

}
