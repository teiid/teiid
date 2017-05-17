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

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;

import javax.resource.ResourceException;

import org.jboss.vfs.VFS;
import org.jboss.vfs.VirtualFile;
import org.junit.Ignore;
import org.junit.Test;
import org.teiid.file.VirtualFileConnection;

@Ignore("Ignore due to this test depend on remote ftp server and reference configuration")
public class TestFtpFileConnection {
    
    private static VirtualFileConnection sample() throws ResourceException {
        FtpManagedConnectionFactory mcf = new FtpManagedConnectionFactory();
        mcf.setParentDirectory("/home/kylin/vsftpd"); //$NON-NLS-1$
        mcf.setHost("10.66.192.120"); //$NON-NLS-1$
        mcf.setPort(21); 
        mcf.setUsername("kylin"); //$NON-NLS-1$
        mcf.setPassword("redhat"); //$NON-NLS-1$
        return mcf.createConnectionFactory().getConnection();
    }
    
    @Test
    public void testGetFile() throws ResourceException, IOException {
        
        VirtualFileConnection conn = sample();
        VirtualFile file = conn.getFile("marketdata-price.txt"); //$NON-NLS-1$
        assertNotNull(file.openStream());
        file = conn.getFile("marketdata-price1.txt"); //$NON-NLS-1$
        assertNotNull(file.openStream());
        conn.close();
    }
    
    @Test
    public void testPatternFilter() {
        String fileName = "*.txt"; //$NON-NLS-1$
        PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:*" + fileName); //$NON-NLS-1$ 
        String[] array = new String[]{"/path/to/marketdata-price.txt", "marketdata-price1.txt"}; //$NON-NLS-1$ //$NON-NLS-2$
        for(String name : array) {
            assertTrue(matcher.matches(Paths.get(name)));
        }
    }
    
    @Test(expected = ResourceException.class)
    public void testGetFiles() throws ResourceException, IOException {
        VirtualFileConnection conn = sample();
        VirtualFile[] files = conn.getFiles("*.txt");
        assertEquals(2, files.length); //$NON-NLS-1$ 
        conn.close();
    }
    
    @Test
    public void testAdd() throws IOException, ResourceException {
        VirtualFileConnection conn = sample();
        VirtualFile file = conn.getFile("pom.xml"); //$NON-NLS-1$
        assertFalse(file.exists());
        VirtualFile pom = VFS.getChild(new File("pom.xml").getAbsolutePath()); //$NON-NLS-1$
        conn.add(pom.openStream(), pom);
        assertTrue(file.exists());
        conn.close();
        conn = sample();
        assertTrue(file.isFile());
        assertNotNull(file.openStream());
        conn.close();
    }
    
    @Test
    public void testRemove() throws ResourceException, IOException {
        VirtualFileConnection conn = sample();
        VirtualFile pom = VFS.getChild(new File("pom.xml").getAbsolutePath()); //$NON-NLS-1$
        VirtualFile file = conn.getFile("pom.xml"); //$NON-NLS-1$
        conn.add(pom.openStream(), pom);
        assertTrue(file.exists());
        conn.remove("pom.xml"); //$NON-NLS-1$
        assertFalse(file.exists());
        conn.close();
    }

}
