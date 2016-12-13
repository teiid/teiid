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

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;

import javax.resource.ResourceException;

import org.jboss.vfs.VirtualFile;
import org.junit.Test;
import org.teiid.file.VirtualFileConnection;

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
    
    @Test
    public void testGetFiles() throws ResourceException, IOException {
        VirtualFileConnection conn = sample();
        VirtualFile[] files = conn.getFiles("*.txt");
        assertEquals(2, files.length); //$NON-NLS-1$ 
        for(VirtualFile file : files) {
            System.out.println(file.openStream());
        }
        conn.close();
    }

}
