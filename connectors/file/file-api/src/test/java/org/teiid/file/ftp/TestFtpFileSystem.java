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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.InputStream;

import org.apache.commons.net.ftp.FTPClient;
import org.jboss.vfs.VFS;
import org.jboss.vfs.VirtualFile;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

@Ignore("Ignore due to this test depend on remote ftp server and reference configuration")
public class TestFtpFileSystem {

    @BeforeClass
    public static void sample() throws Exception {
        FtpFileConnection conn = TestFtpFileConnection.sample();

        VirtualFile mountPoint = VFS.getChild("/home/kylin/vsftpd"); //$NON-NLS-1$
        VFS.mount(mountPoint, new FtpFileSystem(conn.getClient()));
    }

    @Test
    public void testGetFile() throws Exception {
        File file = VFS.getChild("/home/kylin/vsftpd/not-exist.txt").getPhysicalFile(); //$NON-NLS-1$
        assertNull(file);
    }

    @Test
    public void testOpenStream() throws Exception {
        InputStream in = VFS.getChild("/home/kylin/vsftpd/not-exist.txt").openStream(); //$NON-NLS-1$
        assertNull(in);
        in = VFS.getChild("/home/kylin/vsftpd/marketdata-price.txt").openStream(); //$NON-NLS-1$
        assertNotNull(in);
        in.close();
    }

    @Test
    public void testDelete() throws Exception {
        writeFile(new File("pom.xml").getAbsolutePath()); //$NON-NLS-1$
        VirtualFile file = VFS.getChild("/home/kylin/vsftpd/pom.xml"); //$NON-NLS-1$
        assertTrue(file.exists());
        assertTrue(file.delete());
        assertFalse(file.exists());
    }

    private void writeFile(String path) throws Exception {
        VirtualFile file = VFS.getChild(path);
        FtpFileConnection conn = TestFtpFileConnection.sample();
        FTPClient client = conn.getClient();
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
        assertEquals(1480314470000L, VFS.getChild("/home/kylin/vsftpd/marketdata-price.txt").getLastModified()); //$NON-NLS-1$
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
