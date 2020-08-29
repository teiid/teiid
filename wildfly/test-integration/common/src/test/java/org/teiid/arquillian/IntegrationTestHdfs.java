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

package org.teiid.arquillian;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.jboss.AdminFactory;
import org.teiid.core.util.ReaderInputStream;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.AbstractMMQueryTestCase;
import org.teiid.jdbc.TeiidDriver;

@RunWith(Arquillian.class)
@SuppressWarnings("nls")
public class IntegrationTestHdfs extends AbstractMMQueryTestCase {

    private static Admin admin;
    private static MiniDFSCluster cluster;

    @BeforeClass
    public static void setup() throws Exception {
        admin = AdminFactory.getInstance().createAdmin("localhost", AdminUtil.MANAGEMENT_PORT, "admin", "admin".toCharArray());
        startMiniCluster(0);

        Properties props = new Properties();
        props.setProperty("FsUri", "hdfs://localhost:" + cluster.getNameNodePort());
        props.setProperty("class-name", "org.teiid.resource.adapter.hdfs.HdfsManagedConnectionFactory");

        AdminUtil.createDataSource(admin, "hdfsDS", "hdfs", props);

        String vdb = "<vdb name=\"hdfs\" version=\"1\">"
                + "<model name=\"hdfs\" type=\"PHYSICAL\">"
                + "<source name=\"hdfs\" translator-name=\"file1\" connection-jndi-name=\"java:/hdfsDS\"/>"
                + "</model>"
                + "<translator name=\"file1\" type=\"file\">"
                + "<property name=\"exceptionIfFileNotFound\" value=\"false\"/>"
                + "</translator>"
                + "</vdb>";

        admin.deploy("hdfs-vdb.xml", new ReaderInputStream(new StringReader(vdb), Charset.forName("UTF-8")));

        assertTrue(AdminUtil.waitForVDBLoad(admin, "hdfs", 1));
    }

    private static void startMiniCluster(int port) throws IOException {
        System.setProperty("hadoop.home.dir", "/");
        File baseDir = new File(UnitTestUtil.getTestScratchPath(), "miniHDFS");
        Configuration conf = new Configuration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
        conf.setBoolean("dfs.webhdfs.enabled", true);
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
        cluster = builder.manageNameDfsDirs(true).manageDataDfsDirs(true)
                .format(true).nameNodePort(port).nameNodeHttpPort(port==0?0:(port+1)).build();
        cluster.waitClusterUp();
    }

    @AfterClass
    public static void teardown() throws AdminException {
        AdminUtil.cleanUp(admin);
        admin.close();
        if (cluster != null) {
            cluster.shutdown();
        }
    }

    @Before
    public void before() throws SQLException {
        this.internalConnection = TeiidDriver.getInstance()
                .connect("jdbc:teiid:hdfs@mm://localhost:31000;user=user;password=user", null);
    }

    @After
    public void after() throws SQLException {
        if (this.internalConnection != null) {
            this.internalConnection.close();
        }
    }

    @Test
    public void testAdd() throws Exception {
        addFile("folder1/hello");
        List<String> virtualFiles = getFiles("folder1/hello");
        assertEquals("The test fails", "hello", virtualFiles.get(0));
    }

    private void addFile(String name) throws SQLException {
        try (Statement s = this.internalConnection.createStatement()) {
            s.execute("call SAVEFILE('"+name+"', cast(X'AABB' as blob))");
        }
    }

    private boolean removeFile(String name) throws SQLException {
        try (Statement s = this.internalConnection.createStatement()) {
            try {
                s.execute("call DELETEFILE('"+name+"')");
                return true;
            } catch (SQLException e) {
            }
        }
        return false;
    }

    private List<String> getFiles(String pattern) throws SQLException {
        List<String> result = new ArrayList<>();
        try (Statement s = this.internalConnection.createStatement()) {
            s.execute("select filePath from (call GETFILES('"+pattern+"')) f");
            ResultSet rs = s.getResultSet();
            while (rs.next()) {
                result.add(rs.getString(1));
            }
        }
        return result;
    }

    @Test
    public void testDeleteFile() throws Exception {
        String file = "main/hello";
        assertFalse(removeFile(file));
        addFile(file);
        assertTrue(removeFile(file));
        List<String> virtualFiles = getFiles(file);
        assertTrue(virtualFiles.isEmpty());
    }

    @Test
    public void testSearch() throws Exception {
        List<String> virtualFiles = getFiles("/user/aditya/*.txt");
        assertEquals(0, virtualFiles.size());
        addFile("/user/aditya/afile.txt");
        addFile("/user/aditya/bfile.txt");
        addFile("/user/aditya/otherfile");
        addFile("/users/x/file");
        //only text files
        virtualFiles = getFiles("/user/aditya/*.txt");
        assertEquals(2, virtualFiles.size());
        //list directory
        virtualFiles = getFiles("/user/aditya/");
        assertEquals(3, virtualFiles.size());

        virtualFiles = getFiles("/u*/a*/");
        assertEquals(3, virtualFiles.size());

        virtualFiles = getFiles("/u**/a*/");
        assertEquals(0, virtualFiles.size());

        virtualFiles = getFiles("/user/aditya/afile.txt");
        assertEquals(1, virtualFiles.size());
    }

    public static void main(String[] args) throws IOException {
        startMiniCluster(9000);
    }

}
