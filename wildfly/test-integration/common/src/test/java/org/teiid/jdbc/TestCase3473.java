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

package org.teiid.jdbc;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;


/**
 */
@SuppressWarnings("nls")
public class TestCase3473 {

    private DatabaseMetaData dbmd;
    private static FakeServer server;

    @BeforeClass public static void oneTimeSetup() throws Exception {
        server = new FakeServer(true);
        server.deployVDB("test", UnitTestUtil.getTestDataPath() + "/TestCase3473/test.vdb");
    }

    @AfterClass public static void oneTimeTeardown() throws Exception {
        server.stop();
    }

    ////////////////////Query Related Methods///////////////////////////

    @Before public void setUp() throws Exception {
        Connection conn = server.createConnection("jdbc:teiid:test"); //$NON-NLS-1$
        dbmd = conn.getMetaData();
    }

    @Test public void testGetCrossReference() throws Exception {
        ResultSet rs = dbmd.getCrossReference(null,"test", "all_databases", null,"test", "all_models");//$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        ResultSet rs1 = dbmd.getCrossReference(null, "Foo%", "%", null, null, "%"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ResultSet rs2 = dbmd.getCrossReference("foo", "Foo%", "%", null, null, "%"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        ResultSet rs3 = dbmd.getCrossReference(null, null, null, null, null, null);
        TestMMDatabaseMetaData.compareResultSet(rs, rs1, rs2, rs3);
    }

    @Test public void testGetImportedKeys() throws Exception {
        ResultSet rs = dbmd.getImportedKeys(null,"test", "all_models"); //$NON-NLS-1$ //$NON-NLS-2$
        ResultSet rs1 = dbmd.getImportedKeys(null, "Foo%", "%"); //$NON-NLS-1$ //$NON-NLS-2$
        ResultSet rs2 = dbmd.getImportedKeys("foo", "Foo%", "%"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        TestMMDatabaseMetaData.compareResultSet(rs, rs1, rs2);
    }

    @Test public void testGetExportedKeys() throws Exception {
        ResultSet rs = dbmd.getExportedKeys(null,"test", "all_models"); //$NON-NLS-1$ //$NON-NLS-2$
        ResultSet rs1 = dbmd.getExportedKeys(null, "Foo%", "%"); //$NON-NLS-1$ //$NON-NLS-2$
        ResultSet rs2 = dbmd.getExportedKeys("foo", "Foo%", "%"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        TestMMDatabaseMetaData.compareResultSet(rs, rs1, rs2);
    }

    @Test public void testGetPrimaryKeys() throws Exception {
        ResultSet rs = dbmd.getPrimaryKeys(null,"test", "all_models"); //$NON-NLS-1$ //$NON-NLS-2$
        ResultSet rs1 = dbmd.getPrimaryKeys(null, "Foo%", "test.all_models"); //$NON-NLS-1$ //$NON-NLS-2$
        ResultSet rs2 = dbmd.getPrimaryKeys("foo", "Foo%", "test.all_models"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        TestMMDatabaseMetaData.compareResultSet(rs, rs1, rs2);
    }

    @Test public void testGetTables() throws Exception{
        ResultSet rs = dbmd.getTables(null, null, null, null);
        ResultSet rs1 = dbmd.getTables(null, "%foo", null, null); //$NON-NLS-1$
        ResultSet rs2 = dbmd.getTables("foo", "%foo", null, null); //$NON-NLS-1$ //$NON-NLS-2$
        TestMMDatabaseMetaData.compareResultSet(rs, rs1, rs2);
        assertTrue(dbmd.getTables(null, null, null, new String[] {"VIEW"}).next());

        Properties p = new Properties();
        p.setProperty(DatabaseMetaDataImpl.REPORT_AS_VIEWS, "true");
        Connection c = server.getDriver().connect("jdbc:teiid:test", p);
        DatabaseMetaData dmd = c.getMetaData();
        assertTrue(dmd.getTables(null, null, null, new String[] {"VIEW"}).next());
    }
}
