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

package org.teiid.systemmodel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.teiid.adminapi.Model.Type;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBImportMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.FakeServer;
import org.teiid.jdbc.FakeServer.DeployVDBParameter;
import org.teiid.jdbc.TeiidSQLException;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.metadata.FunctionMethod.PushDown;
import org.teiid.metadata.FunctionParameter;
import org.teiid.runtime.HardCodedExecutionFactory;
import org.teiid.translator.loopback.LoopbackExecutionFactory;

@SuppressWarnings("nls")
public class TestMatViews {

    private static final String MATVIEWS = "matviews";
    private Connection conn;
    private FakeServer server;

    private static int count = 0;

    public static int pause() throws InterruptedException {
        synchronized (TestMatViews.class) {
            count++;
            TestMatViews.class.notify();
            while (count < 2) {
                TestMatViews.class.wait();
            }
        }
        return 1;
    }

    @Before public void setUp() throws Exception {
        server = new FakeServer(true);
        HashMap<String, Collection<FunctionMethod>> udfs = new HashMap<String, Collection<FunctionMethod>>();
        udfs.put("funcs", Arrays.asList(new FunctionMethod("pause", null, null, PushDown.CANNOT_PUSHDOWN, TestMatViews.class.getName(), "pause", null, new FunctionParameter("return", DataTypeManager.DefaultDataTypes.INTEGER), true, Determinism.NONDETERMINISTIC)));
        server.deployVDB(MATVIEWS, UnitTestUtil.getTestDataPath() + "/matviews.vdb", new DeployVDBParameter(udfs, null));
        conn = server.createConnection("jdbc:teiid:matviews");
    }

    @After public void tearDown() throws Exception {
        conn.close();
        server.stop();
    }

    @Test public void testSystemMatViewsWithImplicitLoad() throws Exception {
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("select * from MatViews order by name");
        assertTrue(rs.next());
        assertEquals("NEEDS_LOADING", rs.getString("loadstate"));
        assertEquals("#MAT_TEST.ERRORVIEW", rs.getString("targetName"));
        assertTrue(rs.next());
        assertEquals("NEEDS_LOADING", rs.getString("loadstate"));
        assertEquals("#MAT_TEST.MATVIEW", rs.getString("targetName"));
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean("valid"));
        assertEquals("#MAT_TEST.RANDOMVIEW", rs.getString("targetName"));
        rs = s.executeQuery("select * from MatView");
        assertTrue(rs.next());
        rs = s.executeQuery("select * from MatViews where name = 'MatView'");
        assertTrue(rs.next());
        assertEquals("LOADED", rs.getString("loadstate"));
        try {
            s.executeQuery("select * from ErrorView");
        } catch (SQLException e) {

        }
        rs = s.executeQuery("select * from MatViews where name = 'ErrorView'");
        assertTrue(rs.next());
        assertEquals("FAILED_LOAD", rs.getString("loadstate"));
    }

    @Test public void testSystemMatViewsWithExplicitRefresh() throws Exception {
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("select * from (call refreshMatView('TEST.RANDOMVIEW', false)) p");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        rs = s.executeQuery("select * from MatViews where name = 'RandomView'");
        assertTrue(rs.next());
        assertEquals("LOADED", rs.getString("loadstate"));
        assertEquals(true, rs.getBoolean("valid"));
        rs = s.executeQuery("select x from TEST.RANDOMVIEW");
        assertTrue(rs.next());
        double key = rs.getDouble(1);

        rs = s.executeQuery("select * from (call refreshMatView('TEST.RANDOMVIEW', false)) p");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        rs = s.executeQuery("select * from MatViews where name = 'RandomView'");
        assertTrue(rs.next());
        assertEquals("LOADED", rs.getString("loadstate"));
        assertEquals(true, rs.getBoolean("valid"));
        rs = s.executeQuery("select x from TEST.RANDOMVIEW");
        assertTrue(rs.next());
        double key1 = rs.getDouble(1);

        //ensure that invalidate with distributed caching works
        assertTrue(key1 != key);
    }

    @Test public void testSystemMatViewsWithExplictRefreshAndInvalidate() throws Exception {
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("select * from (call refreshMatView('TEST.MATVIEW', false)) p");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        rs = s.executeQuery("select * from MatViews where name = 'MatView'");
        assertTrue(rs.next());
        assertEquals("LOADED", rs.getString("loadstate"));
        assertEquals(true, rs.getBoolean("valid"));

        count = 0;
        s.execute("alter view TEST.MATVIEW as select pause() as x");
        Thread t = new Thread() {
            public void run() {
                try {
                    Statement s1 = conn.createStatement();
                    ResultSet rs = s1.executeQuery("select * from (call refreshMatView('TEST.MATVIEW', true)) p");
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt(1));
                } catch (Exception e) {
                    throw new TeiidRuntimeException(e);
                }
            }
        };
        t.start();
        synchronized (TestMatViews.class) {
            while (count < 1) {
                TestMatViews.class.wait();
            }
        }
        rs = s.executeQuery("select * from MatViews where name = 'MatView'");
        assertTrue(rs.next());
        assertEquals("LOADING", rs.getString("loadstate"));
        assertEquals(false, rs.getBoolean("valid"));

        synchronized (TestMatViews.class) {
            count++;
            TestMatViews.class.notify();
        }
        t.join();

        rs = s.executeQuery("select * from MatViews where name = 'MatView'");
        assertTrue(rs.next());
        assertEquals("LOADED", rs.getString("loadstate"));
        assertEquals(true, rs.getBoolean("valid"));
    }

    @Test(expected=TeiidSQLException.class) public void testSystemMatViewsInvalidView() throws Exception {
        Statement s = conn.createStatement();
        s.execute("call refreshMatView('TEST.NotMat', false)");
    }

    @Test(expected=TeiidSQLException.class) public void testSystemMatViewsInvalidView1() throws Exception {
        Statement s = conn.createStatement();
        s.execute("call refreshMatView('foo', false)");
    }

    @Test(expected=TeiidSQLException.class) public void testSystemMatViewsWithRowRefreshNotAllowed() throws Exception {
        Statement s = conn.createStatement();
        s.execute("alter view test.randomview as select rand() as x, rand() as y");
        ResultSet rs = s.executeQuery("select * from (call refreshMatView('TEST.RANDOMVIEW', false)) p");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        rs = s.executeQuery("select * from MatViews where name = 'RandomView'");
        assertTrue(rs.next());
        assertEquals("LOADED", rs.getString("loadstate"));
        assertEquals(true, rs.getBoolean("valid"));
        rs = s.executeQuery("select x from TEST.RANDOMVIEW");
        assertTrue(rs.next());
        double key = rs.getDouble(1);

        rs = s.executeQuery("select * from (call refreshMatViewRow('TEST.RANDOMVIEW', "+key+")) p");
    }

    @Test public void testSystemMatViewsWithRowRefresh() throws Exception {
        Statement s = conn.createStatement();

        s.execute("alter view test.randomview as /*+ cache(updatable) */ select rand() as x, rand() as y");
        //prior to load refresh of a single row returns -1
        ResultSet rs = s.executeQuery("select * from (call refreshMatViewRow('TEST.RANDOMVIEW', 0)) p");
        assertTrue(rs.next());
        assertEquals(-1, rs.getInt(1));
        assertFalse(rs.next());

        rs = s.executeQuery("select * from (call refreshMatView('TEST.RANDOMVIEW', false)) p");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        rs = s.executeQuery("select * from MatViews where name = 'RandomView'");
        assertTrue(rs.next());
        assertEquals("LOADED", rs.getString("loadstate"));
        assertEquals(true, rs.getBoolean("valid"));
        rs = s.executeQuery("select x from TEST.RANDOMVIEW");
        assertTrue(rs.next());
        double key = rs.getDouble(1);

        rs = s.executeQuery("select * from (call refreshMatViewRow('TEST.RANDOMVIEW', "+key+")) p");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1)); //1 row updated (removed)

        rs = s.executeQuery("select * from TEST.RANDOMVIEW");
        assertFalse(rs.next());

        rs = s.executeQuery("select * from (call refreshMatViewRow('TEST.RANDOMVIEW', "+key+")) p");
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1)); //no rows updated
    }

    @Test(expected=TeiidSQLException.class) public void testSystemMatViewsWithRowRefreshNoPk() throws Exception {
        Statement s = conn.createStatement();
        s.executeQuery("select * from (call refreshMatView('TEST.MATVIEW', false)) p");
        //prior to load refresh of a single row returns -1
        s.executeQuery("select * from (call refreshMatViewRow('TEST.MATVIEW', 0)) p");
    }

    @Test public void testMatViewWithImportedVDB() throws Exception {
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("phy");
        mmd.setSchemaSourceType("DDL");
        mmd.setSchemaText("CREATE FOREIGN TABLE t1 ( col1 string, col2 integer )");
        mmd.addSourceMapping("phy", "loopback", null);

        ModelMetaData mmd1 = new ModelMetaData();
        mmd1.setName("phy_mv");
        mmd1.setSchemaSourceType("DDL");
        mmd1.setSchemaText("CREATE FOREIGN TABLE t1_mv ( col1 string, col2 integer )" +
                 " create foreign table status (VDBNAME STRING, VDBVERSION STRING, "
                + " SCHEMANAME STRING, NAME STRING, TARGETSCHEMANAME STRING, TARGETNAME STRING, "
                + " VALID BOOLEAN, LOADSTATE STRING, CARDINALITY LONG, UPDATED TIMESTAMP, LOADNUMBER LONG, NODENAME STRING, STALECOUNT LONG)");
        mmd1.addSourceMapping("phy_mv", "loopback", null);

        ModelMetaData mmd2 = new ModelMetaData();
        mmd2.setName("view1");
        mmd2.setModelType(Type.VIRTUAL);
        mmd2.setSchemaSourceType("DDL");
        mmd2.setSchemaText("CREATE VIEW v1 ( col1 string, col2 integer ) OPTIONS (MATERIALIZED true, "
                + "MATERIALIZED_TABLE 'phy_mv.t1_mv', \"teiid_rel:MATVIEW_STATUS_TABLE\" 'phy_mv.status', \"teiid_rel:MATVIEW_LOAD_SCRIPT\" 'select 1') AS select t1.col1, t1.col2 FROM t1");
        server.addTranslator(LoopbackExecutionFactory.class);
        server.deployVDB("base", mmd, mmd1, mmd2);

        VDBMetaData vdbMetaData = new VDBMetaData();
        vdbMetaData.setXmlDeployment(true);
        VDBImportMetadata importVDB = new VDBImportMetadata();
        importVDB.setName("base");
        importVDB.setVersion("1");
        vdbMetaData.getVDBImports().add(importVDB);
        vdbMetaData.setName("importing");

        server.deployVDB(vdbMetaData);
    }

    @Test public void testImportedMatView() throws Exception {
        ModelMetaData mmd2 = new ModelMetaData();
        mmd2.setName("view1");
        mmd2.setModelType(Type.PHYSICAL);
        mmd2.setSchemaSourceType("DDL");
        mmd2.setSchemaText("create foreign table x (col integer); CREATE VIEW v1 ( col1 string ) OPTIONS (MATERIALIZED true) AS select current_database() from x");
        mmd2.addSourceMapping("a", "a", null);
        HardCodedExecutionFactory hcef = new HardCodedExecutionFactory();
        hcef.addData("SELECT x.col FROM x", Arrays.asList(Collections.singletonList(1)));
        server.addTranslator("a", hcef);
        server.deployVDB("base", mmd2);

        VDBMetaData vdbMetaData = new VDBMetaData();
        vdbMetaData.setXmlDeployment(true);
        VDBImportMetadata importVDB = new VDBImportMetadata();
        importVDB.setName("base");
        importVDB.setVersion("1");
        vdbMetaData.getVDBImports().add(importVDB);
        vdbMetaData.setName("importing");

        server.deployVDB(vdbMetaData);

        Connection c = server.getDriver().connect("jdbc:teiid:importing", null);
        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("select * from v1");
        rs.next();
        assertEquals("base", rs.getString(1));
    }

    @Test(expected=TeiidSQLException.class) public void testSessionScopingFails() throws Exception {
        Statement s = conn.createStatement();
        s.execute("alter view test.randomview as /*+ cache(scope:session) */ select rand() as x, rand() as y");
        ResultSet rs = s.executeQuery("select * from MatViews where name = 'MatView'");
        assertTrue(rs.next());
        assertEquals("NEEDS_LOADING", rs.getString("loadstate"));
        assertEquals(false, rs.getBoolean("valid"));

        s.executeQuery("select * from randomview");
    }

    @Test public void testCompositeRowUpdate() throws Exception {
        ModelMetaData mmd2 = new ModelMetaData();
        mmd2.setName("view1");
        mmd2.setModelType(Type.VIRTUAL);
        mmd2.setSchemaSourceType("DDL");
        mmd2.setSchemaText("CREATE VIEW v1 ( col integer, col1 string, primary key (col, col1) ) OPTIONS (MATERIALIZED true) AS /*+ cache(updatable) */ select 1, current_database()");
        server.deployVDB("comp", mmd2);

        Connection c = server.getDriver().connect("jdbc:teiid:comp", null);
        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("select * from v1");
        rs.next();
        assertEquals("1", rs.getString(1));

        try {
            rs = s.executeQuery("select * from (call refreshMatViewRow('view1.v1', 0)) p");
            fail();
        } catch (SQLException e) {
            //not enough key parameters
        }

        rs = s.executeQuery("select * from (call refreshMatViewRow('view1.v1', 0, 'a')) p");
        assertTrue(rs.next());
        //row doesn't exist
        assertEquals(0, rs.getInt(1));
        assertFalse(rs.next());

        rs = s.executeQuery("select * from (call refreshMatViewRow('view1.v1', '1', 'comp')) p");
        assertTrue(rs.next());
        //row does exist
        assertEquals(1, rs.getInt(1));
        assertFalse(rs.next());
    }

    @Test public void testCompositeRowsUpdate() throws Exception {
        ModelMetaData mmd2 = new ModelMetaData();
        mmd2.setName("view1");
        mmd2.setModelType(Type.VIRTUAL);
        mmd2.setSchemaSourceType("DDL");
        mmd2.setSchemaText("CREATE VIEW v1 ( col integer, col1 string, primary key (col, col1) ) OPTIONS (MATERIALIZED true) AS /*+ cache(updatable) */ select 1, current_database()");
        server.deployVDB("comp", mmd2);

        Connection c = server.getDriver().connect("jdbc:teiid:comp", null);
        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("select * from v1");
        rs.next();
        assertEquals("1", rs.getString(1));

        try {
            rs = s.executeQuery("select * from (call refreshMatViewRows('view1.v1', (0,))) p");
            fail();
        } catch (SQLException e) {
            //not enough key parameters
        }

        rs = s.executeQuery("select * from (call refreshMatViewRows('view1.v1', (0, 'a'))) p");
        assertTrue(rs.next());
        //row doesn't exist
        assertEquals(0, rs.getInt(1));
        assertFalse(rs.next());

        rs = s.executeQuery("select * from (call refreshMatViewRows('view1.v1', ('1', 'comp'), ('2', 'comp'))) p");
        assertTrue(rs.next());
        //row does exist
        assertEquals(1, rs.getInt(1));
        assertFalse(rs.next());
    }

    @Test public void testMatViewProceduresWithSameName() throws Exception {
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("x");
        mmd.setModelType(Type.VIRTUAL);
        mmd.addSourceMetadata("DDL", "create view T as select 1");
        ModelMetaData mmd1 = new ModelMetaData();
        mmd1.setName("y");
        mmd1.setModelType(Type.VIRTUAL);
        mmd1.addSourceMetadata("DDL", "create view T as select 1");
        server.deployVDB("test", mmd, mmd1);

        Connection c = server.getDriver().connect("jdbc:teiid:test", null);
        Statement s = c.createStatement();
        try {
            s.execute("call sysadmin.matviewstatus('x', 'T')");
        } catch (TeiidSQLException e) {
            e.getTeiidCode().equals("TEIID30167");
        }

        try {
            s.execute("call sysadmin.loadmatview('x', 'T')");
        } catch (TeiidSQLException e) {
            e.getTeiidCode().equals("TEIID30167");
        }

        try {
            s.execute("call sysadmin.updateMatView('x', 'T')");
        } catch (TeiidSQLException e) {
            e.getTeiidCode().equals("TEIID30167");
        }

    }

    @Test public void testMatViewStatusInternal() throws Exception {
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("x");
        mmd.setModelType(Type.VIRTUAL);
        mmd.addSourceMetadata("DDL", "create view T options (materialized true) as select 1");
        server.deployVDB("test", mmd);

        Connection c = server.getDriver().connect("jdbc:teiid:test", null);
        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("call sysadmin.matviewstatus('x', 'T')");
        rs.next();
        assertNull(rs.getString("TargetSchemaName"));
        assertEquals("NEEDS_LOADING", rs.getString("LoadState"));
    }

    @Test
    public void testloadMatViewInternal() throws Exception {
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("x");
        mmd.setModelType(Type.VIRTUAL);
        mmd.addSourceMetadata("DDL", "create view T options (materialized true) as select 1");
        server.deployVDB("test", mmd);

        Connection c = server.getDriver().connect("jdbc:teiid:test", null);
        CallableStatement s = c.prepareCall("call sysadmin.loadMatView('x', 'T', true)");
        assertFalse(s.execute());
        assertEquals(1, s.getInt(1));
    }

    @Test
    public void testUpdateMatViewInternal() throws Exception {

        ModelMetaData mmd2 = new ModelMetaData();
        mmd2.setName("view1");
        mmd2.setModelType(Type.VIRTUAL);
        mmd2.addSourceMetadata("DDL", "CREATE VIEW v1 ( col integer, col1 string, col2 double, primary key (col) ) OPTIONS (MATERIALIZED true) AS /*+ cache(updatable) */ select 1, current_database(), rand()");

        server.deployVDB("comp", mmd2);

        Connection c = server.getDriver().connect("jdbc:teiid:comp", null);

        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("select * from v1");
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("comp", rs.getString(2));
        double previous = rs.getDouble(3);

        rs = s.executeQuery("select * from (call sysadmin.updateMatView('view1', 'v1', 'col = 0')) p");
        rs.next();
        assertEquals(0, rs.getInt(1));

        rs = s.executeQuery("select * from (call sysadmin.updateMatView('view1', 'v1', 'col = 1')) p");
        rs.next();
        assertEquals(1, rs.getInt(1));

        rs = s.executeQuery("select * from v1");
        rs.next();
        assertNotEquals(previous, rs.getDouble(3));
    }

    @Test
    public void testCompositeupdateMatViewInternal() throws Exception {

        ModelMetaData mmd2 = new ModelMetaData();
        mmd2.setName("view1");
        mmd2.setModelType(Type.VIRTUAL);
        mmd2.addSourceMetadata("DDL", "CREATE VIEW v1 ( col integer, col1 string, col2 double, primary key (col, col1) ) OPTIONS (MATERIALIZED true) AS /*+ cache(updatable) */ select 1, current_database(), rand()");

        server.deployVDB("comp", mmd2);

        Connection c = server.getDriver().connect("jdbc:teiid:comp", null);

        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("select * from v1");
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("comp", rs.getString(2));
        double previous = rs.getDouble(3);

        rs = s.executeQuery("select * from (call sysadmin.updateMatView('view1', 'v1', 'col = 0 AND col1 = ''comp''')) p");
        rs.next();
        assertEquals(0, rs.getInt(1));

        rs = s.executeQuery("select * from (call sysadmin.updateMatView('view1', 'v1', 'col = 1 AND col1 = ''comp''')) p");
        rs.next();
        assertEquals(1, rs.getInt(1));

        rs = s.executeQuery("select * from v1");
        rs.next();
        assertNotEquals(previous, rs.getDouble(3));
    }

    @Test(expected=TeiidSQLException.class)
    public void testupdateMatViewInternalNoPK() throws Exception {

        ModelMetaData mmd2 = new ModelMetaData();
        mmd2.setName("view1");
        mmd2.setModelType(Type.VIRTUAL);
        mmd2.addSourceMetadata("DDL", "CREATE VIEW v1 ( col integer, col1 string, col2 double ) OPTIONS (MATERIALIZED true) AS /*+ cache(updatable) */ select 1, current_database(), rand()");

        server.deployVDB("comp", mmd2);

        Connection c = server.getDriver().connect("jdbc:teiid:comp", null);

        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("select * from v1");
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("comp", rs.getString(2));

        s.execute("call sysadmin.updateMatView('view1', 'v1', 'col = 1')");
    }

    @Test public void testInternalWithManagement() throws Exception {
        ModelMetaData mmd2 = new ModelMetaData();
        mmd2.setName("view1");
        mmd2.setModelType(Type.VIRTUAL);
        mmd2.addSourceMetadata("DDL", "CREATE VIEW v1 ( col integer, col1 string, primary key (col, col1) ) "
                + "OPTIONS (MATERIALIZED true, \"teiid_rel:ALLOW_MATVIEW_MANAGEMENT\" true, \"teiid_rel:MATVIEW_TTL\" 200) AS select 1, current_database(); "
                + "CREATE VIEW v2 ( col integer, col1 string, primary key (col, col1) ) "
                + "OPTIONS (MATERIALIZED true, \"teiid_rel:ALLOW_MATVIEW_MANAGEMENT\" true) AS select 1, current_database()");
        VDBMetaData vdb = new VDBMetaData();
        vdb.setXmlDeployment(true);
        vdb.setName("comp");
        vdb.setModels(Arrays.asList(mmd2));
        vdb.addProperty("lazy-invalidate", "true");
        server.deployVDB(vdb);

        Connection c = server.getDriver().connect("jdbc:teiid:comp", null);
        Statement s = c.createStatement();
        Thread.sleep(5000);

        //ensure that we are preloaded
        ResultSet rs = s.executeQuery("select * from MatViews where name = 'v1'");
        assertTrue(rs.next());
        assertTrue("LOADED".equals(rs.getString("loadstate")));
        assertEquals(true, rs.getBoolean("valid"));
        Timestamp ts = rs.getTimestamp("updated");

        rs = s.executeQuery("select * from MatViews where name = 'v2'");
        assertTrue(rs.next());
        assertEquals("LOADED", rs.getString("loadstate"));
        assertEquals(true, rs.getBoolean("valid"));
        Timestamp v2ts = rs.getTimestamp("updated");

        //and queryable
        rs = s.executeQuery("select * from v1");
        rs.next();
        assertEquals("1", rs.getString(1));

        rs = s.executeQuery("select * from v2");
        rs.next();
        assertEquals("1", rs.getString(1));

        Thread.sleep(1000); //wait for ttl to expire

        rs = s.executeQuery("select * from MatViews where name = 'v1'");
        assertTrue(rs.next());
        assertTrue("LOADED".equals(rs.getString("loadstate")) || "NEEDS_LOADING".equals(rs.getString("loadstate")));
        assertEquals(true, rs.getBoolean("valid"));
        Timestamp ts1 = rs.getTimestamp("updated");
        assertTrue(ts1.compareTo(ts) > 0);

        rs = s.executeQuery("select * from MatViews where name = 'v2'");
        assertTrue(rs.next());
        assertEquals("LOADED", rs.getString("loadstate"));
        assertEquals(true, rs.getBoolean("valid"));
        Timestamp v2ts1 = rs.getTimestamp("updated");
        assertEquals(v2ts, v2ts1);
    }

    @Test
    public void testInternalWriteThroughMativew() throws Exception {
        ModelMetaData mmd2 = new ModelMetaData();
        mmd2.setName("m");
        mmd2.setModelType(Type.PHYSICAL);
        mmd2.addSourceMapping("x", "x", null);
        mmd2.addSourceMetadata("DDL", "CREATE foreign TABLE t (col string, colx string) options (updatable true); "
                + "CREATE VIEW v1 (col1 string, col2 string, primary key (col1)) "
                + "OPTIONS (updatable true, MATERIALIZED true, \"teiid_rel:MATVIEW_WRITE_THROUGH\" true) AS /*+ cache(updatable) */ select col, colx from t;");

        HardCodedExecutionFactory hcef = new HardCodedExecutionFactory() {
            @Override
            public boolean supportsCompareCriteriaEquals() {
                return true;
            }
        };
        hcef.addData("SELECT t.col, t.colx FROM t", Arrays.asList(Arrays.asList("a", "ax")));
        hcef.addData("SELECT t.col, t.colx FROM t WHERE t.col = 'b'", Arrays.asList(Arrays.asList("b", "d")));
        hcef.addUpdate("INSERT INTO t (col, colx) VALUES ('b', 'd')", new int[] {1});
        server.addTranslator("x", hcef);

        server.deployVDB("comp", mmd2);

        Connection c = server.getDriver().connect("jdbc:teiid:comp", null);

        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("select * from v1");
        rs.next();
        assertEquals("a", rs.getString(1));

        s.execute("insert into v1 (col1, col2) values ('b', 'd')");
        assertEquals(1, s.getUpdateCount());

        rs = s.executeQuery("select count(*) from v1");
        rs.next();
        assertEquals(2, rs.getInt(1));

        hcef.addUpdate("DELETE FROM t WHERE t.col = 'b'", new int[] {1});
        hcef.addData("SELECT t.col, t.colx FROM t WHERE t.col = 'b'", new ArrayList<List<?>>());
        s.execute("delete from v1 where v1.col1 = 'b'");
        assertEquals(1, s.getUpdateCount());

        rs = s.executeQuery("select count(*) from v1");
        rs.next();
        assertEquals(1, rs.getInt(1));

        hcef.addUpdate("UPDATE t SET colx = 'bx' WHERE t.colx = 'ax'", new int[] {1});
        hcef.addData("SELECT t.col, t.colx FROM t WHERE t.col = 'a'", Arrays.asList(Arrays.asList("a", "ax")));
        s.execute("update v1 set col2 = 'bx' where col2 = 'ax'");
        assertEquals(1, s.getUpdateCount());

        rs = s.executeQuery("select col2, col1 from v1");
        rs.next();
        assertEquals("ax", rs.getString(1));
    }

    @Test
    public void testInternalPollingQuery() throws Exception {
        ModelMetaData mmd2 = new ModelMetaData();
        mmd2.setName("m");
        mmd2.setModelType(Type.PHYSICAL);
        mmd2.addSourceMapping("x", "x", null);
        mmd2.addSourceMetadata("DDL", "CREATE foreign TABLE t (col string, colx string, coly timestamp) options (updatable true); "
                + "CREATE VIEW v1 (col1 string, col2 string, primary key (col1)) "
                + "OPTIONS (updatable true, MATERIALIZED true, "
                + "\"teiid_rel:ALLOW_MATVIEW_MANAGEMENT\" true, "
                + "\"teiid_rel:MATVIEW_POLLING_INTERVAL\" 1000, "
                + "\"teiid_rel:MATVIEW_POLLING_QUERY\" 'SELECT max(coly) from t') AS /*+ cache(updatable) */ select col, colx from t;");

        HardCodedExecutionFactory hcef = new HardCodedExecutionFactory() {
            @Override
            public boolean supportsCompareCriteriaEquals() {
                return true;
            }
        };
        hcef.addData("SELECT t.col, t.colx FROM t", Arrays.asList(Arrays.asList("a", "ax")));
        server.addTranslator("x", hcef);

        server.deployVDB("comp", mmd2);

        Connection c = server.getDriver().connect("jdbc:teiid:comp", null);

        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("select * from v1");
        rs.next();
        assertEquals("a", rs.getString(1));

        hcef.addData("SELECT t.coly FROM t", Arrays.asList(Arrays.asList(new Timestamp(System.currentTimeMillis()+500))));
        hcef.addData("SELECT t.col, t.colx FROM t", Arrays.asList(Arrays.asList("b", "bx")));

        Thread.sleep(2000);

        rs = s.executeQuery("select * from v1");
        rs.next();
        assertEquals("b", rs.getString(1));

        //no longer greater than the last update
        hcef.addData("SELECT t.col, t.colx FROM t", Arrays.asList(Arrays.asList("c", "cx")));

        Thread.sleep(2000);

        rs = s.executeQuery("select * from v1");
        rs.next();
        assertEquals("b", rs.getString(1));

        hcef.addData("SELECT t.coly FROM t", Arrays.asList(Arrays.asList(new Timestamp(System.currentTimeMillis()+500))));

        Thread.sleep(2000);

        //should be updated again
        rs = s.executeQuery("select * from v1");
        rs.next();
        assertEquals("c", rs.getString(1));
    }

    @Test
    public void testPartitionedLoadInternal() throws Exception {
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("x");
        mmd.addSourceMapping("x", "x", null);
        mmd.addSourceMetadata("DDL", "CREATE foreign TABLE physicalTbl (col integer, col1 string) options (updatable true);"
                + "CREATE VIEW v1 (col integer primary key, col1 string) "
                + "OPTIONS (MATERIALIZED true, "
                + "\"teiid_rel:MATVIEW_UPDATABLE\" true, "
                + "\"teiid_rel:MATVIEW_PART_LOAD_COLUMN\" 'col1') "
                + "AS select col, col1 from physicalTbl");

        HardCodedExecutionFactory hcef = new HardCodedExecutionFactory() {
            @Override
            public boolean supportsCompareCriteriaEquals() {
                return true;
            }

            @Override
            public boolean supportsSelectDistinct() {
                return true;
            }
        };

        server.addTranslator("x", hcef);
        server.deployVDB("comp", mmd);

        Connection admin = server.createConnection("jdbc:teiid:comp");
        hcef.addData("SELECT DISTINCT physicalTbl.col1 FROM physicalTbl", Arrays.asList(Arrays.asList("a"), Arrays.asList("b")));
        hcef.addData("SELECT physicalTbl.col FROM physicalTbl WHERE physicalTbl.col1 = 'a'", Arrays.asList(Arrays.asList(1)));
        //full load
        hcef.addData("SELECT physicalTbl.col, physicalTbl.col1 FROM physicalTbl", Arrays.asList(Arrays.asList(1, "a"), Arrays.asList(2, "b")));
        //single row update based upon pk
        hcef.addData("SELECT physicalTbl.col, physicalTbl.col1 FROM physicalTbl WHERE physicalTbl.col = 1", Arrays.asList(Arrays.asList(1, "a")));
        CallableStatement stmt = admin.prepareCall("{? = call sysadmin.loadMatView('x', 'v1')}");

        stmt.execute();
        //should succeed even if one branch is missing
        assertEquals(1, stmt.getInt(1));

        //will now complete fully
        hcef.addData("SELECT physicalTbl.col FROM physicalTbl WHERE physicalTbl.col1 = 'b'", Arrays.asList(Arrays.asList(2)));
        hcef.addData("SELECT physicalTbl.col, physicalTbl.col1 FROM physicalTbl WHERE physicalTbl.col = 2", Arrays.asList(Arrays.asList(2, "b")));
        stmt.execute();
        assertEquals(2, stmt.getInt(1));
        admin.close();
    }

}
