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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.FileInputStream;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.adminapi.Model.Type;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.client.util.ResultsFuture;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.events.EventDistributor;
import org.teiid.jdbc.FakeServer;
import org.teiid.jdbc.util.ResultSetUtil;
import org.teiid.language.Command;
import org.teiid.logging.LogManager;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.runtime.HardCodedExecutionFactory;
import org.teiid.runtime.ReplicatedServer;
import org.teiid.translator.Execution;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.h2.H2ExecutionFactory;

@SuppressWarnings("nls")
public class TestExternalMatViews {
    private static final class DelayableHardCodedExectionFactory extends
            HardCodedExecutionFactory {
        private final boolean supportsEQ;
        private int delay;

        private DelayableHardCodedExectionFactory(boolean supportsEQ) {
            this.supportsEQ = supportsEQ;
        }

        @Override
        public boolean supportsCompareCriteriaEquals() {
            return supportsEQ;
        }

        @Override
        public boolean supportsAggregatesMax() {
            return true;
        }

        @Override
        public boolean supportsSelectDistinct() {
            return true;
        }

        @Override
        public Execution createExecution(Command command,
                ExecutionContext executionContext,
                RuntimeMetadata metadata, Object connection)
                throws TranslatorException {
            if (delay > 0) {
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                }
            }
            return super.createExecution(command, executionContext, metadata, connection);
        }
    }

    private static boolean DEBUG = false;

    private Connection conn;
    private FakeServer server;
    private static DataSource h2DataSource;

    private static final String statusTable = "CREATE TABLE status\n" +
            "(\n" +
            "  VDBName varchar(50) not null,\n" +
            "  VDBVersion varchar(50) not null,\n" +
            "  SchemaName varchar(50) not null,\n" +
            "  Name varchar(256) not null,\n" +
            "  TargetSchemaName varchar(50),\n" +
            "  TargetName varchar(256) not null,\n" +
            "  Valid boolean not null,\n" +
            "  LoadState varchar(25) not null,\n" +
            "  Cardinality long,\n" +
            "  Updated timestamp not null,\n" +
            "  LoadNumber long not null,\n" +
            "  NodeName varchar(25) not null,\n" +
            "  StaleCount long default 0,\n" +
            "  PRIMARY KEY (VDBName, VDBVersion, SchemaName, Name)\n" +
            ")";

    @BeforeClass
    public static void beforeClass() throws Exception {
        h2DataSource = getDatasource();

        Connection c = h2DataSource.getConnection();
        assertNotNull(c);
        c.createStatement().execute(statusTable);

        String matView = "CREATE table mat_v1 (col int primary key, col1 varchar(50))";
        String matView2 = "CREATE table mat_v2 (col int primary key, col1 varchar(50), loadnum long)";
        String matView3 = "CREATE table mat_v3 (col int primary key, col1 varchar(50), loadnum long)";
        String matView1a = "CREATE table mat_v1a (col int primary key, col1 varchar(50), loadnum long)";
        String matViewStage = "CREATE table mat_v1_stage (col int primary key, col1 varchar(50))";
        c.createStatement().execute(matView);
        c.createStatement().execute(matViewStage);
        c.createStatement().execute(matView2);
        c.createStatement().execute(matView3);
        c.createStatement().execute("CREATE table G1 (e1 int primary key, e2 varchar(50), LoadNumber long)");
        c.createStatement().execute(matView1a);
        c.close();

        if (DEBUG) {
            UnitTestUtil.enableTraceLogging("org.teiid");
        }
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        Connection c = h2DataSource.getConnection();
        c.createStatement().execute("DROP ALL OBJECTS");
        c.close();
    }

    @Before
    public void setUp() throws Exception {
        server = new FakeServer(true);

        if (DEBUG)
        LogManager.setLogListener(new org.teiid.logging.Logger() {
            @Override
            public void shutdown() {
            }
            @Override
            public void removeMdc(String key) {
            }
            @Override
            public void putMdc(String key, String val) {
            }

            @Override
            public void log(int level, String context, Throwable t, Object... msg) {
                StringBuilder sb = new StringBuilder();
                for (Object str:msg) {
                    sb.append(str.toString());
                }
                System.out.println(sb.toString());
            }

            @Override
            public void log(int level, String context, Object... msg) {
                StringBuilder sb = new StringBuilder();
                for (Object str:msg) {
                    sb.append(str.toString());
                }
                System.out.println(sb.toString());
            }

            @Override
            public boolean isEnabled(String context, int msgLevel) {
                return msgLevel <= 4;
            }
        });
        Connection c = h2DataSource.getConnection();
        c.createStatement().execute("delete from status");
        c.createStatement().execute("delete from mat_v1");
        c.createStatement().execute("delete from mat_v1_stage");
        c.createStatement().execute("delete from mat_v2");
        c.close();
    }

    @After
    public void tearDown() throws Exception {
        if (conn != null) {
            conn.close();
        }
        if (server != null) {
            server.stop();
        }
    }

    @Test
    public void testSwapScriptWithEagerUpdate() throws Exception {
        withSwapScripts(true);
    }

    @Test
    public void testSwapScriptWithFullRefresh() throws Exception {
        withSwapScripts(false);
    }

    private void withSwapScripts(boolean useUpdateScript) throws Exception {
        HardCodedExecutionFactory hcef = setupData(server);
        ModelMetaData sourceModel = setupSourceModel();
        ModelMetaData matViewModel = setupMatViewModel();

        ModelMetaData viewModel = new ModelMetaData();
        viewModel.setName("view1");
        viewModel.setModelType(Type.VIRTUAL);
        viewModel.addSourceMetadata("DDL", "CREATE VIEW v1 (col integer primary key, col1 string) "
                + "OPTIONS (MATERIALIZED true, "
                + "MATERIALIZED_TABLE 'matview.MAT_V1', "
                + "\"teiid_rel:MATVIEW_TTL\" 3000, "
                + "\"teiid_rel:ALLOW_MATVIEW_MANAGEMENT\" true, "
                + "\"teiid_rel:MATVIEW_STATUS_TABLE\" 'matview.STATUS', "
                + "\"teiid_rel:MATERIALIZED_STAGE_TABLE\" 'matview.MAT_V1_STAGE', "
                + "\"teiid_rel:MATVIEW_BEFORE_LOAD_SCRIPT\" 'execute matview.native(''truncate table MAT_V1_STAGE'')', "
                + "\"teiid_rel:MATVIEW_AFTER_LOAD_SCRIPT\"  "
                            + "'begin "
                                + "execute matview.native(''ALTER TABLE MAT_V1 RENAME TO MAT_V1_TEMP'');"
                                + "execute matview.native(''ALTER TABLE MAT_V1_STAGE RENAME TO MAT_V1'');"
                                + "execute matview.native(''ALTER TABLE MAT_V1_TEMP RENAME TO MAT_V1_STAGE''); "
                            + "end', "
                + "\"teiid_rel:MATVIEW_ONERROR_ACTION\" 'THROW_EXCEPTION') "
                + "AS select col, col1 from source.physicalTbl");
        server.deployVDB("comp", sourceModel, viewModel, matViewModel);

        Thread.sleep(1000);

        assertTest(useUpdateScript, hcef);
    }

    @Test
    public void testViewChaining() throws Exception {
        DelayableHardCodedExectionFactory hcef = setupData(server);
        ModelMetaData sourceModel = setupSourceModel();
        ModelMetaData matViewModel = setupMatViewModel();

        ModelMetaData viewModel = new ModelMetaData();
        viewModel.setName("view1");
        viewModel.setModelType(Type.VIRTUAL);
        viewModel.addSourceMetadata("DDL", "CREATE VIEW v1 (col integer primary key, col1 string) "
                + "OPTIONS (MATERIALIZED true, "
                + "MATERIALIZED_TABLE 'matview.MAT_V1a', "
                + "\"teiid_rel:MATVIEW_TTL\" 5000, "
                + "\"teiid_rel:ALLOW_MATVIEW_MANAGEMENT\" true, "
                + "\"teiid_rel:MATVIEW_STATUS_TABLE\" 'matview.STATUS', "
                + "\"teiid_rel:MATVIEW_LOADNUMBER_COLUMN\" 'loadnum', "
                + "\"teiid_rel:MATVIEW_ONERROR_ACTION\" 'THROW_EXCEPTION') "
                + "AS select col, col1 from source.physicalTbl;"

                + "CREATE VIEW v2 (col integer primary key, col1 string) "
                + "OPTIONS (MATERIALIZED true, "
                + "MATERIALIZED_TABLE 'matview.MAT_V2', "
                + "\"teiid_rel:MATVIEW_TTL\" 5000, "
                + "\"teiid_rel:ALLOW_MATVIEW_MANAGEMENT\" true, "
                + "\"teiid_rel:MATVIEW_STATUS_TABLE\" 'matview.STATUS', "
                + "\"teiid_rel:MATVIEW_LOADNUMBER_COLUMN\" 'loadnum',"
                + "\"teiid_rel:MATVIEW_ONERROR_ACTION\" 'THROW_EXCEPTION') "
                + "AS select col, col1 from v1");

        server.deployVDB("chain", sourceModel, viewModel, matViewModel);
        hcef.delay = 100;
        Connection c = server.getDriver().connect("jdbc:teiid:chain", null);
        Statement s = c.createStatement();

        //should succeed before the ttl
        for (int i = 0; i < 5; i++) {
            try {
                s.execute("select count(*) from v2");
                if (i == 0) {
                    System.out.println("expected first iteration to fail");
                }
                ResultSet rs = s.getResultSet();
                rs.next();
                assertTrue(rs.getInt(1) > 0);
                return;
            } catch (SQLException e) {
            }
            Thread.sleep(400);
        }
    }

    @Test
    public void testViewChainingWait() throws Exception {
        DelayableHardCodedExectionFactory hcef = setupData(server);
        ModelMetaData sourceModel = setupSourceModel();
        ModelMetaData matViewModel = setupMatViewModel();

        ModelMetaData viewModel = new ModelMetaData();
        viewModel.setName("view1");
        viewModel.setModelType(Type.VIRTUAL);
        viewModel.addSourceMetadata("DDL", "CREATE VIEW v1 (col integer primary key, col1 string) "
                + "OPTIONS (MATERIALIZED true, "
                + "MATERIALIZED_TABLE 'matview.MAT_V1a', "
                + "\"teiid_rel:MATVIEW_TTL\" 5000, "
                + "\"teiid_rel:ALLOW_MATVIEW_MANAGEMENT\" true, "
                + "\"teiid_rel:MATVIEW_STATUS_TABLE\" 'matview.STATUS', "
                + "\"teiid_rel:MATVIEW_LOADNUMBER_COLUMN\" 'loadnum', "
                + "\"teiid_rel:MATVIEW_ONERROR_ACTION\" 'WAIT') "
                + "AS select col, col1 from source.physicalTbl;"

                + "CREATE VIEW v2 (col integer primary key, col1 string) "
                + "OPTIONS (MATERIALIZED true, "
                + "MATERIALIZED_TABLE 'matview.MAT_V2', "
                + "\"teiid_rel:MATVIEW_TTL\" 5000, "
                + "\"teiid_rel:ALLOW_MATVIEW_MANAGEMENT\" true, "
                + "\"teiid_rel:MATVIEW_STATUS_TABLE\" 'matview.STATUS', "
                + "\"teiid_rel:MATVIEW_LOADNUMBER_COLUMN\" 'loadnum',"
                + "\"teiid_rel:MATVIEW_ONERROR_ACTION\" 'WAIT') "
                + "AS select col, col1 from v1;"

                + "CREATE VIEW v3 (col integer primary key, col1 string) "
                + "OPTIONS (MATERIALIZED true, "
                + "MATERIALIZED_TABLE 'matview.MAT_V3', "
                + "\"teiid_rel:MATVIEW_TTL\" 5000, "
                + "\"teiid_rel:ALLOW_MATVIEW_MANAGEMENT\" true, "
                + "\"teiid_rel:MATVIEW_STATUS_TABLE\" 'matview.STATUS', "
                + "\"teiid_rel:MATVIEW_LOADNUMBER_COLUMN\" 'loadnum',"
                + "\"teiid_rel:MATVIEW_ONERROR_ACTION\" 'WAIT') "
                + "AS select col, col1 from v2");

        server.deployVDB("chain", sourceModel, viewModel, matViewModel);
        hcef.delay = 100;
        Connection c = server.getDriver().connect("jdbc:teiid:chain", null);
        Statement s = c.createStatement();

        //should succeed before the ttl
        for (int i = 0; i < 5; i++) {
            try {
                s.execute("select * from v3");
                ResultSet rs = s.getResultSet();
                rs.next();
                assertTrue(rs.getInt(1) > 0);
                return;
            } catch (SQLException e) {
            }
            Thread.sleep(400);
        }
    }

    @Test
    public void testMergeDeleteWithFullRefresh() throws Exception {
        withMergeDelete(false);
    }

    @Test
    public void testMergeDeleteWithEagarUpdates() throws Exception {
        withMergeDelete(true);
    }

    private void withMergeDelete(boolean useUpdateScript) throws Exception {
        HardCodedExecutionFactory hcef = setupData(server);
        ModelMetaData sourceModel = setupSourceModel();
        ModelMetaData matViewModel = setupMatViewModel();

        ModelMetaData viewModel = new ModelMetaData();
        viewModel.setName("view1");
        viewModel.setModelType(Type.VIRTUAL);
        viewModel.addSourceMetadata("DDL", "CREATE VIEW v1 (col integer primary key, col1 string) "
                + "OPTIONS (MATERIALIZED true, "
                + "MATERIALIZED_TABLE 'matview.MAT_V2', "
                + "\"teiid_rel:MATVIEW_TTL\" 3000, "
                + "\"teiid_rel:ALLOW_MATVIEW_MANAGEMENT\" true, "
                + "\"teiid_rel:MATVIEW_STATUS_TABLE\" 'matview.STATUS', "
                + "\"teiid_rel:MATVIEW_LOADNUMBER_COLUMN\" 'loadnum') "
                + "AS select col, col1 from source.physicalTbl");
        server.deployVDB("comp", sourceModel, viewModel, matViewModel);

        Thread.sleep(1000);

        assertTest(useUpdateScript, hcef);
    }

    @Test
    public void testVDBImportScopeWarning() throws Exception {
        H2ExecutionFactory executionFactory = new H2ExecutionFactory();
        executionFactory.setSupportsDirectQueryProcedure(true);
        executionFactory.start();
        server.addTranslator("h2", executionFactory);
        server.addConnectionFactory("java:/matview-ds", h2DataSource);
        server.deployVDB(new FileInputStream(UnitTestUtil.getTestDataFile("child-vdb.xml")));
        server.deployVDB(new FileInputStream(UnitTestUtil.getTestDataFile("parent-vdb.xml")));

        Thread.sleep(1000);

        String uidQuery = "SELECT UID FROM Sys.Tables WHERE VDBName = 'parent' AND SchemaName = 'VM1' AND Name = 'G1'";

        conn = server.createConnection("jdbc:teiid:parent");
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery(uidQuery);
        rs.next();
        String uid = rs.getString(1);

        String scopeQuery = "SELECT \"value\" from SYS.Properties WHERE UID = '"+uid
                +"' AND Name = '{http://www.teiid.org/ext/relational/2012}MATVIEW_SHARE_SCOPE'";
        s = conn.createStatement();
        rs = s.executeQuery(scopeQuery);
        rs.next();
        assertEquals("IMPORTED", rs.getString(1)); // check if the default switching working

        s = conn.createStatement();
        rs = s.executeQuery("select * from VM1.G1");
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("2", rs.getString(2));

        Connection c = h2DataSource.getConnection();
        rs = c.createStatement().executeQuery("SELECT VDBVersion FROM Status WHERE VDBName = 'child'");
        rs.next();
        assertEquals(1, rs.getInt(1)); // 1 means IMPORTED

        rs = s.executeQuery("select * from VM2.G1");
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("2", rs.getString(2));

        rs = c.createStatement().executeQuery("SELECT VDBVersion FROm Status WHERE VDBName = 'parent'");
        rs.next();
        assertEquals(0, rs.getInt(1)); // 0 means FULL

        conn.close();
        c.close();
    }

    @Test
    public void testInternalFullRefresh() throws Exception {
        internalWithSameExternalProcedures(false);
    }

    @Test
    public void testInternalWithEargerUpdates() throws Exception {
        internalWithSameExternalProcedures(true);
    }

    private void internalWithSameExternalProcedures(boolean useUpdateScript) throws Exception {
        HardCodedExecutionFactory hcef = setupData(server);
        ModelMetaData sourceModel = setupSourceModel();
        ModelMetaData matViewModel = setupMatViewModel();

        ModelMetaData viewModel = new ModelMetaData();
        viewModel.setName("view1");
        viewModel.setModelType(Type.VIRTUAL);
        viewModel.addSourceMetadata("DDL", "CREATE VIEW v1 (col integer primary key, col1 string) "
                + "OPTIONS (MATERIALIZED true, "
                + "\"teiid_rel:MATVIEW_TTL\" 3000, "
                + "\"teiid_rel:MATVIEW_UPDATABLE\" true, "
                + "\"teiid_rel:MATVIEW_STATUS_TABLE\" 'matview.STATUS', "
                + "\"teiid_rel:ALLOW_MATVIEW_MANAGEMENT\" true) "
                + "AS select col, col1 from source.physicalTbl");
        server.deployVDB("comp", sourceModel, viewModel, matViewModel);

        Thread.sleep(1000);

        assertTest(useUpdateScript, hcef);
    }

    @Test
    public void testRestartServerInMiddleOfLoading() throws Exception {
        HardCodedExecutionFactory hcef = setupData(server);
        ModelMetaData sourceModel = setupSourceModel();
        ModelMetaData matViewModel = setupMatViewModel();

        ModelMetaData viewModel = new ModelMetaData();
        viewModel.setName("view1");
        viewModel.setModelType(Type.VIRTUAL);
        viewModel.addSourceMetadata("DDL", "CREATE VIEW v1 (col integer primary key, col1 string) "
                + "OPTIONS (MATERIALIZED true, "
                + "MATERIALIZED_TABLE 'matview.MAT_V2', "
                + "\"teiid_rel:MATVIEW_TTL\" 3000, "
                + "\"teiid_rel:ALLOW_MATVIEW_MANAGEMENT\" true, "
                + "\"teiid_rel:MATVIEW_STATUS_TABLE\" 'matview.STATUS', "
                + "\"teiid_rel:MATVIEW_LOADNUMBER_COLUMN\" 'loadnum') "
                + "AS select col, col1 from source.physicalTbl");
        server.deployVDB("comp", sourceModel, viewModel, matViewModel);

        Thread.sleep(1000);

        // test that the matview loaded
        conn = server.createConnection("jdbc:teiid:comp");
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("select * from view1.v1 order by col");
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("town", rs.getString(2));

        // now change the status table underneath
        h2DataSource = getDatasource();
        Connection c = h2DataSource.getConnection();
        assertNotNull(c);

        Statement stmt = c.createStatement();
        boolean update = stmt.execute("UPDATE status SET LOADSTATE = 'LOADING' WHERE NAME = 'v1'");
        assertFalse(update); // this is update
        assertEquals(1, stmt.getUpdateCount());

        rs = c.createStatement().executeQuery("SELECT LOADSTATE, NODENAME FROM status");
        assertTrue(rs.next());
        assertEquals("LOADING", rs.getString(1));
        assertEquals("localhost", rs.getString(2));
        server.stop();

        server = new FakeServer(true);
        setupData(server);
        setupSourceModel();
        setupMatViewModel();

        server.deployVDB("comp", sourceModel, viewModel, matViewModel);

        Thread.sleep(1000);

        rs = c.createStatement().executeQuery("SELECT LOADSTATE, NODENAME FROM STATUS WHERE NAME = 'v1'");
        assertTrue(rs.next());
        assertEquals("LOADED", rs.getString(1));
        assertEquals("localhost", rs.getString(2));
    }

    private void assertTest(boolean useUpdateScript, HardCodedExecutionFactory hcef)
            throws Exception, SQLException, InterruptedException {

        conn = server.createConnection("jdbc:teiid:comp");

        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("select * from view1.v1 order by col");
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("town", rs.getString(2));

        rs.next();
        assertEquals(2, rs.getInt(1));
        assertEquals("state", rs.getString(2));

        rs.next();
        assertEquals(3, rs.getInt(1));
        assertEquals("country", rs.getString(2));

        rs.close();
        s.close();

        // data changes. explicit update to matview using updateMatview, this should reset TTL
        hcef.addData("SELECT physicalTbl.col, physicalTbl.col1 FROM physicalTbl",
            Arrays.asList(
                    Arrays.asList(1, "city"), // update
                    Arrays.asList(2, "state"),
                    // delete
                    Arrays.asList(4, "USA"))); // insert

        hcef.addData("SELECT physicalTbl.col FROM physicalTbl",
                Arrays.asList(
                        Arrays.asList(1),
                        Arrays.asList(2),
                        Arrays.asList(4)));

        if (useUpdateScript) {
            Connection admin = server.createConnection("jdbc:teiid:comp");
            CallableStatement stmt = admin.prepareCall("{? = call SYSADMIN.updateMatView(schemaName=>'view1', viewName=>'v1', refreshCriteria=>'v1.col in(1,3,4)')}");
            stmt.registerOutParameter(1, Types.INTEGER);
            stmt.execute();
            assertTrue(stmt.getInt(1) <= 4);
            admin.close();
        } else {
            Thread.sleep(3000);
        }

        Statement s1;
        ResultSet rs1;
        s1 = conn.createStatement();
        rs1 = s1.executeQuery("select * from view1.v1 order by col");
        rs1.next();
        assertEquals(1, rs1.getInt(1));
        assertEquals("city", rs1.getString(2));

        rs1.next();
        assertEquals(2, rs1.getInt(1));
        assertEquals("state", rs1.getString(2));

        rs1.next();
        assertEquals(4, rs1.getInt(1));
        assertEquals("USA", rs1.getString(2));

        rs1.close();
        s1.close();
    }

    private DelayableHardCodedExectionFactory setupData(EmbeddedServer server) throws TranslatorException {
        return setupData(false, server);
    }

    private DelayableHardCodedExectionFactory setupData(final boolean supportsEQ, EmbeddedServer server) throws TranslatorException {
        H2ExecutionFactory executionFactory = new H2ExecutionFactory();
        executionFactory.setSupportsDirectQueryProcedure(true);
        executionFactory.start();
        server.addTranslator("translator-h2", executionFactory);
        server.addConnectionFactory("java:/matview-ds", h2DataSource);

        DelayableHardCodedExectionFactory hcef = new DelayableHardCodedExectionFactory(supportsEQ);
        hcef.addData("SELECT physicalTbl.col, physicalTbl.col1 FROM physicalTbl",
                Arrays.asList(
                        Arrays.asList(1, "town"),
                        Arrays.asList(2, "state"),
                        Arrays.asList(3, "country")));
        hcef.addData("SELECT physicalTbl.col FROM physicalTbl",
                Arrays.asList(
                        Arrays.asList(1),
                        Arrays.asList(2),
                        Arrays.asList(3)));

        server.addTranslator("fixed", hcef);
        return hcef;
    }

    private ModelMetaData setupMatViewModel() {
        ModelMetaData matViewModel = new ModelMetaData();
        matViewModel.setName("matview");
        matViewModel.setModelType(Type.PHYSICAL);
        matViewModel.addSourceMapping("s2", "translator-h2", "java:/matview-ds");
        matViewModel.addProperty("importer.schemaPattern", "PUBLIC");
        matViewModel.addProperty("importer.tableTypes", "TABLE");
        matViewModel.addProperty("importer.useFullSchemaName", "false");
        return matViewModel;
    }

    private ModelMetaData setupSourceModel() {
        ModelMetaData sourceModel = new ModelMetaData();
        sourceModel.setName("source");
        sourceModel.setModelType(Type.PHYSICAL);
        sourceModel.addSourceMetadata("DDL", "create foreign table physicalTbl (col integer, col1 string, col2 timestamp) options (updatable true);");
        sourceModel.addSourceMapping("s1", "fixed", null);
        return sourceModel;
    }

    private static DataSource getDatasource() {
        return JdbcConnectionPool.create("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1", "sa", "sa");
    }

    @Test
    public void test() throws Exception {
        HardCodedExecutionFactory hcef = setupData(server);
        ModelMetaData sourceModel = setupSourceModel();
        ModelMetaData matViewModel = setupMatViewModel();

        ModelMetaData viewModel = new ModelMetaData();
        viewModel.setName("view1");
        viewModel.setModelType(Type.VIRTUAL);
        viewModel.addSourceMetadata("DDL", "CREATE VIEW v1 (col integer primary key, col1 string) "
                + "OPTIONS (MATERIALIZED true, "
                + "MATERIALIZED_TABLE 'matview.MAT_V2', "
                + "\"teiid_rel:MATVIEW_TTL\" 3000, "
                + "\"teiid_rel:ALLOW_MATVIEW_MANAGEMENT\" true, "
                + "\"teiid_rel:MATVIEW_STATUS_TABLE\" 'matview.STATUS', "
                + "\"teiid_rel:MATVIEW_LOADNUMBER_COLUMN\" 'loadnum') "
                + "AS select col, col1 from source.physicalTbl");
        server.deployVDB("comp", sourceModel, viewModel, matViewModel);

        Connection admin = server.createConnection("jdbc:teiid:comp");
        execute(admin, "select * from SYSADMIN.Usage where Name = 'mat_v1'");
        admin.close();
    }

    @Test
    public void testQueryTruncation() throws Exception {
        HardCodedExecutionFactory hcef = setupData(server);
        ModelMetaData sourceModel = setupSourceModel();

        ModelMetaData matViewModel = setupMatViewModel();

        StringBuilder sb = new StringBuilder("CREATE table MAT_V5 (col integer primary key");
        int cols = 200;
        for (int i = 0; i < cols; i++) {
            sb.append(",some_long_col_name_").append(i).append(" integer");
        }
        sb.append(", loadnum bigint)");

        Connection conn = h2DataSource.getConnection();
        conn.createStatement().execute(sb.toString());
        conn.close();

        ModelMetaData viewModel = new ModelMetaData();
        viewModel.setName("view1");
        viewModel.setModelType(Type.VIRTUAL);
        sb = new StringBuilder("CREATE VIEW v1 (col integer primary key");
        for (int i = 0; i < cols; i++) {
            sb.append(",some_long_col_name_").append(i).append(" integer");
        }
        sb.append(") OPTIONS (MATERIALIZED true, "
                + "MATERIALIZED_TABLE 'matview.MAT_V5', "
                + "\"teiid_rel:MATVIEW_STATUS_TABLE\" 'matview.STATUS', "
                + "\"teiid_rel:MATVIEW_ONERROR_ACTION\" 'THROW_EXCEPTION', "
                + "\"teiid_rel:MATVIEW_LOADNUMBER_COLUMN\" 'loadnum') "
                + "AS select col");
        for (int i = 0; i < cols; i++) {
            sb.append(",").append(i);
        }
        sb.append(" from source.physicalTbl");
        viewModel.addSourceMetadata("DDL", sb.toString());
        server.deployVDB("comp", sourceModel, viewModel, matViewModel);

        Connection admin = server.createConnection("jdbc:teiid:comp");

        Statement stmt = admin.createStatement();

        stmt.execute("call sysadmin.loadMatView('view1', 'v1')");
        stmt.execute("call sysadmin.updateMatView('view1', 'v1', 'true')");

        stmt.executeQuery("select * from v1");

        admin.close();
    }

    @Test
    public void testInternalWriteThroughMativew() throws Exception {
        HardCodedExecutionFactory hcef = setupData(true, server);
        ModelMetaData sourceModel = setupSourceModel();
        ModelMetaData matViewModel = setupMatViewModel();

        ModelMetaData viewModel = new ModelMetaData();
        viewModel.setName("view1");
        viewModel.setModelType(Type.VIRTUAL);
        viewModel.addSourceMetadata("DDL", "CREATE VIEW v1 (col integer primary key, col1 string) "
                + "OPTIONS (MATERIALIZED true,"
                + "UPDATABLE true, "
                + "MATERIALIZED_TABLE 'matview.MAT_V2', "
                + "\"teiid_rel:MATVIEW_TTL\" 3000, "
                + "\"teiid_rel:MATVIEW_WRITE_THROUGH\" true, "
                + "\"teiid_rel:ALLOW_MATVIEW_MANAGEMENT\" true, "
                + "\"teiid_rel:MATVIEW_STATUS_TABLE\" 'matview.STATUS', "
                + "\"teiid_rel:MATVIEW_LOADNUMBER_COLUMN\" 'loadnum') "
                + "AS select col, col1 from source.physicalTbl");
        server.deployVDB("comp", sourceModel, viewModel, matViewModel);

        Connection c = server.createConnection("jdbc:teiid:comp");

        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("select count(*) from v1");
        rs.next();
        assertEquals(3, rs.getInt(1));

        hcef.addUpdate("INSERT INTO physicalTbl (col, col1) VALUES (4, 'continent')", new int[] {1});
        s.execute("insert into v1 (col, col1) values (4, 'continent')");
        assertEquals(1, s.getUpdateCount());

        rs = s.executeQuery("select count(*) from v1");
        rs.next();
        assertEquals(4, rs.getInt(1));

        //check that not specifying the pk doesn't update the mat view
        //TODO: this may be addressed in some cases eventually
        hcef.addUpdate("INSERT INTO physicalTbl (col1) VALUES ('continent')", new int[] {1});
        s.execute("insert into v1 (col1) values ('continent')");
        rs = s.executeQuery("select count(*) from v1");
        rs.next();
        assertEquals(4, rs.getInt(1));

        hcef.addUpdate("DELETE FROM physicalTbl WHERE physicalTbl.col1 = 'continent'", new int[] {1});
        s.execute("delete from v1 where v1.col1 = 'continent'");
        assertEquals(1, s.getUpdateCount());

        rs = s.executeQuery("select count(*) from v1");
        rs.next();
        assertEquals(3, rs.getInt(1));

        hcef.addUpdate("UPDATE physicalTbl SET col1 = 'town' WHERE physicalTbl.col1 = 'city'", new int[] {1});
        s.execute("update v1 set col1 = 'town' where col1 = 'city'");
        assertEquals(1, s.getUpdateCount());

        rs = s.executeQuery("select col, col1 from v1 where col = 1");
        rs.next();
        assertEquals("town", rs.getString(2));
    }

    public static boolean execute(Connection connection, String sql) throws Exception {
        boolean hasRs = true;
        try {
            Statement statement = connection.createStatement();
            hasRs = statement.execute(sql);
            if (!hasRs) {
                int cnt = statement.getUpdateCount();
                if (DEBUG) {
                    System.out.println("----------------\r");
                    System.out.println("Updated #rows: " + cnt);
                    System.out.println("----------------\r");
                }
            } else {
                ResultSet results = statement.getResultSet();
                if (DEBUG) {
                    ResultSetUtil.printResultSet(results);
                }
                results.close();
            }
            statement.close();
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
        return hasRs;
    }

    private EmbeddedServer createServer(String nodeName, String ispn, String jgroups) throws Exception {
        return ReplicatedServer.createServer(nodeName, ispn, jgroups);
    }

    @Test
    public void testNodeFailure() throws Exception {
        EmbeddedServer server1 = createServer("server1", "infinispan-replicated-config.xml", "tcp-shared.xml");

        HardCodedExecutionFactory hcef = setupData(server1);
        ModelMetaData sourceModel = setupSourceModel();
        ModelMetaData matViewModel = setupMatViewModel();

        ModelMetaData viewModel = new ModelMetaData();
        viewModel.setName("view1");
        viewModel.setModelType(Type.VIRTUAL);
        viewModel.addSourceMetadata("DDL", "CREATE VIEW v1 (col integer primary key, col1 string) "
                + "OPTIONS (MATERIALIZED true, "
                + "MATERIALIZED_TABLE 'matview.MAT_V2', "
                + "\"teiid_rel:MATVIEW_TTL\" 30000, "
                + "\"teiid_rel:ALLOW_MATVIEW_MANAGEMENT\" true, "
                + "\"teiid_rel:MATVIEW_SHARE_SCOPE\" 'FULL', "
                + "\"teiid_rel:MATVIEW_STATUS_TABLE\" 'matview.STATUS', "
                + "\"teiid_rel:MATVIEW_LOADNUMBER_COLUMN\" 'loadnum') "
                + "AS select col, col1 from source.physicalTbl");
        server1.deployVDB("comp", sourceModel, viewModel, matViewModel);

        Thread.sleep(1000);

        EmbeddedServer server2 = createServer("server2", "infinispan-replicated-config-1.xml", "tcp-shared.xml");
        setupData(server2);
        server2.deployVDB("comp", sourceModel, viewModel, matViewModel);
        Thread.sleep(5000);

        Connection c = h2DataSource.getConnection();
        ResultSet rs = c.createStatement().executeQuery("SELECT LoadState, Nodename FROM Status WHERE VDBName = 'comp'");
        rs.next();
        assertEquals("LOADED", rs.getString(1));
        assertEquals("server1", rs.getString(2));

        int update = c.createStatement().executeUpdate("UPDATE Status SET LoadState = 'LOADING' WHERE VDBName = 'comp' AND Nodename = 'server1'");
        assertEquals(1, update);

        server1.stop();

        Thread.sleep(1000);

        rs = c.createStatement().executeQuery("SELECT LoadState, Nodename FROM Status WHERE VDBName = 'comp'");
        rs.next();
        assertEquals("LOADED", rs.getString(1));
        assertEquals("server2", rs.getString(2));

        server2.stop();
    }

    @Test
    public void testLazyUpdate() throws Exception {
        HardCodedExecutionFactory hcef = setupData(server);
        ModelMetaData sourceModel = setupSourceModel();
        ModelMetaData matViewModel = setupMatViewModel();

        ModelMetaData viewModel = new ModelMetaData();
        viewModel.setName("view1");
        viewModel.setModelType(Type.VIRTUAL);
        viewModel.addSourceMetadata("DDL", "CREATE VIEW v1 (col integer primary key, col1 string) "
                + "OPTIONS (MATERIALIZED true, "
                + "MATERIALIZED_TABLE 'matview.MAT_V2', "
                + "\"teiid_rel:ALLOW_MATVIEW_MANAGEMENT\" true, "
                + "\"teiid_rel:MATVIEW_STATUS_TABLE\" 'matview.STATUS', "
                + "\"teiid_rel:MATVIEW_MAX_STALENESS_PCT\" '10', "
                + "\"teiid_rel:MATVIEW_POLLING_INTERVAL\" '1000', "
                + "\"teiid_rel:MATVIEW_LOADNUMBER_COLUMN\" 'loadnum') "
                + "AS select col, col1 from source.physicalTbl");
        server.deployVDB("comp", sourceModel, viewModel, matViewModel);

        Thread.sleep(1000);

        // check if materialization is loaded
        Connection c = h2DataSource.getConnection();
        ResultSet rs = c.createStatement().executeQuery("SELECT LoadState, StaleCount FROM Status WHERE VDBName = 'comp'");
        rs.next();
        assertEquals("LOADED", rs.getString(1));
        assertEquals(0, rs.getInt(2));

        // verify with querying the database.
        conn = server.createConnection("jdbc:teiid:comp");
        Statement s = conn.createStatement();
        rs = s.executeQuery("select * from view1.v1 order by col");
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("town", rs.getString(2));

        // send a row update
        EventDistributor ed = server.getEventDistributor();
        ResultsFuture<?> f = ed.dataModification("comp", "1", "source", "physicalTbl", new Object[] { 1, "town" },
                new Object[] { 1, "town-modified" }, new String[] { "col1", "col2" });
        f.get();

        int waitTime = 5000;

        long start = System.currentTimeMillis();
        while (true) {
            // check the stale count incremented
            rs = c.createStatement().executeQuery("SELECT LoadState, StaleCount, LoadNumber FROM Status WHERE VDBName = 'comp'");
            rs.next();
            if (rs.getString(1).equalsIgnoreCase("NEEDS_LOADING")) {
                assertEquals(1, rs.getInt(2));
                assertEquals(1, rs.getLong(3));
                break;
            }
            if (rs.getLong(3) == 1) {
                if (System.currentTimeMillis() - start  > waitTime) {
                    fail("never updated the load state");
                }
                Thread.sleep(100);
                continue;
            }
            if (rs.getLong(3) == 2) {
                break;
            }
        }

        //wait for the polling a load to happen
        start = System.currentTimeMillis();
        while (true) {
            // check the stale count to zero and that we are reloaded
            rs = c.createStatement().executeQuery("SELECT LoadState, StaleCount, LoadNumber FROM Status WHERE VDBName = 'comp'");
            rs.next();
            if (!rs.getString(1).equalsIgnoreCase("LOADED")) {
                if (System.currentTimeMillis() - start  > waitTime) {
                    fail("never updated the load state");
                }
                Thread.sleep(100);
                continue;
            }
            assertEquals(0, rs.getInt(2));
            assertEquals(2, rs.getLong(3));
            break;
        }

        String ddl = server.getAdmin().getSchema("comp", "1", "source", null, null);
        assertFalse(ddl.contains("AFTER INSERT"));

        f = ed.dataModification("comp", "1", "source", "physicalTbl", new Object[] { 1, "town" },
                new Object[] { 1, "town-modified" }, new String[] { "col1", "col2" });
        f.get();

        start = System.currentTimeMillis();
        while (true) {
            // check the stale count incremented
            rs = c.createStatement().executeQuery("SELECT LoadState, StaleCount, LoadNumber FROM Status WHERE VDBName = 'comp'");
            rs.next();
            if (rs.getString(1).equalsIgnoreCase("NEEDS_LOADING")) {
                assertEquals(1, rs.getInt(2));
                assertEquals(2, rs.getLong(3));
                break;
            }
            if (rs.getLong(3) == 2) {
                if (System.currentTimeMillis() - start  > waitTime) {
                    fail("never updated the load state");
                }
                Thread.sleep(100);
                continue;
            }
            if (rs.getLong(3) == 3) {
                break;
            }
        }

        //wait for the polling a load to happen
        start = System.currentTimeMillis();
        while (true) {
            // check the stale count to zero and that we are reloaded
            rs = c.createStatement().executeQuery("SELECT LoadState, StaleCount, LoadNumber FROM Status WHERE VDBName = 'comp'");
            rs.next();
            if (!rs.getString(1).equalsIgnoreCase("LOADED")) {
                if (System.currentTimeMillis() - start  > waitTime) {
                    fail("never updated the load state");
                }
                Thread.sleep(100);
                continue;
            }
            assertEquals(0, rs.getInt(2));
            assertEquals(3, rs.getLong(3));
            break;
        }


        Thread.sleep(2000);

        //make sure that nothing has changed
        rs = c.createStatement().executeQuery("SELECT LoadState, StaleCount, LoadNumber FROM Status WHERE VDBName = 'comp'");
        rs.next();
        String loadState = rs.getString(1);
        int staleCount = rs.getInt(2);
        long loadNumber = rs.getLong(3);
        if (rs.next()) {
            rs = c.createStatement().executeQuery("select * from status");
            ResultSetUtil.printResultSet(rs);
            fail("multiple status entries");
        }
        assertEquals("LOADED", loadState);
        assertEquals(0, staleCount);
        assertEquals(3, loadNumber);
    }


    @Test
    public void testPollingQuery() throws Exception {
        HardCodedExecutionFactory hcef = setupData(server);
        ModelMetaData sourceModel = setupSourceModel();
        ModelMetaData matViewModel = setupMatViewModel();

        ModelMetaData viewModel = new ModelMetaData();
        viewModel.setName("view1");
        viewModel.setModelType(Type.VIRTUAL);
        viewModel.addSourceMetadata("DDL", "CREATE VIEW v1 (col integer primary key, col1 string) "
                + "OPTIONS (MATERIALIZED true, "
                + "MATERIALIZED_TABLE 'matview.MAT_V2', "
                + "\"teiid_rel:ALLOW_MATVIEW_MANAGEMENT\" true, "
                + "\"teiid_rel:MATVIEW_STATUS_TABLE\" 'matview.STATUS', "
                + "\"teiid_rel:MATVIEW_POLLING_QUERY\" 'select max(col2) from source.physicalTbl', "
                + "\"teiid_rel:MATVIEW_POLLING_INTERVAL\" '1000', "
                + "\"teiid_rel:MATVIEW_LOADNUMBER_COLUMN\" 'loadnum') "
                + "AS select col, col1 from source.physicalTbl");
        server.deployVDB("comp", sourceModel, viewModel, matViewModel);

        hcef.addData("SELECT MAX(physicalTbl.col2) FROM physicalTbl", Collections.emptyList());

        Thread.sleep(1000);

        // check if materialization is loaded
        Connection c = h2DataSource.getConnection();
        ResultSet rs = c.createStatement().executeQuery("SELECT LoadState, StaleCount FROM Status WHERE VDBName = 'comp'");
        rs.next();
        assertEquals("LOADED", rs.getString(1));
        assertEquals(0, rs.getInt(2));

        // verify with querying the database.
        conn = server.createConnection("jdbc:teiid:comp");
        Statement s = conn.createStatement();
        rs = s.executeQuery("select * from view1.v1 order by col");
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("town", rs.getString(2));

        hcef.addData("SELECT MAX(physicalTbl.col2) FROM physicalTbl", Arrays.asList(Arrays.asList(new Timestamp(System.currentTimeMillis()))));
        hcef.addData("SELECT physicalTbl.col, physicalTbl.col1 FROM physicalTbl",
                Arrays.asList(
                        Arrays.asList(1, "city"),
                        Arrays.asList(2, "state"),
                        Arrays.asList(3, "neighborhoot"),
                        Arrays.asList(4, "USA")));

        //wait for the polling/load to happen
        Thread.sleep(2000);

        // check the stale count to zero and that we are reloaded
        rs = c.createStatement().executeQuery("SELECT LoadState, StaleCount FROM Status WHERE VDBName = 'comp'");
        rs.next();
        assertEquals("LOADED", rs.getString(1));
        assertEquals(0, rs.getInt(2));

        rs = s.executeQuery("select count(*) from view1.v1");
        rs.next();
        assertEquals(4, rs.getInt(1));
    }

    @Test
    public void testPartitionedLoad() throws Exception {
        HardCodedExecutionFactory hcef = setupData(true, server);
        ModelMetaData sourceModel = setupSourceModel();
        ModelMetaData matViewModel = setupMatViewModel();

        ModelMetaData viewModel = new ModelMetaData();
        viewModel.setName("view1");
        viewModel.setModelType(Type.VIRTUAL);
        viewModel.addSourceMetadata("DDL", "CREATE VIEW v1 (col integer primary key, col1 string) "
                + "OPTIONS (MATERIALIZED true, "
                + "MATERIALIZED_TABLE 'matview.MAT_V2', "
                + "\"teiid_rel:MATVIEW_PART_LOAD_COLUMN\" 'col1', "
                + "\"teiid_rel:MATVIEW_STATUS_TABLE\" 'matview.STATUS', "
                + "\"teiid_rel:MATVIEW_LOADNUMBER_COLUMN\" 'loadnum') "
                + "AS select col, col1 from source.physicalTbl");
        server.deployVDB("comp", sourceModel, viewModel, matViewModel);

        Connection admin = server.createConnection("jdbc:teiid:comp");
        hcef.addData("SELECT DISTINCT physicalTbl.col1 FROM physicalTbl", Arrays.asList(Arrays.asList("a"), Arrays.asList("b")));
        CallableStatement stmt = admin.prepareCall("{? = call sysadmin.loadMatView('view1', 'v1')}");
        hcef.addData("SELECT physicalTbl.col, physicalTbl.col1 FROM physicalTbl WHERE physicalTbl.col1 = 'a'", Arrays.asList(Arrays.asList(1, "a")));
        //hcef.addData("SELECT physicalTbl.col, physicalTbl.col1 FROM physicalTbl WHERE physicalTbl.col1 = 'b'", Arrays.asList(Arrays.asList(2, "b")));
        stmt.execute();
        //should succeed even if one branch is missing
        assertEquals(1, stmt.getInt(1));

        //will now complete fully
        hcef.addData("SELECT physicalTbl.col, physicalTbl.col1 FROM physicalTbl WHERE physicalTbl.col1 = 'b'", Arrays.asList(Arrays.asList(2, "b")));
        stmt.execute();
        assertEquals(2, stmt.getInt(1));
        admin.close();
    }
}
