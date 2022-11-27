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

package org.teiid.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.ForkJoinPool;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.postgresql.Driver;
import org.postgresql.core.v3.ExtendedQueryExecutorImpl;
import org.postgresql.util.PSQLException;
import org.teiid.adminapi.Model.Type;
import org.teiid.adminapi.Request.ProcessingState;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.RequestMetadata;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.deployers.PgCatalogMetadataStore;
import org.teiid.jdbc.FakeServer;
import org.teiid.jdbc.TestMMDatabaseMetaData;
import org.teiid.net.socket.AuthenticationType;
import org.teiid.query.unittest.TimestampUtil;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.TestEmbeddedServer;
import org.teiid.runtime.TestEmbeddedServer.MockTransactionManager;
import org.teiid.transport.SSLConfiguration.ClientAuth;

@SuppressWarnings("nls")
public class TestODBCSocketTransport {

    private static final MockTransactionManager TRANSACTION_MANAGER = new TestEmbeddedServer.MockTransactionManager();

    enum Mode {
        LEGACY,//how the test was originally written
        ENABLED,
        WANT,
        SSL_AUTH,
        LOGIN,
        DISABLED
    }

    static class FakeOdbcServer {
        InetSocketAddress addr;
        ODBCSocketListener odbcTransport;
        FakeServer server;

        public void start(Mode mode) throws Exception {
            AuthenticationType type = AuthenticationType.USERPASSWORD;
            SocketConfiguration config = new SocketConfiguration();
            SSLConfiguration sslConfig = new SSLConfiguration();
            switch (mode) {
            case LOGIN:
                sslConfig.setMode(SSLConfiguration.LOGIN);
                break;
            case ENABLED:
            case LEGACY:
                sslConfig.setMode(SSLConfiguration.ENABLED);
                sslConfig.setAuthenticationMode(SSLConfiguration.ONEWAY);
                sslConfig.setKeystoreFilename(UnitTestUtil.getTestDataFile("keystore.jks").getAbsolutePath());
                sslConfig.setKeystorePassword("password");
                break;
            case SSL_AUTH:
                //we'll just reuse want, but this should probably be ClientAuth.NEED
                type = AuthenticationType.SSL;
            case WANT:
                sslConfig.setMode(SSLConfiguration.ENABLED);
                sslConfig.setAuthenticationMode(ClientAuth.WANT);
                sslConfig.setKeystoreFilename(UnitTestUtil.getTestDataFile("keystore.jks").getAbsolutePath());
                sslConfig.setKeystorePassword("password");
                sslConfig.setTruststoreFilename(UnitTestUtil.getTestDataFile("keystore.jks").getAbsolutePath());
                sslConfig.setTruststorePassword("password");
                break;
            default:
                sslConfig.setMode(SSLConfiguration.DISABLED);
                break;
            }
            config.setSSLConfiguration(sslConfig);
            addr = new InetSocketAddress(0);
            config.setBindAddress(addr.getHostName());
            config.setPortNumber(addr.getPort());
            server = new FakeServer(false);
            EmbeddedConfiguration ec = new EmbeddedConfiguration();
            ec.setAuthenticationType(type);
            ec.setTransactionManager(TRANSACTION_MANAGER);
            server.start(ec, false);
            LogonImpl logon = Mockito.mock(LogonImpl.class);
            Mockito.stub(logon.getSessionService()).toReturn(server.getSessionService());
            odbcTransport = new ODBCSocketListener(addr, config, Mockito.mock(ClientServiceRegistryImpl.class), BufferManagerFactory.getStandaloneBufferManager(), 100000, logon, server.getDriver());
            odbcTransport.setMaxBufferSize(1000); //set to a small size to ensure buffering over the limit works
            if (mode == Mode.LEGACY) {
                odbcTransport.setRequireSecure(false);
            }
            server.deployVDB("parts", UnitTestUtil.getTestDataPath() + "/PartsSupplier.vdb");
        }

        public void stop() {
            server.stop();
            odbcTransport.stop();
        }

    }

    static FakeOdbcServer odbcServer = new FakeOdbcServer();

    @BeforeClass public static void oneTimeSetup() throws Exception {
        TimestampWithTimezone.resetCalendar(TimeZone.getTimeZone("GMT"));
        odbcServer.start(Mode.LEGACY);
    }

    @AfterClass public static void oneTimeTearDown() throws Exception {
        TimestampWithTimezone.resetCalendar(null);
        odbcServer.stop();
    }

    Connection conn;

    @Before public void setUp() throws Exception {
        String database = "parts";
        TRANSACTION_MANAGER.reset();
        connect(database);
    }

    private void connect(String database, Map.Entry<String, String>... props) throws SQLException {
        Driver d = new Driver();
        Properties p = new Properties();
        p.setProperty("user", "testuser");
        p.setProperty("password", "testpassword");
        for (Map.Entry<String, String> prop : props) {
            p.setProperty(prop.getKey(), prop.getValue());
        }
        conn = d.connect("jdbc:postgresql://"+odbcServer.addr.getHostName()+":" +odbcServer.odbcTransport.getPort()+"/"+database, p);
    }

    @After public void tearDown() throws Exception {
        if (conn != null) {
            conn.close();
        }
    }

    /**
     * Under the covers this still executes a prepared statement due to the driver handling
     */
    @Test public void testSelect() throws Exception {
        Statement s = conn.createStatement();
        assertTrue(s.execute("select * from sys.tables order by name"));
        TestMMDatabaseMetaData.compareResultSet(s.getResultSet());
    }

    @Test public void testTransactionalMultibatch() throws Exception {
        Statement s = conn.createStatement();
        conn.setAutoCommit(false);
        assertTrue(s.execute("select sys.tables.name from sys.tables, sys.columns limit 1025"));
        int count = 0;
        while (s.getResultSet().next()) {
            count++;
        }
        assertEquals(1025, count);
        conn.setAutoCommit(true);
    }

    @Test public void testMultibatchSelect() throws Exception {
        Statement s = conn.createStatement();
        assertTrue(s.execute("select * from sys.tables, sys.columns limit 7000"));
        ResultSet rs = s.getResultSet();
        int i = 0;
        while (rs.next()) {
            i++;
            rs.getString(1);
        }
        assertEquals(7000, i);
    }

    /**
     * tests that the portal max is handled correctly
     */
    @Test public void testMultibatchSelectPrepared() throws Exception {
        PreparedStatement s = conn.prepareStatement("select * from (select * from sys.tables order by name desc limit 21) t1, (select * from sys.tables order by name desc limit 21) t2 where t1.name > ?");
        conn.setAutoCommit(false);
        s.setFetchSize(100);
        s.setString(1, "0");
        ResultSet rs = s.executeQuery();
        int i = 0;
        while (rs.next()) {
            i++;
            rs.getString(1);
        }
        assertEquals(441, i);
    }

    @Test public void testBlob() throws Exception {
        Statement s = conn.createStatement();
        assertTrue(s.execute("select to_bytes('abc', 'UTF-16')"));
        ResultSet rs = s.getResultSet();
        assertTrue(rs.next());
        byte[] bytes = rs.getBytes(1);
        assertEquals("abc", new String(bytes, Charset.forName("UTF-16")));
    }

    @Test public void testTextCat() throws Exception {
        Statement s = conn.createStatement();
        assertTrue(s.execute("SELECT textcat('a','b')"));
        ResultSet rs = s.getResultSet();
        assertTrue(rs.next());
        String val = rs.getString(1);
        assertEquals("ab", val);
    }

    @Test public void testClob() throws Exception {
        Statement s = conn.createStatement();
        assertTrue(s.execute("select cast('abc' as clob)"));
        ResultSet rs = s.getResultSet();
        assertTrue(rs.next());
        //getting as a clob is unsupported, since it uses the lo logic
        String clob = rs.getString(1);
        assertEquals("abc", clob);
    }

    @Test public void testLargeClob() throws Exception {
        Statement s = conn.createStatement();
        assertTrue(s.execute("select cast(repeat('_', 3000) as clob)"));
        ResultSet rs = s.getResultSet();
        assertTrue(rs.next());
        //getting as a clob is unsupported, since it uses the lo logic
        String clob = rs.getString(1);
        assertEquals(3000, clob.length());
    }

    @Test public void testMultiRowBuffering() throws Exception {
        Statement s = conn.createStatement();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 11; i++) {
            sb.append("select '' union all ");
        }
        sb.append("select ''");
        assertTrue(s.execute(sb.toString()));
        ResultSet rs = s.getResultSet();
        assertTrue(rs.next());
        String str = rs.getString(1);
        assertEquals(0, str.length());
    }

    @Test public void testTransactionCycle() throws Exception {
        //TODO: drill in to ensure that the underlying statement has been set to autocommit false
        conn.setAutoCommit(false);
        Statement s = conn.createStatement();
        assertTrue(s.execute("select * from sys.tables order by name"));
        conn.setAutoCommit(true);
    }

    @Test public void testRollbackSavepointNoOp() throws Exception {
        conn.setAutoCommit(false);
        Statement s = conn.createStatement();
        assertFalse(s.execute("rollback to foo1"));
        assertFalse(conn.getAutoCommit());
    }

    @Test public void testTxnStatement() throws Exception {
        Statement s = conn.createStatement();
        assertFalse(s.execute("begin work"));
        assertFalse(s.execute("rollback transaction"));
    }

    @Test public void testPk() throws Exception {
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select ta.attname, ia.attnum, ic.relname, n.nspname, tc.relname " +//$NON-NLS-1$
            "from pg_catalog.pg_attribute ta, pg_catalog.pg_attribute ia, pg_catalog.pg_class tc, pg_catalog.pg_index i, " +//$NON-NLS-1$
            "pg_catalog.pg_namespace n, pg_catalog.pg_class ic where tc.relname = E'pg_attribute' AND n.nspname = E'pg_catalog'");
        TestMMDatabaseMetaData.compareResultSet(rs);
    }

    @Test public void testPkPrepared() throws Exception {
        PreparedStatement stmt = conn.prepareStatement("select ta.attname, ia.attnum, ic.relname, n.nspname, tc.relname " +//$NON-NLS-1$
                "from pg_catalog.pg_attribute ta, pg_catalog.pg_attribute ia, pg_catalog.pg_class tc, pg_catalog.pg_index i, " +//$NON-NLS-1$
                "pg_catalog.pg_namespace n, pg_catalog.pg_class ic where tc.relname = E'pg_attribute' AND n.nspname = E'pg_catalog'");
        ResultSet rs = stmt.executeQuery();
        TestMMDatabaseMetaData.compareResultSet(rs);
    }

    @Test public void testColumnMetadataWithAlias() throws Exception {
        PreparedStatement stmt = conn.prepareStatement("select ta.attname as x from pg_catalog.pg_attribute ta limit 1");
        ResultSet rs = stmt.executeQuery();
        TestMMDatabaseMetaData.compareResultSet(rs);
    }

    @Test public void testInsertComplete() throws Exception {
        Statement stmt = conn.createStatement();
        stmt.execute("create temporary table foobar (id integer, optional varchar);");

        PreparedStatement ps = conn.prepareStatement("insert into foobar (id, optional) values (?, ?)");
        ps.setInt(1, 1);
        ps.setString(2, "a");
        ps.execute();
        assertEquals(1, ps.getUpdateCount());
    }

    @Test public void testPreparedNull() throws Exception {
        Statement stmt = conn.createStatement();
        stmt.execute("create temporary table foobar (id integer, optional varchar);");

        PreparedStatement ps = conn.prepareStatement("insert into foobar (id, optional) values (?, ?)");
        ps.setInt(1, 1);
        ps.setString(2, null);
        ps.execute();
        assertEquals(1, ps.getUpdateCount());

        ResultSet rs = stmt.executeQuery("select optional from foobar");
        rs.next();
        assertNull(rs.getString(1));
    }

    @Test public void testPreparedError() throws Exception {
        PreparedStatement stmt = conn.prepareStatement("select cast(? as integer)");
        stmt.setString(1, "a");
        try {
            stmt.executeQuery();
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("Error converting"));
        }
    }

    @Test public void testPreparedError1() throws Exception {
        PreparedStatement stmt = conn.prepareStatement("select");
        try {
            stmt.executeQuery();
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("Parsing error"));
        }
    }

    @Test public void testEscapedLiteral() throws Exception {
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select E'\\n\\thello pg'");
        assertTrue(rs.next());
        assertEquals("\n\thello pg", rs.getString(1));
    }

    @Test public void testPgProc() throws Exception {
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select * from pg_proc");
        rs.next();
        assertEquals("oid", rs.getArray("proargtypes").getBaseTypeName());
        TestMMDatabaseMetaData.compareResultSet(rs); //compare the rest
    }

    @Test public void testCursor() throws Exception {
        Statement stmt = conn.createStatement();
        ExtendedQueryExecutorImpl.simplePortal = "foo";
        try {
            assertFalse(stmt.execute("declare \"foo\" cursor for select * from pg_proc limit 13;"));

            //should get a single row
            stmt.execute("fetch \"foo\"");
            ResultSet rs = stmt.getResultSet();
            int rowCount = 0;
            while (rs.next()) {
                rowCount++;
            }
            assertEquals(1, rowCount);

            //move 5
            assertFalse(stmt.execute("move 5 in \"foo\""));

            //fetch 10, but only 7 are left
            stmt.execute("fetch 10 in \"foo\"");
            rs = stmt.getResultSet();
            rowCount = 0;
            while (rs.next()) {
                rowCount++;
            }
            assertEquals(7, rowCount);

            stmt.execute("close \"foo\"");

            //start a new cursor and check failure
            assertFalse(stmt.execute("declare \"foo\" cursor for select * from pg_proc;"));
            try {
                stmt.execute("fetch 9999999999 in \"foo\"");
                fail();
            } catch (SQLException e) {

            }
        } finally {
            ExtendedQueryExecutorImpl.simplePortal = null;
        }

    }

    @Test public void testCursorUnquoted() throws Exception {
        Statement stmt = conn.createStatement();
        ExtendedQueryExecutorImpl.simplePortal = "foo";
        try {
            assertFalse(stmt.execute("declare foo cursor for select * from pg_proc limit 13;"));

            //should get a single row
            stmt.execute("fetch foo");
            ResultSet rs = stmt.getResultSet();
            int rowCount = 0;
            while (rs.next()) {
                rowCount++;
            }
            assertEquals(1, rowCount);

            //move 5
            assertFalse(stmt.execute("move 5 in foo"));

            //fetch 10, but only 7 are left
            stmt.execute("fetch 10 in \"foo\"");
            rs = stmt.getResultSet();
            rowCount = 0;
            while (rs.next()) {
                rowCount++;
            }
            assertEquals(7, rowCount);

            stmt.execute("close foo");
        } finally {
            ExtendedQueryExecutorImpl.simplePortal = null;
        }

    }

    @Test public void testScrollCursor() throws Exception {
        Statement stmt = conn.createStatement();
        ExtendedQueryExecutorImpl.simplePortal = "foo";
        try {
            assertFalse(stmt.execute("declare \"foo\" insensitive scroll cursor for select * from pg_proc limit 11;"));
            assertFalse(stmt.execute("move 5 in \"foo\""));
            stmt.execute("fetch 7 in \"foo\"");
            ResultSet rs = stmt.getResultSet();
            int rowCount = 0;
            while (rs.next()) {
                rowCount++;
            }
            assertEquals(6, rowCount);

            //move past the end
            //assertFalse(stmt.execute("move forward 0 in \"foo\""));
            //move back
            assertFalse(stmt.execute("move backward 2 in \"foo\""));

            stmt.execute("fetch 6 in \"foo\"");
            rs = stmt.getResultSet();
            rowCount = 0;
            while (rs.next()) {
                rowCount++;
            }
            assertEquals(1, rowCount);

            stmt.execute("close \"foo\"");
        } finally {
            ExtendedQueryExecutorImpl.simplePortal = null;
        }

    }

    @Test public void testScrollCursorWithHold() throws Exception {
        Statement stmt = conn.createStatement();
        ExtendedQueryExecutorImpl.simplePortal = "foo";
        try {
            assertFalse(stmt.execute("declare \"foo\" insensitive scroll cursor with hold for select * from pg_proc;"));
            assertFalse(stmt.execute("move 5 in \"foo\""));
            stmt.execute("fetch 7 in \"foo\"");
            ResultSet rs = stmt.getResultSet();
            int rowCount = 0;
            while (rs.next()) {
                rowCount++;
            }
            assertEquals(7, rowCount);
            stmt.execute("close \"foo\"");
        } finally {
            ExtendedQueryExecutorImpl.simplePortal = null;
        }

    }

    @Test public void testScrollCursorOtherFetches() throws Exception {
        Statement stmt = conn.createStatement();
        ExtendedQueryExecutorImpl.simplePortal = "foo";
        try {
            assertFalse(stmt.execute("declare \"foo\" insensitive scroll cursor for values (1), (2), (3);"));
            stmt.execute("fetch first in \"foo\"");
            ResultSet rs = stmt.getResultSet();
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertFalse(rs.next());

            stmt.execute("fetch last in \"foo\"");
            rs = stmt.getResultSet();
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertFalse(rs.next());

            stmt.execute("fetch absolute 2 in \"foo\"");
            rs = stmt.getResultSet();
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertFalse(rs.next());

            stmt.execute("fetch relative 1 in \"foo\"");
            rs = stmt.getResultSet();
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertFalse(rs.next());

        } finally {
            ExtendedQueryExecutorImpl.simplePortal = null;
        }
    }

    @Test public void testPgProcedure() throws Exception {
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select has_function_privilege(100, 'foo')");
        rs.next();
    }

    @Test public void testPreparedUpdate() throws Exception {
        Statement stmt = conn.createStatement();
        assertFalse(stmt.execute("create local temporary table x (y string)"));
        PreparedStatement ps = conn.prepareStatement("delete from x");
        assertFalse(ps.execute());
        assertNull(ps.getMetaData());
    }

    @Test public void testSelectSsl() throws Exception {
        conn.close();
        Driver d = new Driver();
        Properties p = new Properties();
        p.setProperty("user", "testuser");
        p.setProperty("password", "testpassword");
        p.setProperty("ssl", "true");
        p.setProperty("sslfactory", "org.postgresql.ssl.NonValidatingFactory");
        p.setProperty("sslhostnameverifier", TestODBCSSL.AllowAllHostnameVerifier.class.getName());
        conn = d.connect("jdbc:postgresql://"+odbcServer.addr.getHostName()+":" +odbcServer.odbcTransport.getPort()+"/parts", p);
        Statement s = conn.createStatement();
        assertTrue(s.execute("select * from sys.tables order by name"));
        TestMMDatabaseMetaData.compareResultSet("TestODBCSocketTransport/testSelect", s.getResultSet());
    }

    @Test public void testPayload() throws Exception {
        Statement s = conn.createStatement();
        assertFalse(s.execute("SET PAYLOAD x y"));
        assertTrue(s.execute("SELECT commandpayload('x')"));
        ResultSet rs = s.getResultSet();
        assertTrue(rs.next());
        String str = rs.getString(1);
        assertEquals("y", str);
    }

    @Test public void testShowPlan() throws Exception {
        Statement s = conn.createStatement();
        assertFalse(s.execute("SET SHOWPLAN ON"));
        assertTrue(s.execute("SELECT 1"));
        assertTrue(s.execute("SHOW PLAN"));
        ResultSet rs = s.getResultSet();
        assertTrue(rs.next());
        String str = rs.getString(1);
        assertTrue(str.startsWith("ProjectNode\n  + Relational Node ID:0\n  + Output Columns:expr1 (integer)\n  + Statistics:\n    0: Node Output Rows: 1"));
    }

    @Test public void testSetEmptyLiteral() throws Exception {
        Statement s = conn.createStatement();
        assertFalse(s.execute("SET min_client_messages TO ''"));
        assertTrue(s.execute("SHOW min_client_messages"));
        ResultSet rs = s.getResultSet();
        assertTrue(rs.next());
        assertEquals("", rs.getString(1));
    }

    @Test public void testSetNonString() throws Exception {
        Statement s = conn.createStatement();
        assertFalse(s.execute("SET extra_float_digits TO 2"));
        assertTrue(s.execute("SHOW extra_float_digits"));
        ResultSet rs = s.getResultSet();
        assertTrue(rs.next());
        assertEquals("2", rs.getString(1));
    }

    @Test public void testColons() throws Exception {
        Statement s = conn.createStatement();
        //make sure that we aren't mishandling the ::
        ResultSet rs = s.executeQuery("select 'a::b'");
        assertTrue(rs.next());
        assertEquals("a::b", rs.getString(1));

        rs = s.executeQuery("select ' a::b'");
        assertTrue(rs.next());
        assertEquals(" a::b", rs.getString(1));

        rs = s.executeQuery("select name::varchar from sys.tables where name = 'Columns'");
        assertTrue(rs.next());
        assertEquals("Columns", rs.getString(1));
    }

    @Test public void testInt2Vector() throws Exception {
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("select indkey FROM pg_index order by indexrelid");
        TestMMDatabaseMetaData.compareResultSet(rs);
    }

    @Test public void test_pg_client_encoding() throws Exception {
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("select pg_client_encoding()");
        rs.next();
        assertEquals("UTF8", rs.getString(1));

        s.execute("set client_encoding UTF8");
        rs = s.executeQuery("select pg_client_encoding()");
        rs.next();
        assertEquals("UTF8", rs.getString(1));
    }

    /**
     * TODO: we really want an odbc test, but this confirms the pg_description table and ~ rewrite handling
     * @throws Exception
     */
    @Test public void test_table_with_underscore() throws Exception {
        DatabaseMetaData metadata = conn.getMetaData();
        ResultSet rs = metadata.getTables(null, null, "pg_index", null);
        assertTrue(rs.next());
    }

    @Test public void testIndexInfo() throws Exception {
        DatabaseMetaData metadata = conn.getMetaData();
        ResultSet rs = metadata.getIndexInfo(null, null, "pg_index", false, false);
        assertTrue(rs.next());
    }

    @Test public void testPkMetadata() throws Exception {
        DatabaseMetaData metadata = conn.getMetaData();
        ResultSet rs = metadata.getPrimaryKeys(null, null, "pg_index");
        assertTrue(rs.next());
        assertFalse(rs.next());
    }

    @Test public void test_pg_cast() throws Exception {
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("select '2011-01-01'::date");
        rs.next();
        assertEquals("2011-01-01", rs.getString(1));
    }

    /**
     * Ensures that the client is notified about the change.  However the driver will
     * throw an exception as it requires UTF8
     * @throws Exception
     */
    @Test(expected=SQLException.class) public void test_pg_client_encoding1() throws Exception {
        Statement s = conn.createStatement();
        s.execute("set client_encoding LATIN1");
    }

    @Test public void testArray() throws Exception {
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("select (1,2)");
        rs.next();
        Array result = rs.getArray(1);
        ResultSet rs1 = result.getResultSet();
        rs1.next();
        assertEquals(1, rs1.getInt(1));

        //TODO:we are squashing the result to a text array, since
        //that is a known type - eventually we will need typed array support
        //Object array = result.getArray();
        //assertEquals(1, java.lang.reflect.Array.get(array, 0));
    }


    @Test public void testClientIp() throws Exception {
        Statement s = conn.createStatement();
        assertTrue(s.execute("select * from objecttable('teiid_context' COLUMNS y string 'teiid_row.session.IPAddress') as X"));
        ResultSet rs = s.getResultSet();
        assertTrue(rs.next());
        String value = rs.getString(1);
        assertNotNull(value);
    }

    @Test public void testVDBConnectionProperty() throws Exception {
        VDBMetaData vdb = new VDBMetaData();
        vdb.setName("x");
        vdb.addProperty("connection.foo", "bar");
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("x");
        mmd.setSchemaSourceType("ddl");
        mmd.setModelType(Type.VIRTUAL);
        mmd.setSchemaText("create view v as select 1");
        vdb.addModel(mmd);
        odbcServer.server.deployVDB(vdb);
        this.conn.close();
        connect("x");
        Statement s = conn.createStatement();
        assertTrue(s.execute("show foo"));
        ResultSet rs = s.getResultSet();
        assertTrue(rs.next());
        String value = rs.getString(1);
        assertEquals("bar", value);
    }

    @Test public void testDecimalPrecision() throws Exception {
        VDBMetaData vdb = new VDBMetaData();
        vdb.setName("decimal");
        vdb.addProperty("connection.foo", "bar");
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("x");
        mmd.setModelType(Type.VIRTUAL);
        mmd.addSourceMetadata("ddl", "create view v (x decimal(2), y decimal) as select 1.0, 2.0");
        vdb.addModel(mmd);
        odbcServer.server.deployVDB(vdb);
        this.conn.close();
        connect("decimal");
        Statement s = conn.createStatement();
        s.execute("select * from v");
        ResultSetMetaData metadata = s.getResultSet().getMetaData();
        assertEquals(2, metadata.getPrecision(1));
        assertEquals(0, metadata.getScale(1));

        assertEquals(32767, metadata.getPrecision(2));
        assertEquals(16383, metadata.getScale(2));

        s.execute("select atttypmod from pg_attribute where attname = 'y'");
        s.getResultSet().next();
        assertEquals(2147434499, s.getResultSet().getInt(1));
    }

    @Test public void testTransactionCycleDisabled() throws Exception {
        Statement s = conn.createStatement();
        s.execute("set disableLocalTxn true");
        conn.setAutoCommit(false);
        assertTrue(s.execute("select * from sys.tables order by name"));
        conn.setAutoCommit(true);
    }

    @Test public void testGropuByPositional() throws Exception {
        Statement s = conn.createStatement();
        //would normally throw an exception, but is allowable over odbc
        s.execute("select name, count(schemaname) from sys.tables group by 1");
    }

    @Test public void testImplicitPortalClosing() throws Exception {
        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery("select session_id()");
        rs.next();
        String id = rs.getString(1);
        statement.close();

        PreparedStatement s = conn.prepareStatement("select 1");
        s.executeQuery();
        s.executeQuery();
        s.executeQuery();

        //due to asynch close, there may be several requests
        int runningCount = 0;
        for (RequestMetadata request : odbcServer.server.getDqp().getRequestsForSession(id)) {
           if (request.getState() == ProcessingState.PROCESSING) {
               runningCount++;
           }
        }
        assertEquals(1, runningCount);
        s.close();
    }

    @Test public void testExportedKey() throws Exception {
        String sql = ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("exported-fk-query.txt"));

        Statement s = conn.createStatement();
        s.execute(sql);
        ResultSet rs = s.getResultSet();
        assertTrue(rs.next());
        assertEquals("STATUS_ID", rs.getString(4));
        assertFalse(rs.next());
    }

    @Test public void testRegClass() throws Exception {
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("select '\"pg_catalog.pg_class\"'::regclass");
        assertTrue(rs.next());
        int oid = rs.getInt(1);
        rs = s.executeQuery("select oid from pg_class where relname='pg_class'");
        assertTrue(rs.next());
        int oid1 = rs.getInt(1);
        assertEquals(oid, oid1);
    }

    @Test public void testCharType() throws Exception {
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("select cast('a' as char)");
        rs.next();
        assertEquals("a", rs.getString(1));
        assertEquals(1, rs.getMetaData().getColumnType(1));
        assertEquals(1, rs.getMetaData().getColumnDisplaySize(1));
        assertEquals("bpchar", rs.getMetaData().getColumnTypeName(1));
    }

    @Test public void testApplicationName() throws Exception {
        Statement s = conn.createStatement();
        checkApplicationName(s, "ODBC");
        s.execute("set application_name to other");
        checkApplicationName(s, "other");
    }

    @Test public void testGeometry() throws Exception {
        Statement s = conn.createStatement();
        s.execute("SELECT ST_GeomFromText('POLYGON ((40 0, 50 50, 0 50, 0 0, 40 0))')");
        ResultSet rs = s.getResultSet();
        rs.next();
        ResultSetMetaData rsmd = rs.getMetaData();
        assertEquals("geometry", rsmd.getColumnTypeName(1));
        assertEquals("java.lang.Object", rsmd.getColumnClassName(1));
        assertEquals("00200000030000000000000001000000054044000000000000000000000000000040490000000000004049000000000000000000000000000040490000000000000000000000000000000000000000000040440000000000000000000000000000", rs.getString(1));
    }

    @Test public void testGeography() throws Exception {
        Statement s = conn.createStatement();
        s.execute("SELECT ST_GeogFromText('POLYGON ((40 0, 50 50, 0 50, 0 0, 40 0))')");
        ResultSet rs = s.getResultSet();
        rs.next();
        ResultSetMetaData rsmd = rs.getMetaData();
        assertEquals("geography", rsmd.getColumnTypeName(1));
        assertEquals("java.lang.Object", rsmd.getColumnClassName(1));
        assertEquals("0020000003000010E600000001000000054044000000000000000000000000000040490000000000004049000000000000000000000000000040490000000000000000000000000000000000000000000040440000000000000000000000000000", rs.getString(1));
    }

    @Test public void testJson() throws Exception {
        Statement s = conn.createStatement();
        s.execute("SELECT jsonParse('[1,2]', false);");
        ResultSet rs = s.getResultSet();
        rs.next();
        ResultSetMetaData rsmd = rs.getMetaData();
        assertEquals("json", rsmd.getColumnTypeName(1));
        assertEquals("org.postgresql.util.PGobject", rsmd.getColumnClassName(1));
        assertEquals("[1,2]", rs.getString(1));
    }

    @Test(expected=PSQLException.class) public void testCancel() throws Exception {
        final Statement s = conn.createStatement();
        ForkJoinPool.commonPool().execute(() -> {
            try {
                Thread.sleep(100);
                s.cancel();
            } catch (SQLException | InterruptedException e) {
            }
        });
        s.execute("SELECT t1.* from sys.tables t1, sys.tables t2, sys.tables t3, sys.tables t4");
    }

    @Test public void testConstraintDef() throws Exception {
        Statement s = conn.createStatement();
        s.execute("SELECT pg_get_constraintdef((select oid from pg_constraint where contype = 'f' and conrelid = (select oid from pg_class where relname = 'Functions')), true)");
        ResultSet rs = s.getResultSet();
        rs.next();
        assertEquals("FOREIGN KEY (VDBName,SchemaName) REFERENCES SYS.Schemas(VDBName,Name)", rs.getString(1));
    }

    @Test public void testXML() throws Exception {
        Statement s = conn.createStatement();
        s.execute("SELECT xmlelement(name x)");
        ResultSet rs = s.getResultSet();
        rs.next();
        ResultSetMetaData rsmd = rs.getMetaData();
        assertEquals("xml", rsmd.getColumnTypeName(1));
        assertEquals("<x></x>", rs.getString(1));
    }

    @Test public void testVersion() throws Exception {
        Statement s = conn.createStatement();
        s.execute("SELECT version()");
        ResultSet rs = s.getResultSet();
        rs.next();
        assertEquals(PgCatalogMetadataStore.POSTGRESQL_VERSION, rs.getString(1));
        assertEquals(PgCatalogMetadataStore.POSTGRESQL_VERSION, rs.getString("version"));
    }

    @Test public void testBooleanValues() throws Exception {
        Statement s = conn.createStatement();
        s.execute("SELECT true, false, unknown");
        ResultSet rs = s.getResultSet();
        rs.next();
        assertTrue(rs.getBoolean(1));
        assertFalse(rs.getBoolean(2));
        assertTrue(!rs.getBoolean(3) && rs.wasNull());
    }

    @Test public void testEmptySQL() throws Exception {
        PreparedStatement stmt = conn.prepareStatement("");
        stmt.executeUpdate();

        //make sure the unamed portal can close
        Statement s = conn.createStatement();
        s.executeQuery("select 1");
    }

    private void checkApplicationName(Statement s, String value) throws SQLException {
        ResultSet rs = s.executeQuery("show application_name");
        rs.next();
        assertEquals(value, rs.getString(1));
        rs.close();
        rs = s.executeQuery("select session_id()");
        rs.next();
        String sessionId = rs.getString(1);
        rs.close();
        SessionMetadata current = null;
        for (SessionMetadata session : odbcServer.server.getSessionService().getActiveSessions()) {
            if (session.getSessionId().equals(sessionId)) {
                current = session;
                break;
            }
        }
        assertEquals(value, current.getApplicationName());
    }

    @Test public void testExplain() throws Exception {
        Statement s = conn.createStatement();
        s.execute("explain SELECT pg_get_constraintdef((select oid from pg_constraint where contype = 'f' and conrelid = (select oid from pg_class where relname = 'Functions')), true)");
        ResultSet rs = s.getResultSet();
        rs.next();
        assertTrue(rs.getString(1).contains("Select Columns Subplan"));
        assertFalse(rs.next());

        //extended query, so prepared under the covers
        s.execute("explain SELECT pg_get_constraintdef((select oid from pg_constraint where contype = 'f' and conrelid = (select oid from pg_class where relname = 'Functions')), true)");
        rs = s.getResultSet();
        rs.next();
        assertTrue(rs.getString(1).contains("Select Columns Subplan"));
        assertFalse(rs.next());

        PreparedStatement ps = conn.prepareStatement("explain SELECT pg_get_constraintdef((select oid from pg_constraint where contype = 'f' and conrelid = (select oid from pg_class where relname = 'Functions')), true)");
        ps.execute();
        rs = ps.getResultSet();
        rs.next();
        assertTrue(rs.getString(1).contains("Select Columns Subplan"));
        assertFalse(rs.next());

        ps.execute();
        rs = ps.getResultSet();
        rs.next();
        assertTrue(rs.getString(1).contains("Select Columns Subplan"));
        assertFalse(rs.next());
    }

    @Test public void testBinaryTransfer() throws Exception {
        connect("parts", new AbstractMap.SimpleEntry<String, String>("binaryTransfer", "true"));
        PreparedStatement s = conn.prepareStatement("SELECT ?,?,?,?,?,?,?,?,?,?");
        //date and timestamp are not actually sent in binary in the pg driver
        s.setTimestamp(1, TimestampUtil.createTimestamp(100, 1, 2, 3, 4, 5, 6000));
        s.setTime(2, TimestampUtil.createTime(1, 2, 3));
        s.setDate(3, TimestampUtil.createDate(100, 1, 2));
        s.setString(4, "abc");
        s.setByte(5, (byte)3);
        s.setShort(6, (short)4);
        s.setInt(7, 5);
        s.setLong(8, 10000000000l);
        s.setFloat(9, 5.1f);
        s.setDouble(10, 5.2);
        s.execute();
        TestMMDatabaseMetaData.compareResultSet(s.getResultSet());
    }

}
