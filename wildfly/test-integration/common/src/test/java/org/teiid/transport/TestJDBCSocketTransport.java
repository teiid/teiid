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

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.NotSerializableException;
import java.io.StringReader;
import java.net.InetSocketAddress;
import java.sql.BatchUpdateException;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.Properties;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.common.buffer.impl.BufferManagerImpl;
import org.teiid.core.types.ArrayImpl;
import org.teiid.core.types.BlobImpl;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.GeometryType;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.dqp.service.AutoGenDataService;
import org.teiid.jdbc.ConnectionImpl;
import org.teiid.jdbc.ConnectionProfile;
import org.teiid.jdbc.FakeServer;
import org.teiid.jdbc.TeiidDriver;
import org.teiid.jdbc.TeiidSQLException;
import org.teiid.jdbc.TestMMDatabaseMetaData;
import org.teiid.net.CommunicationException;
import org.teiid.net.ConnectionException;
import org.teiid.net.TeiidURL;
import org.teiid.net.socket.SocketServerConnectionFactory;
import org.teiid.query.function.GeometryUtils;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.HardCodedExecutionFactory;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.ws.BinaryWSProcedureExecution;

@SuppressWarnings("nls")
public class TestJDBCSocketTransport {

    private static final int MAX_MESSAGE = 100000;
    private static final int MAX_LOB = 10000;
    static InetSocketAddress addr;
    static SocketListener jdbcTransport;
    static FakeServer server;
    static int delay;

    @BeforeClass public static void oneTimeSetup() throws Exception {
        SocketConfiguration config = new SocketConfiguration();
        config.setSSLConfiguration(new SSLConfiguration());
        addr = new InetSocketAddress(0);
        config.setBindAddress(addr.getHostName());
        config.setPortNumber(0);

        EmbeddedConfiguration dqpConfig = new EmbeddedConfiguration();
        dqpConfig.setMaxActivePlans(2);
        server = new FakeServer(false);
        server.start(dqpConfig, false);
        server.deployVDB("parts", UnitTestUtil.getTestDataPath() + "/PartsSupplier.vdb");

        jdbcTransport = new SocketListener(addr, config, server.getClientServiceRegistry(), BufferManagerFactory.getStandaloneBufferManager()) {
            @Override
            protected SSLAwareChannelHandler createChannelHandler() {
                SSLAwareChannelHandler result = new SSLAwareChannelHandler(this) {
                    public void messageReceived(io.netty.channel.ChannelHandlerContext ctx,
                            Object msg) throws Exception {
                        if (delay > 0) {
                            Thread.sleep(delay);
                        }
                        super.messageReceived(ctx, msg);
                    }
                };
                return result;
            }
        };
        jdbcTransport.setMaxMessageSize(MAX_MESSAGE);
        jdbcTransport.setMaxLobSize(MAX_LOB);

    }

    @AfterClass public static void oneTimeTearDown() throws Exception {
        if (jdbcTransport != null) {
            jdbcTransport.stop();
        }
        server.stop();
    }

    Connection conn;

    @Before public void setUp() throws Exception {
        toggleInline(true);
        Properties p = new Properties();
        p.setProperty("user", "testuser");
        p.setProperty("password", "testpassword");
        conn = TeiidDriver.getInstance().connect("jdbc:teiid:parts@mm://"+addr.getHostName()+":" +jdbcTransport.getPort(), p);
    }

    private void toggleInline(boolean inline) {
        ((BufferManagerImpl)server.getDqp().getBufferManager()).setInlineLobs(inline);
    }

    @After public void tearDown() throws Exception {
        if (conn != null) {
            conn.close();
        }
    }

    @Test public void testSelect() throws Exception {
        Statement s = conn.createStatement();
        assertTrue(s.execute("select * from sys.tables order by name"));
        TestMMDatabaseMetaData.compareResultSet(s.getResultSet());
    }

    @Test public void testLobStreaming() throws Exception {
        Statement s = conn.createStatement();
        assertTrue(s.execute("select xmlelement(name \"root\") from sys.tables"));
        s.getResultSet().next();
        assertEquals("<root></root>", s.getResultSet().getString(1));
        toggleInline(false);
        assertTrue(s.execute("select xmlelement(name \"root\") from sys.tables"));
        s.getResultSet().next();
        assertEquals("<root></root>", s.getResultSet().getString(1));
    }

    @Test public void testLobStreaming1() throws Exception {
        Statement s = conn.createStatement();
        assertTrue(s.execute("select cast('' as clob) from sys.tables"));
        s.getResultSet().next();
        assertEquals("", s.getResultSet().getString(1));
        toggleInline(false);
        assertTrue(s.execute("select cast('' as clob) from sys.tables"));
        s.getResultSet().next();
        assertEquals("", s.getResultSet().getString(1));
    }

    @Test public void testJsonStreaming() throws Exception {
        Statement s = conn.createStatement();
        assertTrue(s.execute("select jsonParse('5', true) from sys.tables"));
        s.getResultSet().next();
        assertEquals("5", s.getResultSet().getString(1));
        toggleInline(false);
        assertTrue(s.execute("select jsonParse('5', true) from sys.tables"));
        s.getResultSet().next();
        assertEquals("5", s.getResultSet().getString(1));
    }

    @Test public void testVarbinary() throws Exception {
        Statement s = conn.createStatement();
        assertTrue(s.execute("select X'aab1'"));
        s.getResultSet().next();
        byte[] bytes = s.getResultSet().getBytes(1);
        assertArrayEquals(new byte[] {(byte)0xaa, (byte)0xb1}, bytes);
        assertArrayEquals(bytes, s.getResultSet().getBlob(1).getBytes(1, 2));
    }

    @Test public void testVarbinaryPrepared() throws Exception {
        PreparedStatement s = conn.prepareStatement("select cast(? as varbinary)");
        s.setBytes(1, "hello".getBytes());
        assertTrue(s.execute());
        s.getResultSet().next();
        byte[] bytes = s.getResultSet().getBytes(1);
        assertEquals("hello", new String(bytes));
    }

    @Test public void testLargeVarbinaryPrepared() throws Exception {
        PreparedStatement s = conn.prepareStatement("select cast(? as varbinary)");
        s.setBytes(1, new byte[1 << 16]);
        assertTrue(s.execute());
        s.getResultSet().next();
        byte[] bytes = s.getResultSet().getBytes(1);
        assertArrayEquals(new byte[1 << 16], bytes);
    }

    @Test public void testXmlTableScrollable() throws Exception {
        Statement s = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        assertTrue(s.execute("select * from xmltable('/root/row' passing (select xmlelement(name \"root\", xmlagg(xmlelement(name \"row\", xmlforest(t.name)) order by t.name)) from (select t.* from sys.tables as t, sys.columns as t1 limit 7000) as t) columns \"Name\" string) as x"));
        ResultSet rs = s.getResultSet();
        int count = 0;
        while (rs.next()) {
            count++;
        }
        assertEquals(7000, count);
        rs.beforeFirst();
        while (rs.next()) {
            count--;
        }
        assertEquals(0, count);
    }

    @Test public void testGeneratedKeys() throws Exception {
        Statement s = conn.createStatement();
        s.execute("set showplan debug");
        s.execute("create local temporary table x (y serial, z integer, primary key (y))");
        assertFalse(s.execute("insert into x (z) values (1)", Statement.RETURN_GENERATED_KEYS));
        ResultSet rs = s.getGeneratedKeys();
        rs.next();
        assertEquals(1, rs.getInt(1));
    }

    /**
     * Ensures if you start more than the maxActivePlans
     * where all the plans take up more than output buffer limit
     * that processing still proceeds
     * @throws Exception
     */
    @Test public void testSimultaneousLargeSelects() throws Exception {
        for (int j = 0; j < 3; j++) {
            Statement s = conn.createStatement();
            assertTrue(s.execute("select * from sys.columns c1, sys.columns c2"));
        }
    }

    /**
     * Tests to ensure that a SynchronousTtl/synchTimeout does
     * not cause a cancel.
     * TODO: had to increase the values since the test would fail on slow machine runs
     * @throws Exception
     */
    @Test public void testSyncTimeout() throws Exception {
        TeiidDriver td = new TeiidDriver();
        td.setSocketProfile(new ConnectionProfile() {

            @Override
            public ConnectionImpl connect(String url, Properties info)
                    throws TeiidSQLException {
                SocketServerConnectionFactory sscf = new SocketServerConnectionFactory();
                sscf.initialize(info);
                try {
                    return new ConnectionImpl(sscf.getConnection(info), info, url);
                } catch (CommunicationException e) {
                    throw TeiidSQLException.create(e);
                } catch (ConnectionException e) {
                    throw TeiidSQLException.create(e);
                }
            }
        });
        Properties p = new Properties();
        p.setProperty("user", "testuser");
        p.setProperty("password", "testpassword");
        ConnectorManagerRepository cmr = server.getConnectorManagerRepository();
        AutoGenDataService agds = new AutoGenDataService() {
            @Override
            public Object getConnectionFactory()
                    throws TranslatorException {
                return null;
            }
        };
        agds.setSleep(2000); //wait longer than the synch ttl/soTimeout, we should still succeed
        cmr.addConnectorManager("source", agds);
        try {
            conn = td.connect("jdbc:teiid:parts@mm://"+addr.getHostName()+":" +jdbcTransport.getPort(), p);
            Statement s = conn.createStatement();
            assertTrue(s.execute("select * from parts"));
        } finally {
            server.setConnectorManagerRepository(cmr);
        }
    }

    @Test public void testProtocolException() throws Exception {
        Statement s = conn.createStatement();
        s.execute("set showplan debug");
        try {
            s.execute("select * from objecttable('teiid_context' columns teiid_row object 'teiid_row') as x");
            fail();
        } catch (SQLException e) {
            assertTrue(e.getCause() instanceof NotSerializableException);
        }
        //make sure the connection is still alive
        s.execute("select 1");
        ResultSet rs = s.getResultSet();
        rs.next();
        assertEquals(1, rs.getInt(1));
    }

    @Test public void testStreamingLob() throws Exception {
        HardCodedExecutionFactory ef = new HardCodedExecutionFactory();
        ef.addData("SELECT helloworld.x FROM helloworld", Arrays.asList(Arrays.asList(new BlobType(new BinaryWSProcedureExecution.StreamingBlob(new ByteArrayInputStream(new byte[100]))))));
        server.addTranslator("custom", ef);
        server.deployVDB(new ByteArrayInputStream("<vdb name=\"test\" version=\"1\"><model name=\"test\"><source name=\"test\" translator-name=\"custom\"/><metadata type=\"DDL\"><![CDATA[CREATE foreign table helloworld (x blob);]]> </metadata></model></vdb>".getBytes("UTF-8")));
        conn = TeiidDriver.getInstance().connect("jdbc:teiid:test@mm://"+addr.getHostName()+":" +jdbcTransport.getPort(), null);

        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("select to_chars(x, 'UTF-8') from helloworld");
        rs.next();
        //TODO: if we use getString streaming will still fail because the string logic gets the length first
        assertEquals(100, ObjectConverterUtil.convertToString(rs.getCharacterStream(1), -1).length());
    }

    @Test public void testDynamicStatementNoResultset() throws Exception {
        Statement s = conn.createStatement();
        s.execute("BEGIN\n" +
                "       declare clob sql_query = 'select 1';\n" +
                "       execute immediate sql_query;\n" +
                "END ;");
        assertNull(s.getResultSet());
    }

    @Test public void testArray() throws Exception {
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("SELECT (1, (1,2))");
        rs.next();
        assertEquals(new ArrayImpl(new Object[] {1, new Object[] {1, 2}}), rs.getArray(1));
        assertEquals("java.sql.Array", rs.getMetaData().getColumnClassName(1));
        assertEquals(Types.ARRAY, rs.getMetaData().getColumnType(1));
        assertEquals("object[]", rs.getMetaData().getColumnTypeName(1));
    }

    @Test public void testLargeMessage() throws Exception {
        Statement s = conn.createStatement();
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT '");
        for (int i = 0; i < MAX_MESSAGE; i++) {
            sb.append('a');
        }
        sb.append('\'');
        try {
            s.executeQuery(sb.toString());
            fail();
        } catch (SQLException e) {

        }
        ResultSet rs = s.executeQuery("select 1");
        rs.next();
        assertEquals(1, rs.getInt(1));
    }

    @Test(expected=TeiidSQLException.class) public void testLoginTimeout() throws SQLException {
        Properties p = new Properties();
        p.setProperty(TeiidURL.CONNECTION.LOGIN_TIMEOUT, "1");
        delay = 1500;
        try {
            conn = TeiidDriver.getInstance().connect("jdbc:teiid:parts@mm://"+addr.getHostName()+":" +(jdbcTransport.getPort()), p);
        } finally {
            delay = 0;
        }
    }

    @Test(expected=TeiidSQLException.class) public void testLargeLob() throws Exception {
        PreparedStatement s = conn.prepareStatement("select to_bytes(?, 'ascii')");
        s.setCharacterStream(1, new StringReader(new String(new char[200000])));
        s.execute();
    }

    @Test public void testLobCase() throws Exception {
        Statement s = conn.createStatement();
        s.execute("select ucase(cast('abc' as clob))");
        s.getResultSet().next();
        assertEquals("ABC", s.getResultSet().getString(1));
    }

    @Test public void testGeometryStreaming() throws Exception {
        StringBuilder geomString = new StringBuilder();
        for (int i = 0; i < 600; i++) {
            geomString.append("100 100,");
        }
        geomString.append("100 100");
        final GeometryType geo = GeometryUtils.geometryFromClob(new ClobType(new ClobImpl("POLYGON ((" + geomString + "))")));
        long length = geo.length();
        PreparedStatement s = conn.prepareStatement("select st_geomfrombinary(?)");
        s.setBlob(1, new BlobImpl(new InputStreamFactory() {

            @Override
            public InputStream getInputStream() throws IOException {
                try {
                    return geo.getBinaryStream();
                } catch (SQLException e) {
                    throw new IOException(e);
                }
            }
        }));
        ResultSet rs = s.executeQuery();
        rs.next();
        Blob b = rs.getBlob(1);
        assertEquals(length, b.length());
        b.getBytes(1, (int) b.length());

        toggleInline(false);
        rs = s.executeQuery();
        rs.next();
        b = rs.getBlob(1);
        assertEquals(length, b.length());
        b.getBytes(1, (int) b.length());
    }

    @Test public void testBatchedUpdateException() throws Exception {
        Statement s = conn.createStatement();
        s.execute("create local temporary table x (y integer, primary key (y))");
        s.addBatch("insert into x values (1)");
        s.addBatch("insert into x values (1)");
        try {
            s.executeBatch();
            fail();
        } catch (BatchUpdateException e) {
            assertEquals(1, e.getUpdateCounts()[0]);
        }

        PreparedStatement ps = conn.prepareStatement("insert into x values (?)");
        ps.setInt(1, 2);
        ps.addBatch();
        ps.setInt(1, 2);
        ps.addBatch();
        try {
            ps.executeBatch();
            fail();
        } catch (BatchUpdateException e) {
            assertEquals(1, e.getUpdateCounts()[0]);
        }

        //make sure no update counts are reported when there's an issue on the first item
        ps = conn.prepareStatement("insert into x values (?)");
        ps.setInt(1, 2);
        ps.addBatch();
        ps.setInt(1, 2);
        ps.addBatch();
        try {
            ps.executeBatch();
            fail();
        } catch (BatchUpdateException e) {
            assertEquals(0, e.getUpdateCounts().length);
        }
    }

    @Test(expected=TeiidSQLException.class) public void testSessionKilling() throws Exception {
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("select session_id()");
        rs.next();
        String session = rs.getString(1);
        server.getAdmin().terminateSession(session);
        s.execute("select 1");
    }

    /**
     * Not strictly a jdbc test, but simple to test here
     * @throws Exception
     */
    @Test public void testTempTableCleanup() throws Exception {
        for (int i = 0; i < 50; i++) {
            Statement s = conn.createStatement();
            s.execute("insert into #temp select t.* from sys.tables t cross join sys.tables t1");
            Properties p = new Properties();
            p.setProperty("user", "testuser");
            p.setProperty("password", "testpassword");
            conn.close();
            conn = TeiidDriver.getInstance().connect("jdbc:teiid:parts@mm://"+addr.getHostName()+":" +jdbcTransport.getPort(), p);
            //there are several managed internal materialization, make sure that we don't grow beyond that
            assertTrue(((BufferManagerImpl)server.getDqp().getBufferManager()).getCache().getCacheGroupCount() < 5);
        }
    }

}
