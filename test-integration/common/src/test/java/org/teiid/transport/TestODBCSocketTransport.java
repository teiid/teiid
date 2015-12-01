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

package org.teiid.transport;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.security.NoSuchAlgorithmException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.postgresql.Driver;
import org.postgresql.core.v3.ExtendedQueryExectutorImpl;
import org.teiid.adminapi.Model.Type;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.FakeServer;
import org.teiid.jdbc.TestMMDatabaseMetaData;
import org.teiid.net.socket.SocketUtil;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.TestEmbeddedServer;
import org.teiid.runtime.TestEmbeddedServer.MockTransactionManager;

@SuppressWarnings("nls")
public class TestODBCSocketTransport {
	
public static class AnonSSLSocketFactory extends SSLSocketFactory {
		
		private SSLSocketFactory sslSocketFactory;
		
		public AnonSSLSocketFactory() {
			try {
				sslSocketFactory = SSLContext.getDefault().getSocketFactory();
			} catch (NoSuchAlgorithmException e) {
				throw new RuntimeException();
			}			
		}

		@Override
		public Socket createSocket() throws IOException {
			return sslSocketFactory.createSocket();
		}

		@Override
		public Socket createSocket(InetAddress address, int port,
				InetAddress localAddress, int localPort) throws IOException {
			return sslSocketFactory.createSocket(address, port, localAddress,
					localPort);
		}

		@Override
		public Socket createSocket(InetAddress host, int port)
				throws IOException {
			return sslSocketFactory.createSocket(host, port);
		}

		@Override
		public Socket createSocket(Socket s, String host, int port,
				boolean autoClose) throws IOException {
			SSLSocket socket = (SSLSocket)sslSocketFactory.createSocket(s, host, port, autoClose);
			SocketUtil.addCipherSuite(socket, SocketUtil.ANON_CIPHER_SUITE);
			return socket;
		}

		@Override
		public Socket createSocket(String host, int port,
				InetAddress localHost, int localPort) throws IOException,
				UnknownHostException {
			return sslSocketFactory.createSocket(host, port, localHost,
					localPort);
		}

		@Override
		public Socket createSocket(String host, int port) throws IOException,
				UnknownHostException {
			return sslSocketFactory.createSocket(host, port);
		}

		@Override
		public String[] getDefaultCipherSuites() {
			return sslSocketFactory.getDefaultCipherSuites();
		}

		@Override
		public String[] getSupportedCipherSuites() {
			return sslSocketFactory.getSupportedCipherSuites();
		}
		
	}

	private static final MockTransactionManager TRANSACTION_MANAGER = new TestEmbeddedServer.MockTransactionManager();

	enum Mode {
		LEGACY,//how the test was originally written
		ENABLED,
		LOGIN,
		DISABLED
	}
	
	static class FakeOdbcServer {
		InetSocketAddress addr;
		ODBCSocketListener odbcTransport;
		FakeServer server;
		
		public void start(Mode mode) throws Exception {
			SocketConfiguration config = new SocketConfiguration();
			SSLConfiguration sslConfig = new SSLConfiguration();
			if (mode == Mode.LOGIN) {
				sslConfig.setMode(SSLConfiguration.LOGIN);
			} else if (mode == Mode.ENABLED || mode == Mode.LEGACY) {
				sslConfig.setMode(SSLConfiguration.ENABLED);
				sslConfig.setAuthenticationMode(SSLConfiguration.ANONYMOUS);
			} else {
				sslConfig.setMode(SSLConfiguration.DISABLED);
			}
			config.setSSLConfiguration(sslConfig);
			addr = new InetSocketAddress(0);
			config.setBindAddress(addr.getHostName());
			config.setPortNumber(addr.getPort());
			server = new FakeServer(false);
			EmbeddedConfiguration ec = new EmbeddedConfiguration();
			ec.setTransactionManager(TRANSACTION_MANAGER);
			server.start(ec, false);
			LogonImpl logon = Mockito.mock(LogonImpl.class);
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
	
	private static FakeOdbcServer odbcServer = new FakeOdbcServer();
	
	@BeforeClass public static void oneTimeSetup() throws Exception {
		odbcServer.start(Mode.LEGACY);
	}
	
	@AfterClass public static void oneTimeTearDown() throws Exception {
		odbcServer.stop();
	}
	
	Connection conn;
	
	@Before public void setUp() throws Exception {
		String database = "parts";
		TRANSACTION_MANAGER.reset();
		connect(database);
	}

	private void connect(String database) throws SQLException {
		Driver d = new Driver();
		Properties p = new Properties();
		p.setProperty("user", "testuser");
		p.setProperty("password", "testpassword");
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
		assertTrue(s.execute("select * from tables order by name"));
		TestMMDatabaseMetaData.compareResultSet(s.getResultSet());
	}
	
	@Test public void testTransactionalMultibatch() throws Exception {
		Statement s = conn.createStatement();
		conn.setAutoCommit(false);
		assertTrue(s.execute("select tables.name from tables, columns limit 1025"));
		int count = 0;
		while (s.getResultSet().next()) {
			count++;
		}
		assertEquals(1025, count);
		conn.setAutoCommit(true);
	}
	
	@Test public void testMultibatchSelect() throws Exception {
		Statement s = conn.createStatement();
		assertTrue(s.execute("select * from tables, columns limit 7000"));
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
		PreparedStatement s = conn.prepareStatement("select * from (select * from tables order by name desc limit 21) t1, (select * from tables order by name desc limit 21) t2 where t1.name > ?");
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
		assertTrue(s.execute("select * from tables order by name"));
		conn.setAutoCommit(true);
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
	
	@Test public void testPreparedError() throws Exception {
		PreparedStatement stmt = conn.prepareStatement("select cast(? as integer)");
		stmt.setString(1, "a");
		try {
			stmt.executeQuery();
		} catch (SQLException e) {
			assertTrue(e.getMessage().contains("Error converting"));
		}
	}
	
	@Test public void testPreparedError1() throws Exception {
		PreparedStatement stmt = conn.prepareStatement("select");
		try {
			stmt.executeQuery();
		} catch (SQLException e) {
			assertTrue(e.getMessage().contains("Parsing error"));
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
		ExtendedQueryExectutorImpl.simplePortal = "foo";
		try {
			assertFalse(stmt.execute("declare \"foo\" cursor for select * from pg_proc;"));
			assertFalse(stmt.execute("move 5 in \"foo\""));
			stmt.execute("fetch 10 in \"foo\"");
			ResultSet rs = stmt.getResultSet();
			int rowCount = 0;
			while (rs.next()) {
				rowCount++;
			}
			assertEquals(8, rowCount);
			stmt.execute("close \"foo\"");
		} finally {
			ExtendedQueryExectutorImpl.simplePortal = null;
		}
		
	}	
	
	@Test public void testScrollCursor() throws Exception {
		Statement stmt = conn.createStatement();
		ExtendedQueryExectutorImpl.simplePortal = "foo";
		try {
			assertFalse(stmt.execute("declare \"foo\" insensitive scroll cursor for select * from pg_proc;"));
			assertFalse(stmt.execute("move 5 in \"foo\""));
			stmt.execute("fetch 6 in \"foo\"");
			ResultSet rs = stmt.getResultSet();
			int rowCount = 0;
			while (rs.next()) {
				rowCount++;
			}
			assertEquals(6, rowCount);
			stmt.execute("close \"foo\"");
		} finally {
			ExtendedQueryExectutorImpl.simplePortal = null;
		}
		
	}
	
	@Test public void testScrollCursorWithHold() throws Exception {
		Statement stmt = conn.createStatement();
		ExtendedQueryExectutorImpl.simplePortal = "foo";
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
			ExtendedQueryExectutorImpl.simplePortal = null;
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
		p.setProperty("sslfactory", AnonSSLSocketFactory.class.getName());
		conn = d.connect("jdbc:postgresql://"+odbcServer.addr.getHostName()+":" +odbcServer.odbcTransport.getPort()+"/parts", p);
		Statement s = conn.createStatement();
		assertTrue(s.execute("select * from tables order by name"));
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
		
		rs = s.executeQuery("select name::varchar from tables where name = 'Columns'");
		assertTrue(rs.next());
		assertEquals("Columns", rs.getString(1));
	}
	
	@Test public void testInt2Vector() throws Exception {
		Statement s = conn.createStatement();
		ResultSet rs = s.executeQuery("select indkey FROM pg_index");
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
	
	@Test public void test_pg_cast() throws Exception {
		Statement s = conn.createStatement();
		ResultSet rs = s.executeQuery("select '2011-01-01'::date");
		rs.next();
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
	
	@Test public void testTransactionCycleDisabled() throws Exception {
		Statement s = conn.createStatement();
		s.execute("set disableLocalTxn true");
		conn.setAutoCommit(false); 
		assertTrue(s.execute("select * from tables order by name"));
		conn.setAutoCommit(true);
	}
	
	@Test public void testGropuByPositional() throws Exception {
		Statement s = conn.createStatement();
		//would normally throw an exception, but is allowable over odbc
		s.execute("select name, count(schemaname) from tables group by 1");
	}
	
	@Test public void testImplicitPortalClosing() throws Exception {
		PreparedStatement s = conn.prepareStatement("select 1");
		s.executeQuery();
		
		s.executeQuery();
		
		assertEquals(1, odbcServer.server.getDqp().getRequests().size());
	}
	
	@Test public void testSomething() throws Exception {
		String sql = ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("exported-fk-query.txt"));
		
		Statement s = conn.createStatement();
		s.execute(sql);
		ResultSet rs = s.getResultSet();
		assertTrue(rs.next());
	}

}
