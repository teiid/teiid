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
import java.sql.Connection;
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
import org.postgresql.Driver;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.FakeServer;
import org.teiid.jdbc.TeiidDriver;
import org.teiid.jdbc.TestMMDatabaseMetaData;
import org.teiid.net.socket.SocketUtil;

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

		public Socket createSocket() throws IOException {
			return sslSocketFactory.createSocket();
		}

		public Socket createSocket(InetAddress address, int port,
				InetAddress localAddress, int localPort) throws IOException {
			return sslSocketFactory.createSocket(address, port, localAddress,
					localPort);
		}

		public Socket createSocket(InetAddress host, int port)
				throws IOException {
			return sslSocketFactory.createSocket(host, port);
		}

		public Socket createSocket(Socket s, String host, int port,
				boolean autoClose) throws IOException {
			SSLSocket socket = (SSLSocket)sslSocketFactory.createSocket(s, host, port, autoClose);
			SocketUtil.addCipherSuite(socket, SocketUtil.ANON_CIPHER_SUITE);
			return socket;
		}

		public Socket createSocket(String host, int port,
				InetAddress localHost, int localPort) throws IOException,
				UnknownHostException {
			return sslSocketFactory.createSocket(host, port, localHost,
					localPort);
		}

		public Socket createSocket(String host, int port) throws IOException,
				UnknownHostException {
			return sslSocketFactory.createSocket(host, port);
		}

		public String[] getDefaultCipherSuites() {
			return sslSocketFactory.getDefaultCipherSuites();
		}

		public String[] getSupportedCipherSuites() {
			return sslSocketFactory.getSupportedCipherSuites();
		}
		
	}
	
	static class FakeOdbcServer {
		InetSocketAddress addr;
		ODBCSocketListener odbcTransport;
		
		public void start() throws Exception {
			SocketConfiguration config = new SocketConfiguration();
			SSLConfiguration sslConfig = new SSLConfiguration();
			sslConfig.setMode(SSLConfiguration.ENABLED);
			sslConfig.setAuthenticationMode(SSLConfiguration.ANONYMOUS);
			config.setSSLConfiguration(sslConfig);
			addr = new InetSocketAddress(0);
			config.setBindAddress(addr.getHostName());
			config.setPortNumber(0);
			odbcTransport = new ODBCSocketListener(config, BufferManagerFactory.getStandaloneBufferManager(), 0, 100000);
			
			FakeServer server = new FakeServer();
			server.setUseCallingThread(false);
			server.deployVDB("parts", UnitTestUtil.getTestDataPath() + "/PartsSupplier.vdb");
			
			TeiidDriver driver = new TeiidDriver();
			driver.setEmbeddedProfile(server);
			odbcTransport.setDriver(driver);
		}
		
		public void stop() {
			odbcTransport.stop();
		}
		
	}
	
	private static FakeOdbcServer odbcServer = new FakeOdbcServer();
	
	@BeforeClass public static void oneTimeSetup() throws Exception {
		odbcServer.start();
	}
	
	@AfterClass public static void oneTimeTearDown() throws Exception {
		odbcServer.stop();
	}
	
	Connection conn;
	
	@Before public void setUp() throws Exception {
		Driver d = new Driver();
		Properties p = new Properties();
		p.setProperty("user", "testuser");
		p.setProperty("password", "testpassword");
		conn = d.connect("jdbc:postgresql://"+odbcServer.addr.getHostName()+":" +odbcServer.odbcTransport.getPort()+"/parts", p);
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
	}
	
	@Test public void testPgProcedure() throws Exception {
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("select has_function_privilege(100, 'foo')");
		rs.next();
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
}
