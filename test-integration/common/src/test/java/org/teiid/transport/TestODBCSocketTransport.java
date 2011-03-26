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

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

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

@SuppressWarnings("nls")
public class TestODBCSocketTransport {

	static InetSocketAddress addr;
	static ODBCSocketListener odbcTransport;
	
	@BeforeClass public static void oneTimeSetup() throws Exception {
		SocketConfiguration config = new SocketConfiguration();
		config.setSSLConfiguration(new SSLConfiguration());
		addr = new InetSocketAddress(0);
		config.setBindAddress(addr.getHostName());
		config.setPortNumber(0);
		odbcTransport = new ODBCSocketListener(config, BufferManagerFactory.getStandaloneBufferManager(), 0, 100000);
		
		FakeServer server = new FakeServer();
		server.deployVDB("parts", UnitTestUtil.getTestDataPath() + "/PartsSupplier.vdb");
		
		TeiidDriver driver = new TeiidDriver();
		driver.setEmbeddedProfile(server);
		odbcTransport.setDriver(driver);
		
	}
	
	@AfterClass public static void oneTimeTearDown() throws Exception {
		if (odbcTransport != null) {
			odbcTransport.stop();
		}
	}
	
	Connection conn;
	
	@Before public void setUp() throws Exception {
		Driver d = new Driver();
		Properties p = new Properties();
		p.setProperty("user", "testuser");
		p.setProperty("password", "testpassword");
		conn = d.connect("jdbc:postgresql://"+addr.getHostName()+":" +odbcTransport.getPort()+"/parts", p);
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
	
	@Test public void testEscapedLiteral() throws Exception {
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("select E'\\n\\thello pg'");
		assertTrue(rs.next());
		assertEquals("\n\thello pg", rs.getString(1));
	}
}
