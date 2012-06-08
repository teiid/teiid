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

package org.teiid.jdbc;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Before;
import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;


@SuppressWarnings("nls")
public class TestResultsCache {

	private Connection conn;
	
	@Before public void setUp() throws Exception {
    	FakeServer server = new FakeServer(true);
    	server.deployVDB("test", UnitTestUtil.getTestDataPath() + "/TestCase3473/test.vdb");
    	conn = server.createConnection("jdbc:teiid:test"); //$NON-NLS-1$ //$NON-NLS-2$		
    }
	
	@Test public void testCacheHint() throws Exception {
		Statement s = conn.createStatement();
		s.execute("set showplan on");
		ResultSet rs = s.executeQuery("/* cache */ select 1");
		assertTrue(rs.next());
		s.execute("set noexec on");
		rs = s.executeQuery("/* cache */ select 1");
		assertTrue(rs.next());
		rs = s.executeQuery("select 1");
		assertFalse(rs.next());
	}
	
	@Test public void testCacheHintTtl() throws Exception {
		Statement s = conn.createStatement();
		s.execute("set showplan on");
		ResultSet rs = s.executeQuery("/* cache(ttl:50) */ select 1");
		assertTrue(rs.next());
		s.execute("set noexec on");
		rs = s.executeQuery("/* cache(ttl:50) */ select 1");
		assertTrue(rs.next());
		Thread.sleep(60);
		rs = s.executeQuery("/* cache(ttl:50) */ select 1");
		assertFalse(rs.next());
	}
	
	@Test public void testExecutionProperty() throws Exception {
		Statement s = conn.createStatement();
		s.execute("set showplan on");
		s.execute("set resultSetCacheMode true");
		ResultSet rs = s.executeQuery("select 1");
		assertTrue(rs.next());
		s.execute("set noexec on");
		rs = s.executeQuery("select 1");
		assertTrue(rs.next());
		s.execute("set resultSetCacheMode false");
		rs = s.executeQuery("select 1");
		assertFalse(rs.next());
	}
	
}
