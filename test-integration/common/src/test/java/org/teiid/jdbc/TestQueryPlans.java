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

import com.metamatrix.core.util.UnitTestUtil;

@SuppressWarnings("nls")
public class TestQueryPlans {

	private Connection conn;
	
	@Before public void setUp() throws Exception {
    	FakeServer server = new FakeServer();
    	server.deployVDB("test", UnitTestUtil.getTestDataPath() + "/TestCase3473/test.vdb");
    	conn = server.createConnection("jdbc:teiid:test"); //$NON-NLS-1$ //$NON-NLS-2$		
    }
	
	@Test public void testNoExec() throws Exception {
		Statement s = conn.createStatement();
		s.execute("set noexec on");
		ResultSet rs = s.executeQuery("select * from all_tables");
		assertFalse(rs.next());
		s.execute("SET NOEXEC off");
		rs = s.executeQuery("select * from all_tables");
		assertTrue(rs.next());
	}
	
	@Test public void testShowPlan() throws Exception {
		Statement s = conn.createStatement();
		s.execute("set showplan on");
		ResultSet rs = s.executeQuery("select * from all_tables");
		assertNotNull(s.unwrap(TeiidStatement.class).getPlanDescription());
		assertNull(s.unwrap(TeiidStatement.class).getDebugLog());
		s.execute("SET showplan debug");
		rs = s.executeQuery("select * from all_tables");
		assertNotNull(s.unwrap(TeiidStatement.class).getDebugLog());
		s.execute("SET showplan off");
		rs = s.executeQuery("select * from all_tables");
		assertNull(s.unwrap(TeiidStatement.class).getPlanDescription());
		assertTrue(rs.next());
	}
	
}
