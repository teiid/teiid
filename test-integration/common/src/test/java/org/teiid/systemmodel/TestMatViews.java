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

package org.teiid.systemmodel;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Before;
import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.FakeServer;

@SuppressWarnings("nls")
public class TestMatViews {
	
    private Connection conn;

	@Before public void setUp() throws Exception {
    	FakeServer server = new FakeServer();
    	server.deployVDB("matviews", UnitTestUtil.getTestDataPath() + "/matviews.vdb");
    	conn = server.createConnection("jdbc:teiid:matviews");
    }
	
	@Test public void testSystemMatViews() throws Exception {
		Statement s = conn.createStatement();
		ResultSet rs = s.executeQuery("select * from SYS.MatViews order by name");
		assertTrue(rs.next());
		assertEquals("NOT_LOADED", rs.getString("state"));
		assertEquals("#MAT_TEST.ERRORVIEW", rs.getString("targetName"));
		assertTrue(rs.next());
		assertEquals("NOT_LOADED", rs.getString("state"));
		assertEquals("#MAT_TEST.MATVIEW", rs.getString("targetName"));
		assertFalse(rs.next());
		rs = s.executeQuery("select * from MatView");
		assertTrue(rs.next());
		rs = s.executeQuery("select * from SYS.MatViews where name = 'MatView'");
		assertTrue(rs.next());
		assertEquals("LOADED", rs.getString("state"));
		try {
			s.executeQuery("select * from ErrorView");
		} catch (SQLException e) {
			
		}
		rs = s.executeQuery("select * from SYS.MatViews where name = 'ErrorView'");
		assertTrue(rs.next());
		assertEquals("FAILED_LOAD", rs.getString("state"));
	}

}
