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
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.FakeServer;
import org.teiid.jdbc.TeiidSQLException;
import org.teiid.metadata.Table;
import org.teiid.query.metadata.TransformationMetadata;

@SuppressWarnings("nls")
public class TestMatViews {
	
    private static final String MATVIEWS = "matviews";
	private Connection conn;
	private FakeServer server;

	@Before public void setUp() throws Exception {
    	server = new FakeServer();
    	server.deployVDB(MATVIEWS, UnitTestUtil.getTestDataPath() + "/matviews.vdb");
    	conn = server.createConnection("jdbc:teiid:matviews");
    }
	
	@Test public void testSystemMatViewsWithImplicitLoad() throws Exception {
		Statement s = conn.createStatement();
		ResultSet rs = s.executeQuery("select * from SYS.MatViews order by name");
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
		rs = s.executeQuery("select * from SYS.MatViews where name = 'MatView'");
		assertTrue(rs.next());
		assertEquals("LOADED", rs.getString("loadstate"));
		try {
			s.executeQuery("select * from ErrorView");
		} catch (SQLException e) {
			
		}
		rs = s.executeQuery("select * from SYS.MatViews where name = 'ErrorView'");
		assertTrue(rs.next());
		assertEquals("FAILED_LOAD", rs.getString("loadstate"));
	}
	
	@Test public void testSystemMatViewsWithExplicitRefresh() throws Exception {
		Statement s = conn.createStatement();
		ResultSet rs = s.executeQuery("select * from (call SYS.refreshMatView('TEST.MATVIEW', false)) p");
		assertTrue(rs.next());
		assertEquals(1, rs.getInt(1));
		rs = s.executeQuery("select * from SYS.MatViews where name = 'MatView'");
		assertTrue(rs.next());
		assertEquals("LOADED", rs.getString("loadstate"));
		assertEquals(true, rs.getBoolean("valid"));
	}
	
	@Test(expected=TeiidSQLException.class) public void testSystemMatViewsInvalidView() throws Exception {
		Statement s = conn.createStatement();
		s.execute("call SYS.refreshMatView('TEST.NotMat', false)");
	}
	
	@Test(expected=TeiidSQLException.class) public void testSystemMatViewsInvalidView1() throws Exception {
		Statement s = conn.createStatement();
		s.execute("call SYS.refreshMatView('foo', false)");
	}
	
	@Test public void testSystemMatViewsWithRowRefresh() throws Exception {
		//TOOD: remove this. it's a workaround for TEIIDDES-549
		VDBMetaData vdb = server.getVDB(MATVIEWS);
		TransformationMetadata tm = vdb.getAttachment(TransformationMetadata.class);
		Table t = tm.getGroupID("TEST.RANDOMVIEW");
		t.setSelectTransformation("/*+ cache(updatable) */ " +  t.getSelectTransformation());
		
		Statement s = conn.createStatement();
		//prior to load refresh of a single row returns -1
		ResultSet rs = s.executeQuery("select * from (call SYS.refreshMatViewRow('TEST.RANDOMVIEW', 0)) p");
		assertTrue(rs.next());
		assertEquals(-1, rs.getInt(1));
		assertFalse(rs.next());
		
		rs = s.executeQuery("select * from (call SYS.refreshMatView('TEST.RANDOMVIEW', false)) p");
		assertTrue(rs.next());
		assertEquals(1, rs.getInt(1));
		rs = s.executeQuery("select * from SYS.MatViews where name = 'RandomView'");
		assertTrue(rs.next());
		assertEquals("LOADED", rs.getString("loadstate"));
		assertEquals(true, rs.getBoolean("valid"));
		rs = s.executeQuery("select x from TEST.RANDOMVIEW");
		assertTrue(rs.next());
		double key = rs.getDouble(1);
		
		rs = s.executeQuery("select * from (call SYS.refreshMatViewRow('TEST.RANDOMVIEW', "+key+")) p");
		assertTrue(rs.next());
		assertEquals(1, rs.getInt(1)); //1 row updated (removed)
		
		rs = s.executeQuery("select * from TEST.RANDOMVIEW");
		assertFalse(rs.next());
		
		rs = s.executeQuery("select * from (call SYS.refreshMatViewRow('TEST.RANDOMVIEW', "+key+")) p");
		assertTrue(rs.next());
		assertEquals(0, rs.getInt(1)); //no rows updated
	}
	
	@Test(expected=TeiidSQLException.class) public void testSystemMatViewsWithRowRefreshNoPk() throws Exception {
		Statement s = conn.createStatement();
		s.executeQuery("select * from (call SYS.refreshMatView('TEST.MATVIEW', false)) p");
		//prior to load refresh of a single row returns -1
		s.executeQuery("select * from (call SYS.refreshMatViewRow('TEST.MATVIEW', 0)) p");
	}

}
