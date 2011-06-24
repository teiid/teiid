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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.FakeServer;
import org.teiid.jdbc.TeiidSQLException;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.FunctionParameter;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.metadata.FunctionMethod.PushDown;

@SuppressWarnings("nls")
public class TestMatViews {
	
    private static final String MATVIEWS = "matviews";
	private Connection conn;
	private FakeServer server;
	
	private static int count = 0;
	
	public static int pause() throws InterruptedException {
		synchronized (TestMatViews.class) {
			count++;
			TestMatViews.class.notify();
			while (count < 2) {
				TestMatViews.class.wait();
			}
		}
		return 1;
	}

	@Before public void setUp() throws Exception {
    	server = new FakeServer();
    	HashMap<String, Collection<FunctionMethod>> udfs = new HashMap<String, Collection<FunctionMethod>>();
    	udfs.put("funcs", Arrays.asList(new FunctionMethod("pause", null, null, PushDown.CANNOT_PUSHDOWN, TestMatViews.class.getName(), "pause", null, new FunctionParameter("return", DataTypeManager.DefaultDataTypes.INTEGER), false, Determinism.NONDETERMINISTIC)));
    	server.deployVDB(MATVIEWS, UnitTestUtil.getTestDataPath() + "/matviews.vdb", udfs);
    	conn = server.createConnection("jdbc:teiid:matviews");
    }
	
	@After public void tearDown() throws Exception {
		server.stop();
		conn.close();
	}
	
	@Test public void testSystemMatViewsWithImplicitLoad() throws Exception {
		Statement s = conn.createStatement();
		ResultSet rs = s.executeQuery("select * from MatViews order by name");
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
		rs = s.executeQuery("select * from MatViews where name = 'MatView'");
		assertTrue(rs.next());
		assertEquals("LOADED", rs.getString("loadstate"));
		try {
			s.executeQuery("select * from ErrorView");
		} catch (SQLException e) {
			
		}
		rs = s.executeQuery("select * from MatViews where name = 'ErrorView'");
		assertTrue(rs.next());
		assertEquals("FAILED_LOAD", rs.getString("loadstate"));
	}
	
	@Test public void testSystemMatViewsWithExplicitRefresh() throws Exception {
		Statement s = conn.createStatement();
		ResultSet rs = s.executeQuery("select * from (call refreshMatView('TEST.RANDOMVIEW', false)) p");
		assertTrue(rs.next());
		assertEquals(1, rs.getInt(1));
		rs = s.executeQuery("select * from MatViews where name = 'RandomView'");
		assertTrue(rs.next());
		assertEquals("LOADED", rs.getString("loadstate"));
		assertEquals(true, rs.getBoolean("valid"));
		rs = s.executeQuery("select x from TEST.RANDOMVIEW");
		assertTrue(rs.next());
		double key = rs.getDouble(1);

		rs = s.executeQuery("select * from (call refreshMatView('TEST.RANDOMVIEW', false)) p");
		assertTrue(rs.next());
		assertEquals(1, rs.getInt(1));
		rs = s.executeQuery("select * from MatViews where name = 'RandomView'");
		assertTrue(rs.next());
		assertEquals("LOADED", rs.getString("loadstate"));
		assertEquals(true, rs.getBoolean("valid"));
		rs = s.executeQuery("select x from TEST.RANDOMVIEW");
		assertTrue(rs.next());
		double key1 = rs.getDouble(1);

		//ensure that invalidate with distributed caching works
		assertTrue(key1 != key);
	}
	
	@Test public void testSystemManViewsWithExplictRefreshAndInvalidate() throws Exception {
		Statement s = conn.createStatement();
		ResultSet rs = s.executeQuery("select * from (call refreshMatView('TEST.MATVIEW', false)) p");
		assertTrue(rs.next());
		assertEquals(1, rs.getInt(1));
		rs = s.executeQuery("select * from MatViews where name = 'MatView'");
		assertTrue(rs.next());
		assertEquals("LOADED", rs.getString("loadstate"));
		assertEquals(true, rs.getBoolean("valid"));
		
		count = 0;
		s.execute("alter view TEST.MATVIEW as select pause() as x");
		Thread t = new Thread() {
			public void run() {
				try {
					Statement s1 = conn.createStatement();
					ResultSet rs = s1.executeQuery("select * from (call refreshMatView('TEST.MATVIEW', true)) p");
					assertTrue(rs.next());
					assertEquals(1, rs.getInt(1));
				} catch (Exception e) {
					throw new TeiidRuntimeException(e);
				}
			}
		};
		t.start();
		synchronized (TestMatViews.class) {
			while (count < 1) {
				TestMatViews.class.wait();
			}
		}
		rs = s.executeQuery("select * from MatViews where name = 'MatView'");
		assertTrue(rs.next());
		assertEquals("NEEDS_LOADING", rs.getString("loadstate"));
		assertEquals(false, rs.getBoolean("valid"));

		synchronized (TestMatViews.class) {
			count++;
			TestMatViews.class.notify();
		}
		t.join();
		
		rs = s.executeQuery("select * from MatViews where name = 'MatView'");
		assertTrue(rs.next());
		assertEquals("LOADED", rs.getString("loadstate"));
		assertEquals(true, rs.getBoolean("valid"));
	}
	
	@Test(expected=TeiidSQLException.class) public void testSystemMatViewsInvalidView() throws Exception {
		Statement s = conn.createStatement();
		s.execute("call refreshMatView('TEST.NotMat', false)");
	}
	
	@Test(expected=TeiidSQLException.class) public void testSystemMatViewsInvalidView1() throws Exception {
		Statement s = conn.createStatement();
		s.execute("call refreshMatView('foo', false)");
	}
	
	@Test(expected=TeiidSQLException.class) public void testSystemMatViewsWithRowRefreshNotAllowed() throws Exception {
		Statement s = conn.createStatement();
		s.execute("alter view test.randomview as select rand() as x, rand() as y");
		ResultSet rs = s.executeQuery("select * from (call refreshMatView('TEST.RANDOMVIEW', false)) p");
		assertTrue(rs.next());
		assertEquals(1, rs.getInt(1));
		rs = s.executeQuery("select * from MatViews where name = 'RandomView'");
		assertTrue(rs.next());
		assertEquals("LOADED", rs.getString("loadstate"));
		assertEquals(true, rs.getBoolean("valid"));
		rs = s.executeQuery("select x from TEST.RANDOMVIEW");
		assertTrue(rs.next());
		double key = rs.getDouble(1);
		
		rs = s.executeQuery("select * from (call refreshMatViewRow('TEST.RANDOMVIEW', "+key+")) p");
	}
	
	@Test public void testSystemMatViewsWithRowRefresh() throws Exception {
		Statement s = conn.createStatement();
		
		s.execute("alter view test.randomview as /*+ cache(updatable) */ select rand() as x, rand() as y");
		//prior to load refresh of a single row returns -1
		ResultSet rs = s.executeQuery("select * from (call refreshMatViewRow('TEST.RANDOMVIEW', 0)) p");
		assertTrue(rs.next());
		assertEquals(-1, rs.getInt(1));
		assertFalse(rs.next());
		
		rs = s.executeQuery("select * from (call refreshMatView('TEST.RANDOMVIEW', false)) p");
		assertTrue(rs.next());
		assertEquals(1, rs.getInt(1));
		rs = s.executeQuery("select * from MatViews where name = 'RandomView'");
		assertTrue(rs.next());
		assertEquals("LOADED", rs.getString("loadstate"));
		assertEquals(true, rs.getBoolean("valid"));
		rs = s.executeQuery("select x from TEST.RANDOMVIEW");
		assertTrue(rs.next());
		double key = rs.getDouble(1);
		
		rs = s.executeQuery("select * from (call refreshMatViewRow('TEST.RANDOMVIEW', "+key+")) p");
		assertTrue(rs.next());
		assertEquals(1, rs.getInt(1)); //1 row updated (removed)
		
		rs = s.executeQuery("select * from TEST.RANDOMVIEW");
		assertFalse(rs.next());
		
		rs = s.executeQuery("select * from (call refreshMatViewRow('TEST.RANDOMVIEW', "+key+")) p");
		assertTrue(rs.next());
		assertEquals(0, rs.getInt(1)); //no rows updated
	}
	
	@Test(expected=TeiidSQLException.class) public void testSystemMatViewsWithRowRefreshNoPk() throws Exception {
		Statement s = conn.createStatement();
		s.executeQuery("select * from (call refreshMatView('TEST.MATVIEW', false)) p");
		//prior to load refresh of a single row returns -1
		s.executeQuery("select * from (call refreshMatViewRow('TEST.MATVIEW', 0)) p");
	}

}
