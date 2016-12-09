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
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Types;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.client.plan.PlanNode;
import org.teiid.client.plan.PlanNode.Property;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.analysis.AnalysisRecord;


@SuppressWarnings("nls")
public class TestQueryPlans {
	static FakeServer server;
	private static Connection conn;
	
	@BeforeClass public static void setUp() throws Exception {
    	server = new FakeServer(true);
    	server.deployVDB("test", UnitTestUtil.getTestDataPath() + "/TestCase3473/test.vdb");
    	conn = server.createConnection("jdbc:teiid:test"); //$NON-NLS-1$ //$NON-NLS-2$		
    }
	
	@AfterClass public static void tearDown() throws Exception {
		conn.close();
		server.stop();
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
		assertNull(s.unwrap(TeiidStatement.class).getDebugLog());
		
		rs = s.executeQuery("show plan");
		assertTrue(rs.next());
		assertEquals(rs.getMetaData().getColumnType(1), Types.CLOB);
		assertTrue(rs.getString(1).startsWith("ProjectNode"));
		SQLXML plan = rs.getSQLXML(2);
		assertTrue(plan.getString().startsWith("<?xml"));
		assertNull(rs.getObject("DEBUG_LOG"));
		assertNotNull(rs.getObject("PLAN_TEXT"));
		
		s.execute("SET showplan debug");
		rs = s.executeQuery("select * from all_tables");
		assertNotNull(s.unwrap(TeiidStatement.class).getDebugLog());
		PlanNode node = s.unwrap(TeiidStatement.class).getPlanDescription();
		Property p = node.getProperty(AnalysisRecord.PROP_DATA_BYTES_SENT);
		assertEquals("20", p.getValues().get(0));
		
		rs = s.executeQuery("show plan");
		assertTrue(rs.next());
		assertNotNull(rs.getObject("DEBUG_LOG"));
		
		s.execute("SET showplan off");
		rs = s.executeQuery("select * from all_tables");
		assertNull(s.unwrap(TeiidStatement.class).getPlanDescription());
		assertTrue(rs.next());
	}
	
	@Test public void testShow() throws Exception {
		Statement s = conn.createStatement();
		ResultSet rs = s.executeQuery("show all");
		assertTrue(rs.next());
		assertNotNull(rs.getString("NAME"));
		
		s.execute("set showplan on");
		
		rs = s.executeQuery("show showplan");
		rs.next();
		assertEquals("on", rs.getString(1));
	}
	
}
