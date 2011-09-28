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
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;

import org.jgroups.JChannelFactory;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.FakeServer;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.FunctionParameter;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.metadata.FunctionMethod.PushDown;
import org.teiid.replication.jboss.JGroupsObjectReplicator;

@SuppressWarnings("nls")
public class TestMatViewReplication {
	
    private static final String MATVIEWS = "matviews";
    private static final boolean DEBUG = false;
    
    @BeforeClass public static void oneTimeSetup() {
    	System.setProperty("jgroups.bind_addr", "127.0.0.1");
    }
    
    @Test public void testReplication() throws Exception {
    	if (DEBUG) {
	    	UnitTestUtil.enableTraceLogging("org.teiid");
    	}
    	
		FakeServer server1 = createServer();
		
		Connection c1 = server1.createConnection("jdbc:teiid:matviews");
		Statement stmt = c1.createStatement();
		stmt.execute("select * from TEST.RANDOMVIEW");
		ResultSet rs = stmt.getResultSet();
		assertTrue(rs.next());
		double d1 = rs.getDouble(1);
		double d2 = rs.getDouble(2);
		
		FakeServer server2 = createServer();
		Connection c2 = server2.createConnection("jdbc:teiid:matviews");
		Statement stmt2 = c2.createStatement();
		ResultSet rs2 = stmt2.executeQuery("select * from matviews where name = 'RandomView'");
		assertTrue(rs2.next());
		assertEquals("LOADED", rs2.getString("loadstate"));
		assertEquals(true, rs2.getBoolean("valid"));
		stmt2.execute("select * from TEST.RANDOMVIEW");
		rs2 = stmt2.getResultSet();
		assertTrue(rs2.next());
		assertEquals(d1, rs2.getDouble(1), 0);
		assertEquals(d2, rs2.getDouble(2), 0);
		
		rs2 = stmt2.executeQuery("select * from (call refreshMatView('TEST.RANDOMVIEW', false)) p");
		
		Thread.sleep(1000);

		//make sure we're still valid and the same
		stmt.execute("select * from TEST.RANDOMVIEW");
		rs = stmt.getResultSet();
		assertTrue(rs.next());
		d1 = rs.getDouble(1);
		d2 = rs.getDouble(2);
		stmt2.execute("select * from TEST.RANDOMVIEW");
		rs2 = stmt2.getResultSet();
		assertTrue(rs2.next());
		assertEquals(d1, rs2.getDouble(1), 0);
		assertEquals(d2, rs2.getDouble(2), 0);

		//ensure a lookup is usable on each side
		rs2 = stmt2.executeQuery("select lookup('sys.schemas', 'VDBName', 'name', 'SYS')");
		Thread.sleep(1000);

		rs = stmt.executeQuery("select lookup('sys.schemas', 'VDBName', 'name', 'SYS')");
		rs.next();
		assertEquals("matviews", rs.getString(1));
		
		server1.stop();
		server2.stop();
    }

	private FakeServer createServer() throws Exception {
		FakeServer server = new FakeServer();
		
		JGroupsObjectReplicator jor = new JGroupsObjectReplicator();
        jor.setClusterName("demo");
        jor.setMultiplexerStack("tcp");
        JChannelFactory jcf = new JChannelFactory();
        jcf.setMultiplexerConfig(this.getClass().getClassLoader().getResource("stacks.xml")); //$NON-NLS-1$
        jor.setChannelFactory(jcf);

		server.setReplicator(jor);
    	HashMap<String, Collection<FunctionMethod>> udfs = new HashMap<String, Collection<FunctionMethod>>();
    	udfs.put("funcs", Arrays.asList(new FunctionMethod("pause", null, null, PushDown.CANNOT_PUSHDOWN, TestMatViews.class.getName(), "pause", null, new FunctionParameter("return", DataTypeManager.DefaultDataTypes.INTEGER), false, Determinism.NONDETERMINISTIC)));
    	server.deployVDB(MATVIEWS, UnitTestUtil.getTestDataPath() + "/matviews.vdb", udfs);
		return server;
	}
	
}
