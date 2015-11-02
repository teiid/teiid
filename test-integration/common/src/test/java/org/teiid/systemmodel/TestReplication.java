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

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.teiid.adminapi.Model.Type;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.deployers.VirtualDatabaseException;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository.ConnectorManagerException;
import org.teiid.jdbc.FakeServer;
import org.teiid.jdbc.FakeServer.DeployVDBParameter;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.metadata.FunctionMethod.PushDown;
import org.teiid.metadata.FunctionParameter;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.translator.TranslatorException;

@SuppressWarnings("nls")
@Ignore
public class TestReplication {
	
    private static final String MATVIEWS = "matviews";
    private static final boolean DEBUG = false;
	private FakeServer server1;
	private FakeServer server2;
    
    @BeforeClass public static void oneTimeSetup() {
    	if (DEBUG) {
    		UnitTestUtil.enableTraceLogging("org.teiid");
    	}
    	System.setProperty("jgroups.bind_addr", "127.0.0.1");
    }
    
    @After public void tearDown() {
    	if (server1 != null) {
    		server1.stop();
    	}
    	if (server2 != null) {
    		server2.stop();
    	}
    }
    
    @Test public void testReplication() throws Exception {
		server1 = createServer("infinispan-replicated-config.xml", "tcp-shared.xml");
    	deployMatViewVDB(server1);    	

		Connection c1 = server1.createConnection("jdbc:teiid:matviews");
		Statement stmt = c1.createStatement();
		stmt.execute("select * from TEST.RANDOMVIEW");
		ResultSet rs = stmt.getResultSet();
		assertTrue(rs.next());
		double d1 = rs.getDouble(1);
		double d2 = rs.getDouble(2);
		
		server2 = createServer("infinispan-replicated-config-1.xml", "tcp-shared-1.xml");
    	deployMatViewVDB(server2);    	

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
		
		//result set cache replication
		
		rs = stmt.executeQuery("/*+ cache(scope:vdb) */ select rand()"); //$NON-NLS-1$
		assertTrue(rs.next());
		d1 = rs.getDouble(1);
		
		//no wait is needed as we perform a synch pull
		rs2 = stmt2.executeQuery("/*+ cache(scope:vdb) */ select rand()"); //$NON-NLS-1$
		assertTrue(rs2.next());
		d2 = rs2.getDouble(1);
		
		assertEquals(d1, d2, 0);
    }
    
    @Test public void testLargeReplication() throws Exception {
		server1 = createServer("infinispan-replicated-config.xml", "tcp-shared.xml");
    	deployLargeVDB(server1);    	

		Connection c1 = server1.createConnection("jdbc:teiid:large");
		Statement stmt = c1.createStatement();
		stmt.execute("select * from c");
		ResultSet rs = stmt.getResultSet();
		int rowCount = 0;
		while (rs.next()) {
			rowCount++;
		}
		
		Thread.sleep(1000);
		
		server2 = createServer("infinispan-replicated-config-1.xml", "tcp-shared-1.xml");
    	deployLargeVDB(server2);    	

		Connection c2 = server2.createConnection("jdbc:teiid:large");
		
		Statement stmt2 = c2.createStatement();
		ResultSet rs2 = stmt2.executeQuery("select * from matviews where name = 'c'");
		assertTrue(rs2.next());
		assertEquals("LOADED", rs2.getString("loadstate"));
		
		stmt2 = c2.createStatement();
		rs2 = stmt2.executeQuery("select * from c");
		
		int rowCount2 = 0;
		while (rs2.next()) {
			rowCount2++;
		}
		
		System.out.println(rowCount);
		
		assertEquals(rowCount, rowCount2);
    }

	private FakeServer createServer(String ispn, String jgroups) throws Exception {
		FakeServer server = new FakeServer(false);

		EmbeddedConfiguration config = new EmbeddedConfiguration();
		config.setInfinispanConfigFile(ispn);
		config.setJgroupsConfigFile(jgroups);
		server.start(config, true);
		return server;
	}

	private void deployLargeVDB(FakeServer server)
			throws ConnectorManagerException, VirtualDatabaseException,
			TranslatorException {
		ModelMetaData mmd = new ModelMetaData();
    	mmd.setName("mv");
    	mmd.setModelType(Type.VIRTUAL);
    	mmd.addSourceMetadata("ddl", "create view c options (materialized true) as WITH t(n) AS ( VALUES (1) UNION ALL SELECT n+1 FROM t WHERE n < 10000 ) SELECT n, n || 'a', n + n FROM t");
    	
    	server.deployVDB("large", mmd);
	}

	private void deployMatViewVDB(FakeServer server) throws Exception {
		HashMap<String, Collection<FunctionMethod>> udfs = new HashMap<String, Collection<FunctionMethod>>();
    	udfs.put("funcs", Arrays.asList(new FunctionMethod("pause", null, null, PushDown.CANNOT_PUSHDOWN, TestMatViews.class.getName(), "pause", null, new FunctionParameter("return", DataTypeManager.DefaultDataTypes.INTEGER), true, Determinism.NONDETERMINISTIC)));
    	server.deployVDB(MATVIEWS, UnitTestUtil.getTestDataPath() + "/matviews.vdb", new DeployVDBParameter(udfs, null));
	}
	
}
