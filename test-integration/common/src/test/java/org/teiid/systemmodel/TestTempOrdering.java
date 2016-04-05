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
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.FakeServer;
import org.teiid.runtime.EmbeddedConfiguration;

@SuppressWarnings("nls")
public class TestTempOrdering {

	private static final String VDB = "PartsSupplier"; //$NON-NLS-1$
	
	private static FakeServer server;
    
    @BeforeClass public static void setup() throws Exception {
    	server = new FakeServer(false);
    	EmbeddedConfiguration ec = new EmbeddedConfiguration();
    	Properties p = new Properties();
    	p.setProperty("org.teiid.defaultNullOrder", "HIGH");
    	ec.setProperties(p);
    	server.start(ec, false);
    	server.deployVDB(VDB, UnitTestUtil.getTestDataPath() + "/PartsSupplier.vdb");
    }
    
    @AfterClass public static void teardown() throws Exception {
    	server.stop();
    }
    
    @Test public void testNullOrder() throws Exception {
    	Connection c = server.createConnection("jdbc:teiid:PartsSupplier");
    	Statement s = c.createStatement();
    	s.execute("insert into #temp (a) values (null),(1)");
    	
    	//will be high based upon the system property
    	ResultSet rs = s.executeQuery("select * from #temp order by a");
    	rs.next();
    	assertNotNull(rs.getObject(1));
    	
    	rs = s.executeQuery("select * from #temp order by a nulls first");
    	rs.next();
    	assertNull(rs.getObject(1));
    }
}
