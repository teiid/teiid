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
import java.util.LinkedHashMap;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.teiid.core.types.DataTypeManager;
import org.teiid.deployers.VDBRepository;
import org.teiid.jdbc.FakeServer;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Table;
import org.teiid.metadata.index.VDBMetadataFactory;
import org.teiid.query.metadata.TransformationMetadata.Resource;

@SuppressWarnings("nls")
public class TestMatViewAliasing {
	
    private static final String MATVIEWS = "matviews";
	private Connection conn;
	private FakeServer server;
	
	@Before public void setUp() throws Exception {
    	server = new FakeServer();
    	
    	VDBRepository vdbRepository = new VDBRepository();
    	vdbRepository.setSystemStore(VDBMetadataFactory.getSystem());
    	MetadataFactory mf = new MetadataFactory("foo", vdbRepository.getBuiltinDatatypes(), new Properties());
    	
    	Table mat = mf.addTable("mat");
    	mat.setVirtual(true);
    	mat.setMaterialized(true);
    	mat.setSelectTransformation("/*+ cache(ttl:0) */ select 1 as x, 'y' as Name");
    	
    	mf.addColumn("x", DataTypeManager.DefaultDataTypes.INTEGER, mat);
    	mf.addColumn("Name", DataTypeManager.DefaultDataTypes.STRING, mat);
    	
    	MetadataStore ms = mf.getMetadataStore();
    	
    	server.deployVDB(MATVIEWS, ms, new LinkedHashMap<String, Resource>());
    	conn = server.createConnection("jdbc:teiid:"+MATVIEWS);
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
		assertEquals(false, rs.getBoolean("valid"));

		rs = s.executeQuery("select * from mat order by x");
		assertTrue(rs.next());
		rs = s.executeQuery("select * from MatViews where name = 'mat'");
		assertTrue(rs.next());
		assertEquals("LOADED", rs.getString("loadstate"));
		
		rs = s.executeQuery("select * from mat as a, mat as b where a.x = b.name order by a.x");
		assertFalse(rs.next());
	}
	
}
