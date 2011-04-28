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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.FakeServer;

@SuppressWarnings("nls")
public class TestStats {
	
    static Connection connection;
    
    static final String VDB = "PartsSupplier";
    
	@BeforeClass public static void setUp() throws Exception {
    	FakeServer server = new FakeServer();
    	server.deployVDB(VDB, UnitTestUtil.getTestDataPath() + "/PartsSupplier.vdb");
    	connection = server.createConnection("jdbc:teiid:" + VDB); //$NON-NLS-1$ //$NON-NLS-2$		
    }
    
    @AfterClass public static void tearDown() throws SQLException {
    	connection.close();
    }

    @Test public void testSetTableStats() throws Exception {
    	Statement s = connection.createStatement();
    	ResultSet rs = s.executeQuery("select cardinality from tables where name = 'PARTSSUPPLIER.PARTS'");
    	rs.next();
    	assertEquals(16, rs.getInt(1));
    	s.execute("call setTableStats(tableName=>'partssupplier.partssupplier.parts', cardinality=>32)");
    	rs = s.executeQuery("select cardinality from tables where name = 'PARTSSUPPLIER.PARTS'");
    	rs.next();
    	assertEquals(32, rs.getInt(1));
    }
    
    @Test public void testSetColumnStats() throws Exception {
    	Statement s = connection.createStatement();
    	ResultSet rs = s.executeQuery("select MinRange, MaxRange, DistinctCount, NullCount from columns where name = 'PART_ID'");
    	rs.next();
    	assertEquals(null, rs.getString(1));
    	assertEquals(null, rs.getString(2));
    	assertEquals(-1, rs.getInt(3));
    	assertEquals(-1, rs.getInt(4));
    	s.execute("call setColumnStats(tableName=>'partssupplier.partssupplier.parts', columnName=>'PART_ID', max=>32, nullcount=>0)");
    	rs = s.executeQuery("select MinRange, MaxRange, DistinctCount, NullCount from columns where name = 'PART_ID'");
    	rs.next();
    	assertEquals(null, rs.getString(1));
    	assertEquals("32", rs.getString(2));
    	assertEquals(-1, rs.getInt(3));
    	assertEquals(0, rs.getInt(4));
    }
    
    @Test(expected=SQLException.class) public void testSetColumnStatsInvalidColumn() throws Exception {
    	Statement s = connection.createStatement();
    	s.execute("call setColumnStats(tableName=>'partssupplier.partssupplier.parts', columnName=>'foo', max=>32, nullcount=>0)");
    }
}
