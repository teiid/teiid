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

import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.adminapi.Model.Type;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.AbstractMMQueryTestCase;
import org.teiid.jdbc.FakeServer;
import org.teiid.jdbc.TestMMDatabaseMetaData;


/**
 * Exercises each virtual table in the system model.
 */
@SuppressWarnings("nls")
public class TestSystemVirtualModel extends AbstractMMQueryTestCase {
	private static final String VDB = "PartsSupplier"; //$NON-NLS-1$
	
	private static FakeServer server;
    
    @BeforeClass public static void setup() throws Exception {
    	server = new FakeServer(true);
    	server.deployVDB(VDB, UnitTestUtil.getTestDataPath() + "/PartsSupplier.vdb");
    	ModelMetaData mmd = new ModelMetaData();
    	mmd.setName("x");
    	mmd.setModelType(Type.VIRTUAL);
    	mmd.setSchemaSourceType("DDL");
    	mmd.setSchemaText("create view t as select 1");
    	ModelMetaData mmd1 = new ModelMetaData();
    	mmd1.setName("y");
    	mmd1.setModelType(Type.VIRTUAL);
    	mmd1.setSchemaSourceType("DDL");
    	mmd1.setSchemaText("create view T as select 1");
    	server.deployVDB("test", mmd, mmd1);
    }
    
    @AfterClass public static void teardown() throws Exception {
    	server.stop();
    }

	public TestSystemVirtualModel() {
		// this is needed because the result files are generated
		// with another tool which uses tab as delimiter
		super.DELIMITER = "\t"; //$NON-NLS-1$
	}
	
    @Before public void setUp() throws Exception {
    	this.internalConnection = server.createConnection("jdbc:teiid:" + VDB); //$NON-NLS-1$ //$NON-NLS-2$	
   	}
   
    protected void checkResult(String testName, String query) throws Exception {
    	execute(query);
    	TestMMDatabaseMetaData.compareResultSet("TestSystemVirtualModel/" + testName, this.internalResultSet);
    }
    
	@Test public void testModels() throws Exception {
		checkResult("testSchemas", "select* from SYS.Schemas order by Name"); //$NON-NLS-1$ //$NON-NLS-2$
	}

	@Test public void testKeys() throws Exception {
		checkResult("testKeys", "select* from SYS.Keys order by Name"); //$NON-NLS-1$ //$NON-NLS-2$
	}

	@Test public void testGroups() throws Exception {
		checkResult("testTables", "select* from SYS.Tables order by Name"); //$NON-NLS-1$ //$NON-NLS-2$
	}

	@Test public void testDataTypes() throws Exception {
		checkResult("testDataTypes", "select * from SYS.DataTypes order by name"); //$NON-NLS-1$ //$NON-NLS-2$
	}

	@Test public void testProcedureParams() throws Exception {
		checkResult("testProcedureParams", "select * from SYS.ProcedureParams order by Name"); //$NON-NLS-1$ //$NON-NLS-2$
	}

	@Test public void testProcedures() throws Exception {
		checkResult("testProcedures", "select* from SYS.Procedures order by Name"); //$NON-NLS-1$ //$NON-NLS-2$
	}

	@Test public void testProperties() throws Exception {
		checkResult("testProperties", "select* from SYS.Properties"); //$NON-NLS-1$
	}

	@Test public void testVirtualDatabase() throws Exception {

		String[] expected = { "Name[string]	Version[string]	", "PartsSupplier	1", //$NON-NLS-1$ //$NON-NLS-2$

		};
		executeAndAssertResults("select* from SYS.VirtualDatabases", //$NON-NLS-1$
				expected);
	}

	@Test public void testKeyColumns() throws Exception {
		checkResult("testKeyColumns", "select* from SYS.KeyColumns order by Name, KeyName"); //$NON-NLS-1$ //$NON-NLS-2$
	}

	@Test public void testVDBResources() throws IOException, SQLException {
		execute("select * from vdbresources order by resourcePath",new Object[] {}); //$NON-NLS-1$
		TestMMDatabaseMetaData.compareResultSet(this.internalResultSet);
	}

	@Test public void testColumns() throws Exception {
		checkResult("testColumns", "select* from SYS.Columns order by Name, uid"); //$NON-NLS-1$ //$NON-NLS-2$
	}

	@Test public void testTableType() throws Exception {

		String[] expected = { "Type[string]	", "Table", }; //$NON-NLS-1$ //$NON-NLS-2$
		executeAndAssertResults(
				"select distinct Type from SYS.Tables order by Type", //$NON-NLS-1$
				expected);
	}

	@Test public void testTableIsSystem() throws Exception {
		checkResult("testTableIsSystem", "select Name from SYS.Tables where IsSystem = 'false' order by Name"); //$NON-NLS-1$ //$NON-NLS-2$
	}

	@Test public void testDefect12064() throws Exception {
		checkResult("testDefect12064", "select KeyName, RefKeyUID FROM SYS.KeyColumns WHERE RefKeyUID IS NULL order by KeyName"); //$NON-NLS-1$
	}

	@Test public void testReferenceKeyColumns() throws Exception {
		checkResult("testReferenceKeyColumns", "select* FROM SYS.ReferenceKeyColumns order by PKTABLE_NAME"); //$NON-NLS-1$ //$NON-NLS-2$
	}
	
	@Test public void testLogMsg() throws Exception {
		execute("call logMsg(level=>'DEBUG', context=>'org.teiid.foo', msg=>'hello world')"); //$NON-NLS-1$
	}
	
	@Test(expected=SQLException.class) public void testLogMsg1() throws Exception {
		execute("call logMsg(level=>'foo', context=>'org.teiid.foo', msg=>'hello world')"); //$NON-NLS-1$
	}
	
	@Test public void testCallableParametersByName() throws Exception {
		CallableStatement cs = this.internalConnection.prepareCall("{? = call logMsg(?, ?, ?)}");
		ParameterMetaData pmd = cs.getParameterMetaData();
		assertEquals(3, pmd.getParameterCount());
		cs.registerOutParameter("logged", Types.BOOLEAN);
		//different case
		cs.setString("LEVEL", "DEBUG");
		try {
			//invalid param
			cs.setString("n", "");
			fail();
		} catch (SQLException e) {
		}
		cs.setString("context", "org.teiid.foo");
		cs.setString("msg", "hello world");
		cs.execute();
		assertEquals(cs.getBoolean(1), cs.getBoolean("logged"));
	}
	
	@Test public void testArrayAggType() throws Exception {
		String sql = "SELECT array_agg(name) from tables"; //$NON-NLS-1$
		checkResult("testArrayAggType", sql); //$NON-NLS-1$ //$NON-NLS-2$
	}
	
	@Test public void testExecuteUpdateWithStoredProcedure() throws Exception {
		PreparedStatement cs = this.internalConnection.prepareStatement("call logMsg(?, ?, ?)");
		//different case
		cs.setString(1, "DEBUG");
		cs.setString(2, "org.teiid.foo");
		cs.setString(3, "hello world");
		assertEquals(0, cs.executeUpdate());
		
		Statement s = this.internalConnection.createStatement();
		assertEquals(0, s.executeUpdate("call logMsg('DEBUG', 'org.teiid.foo', 'hello world')"));
	}
	
	@Test public void testExpectedTypes() throws Exception {
		ResultSet rs = this.internalConnection.createStatement().executeQuery("select name from tables where schemaname in ('SYS', 'SYSADMIN')");
		while (rs.next()) {
			String name = rs.getString(1);
			ResultSet rs1 = this.internalConnection.createStatement().executeQuery("select * from " + name + " limit 1");
			ResultSetMetaData metadata = rs1.getMetaData();
			if (rs1.next()) {
				for (int i = 1; i <= metadata.getColumnCount(); i++) {
					Object o = rs1.getObject(i);
					assertTrue("Type mismatch for " + name + " " + metadata.getColumnName(i), o == null || Class.forName(metadata.getColumnClassName(i)).isAssignableFrom(o.getClass()));
				}
			}
		}
	}
	
	@Test public void testPrefixSearches() throws Exception {
		this.execute("select name from schemas where ucase(name) >= 'BAZ_BAR' and ucase(name) <= 'A'");
		//should be 0 rows rather than an exception
		assertRowCount(0); 

		this.execute("select name from schemas where upper(name) like 'ab[_'");
		//should be 0 rows rather than an exception
		assertRowCount(0);		
	}
	
	@Test public void testColumnsIn() throws Exception {
		this.internalConnection.close();
		this.internalConnection = server.createConnection("jdbc:teiid:test");
		this.execute("select tablename, name from columns where tablename in ('t', 'T')");
		//should be 2, not 4 rows
		assertRowCount(2);
		
		this.execute("select tablename, name from columns where upper(tablename) in ('t', 's')");
		assertRowCount(0);
	}
	
}
