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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.client.util.ResultsFuture;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.AbstractMMQueryTestCase;
import org.teiid.jdbc.FakeServer;
import org.teiid.jdbc.RequestOptions;
import org.teiid.jdbc.StatementCallback;
import org.teiid.jdbc.TeiidStatement;
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
		execute("select * from vdbresources",new Object[] {}); //$NON-NLS-1$
		TestMMDatabaseMetaData.compareResultSet(this.internalResultSet);
	}

	@Test public void testColumns() throws Exception {
		checkResult("testColumns", "select* from SYS.Columns order by Name"); //$NON-NLS-1$ //$NON-NLS-2$
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
	
	
	@Test public void test_UID_OID_are_Equal()  throws Exception {
		execute("select distinct(UID) FROM SYS.Schemas"); //$NON-NLS-1$
		int uidCount = getRowCount();
		execute("select distinct(OID) FROM SYS.Schemas"); //$NON-NLS-1$
		int oidCount = getRowCount();
		assertEquals(uidCount, oidCount);
		
		execute("select distinct(UID) FROM SYS.DataTypes"); //$NON-NLS-1$
		uidCount = getRowCount();
		execute("select distinct(OID) FROM SYS.DataTypes"); //$NON-NLS-1$
		oidCount = getRowCount();
		assertEquals(uidCount, oidCount);		
	}
	
	@Test public void testLogMsg() throws Exception {
		execute("call logMsg(level=>'DEBUG', context=>'org.teiid.foo', msg=>'hello world')"); //$NON-NLS-1$
	}
	
	@Test(expected=SQLException.class) public void testLogMsg1() throws Exception {
		execute("call logMsg(level=>'foo', context=>'org.teiid.foo', msg=>'hello world')"); //$NON-NLS-1$
	}
	
	@Test public void testAsynch() throws Exception {
		Statement stmt = this.internalConnection.createStatement();
		TeiidStatement ts = stmt.unwrap(TeiidStatement.class);
		final ResultsFuture<Integer> result = new ResultsFuture<Integer>(); 
		ts.submitExecute("select * from SYS.Schemas", new StatementCallback() {
			int rowCount;
			@Override
			public void onRow(Statement s, ResultSet rs) {
				rowCount++;
			}
			
			@Override
			public void onException(Statement s, Exception e) {
				result.getResultsReceiver().exceptionOccurred(e);
			}
			
			@Override
			public void onComplete(Statement s) {
				result.getResultsReceiver().receiveResults(rowCount);
			}
		}, new RequestOptions());
		assertEquals(4, result.get().intValue());
	}
	
	@Test public void testAsynchContinuous() throws Exception {
		Statement stmt = this.internalConnection.createStatement();
		TeiidStatement ts = stmt.unwrap(TeiidStatement.class);
		final ResultsFuture<Integer> result = new ResultsFuture<Integer>(); 
		ts.submitExecute("select * from SYS.Schemas", new StatementCallback() {
			int rowCount;
			@Override
			public void onRow(Statement s, ResultSet rs) throws SQLException {
				rowCount++;
				if (rowCount == 1024) {
					s.close();
				}
			}
			
			@Override
			public void onException(Statement s, Exception e) {
				result.getResultsReceiver().exceptionOccurred(e);
			}
			
			@Override
			public void onComplete(Statement s) {
				result.getResultsReceiver().receiveResults(rowCount);
			}
		}, new RequestOptions().continuous(true));
		assertEquals(1024, result.get().intValue());
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
	
}
