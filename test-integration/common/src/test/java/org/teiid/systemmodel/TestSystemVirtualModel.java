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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Before;
import org.junit.Test;
import org.teiid.client.util.ResultsFuture;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.AbstractMMQueryTestCase;
import org.teiid.jdbc.FakeServer;
import org.teiid.jdbc.StatementCallback;
import org.teiid.jdbc.TeiidStatement;
import org.teiid.jdbc.TestMMDatabaseMetaData;


/**
 * Exercises each virtual table in the system model.
 */
@SuppressWarnings("nls")
public class TestSystemVirtualModel extends AbstractMMQueryTestCase {
	private static final String VDB = "PartsSupplier"; //$NON-NLS-1$

	public TestSystemVirtualModel() {
		// this is needed because the result files are generated
		// with another tool which uses tab as delimiter
		super.DELIMITER = "\t"; //$NON-NLS-1$
	}
	
    @Before public void setUp() throws Exception {
    	FakeServer server = new FakeServer();
    	server.deployVDB(VDB, UnitTestUtil.getTestDataPath() + "/PartsSupplier.vdb");
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
		String[] expected = { "Name[string]	Value[string]	UID[string]	OID[integer]	ClobValue[clob]",
				"pg_type:oid	30	mmuid:ffa4ac73-b549-470e-931f-dc36330cb8c4	1	30",
				"pg_type:oid	1009	mmuid:d9f36bdc-7b25-4af0-b9f5-a96aac6d3094	2	1009",
				"pg_type:oid	1002	mmuid:bcbed548-176c-4116-a5d6-7638cb0206e1	3	1002",
				"pg_type:oid	1028	mmuid:a385751f-a31a-4d5d-9197-3fbd390b0251	4	1028"
		
		}; //$NON-NLS-1$
		executeAndAssertResults("select* from SYS.Properties", expected); //$NON-NLS-1$
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
		String[] expected = { 
				"KeyName[string]	RefKeyUID[string]	",  //$NON-NLS-1$
				"PK_PARTS	null", //$NON-NLS-1$
				"PK_SHIP_VIA	null", //$NON-NLS-1$
				"PK_STATUS	null",  //$NON-NLS-1$
				"PK_SUPPLIER	null",  //$NON-NLS-1$
				"PK_SUPPLIER_PARTS	null", //$NON-NLS-1$
				"PK_SUPPLIER_PARTS	null",  //$NON-NLS-1$
				"idx_matpg_relatt_ids	null",
				"idx_matpg_relatt_ids	null",
				"matpg_datatype_ids	null",
				"matpg_datatype_ids	null",
				"matpg_datatype_names	null",
				"matpg_datatype_names	null",				
				"pk_matpg_relatt_names	null",
				"pk_matpg_relatt_names	null",
				"pk_matpg_relatt_names	null",
				"pk_pg_attr	null",  //$NON-NLS-1$
				"pk_pg_class	null",  //$NON-NLS-1$
				"pk_pg_index	null",  //$NON-NLS-1$
				"pk_pg_proc	null",  //$NON-NLS-1$
				

		};
		executeAndAssertResults("select KeyName, RefKeyUID FROM SYS.KeyColumns WHERE RefKeyUID IS NULL order by KeyName",expected); //$NON-NLS-1$
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
		Statement s = this.internalConnection.createStatement();
		TeiidStatement ts = s.unwrap(TeiidStatement.class);
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
		});
		assertEquals(4, result.get().intValue());
	}
}
