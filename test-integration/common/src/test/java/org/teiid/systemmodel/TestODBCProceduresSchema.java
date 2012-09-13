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

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.AbstractMMQueryTestCase;
import org.teiid.jdbc.FakeServer;
import org.teiid.jdbc.TestMMDatabaseMetaData;

@SuppressWarnings("nls")
public class TestODBCProceduresSchema extends AbstractMMQueryTestCase {
	private static final String VDB = "bqt"; //$NON-NLS-1$
	private static FakeServer server;

	public TestODBCProceduresSchema() {
		// this is needed because the result files are generated
		// with another tool which uses tab as delimiter
		super.DELIMITER = "\t"; //$NON-NLS-1$
	}
	
	@BeforeClass public static void oneTimeSetup() throws Exception {
		server = new FakeServer(true);
    	server.deployVDB(VDB, UnitTestUtil.getTestDataPath() + "/bqt.vdb");
	}
	
	@AfterClass public static void oneTimeTeardown() throws Exception {
		server.stop();
	}
	
    @Before public void setUp() throws Exception {
    	this.internalConnection = server.createConnection("jdbc:teiid:" + VDB); //$NON-NLS-1$ //$NON-NLS-2$	
   	}
   
	@Test public void test_Pg_Proc_alltypes() throws Exception {
		execute("select oid, proname, proretset,prorettype, pronargs, proargtypes, proargnames, proargmodes, proallargtypes, pronamespace FROM pg_proc where proname='bigProcedure'"); //$NON-NLS-1$
		TestMMDatabaseMetaData.compareResultSet(this.internalResultSet);
	}
	
	@Test public void test_Pg_Proc_void() throws Exception {
		execute("select oid, proname, proretset,prorettype, pronargs, proargtypes, proargnames, proargmodes, proallargtypes, pronamespace FROM pg_proc where proname='VoidProcedure'"); //$NON-NLS-1$
		TestMMDatabaseMetaData.compareResultSet(this.internalResultSet);
	}
	
	@Test public void test_Pg_Proc_with_return() throws Exception {
		execute("select oid, proname, proretset,prorettype, pronargs, proargtypes, proargnames, proargmodes, proallargtypes, pronamespace FROM pg_proc where proname='ProcedureWithReturn'"); //$NON-NLS-1$
		TestMMDatabaseMetaData.compareResultSet(this.internalResultSet);
	}
	
	@Test public void test_Pg_Proc_with_return_table() throws Exception {
		execute("select oid, proname, proretset,prorettype, pronargs, proargtypes, proargnames, proargmodes, proallargtypes, pronamespace FROM pg_proc where proname='ProcedureReturnTable'"); //$NON-NLS-1$
		TestMMDatabaseMetaData.compareResultSet(this.internalResultSet);
	}	
}
