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


import org.junit.Before;
import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.AbstractMMQueryTestCase;
import org.teiid.jdbc.FakeServer;

@SuppressWarnings("nls")
public class TestODBCProceduresSchema extends AbstractMMQueryTestCase {
	private static final String VDB = "bqt"; //$NON-NLS-1$

	public TestODBCProceduresSchema() {
		// this is needed because the result files are generated
		// with another tool which uses tab as delimiter
		super.DELIMITER = "\t"; //$NON-NLS-1$
	}
	
    @Before public void setUp() throws Exception {
    	FakeServer server = new FakeServer();
    	server.deployVDB(VDB, UnitTestUtil.getTestDataPath() + "/bqt.vdb");
    	this.internalConnection = server.createConnection("jdbc:teiid:" + VDB); //$NON-NLS-1$ //$NON-NLS-2$	
   	}
   
	@Test public void test_Pg_Proc_alltypes() throws Exception {
		execute("select oid, proname, proretset,prorettype, pronargs, proargtypes, proargnames, proargmodes, proallargtypes, pronamespace FROM pg_proc where proname='bigProcedure'"); //$NON-NLS-1$
		if (this.internalResultSet.next()) {
			assertEquals(1, this.internalResultSet.getInt(1)); //oid
			assertEquals("bigProcedure", this.internalResultSet.getString(2)); //proname
			assertEquals(true, this.internalResultSet.getBoolean(3)); //proretset
			assertEquals(2249, this.internalResultSet.getInt(4)); //prorettype
			assertEquals(14, this.internalResultSet.getInt(5)); //pronargs
			assertArrayEquals(new Object[] {1700,1043,700,20,701,21,1082,1083,1114,16,1043,21,1700,1700}, (Object[])this.internalResultSet.getObject(6)); //proargtypes
			assertArrayEquals(new Object[] {"intNum","stringNum","floatNum","longNum","doubleNum","byteNum","dateValue","timeValue","timestampValue","booValue","charValue","shortNum","bigIntNum","bigdecimalNum","col","col2"}, (Object[])this.internalResultSet.getObject(7)); //proargnames
			assertArrayEquals(new Object[] {"i","i","i","i","i","i","i","i","i","i","i","i","i","i","t","t"}, (Object[])this.internalResultSet.getObject(8)); //proargmodes
			assertArrayEquals(new Object[] {1700,1043,700,20,701,21,1082,1083,1114,16,1043,21,1700,1700,1043,1700}, (Object[])this.internalResultSet.getObject(9)); //proallargtypes
			assertEquals(1, this.internalResultSet.getInt(10)); //pronamespace
		}
		else {
			fail("no results");
		}
	}
	
	@Test public void test_Pg_Proc_void() throws Exception {
		execute("select oid, proname, proretset,prorettype, pronargs, proargtypes, proargnames, proargmodes, proallargtypes, pronamespace FROM pg_proc where proname='VoidProcedure'"); //$NON-NLS-1$
		if (this.internalResultSet.next()) {
			assertEquals(4, this.internalResultSet.getInt(1)); //oid
			assertEquals("VoidProcedure", this.internalResultSet.getString(2)); //proname
			assertEquals(false, this.internalResultSet.getBoolean(3)); //proretset
			assertEquals(2278, this.internalResultSet.getInt(4)); //prorettype
			assertEquals(2, this.internalResultSet.getInt(5)); //pronargs
			assertArrayEquals(new Object[] {1700,1043}, (Object[])this.internalResultSet.getObject(6)); //proargtypes
			assertArrayEquals(new Object[] {"intNum","stringNum"}, (Object[])this.internalResultSet.getObject(7)); //proargnames
			assertArrayEquals(null, (Object[])this.internalResultSet.getObject(8)); //proargmodes
			assertArrayEquals(new Object[] {1700,1043}, (Object[])this.internalResultSet.getObject(9)); //proallargtypes
			assertEquals(1, this.internalResultSet.getInt(10)); //pronamespace
		}
		else {
			fail("no results");
		}
	}
	
	@Test public void test_Pg_Proc_with_return() throws Exception {
		execute("select oid, proname, proretset,prorettype, pronargs, proargtypes, proargnames, proargmodes, proallargtypes, pronamespace FROM pg_proc where proname='ProcedureWithReturn'"); //$NON-NLS-1$
		if (this.internalResultSet.next()) {
			assertEquals(3, this.internalResultSet.getInt(1)); //oid
			assertEquals("ProcedureWithReturn", this.internalResultSet.getString(2)); //proname
			assertEquals(false, this.internalResultSet.getBoolean(3)); //proretset
			assertEquals(20, this.internalResultSet.getInt(4)); //prorettype
			assertEquals(3, this.internalResultSet.getInt(5)); //pronargs
			assertArrayEquals(new Object[] {1700,1043,700}, (Object[])this.internalResultSet.getObject(6)); //proargtypes
			assertArrayEquals(new Object[] {"intNum","stringNum","floatNum"}, (Object[])this.internalResultSet.getObject(7)); //proargnames
			assertArrayEquals(null, (Object[])this.internalResultSet.getObject(8)); //proargmodes
			assertArrayEquals(new Object[] {1700,1043,700}, (Object[])this.internalResultSet.getObject(9)); //proallargtypes
			assertEquals(1, this.internalResultSet.getInt(10)); //pronamespace
		}
		else {
			fail("no results");
		}
	}
	@Test public void test_Pg_Proc_with_return_table() throws Exception {
		execute("select oid, proname, proretset,prorettype, pronargs, proargtypes, proargnames, proargmodes, proallargtypes, pronamespace FROM pg_proc where proname='ProcedureReturnTable'"); //$NON-NLS-1$
		if (this.internalResultSet.next()) {
			assertEquals(2, this.internalResultSet.getInt(1)); //oid
			assertEquals("ProcedureReturnTable", this.internalResultSet.getString(2)); //proname
			assertEquals(true, this.internalResultSet.getBoolean(3)); //proretset
			assertEquals(2249, this.internalResultSet.getInt(4)); //prorettype
			assertEquals(2, this.internalResultSet.getInt(5)); //pronargs
			assertArrayEquals(new Object[] {1700,1700}, (Object[])this.internalResultSet.getObject(6)); //proargtypes
			assertArrayEquals(new Object[] {"intNum","bigDecimalNum","col1","col2"}, (Object[])this.internalResultSet.getObject(7)); //proargnames
			assertArrayEquals(new Object[] {"i","i","t","t"}, (Object[])this.internalResultSet.getObject(8)); //proargmodes
			assertArrayEquals(new Object[] {1700,1700,1043,1114}, (Object[])this.internalResultSet.getObject(9)); //proallargtypes
			assertEquals(1, this.internalResultSet.getInt(10)); //pronamespace
		}
		else {
			fail("no results");
		}
	}	
}
