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

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.AbstractMMQueryTestCase;
import org.teiid.jdbc.FakeServer;
import org.teiid.jdbc.TestMMDatabaseMetaData;


/**
 * Tests xml virtual documents which are built on top of virtual
 * procedures (see defect 12309 and all related).
 */
@SuppressWarnings("nls")
public class TestVirtualDocWithVirtualProc extends AbstractMMQueryTestCase {

    private static final String VDB = "xmlvp"; //$NON-NLS-1$
	private static FakeServer server;

    public TestVirtualDocWithVirtualProc() {
    	// this is needed because the result files are generated 
    	// with another tool which uses tab as delimiter 
    	super.DELIMITER = "\t"; //$NON-NLS-1$
    }
    
    @BeforeClass public static void oneTimeSetup() throws Exception {
    	server = new FakeServer(true);
    	server.setThrowMetadataErrors(false); //this vdb has invalid update procedures
    	server.deployVDB(VDB, UnitTestUtil.getTestDataPath() + "/xml-vp/xmlvp_1.vdb");
    }
    
    @AfterClass public static void oneTimeTeardown() throws Exception {
    	server.stop();
    }
    
    @Before public void setUp() throws Exception {
    	this.internalConnection = server.createConnection("jdbc:teiid:" + VDB); //$NON-NLS-1$ //$NON-NLS-2$	    	
    }
    
    @Override
	@After public void tearDown() {
    	closeConnection();
    }
    
    @Test public void testDefect15241() throws Exception {

    	String sql = "SELECT SchemaName, Name, Description FROM SYS.Tables WHERE Name = 'yyyTestDocument'"; //$NON-NLS-1$

    	String[] expected ={
			"SchemaName[string]	Name[string]	Description[string]",	 //$NON-NLS-1$
			"test13326Doc	yyyTestDocument	null", //$NON-NLS-1$
			"testDoc	yyyTestDocument	This is a test description of virtual doc yyyTestDocument" //$NON-NLS-1$
    	};
    	executeAndAssertResults(sql, expected);
    }

    @Test public void testDefect15241a() throws Exception {
    	String sql = "SELECT TableName, Name, Description FROM SYS.Columns WHERE Name = 'IntKey'"; //$NON-NLS-1$
    	String[] expected ={
		    "TableName[string]	Name[string]	Description[string]",	 //$NON-NLS-1$
		    "HugeA	IntKey	null", //$NON-NLS-1$
		    "HugeB	IntKey	null", //$NON-NLS-1$
		    "LargeA	IntKey	null", //$NON-NLS-1$
		    "LargeB	IntKey	null", //$NON-NLS-1$
		    "MediumA	IntKey	null", //$NON-NLS-1$
		    "MediumB	IntKey	null", //$NON-NLS-1$
		    "SmallA	IntKey	This is a test description of SmallA.IntKey element", //$NON-NLS-1$
		    "SmallB	IntKey	null" //$NON-NLS-1$
    	};
    	executeAndAssertResults(sql, expected);
    }

    @Test public void testDefect15241b() throws Exception {
    	String sql = "SELECT p.Name, p.Value, UID FROM SYS.Properties p order by p.Name"; //$NON-NLS-1$
    	execute(sql);
    	TestMMDatabaseMetaData.compareResultSet(this.internalResultSet);
    }
    
    @Test public void testPropertyJoin() throws Exception {
    	String sql = "select * from sys.tables c left outer JOIN sys.Properties p ON p.UID=c.UID where c.name = 'yyyTestDocument' and schemaName='testDoc'"; //$NON-NLS-1$
    	execute(sql);
    	assertRowCount(2);
    }

}
