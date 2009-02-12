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

package com.metamatrix.systemmodel;

import java.sql.Connection;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.jdbc.api.AbstractMMQueryTestCase;

/**
 * Tests xml virtual documents which are built on top of virtual
 * procedures (see defect 12309 and all related).
 */
public class TestVirtualDocWithVirtualProc extends AbstractMMQueryTestCase {

	private static final String DQP_PROP_FILE = UnitTestUtil.getTestDataPath() + "/xml-vp/xmlvp.properties"; //$NON-NLS-1$
    private static final String VDB = "xmlvp"; //$NON-NLS-1$

    static Connection connection;
    
    public TestVirtualDocWithVirtualProc() {
    	// this is needed because the result files are generated 
    	// with another tool which uses tab as delimiter 
    	super.DELIMITER = "\t"; //$NON-NLS-1$
    }
    
    public static Test suite() {
		TestSuite suite = new TestSuite();
		suite.addTestSuite(TestVirtualDocWithVirtualProc.class);
		return createOnceRunSuite(suite, new ConnectionFactory() {

			public com.metamatrix.jdbc.api.Connection createSingleConnection()
					throws Exception {
				return createConnection(VDB, DQP_PROP_FILE, ""); //$NON-NLS-1$
			}});
	}    
        
    public void testDefect15241() {

    	String sql = "SELECT ModelName, Name, Description FROM System.Groups WHERE Name = 'yyyTestDocument'"; //$NON-NLS-1$

    	String[] expected ={
			"ModelName[string]	Name[string]	Description[string]",	 //$NON-NLS-1$
			"test13326Doc	yyyTestDocument	null", //$NON-NLS-1$
			"testDoc	yyyTestDocument	This is a test description of virtual doc yyyTestDocument" //$NON-NLS-1$
    	};
    	executeAndAssertResults(sql, expected);
    }

    public void testDefect15241a() {
    	String sql = "SELECT GroupName, Name, Description FROM System.Elements WHERE Name = 'IntKey'"; //$NON-NLS-1$
    	String[] expected ={
		    "GroupName[string]	Name[string]	Description[string]",	 //$NON-NLS-1$
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

    public void testDefect15241b() {
    	
    	String sql = "SELECT GroupName, Name, Value, UID FROM System.GroupProperties WHERE ModelName = 'testDoc'"; //$NON-NLS-1$
    	String[] expected ={
	    "GroupName[string]	Name[string]	Value[string]	UID[string]",	 //$NON-NLS-1$
	    "yyyTestDocument	NugentXAttribute	Nuuuuuge22222	mmuuid:4789b280-841c-1f15-9526-ebd0cace03e1", //$NON-NLS-1$
	    "yyyTestDocument	NugentYAttribute	Nuuuuuge44444	mmuuid:4789b280-841c-1f15-9526-ebd0cace03e1" //$NON-NLS-1$
    	};
    	executeAndAssertResults(sql, expected);
    }

}
