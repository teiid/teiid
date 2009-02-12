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

package com.metamatrix.query.parser;

import com.metamatrix.query.sql.lang.*;
import junit.framework.*;

public class TestParseCommandType extends TestCase {

	// ################################## FRAMEWORK ################################
	
	public TestParseCommandType(String name) { 
		super(name);
	}	
	
	// ################################## TEST HELPERS ################################
	
	private void helpTest(String sql, int expectedType) {
	    int actualType = QueryParser.getCommandType(sql);		    
	    assertEquals("Did not get expected type", expectedType, actualType); //$NON-NLS-1$
	}
	
	// ################################## ACTUAL TESTS ################################
	
	public void testQuery1() {
		helpTest("SELECT a FROM g", Command.TYPE_QUERY);		 //$NON-NLS-1$
	}

	public void testQuery2() {
		helpTest("select a FROM g", Command.TYPE_QUERY);		 //$NON-NLS-1$
	}

	public void testQuery3() {
		helpTest("select", Command.TYPE_QUERY);		 //$NON-NLS-1$
	}

	public void testSetQuery1() {
		helpTest("SELECT a FROM g UNION SELECT b from z", Command.TYPE_QUERY);		 //$NON-NLS-1$
	}

	public void testSetQuery2() {
		helpTest("(Select a FROM g UNION SELECT b from z) union select c from r order by s", Command.TYPE_QUERY);		 //$NON-NLS-1$
	}
	
	public void testInsert() {
		helpTest("insert g (b) values (1)", Command.TYPE_INSERT);     //$NON-NLS-1$
	}

	public void testUpdate() {
		helpTest("update g set b=1", Command.TYPE_UPDATE);     //$NON-NLS-1$
	}

	public void testDelete() {
		helpTest("delete from g where x=1", Command.TYPE_DELETE);     //$NON-NLS-1$
	}
	
	public void testUnknown1() {
		helpTest("", Command.TYPE_UNKNOWN);     //$NON-NLS-1$
	}

	public void testUnknown2() {
		helpTest("abc select", Command.TYPE_UNKNOWN);     //$NON-NLS-1$
	}

	public void testUnknown3() {
		helpTest("selec", Command.TYPE_UNKNOWN);		 //$NON-NLS-1$
	}

	public void testUnknown4() {
		helpTest("selec ", Command.TYPE_UNKNOWN);		 //$NON-NLS-1$
	}
}
