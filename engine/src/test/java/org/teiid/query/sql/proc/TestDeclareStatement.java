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

package org.teiid.query.sql.proc;

import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.sql.proc.DeclareStatement;
import org.teiid.query.sql.symbol.ElementSymbol;

import junit.framework.TestCase;


/**
 *
 * @author gchadalavadaDec 11, 2002
 */
public class TestDeclareStatement  extends TestCase {

	/**
	 * Constructor for TestAssignmentStatement.
	 */
	public TestDeclareStatement(String name) { 
		super(name);
	}
	
	// ################################## TEST HELPERS ################################	

	public static final DeclareStatement sample1() { 
		return new DeclareStatement(new ElementSymbol("a"), "String"); //$NON-NLS-1$ //$NON-NLS-2$
	}
	
	public static final DeclareStatement sample2() {
		return new DeclareStatement(new ElementSymbol("b"), "Integer"); //$NON-NLS-1$ //$NON-NLS-2$
	}
	
	// ################################## ACTUAL TESTS ################################	
	
	public void testGetVariable() {
		DeclareStatement s1 = sample1();
		assertEquals("Incorrect variable ", s1.getVariable(), new ElementSymbol("a"));		 //$NON-NLS-1$ //$NON-NLS-2$
	}
	
	public void testGetVariableType() {
		DeclareStatement s1 = sample1();
		assertEquals("Incorrect variable type ", s1.getVariableType(), "String"); //$NON-NLS-1$ //$NON-NLS-2$
	}
	
	public void testSelfEquivalence(){
		DeclareStatement s1 = sample1();
		int equals = 0;
		UnitTestUtil.helpTestEquivalence(equals, s1, s1);
	}

	public void testEquivalence(){
		DeclareStatement s1 = sample1();
		DeclareStatement s1a = sample1();
		int equals = 0;
		UnitTestUtil.helpTestEquivalence(equals, s1, s1a);
	}
	
	public void testNonEquivalence(){
		DeclareStatement s1 = sample1();
		DeclareStatement s2 = sample2();
		int equals = -1;
		UnitTestUtil.helpTestEquivalence(equals, s1, s2);
	}
    
    public void testClone() {
        DeclareStatement s1 = sample1();
        DeclareStatement s2 = (DeclareStatement)s1.clone();
        
        assertTrue(s1 != s2);
        assertEquals(s1, s2);
    }
}
