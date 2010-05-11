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
import org.teiid.query.sql.proc.RaiseErrorStatement;
import org.teiid.query.sql.symbol.Constant;

import junit.framework.*;

/**
 *
 * @author gchadalavadaDec 11, 2002
 */
public class TestRaiseErrorStatement  extends TestCase {

	/**
	 * Constructor for TestAssignmentStatement.
	 */
	public TestRaiseErrorStatement(String name) { 
		super(name);
	}
	
	// ################################## TEST HELPERS ################################	

	public static final RaiseErrorStatement sample1() { 
		return new RaiseErrorStatement(new Constant("a")); //$NON-NLS-1$
	}
	
	public static final RaiseErrorStatement sample2() {
		return new RaiseErrorStatement(new Constant("b")); //$NON-NLS-1$
	}
	
	// ################################## ACTUAL TESTS ################################	
	
	public void testSelfEquivalence(){
		RaiseErrorStatement s1 = sample1();
		int equals = 0;
		UnitTestUtil.helpTestEquivalence(equals, s1, s1);
	}

	public void testEquivalence(){
		RaiseErrorStatement s1 = sample1();
		RaiseErrorStatement s1a = sample1();
		int equals = 0;
		UnitTestUtil.helpTestEquivalence(equals, s1, s1a);
	}
	
	public void testNonEquivalence(){
		RaiseErrorStatement s1 = sample1();
		RaiseErrorStatement s2 = sample2();
		int equals = -1;
		UnitTestUtil.helpTestEquivalence(equals, s1, s2);
	}
}
