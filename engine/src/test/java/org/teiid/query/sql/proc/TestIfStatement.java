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
import org.teiid.query.sql.lang.*;
import org.teiid.query.sql.proc.Block;
import org.teiid.query.sql.proc.IfStatement;

import junit.framework.*;

/**
 *
 * @author gchadalavadaDec 9, 2002
 */
public class TestIfStatement  extends TestCase {

	// ################################## FRAMEWORK ################################
	
	public TestIfStatement(String name) { 
		super(name);
	}	
	
	// ################################## TEST HELPERS ################################	

	public static final IfStatement sample1() {
		Block ifBlock = TestBlock.sample1();
		Block elseBlock = TestBlock.sample2();
		Criteria criteria = TestHasCriteria.sample1();
		return new IfStatement(criteria, ifBlock, elseBlock);
	}

	public static final IfStatement sample2() { 
		Block ifBlock = TestBlock.sample2();
		Block elseBlock = TestBlock.sample1();
		Criteria criteria = TestHasCriteria.sample2();
		return new IfStatement(criteria, ifBlock, elseBlock);
	}
	
	// ################################## ACTUAL TESTS ################################	


	public void testGetIfBlock() {
		IfStatement b1 = sample1();
        assertTrue("Incorrect IfBlock on statement", b1.getIfBlock().equals(TestBlock.sample1())); //$NON-NLS-1$
	}
	
	public void testGetElseBlock() {
		IfStatement b1 = sample1();
        assertTrue("Incorrect IfBlock on statement", b1.getElseBlock().equals(TestBlock.sample2())); //$NON-NLS-1$
	}
	
	public void testGetCondition() {
		IfStatement b1 = sample1();
        assertTrue("Incorrect IfBlock on statement", b1.getCondition().equals(TestHasCriteria.sample1())); //$NON-NLS-1$
	}
	
	public void testSelfEquivalence(){
		IfStatement s1 = sample1();
		int equals = 0;
		UnitTestUtil.helpTestEquivalence(equals, s1, s1);
	}

	public void testEquivalence(){
		IfStatement s1 = sample1();
		IfStatement s1a = sample1();
		int equals = 0;
		UnitTestUtil.helpTestEquivalence(equals, s1, s1a);
	}
	
	public void testNonEquivalence(){
		IfStatement s1 = sample1();
		IfStatement s2 = sample2();
		int equals = -1;
		UnitTestUtil.helpTestEquivalence(equals, s1, s2);
	}

}
