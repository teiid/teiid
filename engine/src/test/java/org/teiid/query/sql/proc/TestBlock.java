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

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;

@SuppressWarnings("nls")
public class TestBlock {

	// ################################## TEST HELPERS ################################	

	public static final Block sample1() { 
		Block block = new Block();
		block.addStatement(TestAssignmentStatement.sample1());
		block.addStatement(TestCommandStatement.sample1());
		block.addStatement(TestRaiseErrorStatement.sample1());
		block.addStatement(TestAssignmentStatement.sample1());
	    return block;
	}

	public static final Block sample2() { 
		Block block = new Block();
		block.addStatement(TestAssignmentStatement.sample2());
		block.addStatement(TestCommandStatement.sample2());
		block.addStatement(TestRaiseErrorStatement.sample2());
		block.addStatement(TestAssignmentStatement.sample2());
	    return block;
	}
			
	// ################################## ACTUAL TESTS ################################
	
	@Test public void testGetStatements1() {
		Block b1 = sample1();
		List<Statement> stmts = b1.getStatements();
        assertTrue("Incorrect number of statements in the Block", (stmts.size() == 4)); //$NON-NLS-1$
	}
	
	@Test public void testGetStatements2() {
		Block b1 = sample1();
		Statement stmt = b1.getStatements().get(1);
        assertTrue("Incorrect statement in the Block", stmt.equals(TestCommandStatement.sample1())); //$NON-NLS-1$
	}
	
	@Test public void testaddStatement1() {
		Block b1 = sample1().clone();
		b1.addStatement(TestCommandStatement.sample2());
        assertTrue("Incorrect number of statements in the Block", (b1.getStatements().size() == 5)); //$NON-NLS-1$
	}
	
	@Test public void testaddStatement2() {
		Block b1 = sample2().clone();
		b1.addStatement(TestCommandStatement.sample2());
		Statement stmt = b1.getStatements().get(4);
        assertTrue("Incorrect statement in the Block", stmt.equals(TestCommandStatement.sample2())); //$NON-NLS-1$
	}

	@Test public void testSelfEquivalence(){
		Block b1 = sample1();
		int equals = 0;
		UnitTestUtil.helpTestEquivalence(equals, b1, b1);
	}

	@Test public void testEquivalence(){
		Block b1 = sample1();
		Block b1a = sample1();
		int equals = 0;
		UnitTestUtil.helpTestEquivalence(equals, b1, b1a);
	}
	
	@Test public void testNonEquivalence(){
		Block b1 = sample1();
		Block b2 = sample2();
		int equals = -1;
		UnitTestUtil.helpTestEquivalence(equals, b1, b2);
	}
	
	@Test public void testClone() {
		Block b1 = sample1();
		Block b2 = b1.clone();
		UnitTestUtil.helpTestEquivalence(0, b1, b2);
		assertNotSame(b1.getStatements().get(0), b2.getStatements().get(0));
	}
	
	@Test public void testExceptionGroup() {
		Block b1 = sample1();
		Block b2 = b1.clone();
		b1.setExceptionGroup("x");
		b2.setExceptionGroup("y");
		assertFalse(b1.equals(b2));
	}

}
