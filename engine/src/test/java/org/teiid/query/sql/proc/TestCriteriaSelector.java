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

import java.util.*;

import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.sql.proc.CriteriaSelector;
import org.teiid.query.sql.symbol.*;

import junit.framework.*;

/**
 *
 * @author gchadalavadaDec 9, 2002
 */
public class TestCriteriaSelector  extends TestCase {

	// ################################## FRAMEWORK ################################
	
	public TestCriteriaSelector(String name) { 
		super(name);
	}	
	
	// ################################## TEST HELPERS ################################	

	public static final CriteriaSelector sample1() {
		ElementSymbol sy1 = new ElementSymbol("a"); //$NON-NLS-1$
		ElementSymbol sy2 = new ElementSymbol("b"); //$NON-NLS-1$
		ElementSymbol sy3 = new ElementSymbol("c"); //$NON-NLS-1$
		List elmnts = new ArrayList(3);
		elmnts.add(sy1);
		elmnts.add(sy2);
		elmnts.add(sy3);				
		CriteriaSelector cs = new CriteriaSelector(CriteriaSelector.COMPARE_EQ, elmnts);
	    return cs;
	}

	public static final CriteriaSelector sample2() { 
		ElementSymbol sy1 = new ElementSymbol("x"); //$NON-NLS-1$
		ElementSymbol sy2 = new ElementSymbol("y"); //$NON-NLS-1$
		ElementSymbol sy3 = new ElementSymbol("z"); //$NON-NLS-1$
		List elmnts = new ArrayList(3);
		elmnts.add(sy1);
		elmnts.add(sy2);
		elmnts.add(sy3);				
		CriteriaSelector cs = new CriteriaSelector(CriteriaSelector.LIKE, elmnts);
	    return cs;
	}
	
	// ################################## ACTUAL TESTS ################################
	
	public void testGetElements() {
		CriteriaSelector cs1 = sample1();
		Collection elmts = cs1.getElements();
        assertTrue("Incorrect number of elements in the selector", (elmts.size() == 3)); //$NON-NLS-1$
	}
	
	public void testGetType() {
		CriteriaSelector cs1 = sample1();
        assertTrue("Incorrect type in the selector", (cs1.getSelectorType() == CriteriaSelector.COMPARE_EQ)); //$NON-NLS-1$
	}
	
	public void testaddElement1() {
		CriteriaSelector cs1 = (CriteriaSelector) sample1().clone();
		cs1.addElement(new ElementSymbol("d")); //$NON-NLS-1$
        assertTrue("Incorrect number of statements in the Block", (cs1.getElements().size() == 4)); //$NON-NLS-1$
	}
	
	public void testSelfEquivalence(){
		CriteriaSelector s1 = sample1();
		int equals = 0;
		UnitTestUtil.helpTestEquivalence(equals, s1, s1);
	}

	public void testEquivalence(){
		CriteriaSelector s1 = sample1();
		CriteriaSelector s1a = sample1();
		int equals = 0;
		UnitTestUtil.helpTestEquivalence(equals, s1, s1a);
	}
	
	public void testNonEquivalence(){
		CriteriaSelector s1 = sample1();
		CriteriaSelector s2 = sample2();
		int equals = -1;
		UnitTestUtil.helpTestEquivalence(equals, s1, s2);
	}
}
