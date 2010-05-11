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
import org.teiid.query.sql.proc.HasCriteria;

import junit.framework.*;

/**
 *
 * @author gchadalavadaDec 9, 2002
 */
public class TestHasCriteria  extends TestCase {

	// ################################## FRAMEWORK ################################
	
	public TestHasCriteria(String name) { 
		super(name);
	}	
	
	// ################################## TEST HELPERS ################################	

	public static final HasCriteria sample1() { 
		HasCriteria hc = new HasCriteria(TestCriteriaSelector.sample1());
	    return hc;
	}

	public static final HasCriteria sample2() { 
		HasCriteria hc = new HasCriteria(TestCriteriaSelector.sample2());
	    return hc;
	}
	
	// ################################## ACTUAL TESTS ################################	

	public void testGetCriteriaSelector() {
		HasCriteria hc1 = sample1();
		assertEquals("Incorrect criteria selector obtained", hc1.getSelector(), TestCriteriaSelector.sample1()); //$NON-NLS-1$
	}
	
	public void testSetCriteriaSelector() {
		HasCriteria hc1 = (HasCriteria) sample1().clone();
		hc1.setSelector(TestCriteriaSelector.sample2());
		assertEquals("Incorrect criteria selector obtained", hc1.getSelector(), TestCriteriaSelector.sample2()); //$NON-NLS-1$
	}	
	
	public void testSelfEquivalence(){
		HasCriteria s1 = sample1();
		int equals = 0;
		UnitTestUtil.helpTestEquivalence(equals, s1, s1);
	}

	public void testEquivalence(){
		HasCriteria s1 = sample1();
		HasCriteria s1a = sample1();
		int equals = 0;
		UnitTestUtil.helpTestEquivalence(equals, s1, s1a);
	}
	
	public void testNonEquivalence(){
		HasCriteria s1 = sample1();
		HasCriteria s2 = sample2();
		int equals = -1;
		UnitTestUtil.helpTestEquivalence(equals, s1, s2);
	}

}
