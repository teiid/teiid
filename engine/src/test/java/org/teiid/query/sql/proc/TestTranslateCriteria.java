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
import org.teiid.query.sql.proc.TranslateCriteria;

import junit.framework.*;

/**
 *
 * @author gchadalavadaDec 9, 2002
 */
public class TestTranslateCriteria  extends TestCase {

	// ################################## FRAMEWORK ################################
	
	public TestTranslateCriteria(String name) { 
		super(name);
	}	
	
	// ################################## TEST HELPERS ################################	

	public static final TranslateCriteria sample1() { 
		TranslateCriteria tc = new TranslateCriteria(TestCriteriaSelector.sample1());
	    return tc;
	}

	public static final TranslateCriteria sample2() { 
		TranslateCriteria tc = new TranslateCriteria(TestCriteriaSelector.sample2());
	    return tc;
	}
	
	// ################################## ACTUAL TESTS ################################	
	
	public void testSelfEquivalence(){
		TranslateCriteria s1 = sample1();
		int equals = 0;
		UnitTestUtil.helpTestEquivalence(equals, s1, s1);
	}

	public void testEquivalence(){
		TranslateCriteria s1 = sample1();
		TranslateCriteria s1a = sample1();
		int equals = 0;
		UnitTestUtil.helpTestEquivalence(equals, s1, s1a);
	}
	
	public void testNonEquivalence(){
		TranslateCriteria s1 = sample1();
		TranslateCriteria s2 = sample2();
		int equals = -1;
		UnitTestUtil.helpTestEquivalence(equals, s1, s2);
	}
}
