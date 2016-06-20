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

package org.teiid.query.sql.lang;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;

public class TestSetCriteria {

	public static final SetCriteria sample1() { 
	    SetCriteria c1 = new SetCriteria();
	    c1.setExpression(new ElementSymbol("e1")); //$NON-NLS-1$
		List vals = new ArrayList();
		vals.add(new Constant("a")); //$NON-NLS-1$
		vals.add(new Constant("b")); //$NON-NLS-1$
		c1.setValues(vals);
		return c1;
	}

	public static final SetCriteria sample2() { 
	    SetCriteria c1 = new SetCriteria();
	    c1.setExpression(new ElementSymbol("e2")); //$NON-NLS-1$
		List vals = new ArrayList();
		vals.add(new Constant("c")); //$NON-NLS-1$
		vals.add(new Constant("d")); //$NON-NLS-1$
		c1.setValues(vals);
		return c1;
	}
		
	// ################################## ACTUAL TESTS ################################
	
	@Test public void testEquals1() {   
	    SetCriteria c1 = new SetCriteria();
	    c1.setExpression(new ElementSymbol("e1")); //$NON-NLS-1$
		List vals = new ArrayList();
		vals.add(new Constant("a")); //$NON-NLS-1$
		vals.add(new Constant("b")); //$NON-NLS-1$
		c1.setValues(vals);
		
		SetCriteria c2 = (SetCriteria) c1.clone();
		
		assertTrue("Equivalent set criteria don't compare as equal: " + c1 + ", " + c2, c1.equals(c2));				 //$NON-NLS-1$ //$NON-NLS-2$
	}

	@Test public void testEquals2() {   
	    SetCriteria c1 = new SetCriteria();
        c1.setNegated(true);
	    c1.setExpression(new ElementSymbol("e1")); //$NON-NLS-1$
		List vals1 = new ArrayList();
		vals1.add(new Constant("a")); //$NON-NLS-1$
		vals1.add(new Constant("b")); //$NON-NLS-1$
		c1.setValues(vals1);
		
	    SetCriteria c2 = new SetCriteria();
        c2.setNegated(true);
	    c2.setExpression(new ElementSymbol("e1")); //$NON-NLS-1$
		List vals2 = new ArrayList();
		vals2.add(new Constant("b")); //$NON-NLS-1$
		vals2.add(new Constant("a")); //$NON-NLS-1$
		c2.setValues(vals2);
		
		assertTrue("Equivalent set criteria don't compare as equal: " + c1 + ", " + c2, c1.equals(c2));				 //$NON-NLS-1$ //$NON-NLS-2$
	}

	@Test public void testSelfEquivalence(){
		Object s1 = sample1();
		int equals = 0;
		UnitTestUtil.helpTestEquivalence(equals, s1, s1);
	}

	@Test public void testEquivalence(){
		Object s1 = sample1();
		Object s1a = sample1();
		int equals = 0;
		UnitTestUtil.helpTestEquivalence(equals, s1, s1a);
	}
	
    @Test public void testNonEquivalence1(){
        Object s1 = sample1();
        Object s2 = sample2();
        int equals = -1;
        UnitTestUtil.helpTestEquivalence(equals, s1, s2);
    }
    
    @Test public void testNonEquivalence2(){
        SetCriteria c1 = new SetCriteria();
        c1.setExpression(new ElementSymbol("e1")); //$NON-NLS-1$
        List vals = new ArrayList();
        vals.add(new Constant("a")); //$NON-NLS-1$
        vals.add(new Constant("b")); //$NON-NLS-1$
        c1.setValues(vals);
        
        SetCriteria c2 = (SetCriteria) c1.clone();
        assertNotSame(c1.getValues().iterator().next(), c2.getValues().iterator().next());
        c2.setNegated(true);
        int equals = -1;
        UnitTestUtil.helpTestEquivalence(equals, c1, c2);
    }
    
    @Test public void testNonHashableClone(){
        SetCriteria c1 = new SetCriteria();
        c1.setExpression(new ElementSymbol("e1")); //$NON-NLS-1$
        TreeSet vals = new TreeSet();
        vals.add(new Constant(BigDecimal.valueOf(1.1)));
        vals.add(new Constant(BigDecimal.valueOf(1.2)));
        c1.setValues(vals);
        c1.setAllConstants(true);
        
        SetCriteria c2 = (SetCriteria) c1.clone();
        assertTrue(c2.getValues() instanceof TreeSet);
    }
}
