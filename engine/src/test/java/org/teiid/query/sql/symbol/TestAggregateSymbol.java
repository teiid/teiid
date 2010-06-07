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

package org.teiid.query.sql.symbol;

import junit.framework.TestCase;

import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.Function;


public class TestAggregateSymbol extends TestCase {

	// ################################## FRAMEWORK ################################
	
	public TestAggregateSymbol(String name) { 
		super(name);
	}	
	
	// ################################## TEST HELPERS ################################
	
	public static final ElementSymbol sampleElement() { 
		return new ElementSymbol("m.g.c"); //$NON-NLS-1$
	}

	public static final ElementSymbol sampleElement2() { 
		return new ElementSymbol("m.g.c2"); //$NON-NLS-1$
	}
	
	public static final Constant sampleConstant() { 
		return new Constant(new Integer(5));
	}
	
	public static final Function sampleFunction() { 
		return new Function("+", new Expression[] { sampleElement(), sampleConstant() }); //$NON-NLS-1$
	}

	private void helpParser(AggregateSymbol as, String expected) {
		String toString = as.toString();		
		assertEquals("Parser string does not match", expected, toString);		 //$NON-NLS-1$
	}
	
	private void helpEquals(AggregateSymbol as1, AggregateSymbol as2, boolean equal) {
		if(equal) {
			assertTrue("Aggregate symbols should be equal: " + as1 + ", " + as2, as1.equals(as2)); //$NON-NLS-1$ //$NON-NLS-2$
		} else {
			assertTrue("Aggregate symbols should not be equal: " + as1 + ", " + as2, ! as1.equals(as2)); //$NON-NLS-1$ //$NON-NLS-2$
		}	
	}
	
		
	// ################################## ACTUAL TESTS ################################
	
	public void testParser1() {
		AggregateSymbol as = new AggregateSymbol("count", NonReserved.COUNT, false, sampleElement()); //$NON-NLS-1$
		helpParser(as, "COUNT(m.g.c)"); //$NON-NLS-1$
	}

	public void testParser2() {
		AggregateSymbol as = new AggregateSymbol("count", NonReserved.COUNT, true, sampleElement()); //$NON-NLS-1$
		helpParser(as, "COUNT(DISTINCT m.g.c)"); //$NON-NLS-1$
	}

	public void testParser3() {
		AggregateSymbol as = new AggregateSymbol("x", NonReserved.MIN, false, sampleConstant()); //$NON-NLS-1$
		helpParser(as, "MIN(5)"); //$NON-NLS-1$
	}

	public void testParser4() {
		AggregateSymbol as = new AggregateSymbol("x", NonReserved.MAX, false, sampleFunction()); //$NON-NLS-1$
		helpParser(as, "MAX((m.g.c + 5))"); //$NON-NLS-1$
	}

	public void testParser5() {
		AggregateSymbol as = new AggregateSymbol("x", NonReserved.COUNT, false, null); //$NON-NLS-1$
		helpParser(as, "COUNT(*)"); //$NON-NLS-1$
	}
		
	public void testEquals1() { 
		AggregateSymbol as = new AggregateSymbol("x", NonReserved.COUNT, true, sampleElement()); //$NON-NLS-1$
		helpEquals(as, as, true);		
	}

	public void testEquals2() { 
		AggregateSymbol as1 = new AggregateSymbol("x", NonReserved.COUNT, true, sampleElement()); //$NON-NLS-1$
		AggregateSymbol as2 = (AggregateSymbol) as1.clone();
		helpEquals(as1, as2, true);		
	}

    //just changing the name of an aggregatesymbol doesn't matter
	public void testEquals3() { 
		AggregateSymbol as1 = new AggregateSymbol("x", NonReserved.COUNT, true, sampleElement()); //$NON-NLS-1$
		AggregateSymbol as2 = new AggregateSymbol("y", NonReserved.COUNT, true, sampleElement()); //$NON-NLS-1$
		helpEquals(as1, as2, true);		
	}
	
	public void testEquals4() { 
		AggregateSymbol as1 = new AggregateSymbol("count", NonReserved.COUNT, false, null); //$NON-NLS-1$
		AggregateSymbol as2 = (AggregateSymbol) as1.clone();
		helpEquals(as1, as2, true);		
	}

	public void testSelfEquivalence(){
		AggregateSymbol test = new AggregateSymbol("x", NonReserved.COUNT, true, sampleElement()); //$NON-NLS-1$
		int equals = 0;
		UnitTestUtil.helpTestEquivalence(equals, test, test);
	}

	public void testEquivalence(){
		AggregateSymbol test1 = new AggregateSymbol("x", NonReserved.COUNT, true, sampleElement()); //$NON-NLS-1$
		AggregateSymbol test2 = new AggregateSymbol("x", NonReserved.COUNT, true, sampleElement()); //$NON-NLS-1$
		int equals = 0;
		UnitTestUtil.helpTestEquivalence(equals, test1, test2);
	}
    
    public void testEquivalenceCountStar(){
        AggregateSymbol test1 = new AggregateSymbol("x", NonReserved.COUNT, false, null); //$NON-NLS-1$
        AggregateSymbol test2 = new AggregateSymbol("x", NonReserved.COUNT, false, null); //$NON-NLS-1$
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, test1, test2);
    }

	public void testEquivalenceCaseInsens(){
		AggregateSymbol test1 = new AggregateSymbol("x", NonReserved.COUNT, true, sampleElement()); //$NON-NLS-1$
		AggregateSymbol test2 = new AggregateSymbol("X", NonReserved.COUNT, true, sampleElement()); //$NON-NLS-1$
		int equals = 0;
		UnitTestUtil.helpTestEquivalence(equals, test1, test2);
	}
    
    public void testNonEquivalenceUsingDiffElements(){ 
        AggregateSymbol test1 = new AggregateSymbol("x", NonReserved.COUNT, true, sampleElement()); //$NON-NLS-1$ 
        AggregateSymbol test2 = new AggregateSymbol("X", NonReserved.COUNT, true, sampleElement2()); //$NON-NLS-1$ 
        int equals = -1; 
        UnitTestUtil.helpTestEquivalence(equals, test1, test2); 
    } 
	
	public void testNonEquivalence(){
		AggregateSymbol test1 = new AggregateSymbol("x", NonReserved.COUNT, true, sampleElement()); //$NON-NLS-1$
		AggregateSymbol test2 = new AggregateSymbol("y", NonReserved.COUNT, true, sampleElement2()); //$NON-NLS-1$
		int equals = -1;
		UnitTestUtil.helpTestEquivalence(equals, test1, test2);
	}
    
    public void testNonEquivalence1(){
        AggregateSymbol test1 = new AggregateSymbol("x", NonReserved.COUNT, true, sampleElement()); //$NON-NLS-1$
        AggregateSymbol test2 = new AggregateSymbol("x", NonReserved.COUNT, true, sampleElement2()); //$NON-NLS-1$
        int equals = -1;
        UnitTestUtil.helpTestEquivalence(equals, test1, test2);
    }
    
    public void testNonEquivalence2(){
        AggregateSymbol test1 = new AggregateSymbol("x", NonReserved.MAX, true, sampleElement()); //$NON-NLS-1$
        AggregateSymbol test2 = new AggregateSymbol("x", NonReserved.COUNT, true, sampleElement2()); //$NON-NLS-1$
        int equals = -1;
        UnitTestUtil.helpTestEquivalence(equals, test1, test2);
    }
}
