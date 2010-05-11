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

import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.sql.lang.BetweenCriteria;
import org.teiid.query.sql.symbol.*;

import junit.framework.*;

public class TestBetweenCriteria extends TestCase {

	// ################################## FRAMEWORK ################################
	
	public TestBetweenCriteria(String name) { 
		super(name);
	}	
	
	// ################################## TEST HELPERS ################################	
		
	public static BetweenCriteria example(String element, int lower, int upper, boolean negated) {
        BetweenCriteria criteria = new BetweenCriteria(new ElementSymbol(element),
                                                       new Constant(new Integer(lower)),
                                                       new Constant(new Integer(upper)));
        criteria.setNegated(negated);
		return criteria;		    
    }
		
	// ################################## ACTUAL TESTS ################################
	
	public void testEquals1() {
        BetweenCriteria c1 = example("x", 1, 20, false); //$NON-NLS-1$
        BetweenCriteria c2 = example("x", 1, 20, false); //$NON-NLS-1$
		assertTrue("Equivalent between criteria don't compare as equal: " + c1 + ", " + c2, c1.equals(c2));				 //$NON-NLS-1$ //$NON-NLS-2$
	}

    public void testEquals2() {    
        BetweenCriteria c1 = example("x", 1, 20, true); //$NON-NLS-1$
        BetweenCriteria c2 = (BetweenCriteria)c1.clone();
        assertTrue("Equivalent between criteria don't compare as equal: " + c1 + ", " + c2, c1.equals(c2));              //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testEquals3() {    
        BetweenCriteria c1 = example("x", 1, 20, true); //$NON-NLS-1$
        BetweenCriteria c2 = (BetweenCriteria)c1.clone();
        c2.setNegated(false);
        assertFalse("Criteria should not be equal: " + c1 + ", " + c2, c1.equals(c2));              //$NON-NLS-1$ //$NON-NLS-2$
    }

	public void testSelfEquivalence(){
        BetweenCriteria c1 = example("x", 1, 20, false); //$NON-NLS-1$
		int equals = 0;
		UnitTestUtil.helpTestEquivalence(equals, c1, c1);
	}

	public void testEquivalence(){
        BetweenCriteria c1 = example("x", 1, 20, false); //$NON-NLS-1$
        BetweenCriteria c2 = example("x", 1, 20, false); //$NON-NLS-1$
		int equals = 0;
		UnitTestUtil.helpTestEquivalence(equals, c1, c2);
	}
	
	public void testCloneEquivalence(){
        BetweenCriteria c1 = example("x", 1, 20, true); //$NON-NLS-1$
        BetweenCriteria c2 = (BetweenCriteria)c1.clone();
		int equals = 0;
		UnitTestUtil.helpTestEquivalence(equals, c1, c2);
	}	
	
    public void testNonEquivalence1(){
        //test transitivity with two nonequal Objects
        BetweenCriteria c1 = example("xyz", 1, 20, false); //$NON-NLS-1$
        BetweenCriteria c2 = example("abc", 1, 20, false); //$NON-NLS-1$
        int equals = -1;
        UnitTestUtil.helpTestEquivalence(equals, c1, c2);
    }
    
    public void testNonEquivalence2(){
        BetweenCriteria c1 = example("x", 1, 20, true); //$NON-NLS-1$
        BetweenCriteria c2 = example("x", 1, 20, false); //$NON-NLS-1$
        int equals = -1;
        UnitTestUtil.helpTestEquivalence(equals, c1, c2);
    }
   
}
