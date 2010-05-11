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
import org.teiid.query.sql.lang.IsNullCriteria;
import org.teiid.query.sql.symbol.*;

import junit.framework.*;

public class TestIsNullCriteria extends TestCase {

	// ################################## FRAMEWORK ################################
	
	public TestIsNullCriteria(String name) { 
		super(name);
	}	
	
	// ################################## TEST HELPERS ################################	
		
	public static IsNullCriteria example(String element, boolean negated) {
		IsNullCriteria crit = new IsNullCriteria();
        crit.setNegated(negated);
		crit.setExpression(new ElementSymbol(element));
		return crit;		    
    }
		
	// ################################## ACTUAL TESTS ################################
	
	public void testEquals1() {
        IsNullCriteria c1 = example("abc", true); //$NON-NLS-1$
        IsNullCriteria c2 = example("abc", true); //$NON-NLS-1$
        
		assertTrue("Equivalent is null criteria don't compare as equal: " + c1 + ", " + c2, c1.equals(c2));				 //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testEquals2() {    
        IsNullCriteria c1 = example("abc", false); //$NON-NLS-1$
        IsNullCriteria c2 = (IsNullCriteria)c1.clone();
		assertTrue("Equivalent is null criteria don't compare as equal: " + c1 + ", " + c2, c1.equals(c2));				 //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testSelfEquivalence(){
        IsNullCriteria c1 = new IsNullCriteria();
        c1.setNegated(true);
        c1.setExpression(new Constant("abc")); //$NON-NLS-1$
		int equals = 0;
		UnitTestUtil.helpTestEquivalence(equals, c1, c1);
	}

	public void testEquivalence(){
        IsNullCriteria c1 = example("abc", true); //$NON-NLS-1$
        IsNullCriteria c2 = example("abc", true); //$NON-NLS-1$
		int equals = 0;
		UnitTestUtil.helpTestEquivalence(equals, c1, c2);
	}
	
	public void testCloneEquivalence(){
        IsNullCriteria c1 = example("abc", false); //$NON-NLS-1$
        
        IsNullCriteria c2 = (IsNullCriteria)c1.clone();
		int equals = 0;
		UnitTestUtil.helpTestEquivalence(equals, c1, c2);
	}	
	
    public void testNonEquivalence1(){
        //test transitivity with two nonequal Objects
        IsNullCriteria c1 = example("abc", true); //$NON-NLS-1$
        IsNullCriteria c2 = example("xyz", true); //$NON-NLS-1$
        int equals = -1;
        UnitTestUtil.helpTestEquivalence(equals, c1, c2);
    }
    
    public void testNonEquivalence2(){
        IsNullCriteria c1 = example("abc", true); //$NON-NLS-1$
        IsNullCriteria c2 = example("abc", false); //$NON-NLS-1$
        int equals = -1;
        UnitTestUtil.helpTestEquivalence(equals, c1, c2);
    }

}
