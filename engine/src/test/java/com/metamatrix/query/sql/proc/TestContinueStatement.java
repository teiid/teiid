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

/*
 */
package com.metamatrix.query.sql.proc;

import junit.framework.TestCase;

import com.metamatrix.core.util.UnitTestUtil;

/**
 * @author LLiang
 *
 * To change the template for this generated type comment go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
public class TestContinueStatement  extends TestCase{

    /**
     * Constructor for TestAssignmentStatement.
     */
    public TestContinueStatement(String name) { 
        super(name);
    }
    
    // ################################## TEST HELPERS ################################ 

    public static final ContinueStatement sample1() { 
        return new ContinueStatement();
    }
    
    public static final ContinueStatement sample2() { 
        return new ContinueStatement();
    }
    
    // ################################## ACTUAL TESTS ################################ 
    
    public void testSelfEquivalence(){
        ContinueStatement s1 = sample1();
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, s1, s1);
    }

    public void testEquivalence(){
        ContinueStatement s1 = sample1();
        ContinueStatement s1a = sample2();
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, s1, s1a);
    }
    
    public void testNonEquivalence(){
        ContinueStatement s1 = sample1();
        int equals = -1;
        UnitTestUtil.helpTestEquivalence(equals, s1, new BreakStatement());
    }

}
