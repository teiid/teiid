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
package org.teiid.query.sql.proc;

import junit.framework.TestCase;

import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.sql.proc.BranchingStatement.BranchingMode;

public class TestBreakStatement  extends TestCase{

    /**
     * Constructor for TestAssignmentStatement.
     */
    public TestBreakStatement(String name) { 
        super(name);
    }
    
    // ################################## TEST HELPERS ################################ 

    public static final BranchingStatement sample1() { 
        return new BranchingStatement();
    }
    
    public static final BranchingStatement sample2() { 
        return new BranchingStatement();
    }
    
    // ################################## ACTUAL TESTS ################################ 
    
    public void testSelfEquivalence(){
        BranchingStatement s1 = sample1();
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, s1, s1);
    }

    public void testEquivalence(){
        BranchingStatement s1 = sample1();
        BranchingStatement s1a = sample2();
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, s1, s1a);
    }
    
    public void testNonEquivalence(){
        BranchingStatement s1 = sample1();
        int equals = -1;
        UnitTestUtil.helpTestEquivalence(equals, s1, new BranchingStatement(BranchingMode.CONTINUE));
    }
}
