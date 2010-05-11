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

import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.proc.Block;
import org.teiid.query.sql.proc.WhileStatement;

import junit.framework.TestCase;


public class TestWhileStatement  extends TestCase{
    
    public TestWhileStatement(String name) { 
        super(name);
    }   
    
    // ################################## TEST HELPERS ################################ 

    public static final WhileStatement sample1() {
        Block block = TestBlock.sample1();
        Criteria criteria = TestHasCriteria.sample1();
        return new WhileStatement(criteria, block);
    }

    public static final WhileStatement sample2() { 
        Block block = TestBlock.sample2();
        Criteria criteria = TestHasCriteria.sample2();
        return new WhileStatement(criteria, block);
    }
    
    // ################################## ACTUAL TESTS ################################ 


    public void testGetIfBlock() {
        WhileStatement b1 = sample1();
        assertTrue("Incorrect Block on statement", b1.getBlock().equals(TestBlock.sample1())); //$NON-NLS-1$
    }
    
    public void testGetCondition() {
        WhileStatement b1 = sample1();
        assertTrue("Incorrect Block on statement", b1.getCondition().equals(TestHasCriteria.sample1())); //$NON-NLS-1$
    }
    
    public void testSelfEquivalence(){
        WhileStatement s1 = sample1();
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, s1, s1);
    }

    public void testEquivalence(){
        WhileStatement s1 = sample1();
        WhileStatement s1a = sample1();
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, s1, s1a);
    }
    
    public void testNonEquivalence(){
        WhileStatement s1 = sample1();
        WhileStatement s2 = sample2();
        int equals = -1;
        UnitTestUtil.helpTestEquivalence(equals, s1, s2);
    }

}
