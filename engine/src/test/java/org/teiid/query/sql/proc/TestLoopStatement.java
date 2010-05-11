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
import org.teiid.query.sql.lang.From;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.proc.Block;
import org.teiid.query.sql.proc.LoopStatement;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;

import junit.framework.TestCase;


public class TestLoopStatement  extends TestCase{
    
    public TestLoopStatement(String name) { 
        super(name);
    }   
    
    // ################################## TEST HELPERS ################################ 
    public static final Query query1() { 
        Query q1 = new Query();
        Select select = new Select();
        select.addSymbol(new ElementSymbol("x"));        //$NON-NLS-1$
        q1.setSelect(select);        
        From from = new From();
        from.addGroup(new GroupSymbol("g")); //$NON-NLS-1$
        q1.setFrom(from);
        return q1;
    }
    
    public static final Query query2() { 
        Query q1 = new Query();
        Select select = new Select();
        select.addSymbol(new ElementSymbol("x2"));        //$NON-NLS-1$
        q1.setSelect(select);        
        From from = new From();
        from.addGroup(new GroupSymbol("g2")); //$NON-NLS-1$
        q1.setFrom(from);
        return q1;
    }
    
    public static final LoopStatement sample1() {
        Block block = TestBlock.sample1();
        return new LoopStatement(block, query1(), "cursor"); //$NON-NLS-1$
    }

    public static final LoopStatement sample2() { 
        Block block = TestBlock.sample2();
        return new LoopStatement(block, query2(), "cursor"); //$NON-NLS-1$
    }
    
    // ################################## ACTUAL TESTS ################################ 


    public void testGetBlock() {
        LoopStatement b1 = sample1();
        assertTrue("Incorrect Block on statement", b1.getBlock().equals(TestBlock.sample1())); //$NON-NLS-1$
    }
    
    public void testGetQuery() {
        LoopStatement b1 = sample1();
        assertTrue("Incorrect Query on statement", b1.getCommand().equals(query1())); //$NON-NLS-1$
    }
    
    public void testGetCursorName(){
        LoopStatement b1 = sample1();
        LoopStatement b2 = sample2();
        assertEquals(b1.getCursorName(), b2.getCursorName());
    }
    
    public void testSelfEquivalence(){
        LoopStatement s1 = sample1();
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, s1, s1);
    }

    public void testEquivalence(){
        LoopStatement s1 = sample1();
        LoopStatement s1a = sample1();
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, s1, s1a);
    }
    
    public void testNonEquivalence(){
        LoopStatement s1 = sample1();
        LoopStatement s2 = sample2();
        int equals = -1;
        UnitTestUtil.helpTestEquivalence(equals, s1, s2);
    }

}
