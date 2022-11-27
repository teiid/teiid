/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 */
package org.teiid.query.sql.proc;

import junit.framework.TestCase;

import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.sql.lang.From;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.lang.TestSetQuery;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;


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

    public void testCloneNonSimpleQuery(){
        Block block = TestBlock.sample1();
        LoopStatement ls = new LoopStatement(block, TestSetQuery.sample1(), "cursor"); //$NON-NLS-1$
        LoopStatement clone = (LoopStatement) ls.clone();
        UnitTestUtil.helpTestEquivalence(0, ls, clone);
    }

}
