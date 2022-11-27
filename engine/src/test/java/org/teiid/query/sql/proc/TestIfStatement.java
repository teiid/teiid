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

package org.teiid.query.sql.proc;

import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.sql.lang.*;
import org.teiid.query.sql.proc.Block;
import org.teiid.query.sql.proc.IfStatement;

import junit.framework.*;

/**
 *
 * @author gchadalavadaDec 9, 2002
 */
public class TestIfStatement  extends TestCase {

    // ################################## FRAMEWORK ################################

    public TestIfStatement(String name) {
        super(name);
    }

    // ################################## TEST HELPERS ################################

    public static final IfStatement sample1() {
        Block ifBlock = TestBlock.sample1();
        Block elseBlock = TestBlock.sample2();
        Criteria criteria = TestSetCriteria.sample1();
        return new IfStatement(criteria, ifBlock, elseBlock);
    }

    public static final IfStatement sample2() {
        Block ifBlock = TestBlock.sample2();
        Block elseBlock = TestBlock.sample1();
        Criteria criteria = TestSetCriteria.sample2();
        return new IfStatement(criteria, ifBlock, elseBlock);
    }

    // ################################## ACTUAL TESTS ################################


    public void testGetIfBlock() {
        IfStatement b1 = sample1();
        assertTrue("Incorrect IfBlock on statement", b1.getIfBlock().equals(TestBlock.sample1())); //$NON-NLS-1$
    }

    public void testGetElseBlock() {
        IfStatement b1 = sample1();
        assertTrue("Incorrect IfBlock on statement", b1.getElseBlock().equals(TestBlock.sample2())); //$NON-NLS-1$
    }

    public void testGetCondition() {
        IfStatement b1 = sample1();
        assertTrue("Incorrect IfBlock on statement", b1.getCondition().equals(TestSetCriteria.sample1())); //$NON-NLS-1$
    }

    public void testSelfEquivalence(){
        IfStatement s1 = sample1();
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, s1, s1);
    }

    public void testEquivalence(){
        IfStatement s1 = sample1();
        IfStatement s1a = sample1();
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, s1, s1a);
    }

    public void testNonEquivalence(){
        IfStatement s1 = sample1();
        IfStatement s2 = sample2();
        int equals = -1;
        UnitTestUtil.helpTestEquivalence(equals, s1, s2);
    }

}
