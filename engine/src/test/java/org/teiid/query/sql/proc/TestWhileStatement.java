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
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.TestSetCriteria;


public class TestWhileStatement  extends TestCase{

    public TestWhileStatement(String name) {
        super(name);
    }

    // ################################## TEST HELPERS ################################

    public static final WhileStatement sample1() {
        Block block = TestBlock.sample1();
        Criteria criteria = TestSetCriteria.sample1();
        return new WhileStatement(criteria, block);
    }

    public static final WhileStatement sample2() {
        Block block = TestBlock.sample2();
        Criteria criteria = TestSetCriteria.sample2();
        return new WhileStatement(criteria, block);
    }

    // ################################## ACTUAL TESTS ################################


    public void testGetIfBlock() {
        WhileStatement b1 = sample1();
        assertTrue("Incorrect Block on statement", b1.getBlock().equals(TestBlock.sample1())); //$NON-NLS-1$
    }

    public void testGetCondition() {
        WhileStatement b1 = sample1();
        assertTrue("Incorrect Block on statement", b1.getCondition().equals(TestSetCriteria.sample1())); //$NON-NLS-1$
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
