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
