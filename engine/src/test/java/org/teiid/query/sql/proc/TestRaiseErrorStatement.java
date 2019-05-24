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
import org.teiid.query.sql.proc.RaiseStatement;
import org.teiid.query.sql.symbol.Constant;

import junit.framework.*;

/**
 *
 * @author gchadalavadaDec 11, 2002
 */
public class TestRaiseErrorStatement  extends TestCase {

    /**
     * Constructor for TestAssignmentStatement.
     */
    public TestRaiseErrorStatement(String name) {
        super(name);
    }

    // ################################## TEST HELPERS ################################

    public static final RaiseStatement sample1() {
        return new RaiseStatement(new Constant("a")); //$NON-NLS-1$
    }

    public static final RaiseStatement sample2() {
        return new RaiseStatement(new Constant("b")); //$NON-NLS-1$
    }

    // ################################## ACTUAL TESTS ################################

    public void testSelfEquivalence(){
        RaiseStatement s1 = sample1();
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, s1, s1);
    }

    public void testEquivalence(){
        RaiseStatement s1 = sample1();
        RaiseStatement s1a = sample1();
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, s1, s1a);
    }

    public void testNonEquivalence(){
        RaiseStatement s1 = sample1();
        RaiseStatement s2 = sample2();
        int equals = -1;
        UnitTestUtil.helpTestEquivalence(equals, s1, s2);
    }
}
