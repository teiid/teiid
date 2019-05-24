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
import org.teiid.query.sql.proc.DeclareStatement;
import org.teiid.query.sql.symbol.ElementSymbol;

import junit.framework.TestCase;


/**
 *
 * @author gchadalavadaDec 11, 2002
 */
public class TestDeclareStatement  extends TestCase {

    /**
     * Constructor for TestAssignmentStatement.
     */
    public TestDeclareStatement(String name) {
        super(name);
    }

    // ################################## TEST HELPERS ################################

    public static final DeclareStatement sample1() {
        return new DeclareStatement(new ElementSymbol("a"), "String"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public static final DeclareStatement sample2() {
        return new DeclareStatement(new ElementSymbol("b"), "Integer"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // ################################## ACTUAL TESTS ################################

    public void testGetVariable() {
        DeclareStatement s1 = sample1();
        assertEquals("Incorrect variable ", s1.getVariable(), new ElementSymbol("a"));         //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testGetVariableType() {
        DeclareStatement s1 = sample1();
        assertEquals("Incorrect variable type ", s1.getVariableType(), "String"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testSelfEquivalence(){
        DeclareStatement s1 = sample1();
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, s1, s1);
    }

    public void testEquivalence(){
        DeclareStatement s1 = sample1();
        DeclareStatement s1a = sample1();
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, s1, s1a);
    }

    public void testNonEquivalence(){
        DeclareStatement s1 = sample1();
        DeclareStatement s2 = sample2();
        int equals = -1;
        UnitTestUtil.helpTestEquivalence(equals, s1, s2);
    }

    public void testClone() {
        DeclareStatement s1 = sample1();
        DeclareStatement s2 = (DeclareStatement)s1.clone();

        assertTrue(s1 != s2);
        assertEquals(s1, s2);
    }
}
