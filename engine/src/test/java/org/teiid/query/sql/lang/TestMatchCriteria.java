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

package org.teiid.query.sql.lang;

import static org.junit.Assert.*;

import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;

public class TestMatchCriteria {

    // ################################## TEST HELPERS ################################

    public static MatchCriteria example(String element, String str) {
        MatchCriteria crit = new MatchCriteria();
        crit.setLeftExpression(new ElementSymbol(element));
        crit.setRightExpression(new Constant(str));
        return crit;
    }

    public static MatchCriteria example(String str) {
        MatchCriteria crit = new MatchCriteria();
        crit.setLeftExpression(new ElementSymbol("m.g1.e1")); //$NON-NLS-1$
        crit.setRightExpression(new Constant(str));
        return crit;
    }

    public static MatchCriteria example(String str, char escapeChar) {
        MatchCriteria crit = new MatchCriteria();
        crit.setLeftExpression(new ElementSymbol("m.g1.e1")); //$NON-NLS-1$
        crit.setRightExpression(new Constant(str));
        crit.setEscapeChar(escapeChar);
        return crit;
    }

    // ################################## ACTUAL TESTS ################################

    @Test public void testEquals1() {
        MatchCriteria c1 = example("abc"); //$NON-NLS-1$
        MatchCriteria c2 = example("abc"); //$NON-NLS-1$
        assertTrue("Equivalent match criteria don't compare as equal: " + c1 + ", " + c2, c1.equals(c2));                 //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testEquals2() {
        MatchCriteria c1 = example("abc", '#'); //$NON-NLS-1$
        c1.setNegated(true);
        MatchCriteria c2 = example("abc", '#'); //$NON-NLS-1$
        c2.setNegated(true);
        assertTrue("Equivalent match criteria don't compare as equal: " + c1 + ", " + c2, c1.equals(c2));                 //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testEquals3() {
        MatchCriteria c1 = example("abc", '#'); //$NON-NLS-1$
        c1.setNegated(true);
        MatchCriteria c2 = (MatchCriteria) c1.clone();
        assertTrue("Equivalent match criteria don't compare as equal: " + c1 + ", " + c2, c1.equals(c2));                 //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testEquals4() {
        MatchCriteria c1 = example("abc"); //$NON-NLS-1$
        MatchCriteria c2 = example("abc", '#'); //$NON-NLS-1$
        assertTrue("Different match criteria compare as equal: " + c1 + ", " + c2, ! c1.equals(c2));                 //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testEquals5() {
        MatchCriteria c1 = example("e1", "abc"); //$NON-NLS-1$ //$NON-NLS-2$
        MatchCriteria c2 = example("e2", "abc"); //$NON-NLS-1$ //$NON-NLS-2$
        assertTrue("Different match criteria compare as equal: " + c1 + ", " + c2, ! c1.equals(c2));                 //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testSelfEquivalence(){
        MatchCriteria c1 = example("abc"); //$NON-NLS-1$
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, c1, c1);
    }

    @Test public void testEquivalence(){
        MatchCriteria c1 = example("abc"); //$NON-NLS-1$
        MatchCriteria c2 = example("abc"); //$NON-NLS-1$
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, c1, c2);
    }

    @Test public void testCloneEquivalence(){
        MatchCriteria c1 = example("abc"); //$NON-NLS-1$
        MatchCriteria c2 = (MatchCriteria)c1.clone();
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, c1, c2);
    }

    @Test public void testNonEquivalence1(){
        //test transitivity with two nonequal Objects
        MatchCriteria c1 = example("e1", "abc"); //$NON-NLS-1$ //$NON-NLS-2$
        MatchCriteria c2 = example("ozzy", '#'); //$NON-NLS-1$
        int equals = -1;
        UnitTestUtil.helpTestEquivalence(equals, c1, c2);
    }

    @Test public void testNonEquivalence2(){
        MatchCriteria c1 = example("abc", '#'); //$NON-NLS-1$
        c1.setNegated(true);
        MatchCriteria c2 = example("abc", '#'); //$NON-NLS-1$
        c2.setNegated(false);
        int equals = -1;
        UnitTestUtil.helpTestEquivalence(equals, c1, c2);
    }
}
