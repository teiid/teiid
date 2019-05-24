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

import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.sql.lang.BetweenCriteria;
import org.teiid.query.sql.symbol.*;

import junit.framework.*;

public class TestBetweenCriteria extends TestCase {

    // ################################## FRAMEWORK ################################

    public TestBetweenCriteria(String name) {
        super(name);
    }

    // ################################## TEST HELPERS ################################

    public static BetweenCriteria example(String element, int lower, int upper, boolean negated) {
        BetweenCriteria criteria = new BetweenCriteria(new ElementSymbol(element),
                                                       new Constant(new Integer(lower)),
                                                       new Constant(new Integer(upper)));
        criteria.setNegated(negated);
        return criteria;
    }

    // ################################## ACTUAL TESTS ################################

    public void testEquals1() {
        BetweenCriteria c1 = example("x", 1, 20, false); //$NON-NLS-1$
        BetweenCriteria c2 = example("x", 1, 20, false); //$NON-NLS-1$
        assertTrue("Equivalent between criteria don't compare as equal: " + c1 + ", " + c2, c1.equals(c2));                 //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testEquals2() {
        BetweenCriteria c1 = example("x", 1, 20, true); //$NON-NLS-1$
        BetweenCriteria c2 = (BetweenCriteria)c1.clone();
        assertTrue("Equivalent between criteria don't compare as equal: " + c1 + ", " + c2, c1.equals(c2));              //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testEquals3() {
        BetweenCriteria c1 = example("x", 1, 20, true); //$NON-NLS-1$
        BetweenCriteria c2 = (BetweenCriteria)c1.clone();
        c2.setNegated(false);
        assertFalse("Criteria should not be equal: " + c1 + ", " + c2, c1.equals(c2));              //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testSelfEquivalence(){
        BetweenCriteria c1 = example("x", 1, 20, false); //$NON-NLS-1$
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, c1, c1);
    }

    public void testEquivalence(){
        BetweenCriteria c1 = example("x", 1, 20, false); //$NON-NLS-1$
        BetweenCriteria c2 = example("x", 1, 20, false); //$NON-NLS-1$
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, c1, c2);
    }

    public void testCloneEquivalence(){
        BetweenCriteria c1 = example("x", 1, 20, true); //$NON-NLS-1$
        BetweenCriteria c2 = (BetweenCriteria)c1.clone();
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, c1, c2);
    }

    public void testNonEquivalence1(){
        //test transitivity with two nonequal Objects
        BetweenCriteria c1 = example("xyz", 1, 20, false); //$NON-NLS-1$
        BetweenCriteria c2 = example("abc", 1, 20, false); //$NON-NLS-1$
        int equals = -1;
        UnitTestUtil.helpTestEquivalence(equals, c1, c2);
    }

    public void testNonEquivalence2(){
        BetweenCriteria c1 = example("x", 1, 20, true); //$NON-NLS-1$
        BetweenCriteria c2 = example("x", 1, 20, false); //$NON-NLS-1$
        int equals = -1;
        UnitTestUtil.helpTestEquivalence(equals, c1, c2);
    }

}
