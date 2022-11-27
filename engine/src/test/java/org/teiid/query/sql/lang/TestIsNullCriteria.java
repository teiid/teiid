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
import org.teiid.query.sql.lang.IsNullCriteria;
import org.teiid.query.sql.symbol.*;

import junit.framework.*;

public class TestIsNullCriteria extends TestCase {

    // ################################## FRAMEWORK ################################

    public TestIsNullCriteria(String name) {
        super(name);
    }

    // ################################## TEST HELPERS ################################

    public static IsNullCriteria example(String element, boolean negated) {
        IsNullCriteria crit = new IsNullCriteria();
        crit.setNegated(negated);
        crit.setExpression(new ElementSymbol(element));
        return crit;
    }

    // ################################## ACTUAL TESTS ################################

    public void testEquals1() {
        IsNullCriteria c1 = example("abc", true); //$NON-NLS-1$
        IsNullCriteria c2 = example("abc", true); //$NON-NLS-1$

        assertTrue("Equivalent is null criteria don't compare as equal: " + c1 + ", " + c2, c1.equals(c2));                 //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testEquals2() {
        IsNullCriteria c1 = example("abc", false); //$NON-NLS-1$
        IsNullCriteria c2 = (IsNullCriteria)c1.clone();
        assertTrue("Equivalent is null criteria don't compare as equal: " + c1 + ", " + c2, c1.equals(c2));                 //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testSelfEquivalence(){
        IsNullCriteria c1 = new IsNullCriteria();
        c1.setNegated(true);
        c1.setExpression(new Constant("abc")); //$NON-NLS-1$
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, c1, c1);
    }

    public void testEquivalence(){
        IsNullCriteria c1 = example("abc", true); //$NON-NLS-1$
        IsNullCriteria c2 = example("abc", true); //$NON-NLS-1$
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, c1, c2);
    }

    public void testCloneEquivalence(){
        IsNullCriteria c1 = example("abc", false); //$NON-NLS-1$

        IsNullCriteria c2 = (IsNullCriteria)c1.clone();
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, c1, c2);
    }

    public void testNonEquivalence1(){
        //test transitivity with two nonequal Objects
        IsNullCriteria c1 = example("abc", true); //$NON-NLS-1$
        IsNullCriteria c2 = example("xyz", true); //$NON-NLS-1$
        int equals = -1;
        UnitTestUtil.helpTestEquivalence(equals, c1, c2);
    }

    public void testNonEquivalence2(){
        IsNullCriteria c1 = example("abc", true); //$NON-NLS-1$
        IsNullCriteria c2 = example("abc", false); //$NON-NLS-1$
        int equals = -1;
        UnitTestUtil.helpTestEquivalence(equals, c1, c2);
    }

}
