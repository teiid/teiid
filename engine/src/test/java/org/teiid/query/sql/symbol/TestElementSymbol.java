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

package org.teiid.query.sql.symbol;

import static org.junit.Assert.*;

import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;

@SuppressWarnings("nls")
public class TestElementSymbol {

    /** Unaliased group "m.g" */
    public static final GroupSymbol sampleGroup1() {
        return new GroupSymbol("m.g"); //$NON-NLS-1$
    }

    /** Aliased group "m.g as gg" */
    public static final GroupSymbol sampleGroup2() {
        return new GroupSymbol("gg", "m.g"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /** Short element from unaliased group: "c" */
    public static final ElementSymbol sampleElement1() {
        ElementSymbol element = new ElementSymbol("m.g.c", false); //$NON-NLS-1$
        element.setGroupSymbol(sampleGroup1());
        return element;
    }

    /** Long element from unaliased group: "m.g.c" */
    public static final ElementSymbol sampleElement2() {
        ElementSymbol element = new ElementSymbol("m.g.c"); //$NON-NLS-1$
        element.setGroupSymbol(sampleGroup1());
        return element;
    }

    /** Short element from aliased group: "c" */
    public static final ElementSymbol sampleElement3() {
        ElementSymbol element = new ElementSymbol("gg.c", false); //$NON-NLS-1$
        element.setGroupSymbol(sampleGroup2());
        return element;
    }

    /** Long element from aliased group: "gg.c" */
    public static final ElementSymbol sampleElement4() {
        ElementSymbol element = new ElementSymbol("gg.c"); //$NON-NLS-1$
        element.setGroupSymbol(sampleGroup2());
        return element;
    }

    private void helpParser(ElementSymbol es, String expected) {
        String toString = es.toString();
        assertEquals("Parser string does not match", expected, toString); //$NON-NLS-1$
    }

    private void helpEquals(ElementSymbol es1, ElementSymbol es2, boolean equal) {
        if(equal) {
            assertTrue("Element symbols should be equal: " + es1 + ", " + es2, es1.equals(es2)); //$NON-NLS-1$ //$NON-NLS-2$
            assertTrue("Equal symbols should have same hash code: " + es1 + ", " + es2, es1.hashCode() == es2.hashCode()); //$NON-NLS-1$ //$NON-NLS-2$
        } else {
            assertTrue("Element symbols should not be equal: " + es1 + ", " + es2, ! es1.equals(es2)); //$NON-NLS-1$ //$NON-NLS-2$
        }
    }


    // ################################## ACTUAL TESTS ################################

    @Test public void testParser1() {
        helpParser(sampleElement1(), "c"); //$NON-NLS-1$
    }

    @Test public void testParser2() {
        helpParser(sampleElement2(), "m.g.c"); //$NON-NLS-1$
    }

    @Test public void testParser3() {
        helpParser(sampleElement3(), "c"); //$NON-NLS-1$
    }

    @Test public void testParser4() {
        helpParser(sampleElement4(), "gg.c"); //$NON-NLS-1$
    }

    // Compare elements to themselves

    @Test public void testEquals1() {
        ElementSymbol es = sampleElement1();
        helpEquals(es, es, true);
    }

    @Test public void testEquals2() {
        ElementSymbol es = sampleElement2();
        helpEquals(es, es, true);
    }

    @Test public void testEquals3() {
        ElementSymbol es = sampleElement3();
        helpEquals(es, es, true);
    }

    @Test public void testEquals4() {
        ElementSymbol es = sampleElement4();
        helpEquals(es, es, true);
    }

    // Compare elements to their clones

    @Test public void testEquals5() {
        ElementSymbol es = sampleElement1();
        helpEquals(es, es.clone(), true);
    }

    @Test public void testEquals6() {
        ElementSymbol es = sampleElement2();
        helpEquals(es, es.clone(), true);
    }

    @Test public void testEquals7() {
        ElementSymbol es = sampleElement3();
        helpEquals(es, es.clone(), true);
    }

    @Test public void testEquals8() {
        ElementSymbol es = sampleElement4();
        helpEquals(es, es.clone(), true);
    }

    // Compare fully-qualified to short versions

    @Test public void testEquals9() {
        helpEquals(sampleElement1(), sampleElement2(), true);
    }

    @Test public void testEquals10() {
        helpEquals(sampleElement3(), sampleElement4(), true);
    }

    // Compare same-named elements with same groups but different group contexts

    @Test public void testEquals11() {
        helpEquals(sampleElement1(), sampleElement3(), false);
    }

    @Test public void testEquals12() {
        helpEquals(sampleElement2(), sampleElement4(), false);
    }

    // Test case sensitivity
    @Test public void testEquals13() {
        ElementSymbol es1 = new ElementSymbol("abcd"); //$NON-NLS-1$
        es1.setGroupSymbol(sampleGroup1());
        ElementSymbol es2 = new ElementSymbol("AbCd"); //$NON-NLS-1$
        es2.setGroupSymbol(sampleGroup1());

        helpEquals(es1, es2, false);
    }

    @Test public void testSelfEquivalence(){
        Object s1 = sampleElement1();
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, s1, s1);
    }

    @Test public void testEquivalence(){
        Object s1 = sampleElement1();
        Object s1a = sampleElement1();
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, s1, s1a);
    }

    @Test public void testCloneEquivalence(){
        ElementSymbol s1 = sampleElement1();
        ElementSymbol s2 = s1.clone();
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, s1, s2);
    }

    @Test public void testNonEquivalence(){
        Object s1 = sampleElement1();
        Object s3 = sampleElement3();
        int equals = -1;
        UnitTestUtil.helpTestEquivalence(equals, s1, s3);
    }

    /*
    @Test public void testEqualsMethod(){
        QueryUnitTestUtils.helpTestEquals(sampleElement1());
    }

    @Test public void testEqualsMethodTransitivity(){
        //test transitivity with two equal Objects
        QueryUnitTestUtils.helpTestEqualsTransitivity(sampleElement1(), sampleElement1());
    }

    @Test public void testEqualsMethodTransitivity2(){
        //test transitivity with two nonequal Objects
        QueryUnitTestUtils.helpTestEqualsTransitivity(sampleElement1(), sampleElement2());
    }

    @Test public void testHashCodeConsistentWithEquals(){
        //test hash code with two equal Objects
        QueryUnitTestUtils.helpTestHashCodeConsistentWithEquals(sampleElement1(), sampleElement2());
    }
    */

    @Test public void testClone1() {
        ElementSymbol e1 = sampleElement1();
        ElementSymbol copy = e1.clone();
        helpEquals(e1, copy, true);
    }

    @Test public void testClone2() {
        ElementSymbol e1 = sampleElement2();
        ElementSymbol copy = e1.clone();
        helpEquals(e1, copy, true);
    }

    @Test public void testClone3() {
        ElementSymbol e1 = sampleElement3();
        ElementSymbol copy = e1.clone();
        helpEquals(e1, copy, true);
    }

    @Test public void testClone4() {
        ElementSymbol e1 = sampleElement4();
        ElementSymbol copy = e1.clone();
        helpEquals(e1, copy, true);
    }

    @Test public void testClone5() {
        ElementSymbol e1 = sampleElement1();
        ElementSymbol copy = e1.clone();
        helpEquals(e1, copy, true);

        // Change original, clone shouldn't change
        String originalName = e1.getName();
        assertTrue("Cloned value changed but should not have: ", copy.getName().equals(originalName)); //$NON-NLS-1$

        GroupSymbol originalGroup = e1.getGroupSymbol();
        e1.setGroupSymbol(new GroupSymbol("b")); //$NON-NLS-1$
        assertTrue("Cloned value changed but should not have: ", copy.getGroupSymbol().equals(originalGroup)); //$NON-NLS-1$

        boolean fullyQualified = e1.getDisplayFullyQualified();
        e1.setDisplayFullyQualified(!fullyQualified);
        assertTrue("Cloned value changed but should not have: ", copy.getDisplayFullyQualified() == fullyQualified);         //$NON-NLS-1$
    }

    @Test public void testEqualsWithAndWithoutGroupSymbol() {
        ElementSymbol e1 = new ElementSymbol("x.y");

        ElementSymbol e2 = new ElementSymbol("y");
        e2.setGroupSymbol(new GroupSymbol("x"));
        helpEquals(e1, e2, true);
    }

    @Test public void testClone6() {
        ElementSymbol e1 = new ElementSymbol("x.y.z", new GroupSymbol("doc"));
        ElementSymbol copy = e1.clone();
        helpEquals(e1, copy, true);
    }

}
