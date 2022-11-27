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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Reference;

public class TestSetCriteria {

    public static final SetCriteria sample1() {
        SetCriteria c1 = new SetCriteria();
        c1.setExpression(new ElementSymbol("e1")); //$NON-NLS-1$
        List vals = new ArrayList();
        vals.add(new Constant("a")); //$NON-NLS-1$
        vals.add(new Constant("b")); //$NON-NLS-1$
        c1.setValues(vals);
        return c1;
    }

    public static final SetCriteria sample2() {
        SetCriteria c1 = new SetCriteria();
        c1.setExpression(new ElementSymbol("e2")); //$NON-NLS-1$
        List vals = new ArrayList();
        vals.add(new Constant("c")); //$NON-NLS-1$
        vals.add(new Constant("d")); //$NON-NLS-1$
        c1.setValues(vals);
        return c1;
    }

    // ################################## ACTUAL TESTS ################################

    @Test public void testEquals1() {
        SetCriteria c1 = new SetCriteria();
        c1.setExpression(new ElementSymbol("e1")); //$NON-NLS-1$
        List vals = new ArrayList();
        vals.add(new Constant("a")); //$NON-NLS-1$
        vals.add(new Constant("b")); //$NON-NLS-1$
        c1.setValues(vals);

        SetCriteria c2 = (SetCriteria) c1.clone();

        assertTrue("Equivalent set criteria don't compare as equal: " + c1 + ", " + c2, c1.equals(c2));                 //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testEquals2() {
        SetCriteria c1 = new SetCriteria();
        c1.setNegated(true);
        c1.setExpression(new ElementSymbol("e1")); //$NON-NLS-1$
        List vals1 = new ArrayList();
        vals1.add(new Constant("a")); //$NON-NLS-1$
        vals1.add(new Constant("b")); //$NON-NLS-1$
        c1.setValues(vals1);

        SetCriteria c2 = new SetCriteria();
        c2.setNegated(true);
        c2.setExpression(new ElementSymbol("e1")); //$NON-NLS-1$
        List vals2 = new ArrayList();
        vals2.add(new Constant("b")); //$NON-NLS-1$
        vals2.add(new Constant("a")); //$NON-NLS-1$
        c2.setValues(vals2);

        assertTrue("Equivalent set criteria don't compare as equal: " + c1 + ", " + c2, c1.equals(c2));                 //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testSelfEquivalence(){
        Object s1 = sample1();
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, s1, s1);
    }

    @Test public void testEquivalence(){
        Object s1 = sample1();
        Object s1a = sample1();
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, s1, s1a);
    }

    @Test public void testNonEquivalence(){
        Object s1 = sample1();
        SetCriteria s1a = sample1();
        s1a.setValues(Arrays.asList(new Reference(1), new Reference(2)));
        int equals = -1;
        UnitTestUtil.helpTestEquivalence(equals, s1, s1a);
    }

    @Test public void testNonEquivalence1(){
        Object s1 = sample1();
        Object s2 = sample2();
        int equals = -1;
        UnitTestUtil.helpTestEquivalence(equals, s1, s2);
    }

    @Test public void testNonEquivalence2(){
        SetCriteria c1 = new SetCriteria();
        c1.setExpression(new ElementSymbol("e1")); //$NON-NLS-1$
        List vals = new ArrayList();
        vals.add(new Constant("a")); //$NON-NLS-1$
        vals.add(new Constant("b")); //$NON-NLS-1$
        c1.setValues(vals);

        SetCriteria c2 = (SetCriteria) c1.clone();
        assertNotSame(c1.getValues().iterator().next(), c2.getValues().iterator().next());
        c2.setNegated(true);
        int equals = -1;
        UnitTestUtil.helpTestEquivalence(equals, c1, c2);
    }

    @Test public void testNonHashableClone(){
        SetCriteria c1 = new SetCriteria();
        c1.setExpression(new ElementSymbol("e1")); //$NON-NLS-1$
        TreeSet vals = new TreeSet();
        vals.add(new Constant(BigDecimal.valueOf(1.1)));
        vals.add(new Constant(BigDecimal.valueOf(1.2)));
        c1.setValues(vals);
        c1.setAllConstants(true);

        SetCriteria c2 = (SetCriteria) c1.clone();
        assertTrue(c2.getValues() instanceof TreeSet);
    }

    @Test public void testNonEquivalence3(){
        Object s1 = sample1();
        SetCriteria s1a = sample1();
        s1a.setValues(Arrays.asList(new Constant("a"), new Constant("a")));
        int equals = -1;
        UnitTestUtil.helpTestEquivalence(equals, s1, s1a);
    }
}
