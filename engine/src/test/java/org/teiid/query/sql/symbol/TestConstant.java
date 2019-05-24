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

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.junit.Test;
import org.teiid.core.types.ArrayImpl;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.DataTypeManager.DefaultDataClasses;
import org.teiid.core.util.UnitTestUtil;

@SuppressWarnings("nls")
public class TestConstant {

    // ################################## TEST HELPERS ################################

    public static final Constant sample1() {
        String s = "the string"; //$NON-NLS-1$
        return new Constant(s);
    }

    public static final Constant sample2() {
        Integer i = new Integer(5);
        return new Constant(i);
    }

    // ################################## ACTUAL TESTS ################################

    @Test public void testString() {
        String s = "the string"; //$NON-NLS-1$
        Constant c = new Constant(s);
        assertEquals("Value is incorrect: ", s, c.getValue()); //$NON-NLS-1$
        assertEquals("Type is incorrect: ", DataTypeManager.DefaultDataClasses.STRING, c.getType()); //$NON-NLS-1$
        assertEquals("Should be non-null: ", false, c.isNull()); //$NON-NLS-1$
        assertEquals("Object does not equal itself", c, c); //$NON-NLS-1$

        Constant c2 = new Constant(s);
        assertEquals("Constants for same object aren't equal: ", c, c2); //$NON-NLS-1$

        Constant cc = (Constant) c.clone();
        assertEquals("Cloned object not equal to original: ", c, cc); //$NON-NLS-1$
    }

    @Test public void testInteger() {
        Integer i = new Integer(5);
        Constant c = new Constant(i);
        assertEquals("Value is incorrect: ", i, c.getValue()); //$NON-NLS-1$
        assertEquals("Type is incorrect: ", DataTypeManager.DefaultDataClasses.INTEGER, c.getType()); //$NON-NLS-1$
        assertEquals("Should be non-null: ", false, c.isNull()); //$NON-NLS-1$
        assertEquals("Object does not equal itself", c, c); //$NON-NLS-1$

        Constant c2 = new Constant(i);
        assertEquals("Constants for same object aren't equal: ", c, c2); //$NON-NLS-1$

        Constant cc = (Constant) c.clone();
        assertEquals("Cloned object not equal to original: ", c, cc); //$NON-NLS-1$
    }

    @Test public void testNoTypeNull() {
        Constant c = new Constant(null);
        assertEquals("Value is incorrect: ", null, c.getValue()); //$NON-NLS-1$
        assertEquals("Type is incorrect: ", DataTypeManager.DefaultDataClasses.NULL, c.getType()); //$NON-NLS-1$
        assertEquals("Should be null: ", true, c.isNull()); //$NON-NLS-1$
        assertEquals("Object does not equal itself", c, c); //$NON-NLS-1$

        Constant c2 = new Constant(null);
        assertEquals("Constants for same object aren't equal: ", c, c2); //$NON-NLS-1$

        Constant cc = (Constant) c.clone();
        assertEquals("Cloned object not equal to original: ", c, cc); //$NON-NLS-1$
    }

    @Test public void testTypedNull() {
        Constant c = new Constant(null, DataTypeManager.DefaultDataClasses.STRING);
        assertEquals("Value is incorrect: ", null, c.getValue()); //$NON-NLS-1$
        assertEquals("Type is incorrect: ", DataTypeManager.DefaultDataClasses.STRING, c.getType()); //$NON-NLS-1$
        assertEquals("Should be null: ", true, c.isNull()); //$NON-NLS-1$
        assertEquals("Object does not equal itself", c, c); //$NON-NLS-1$

        Constant c2 = new Constant(null, DataTypeManager.DefaultDataClasses.STRING);
        assertEquals("Constants for same object aren't equal: ", c, c2); //$NON-NLS-1$

        Constant cc = (Constant) c.clone();
        assertEquals("Cloned object not equal to original: ", c, cc); //$NON-NLS-1$

        Constant c3 = new Constant(null);
        assertEquals("Typed null not equal to non-typed null: ", c, c3); //$NON-NLS-1$
    }

    @Test public void testClone() {
        // Use this object as the "object"-type value for c1
        StringBuffer value = new StringBuffer("x"); //$NON-NLS-1$

        Constant c1 = new Constant(value, DataTypeManager.DefaultDataClasses.OBJECT);
        Constant copy = (Constant) c1.clone();

        // Check equality
        assertEquals("Cloned object not equal to original: ", c1, copy); //$NON-NLS-1$

        // Check that modifying original value changes c1 and clone - this is expected as Constant
        // uses a shallow clone
        value.append("y"); //$NON-NLS-1$

        assertTrue("Original object has not changed, but should have", ((StringBuffer)c1.getValue()).toString().equals("xy"));         //$NON-NLS-1$ //$NON-NLS-2$
        assertTrue("Cloned object has not changed, but should have", ((StringBuffer)copy.getValue()).toString().equals("xy"));                         //$NON-NLS-1$ //$NON-NLS-2$
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
        Object s2 = sample2();
        int equals = -1;
        try{
            UnitTestUtil.helpTestEquivalence(equals, s1, s2);
        }catch(ClassCastException e) {
           // do nothing - this is caught because the method above compares two different objects
           // this exception should be thrown
        }
    }

    @Test public void testPaddedStringComparison(){
        assertEquals(1, Constant.comparePadded("a", ""));
        assertEquals(0, Constant.comparePadded("a", "a "));
        assertEquals(-24, Constant.comparePadded("ab ", "az "));
        assertEquals(66, Constant.comparePadded("ab ", "a "));
        assertEquals(0, Constant.comparePadded("a1 ", "a1"));
    }

    @Test public void testCollation() {
        Comparator<Object> c = Constant.getComparator("es", true);

        List<String> vals = Arrays.asList("ñ", "n", "o");
        Collections.sort(vals, c);
        assertEquals("ñ", vals.get(1));

        assertEquals(0, c.compare("a ", "a"));
    }

    @Test public void testArrayImplType() {
        Constant c = new Constant(new ArrayImpl(new Integer[0]));
        assertEquals(DataTypeManager.getArrayType(DefaultDataClasses.OBJECT), c.getType());
    }

    @Test public void testArrayCompare() {
        Constant c = new Constant(new ArrayImpl(new Integer[0]));
        assertEquals(DataTypeManager.getArrayType(DefaultDataClasses.OBJECT), c.getType());
    }

    @Test public void testEqualsWithDifferentTypes() {
        Constant c1 = new Constant("a");
        Constant c2 = new Constant("a", DefaultDataClasses.OBJECT);
        assertEquals(c1, c2);
    }

}
