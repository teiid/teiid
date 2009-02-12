/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package com.metamatrix.core.util;

import junit.framework.TestCase;

import com.metamatrix.core.CorePlugin;

/**
 * TestAssertion
 */
public class TestAssertion extends TestCase {

    private static final String TEST_MESSAGE = "This is a test assertion message"; //$NON-NLS-1$

    /**
     * Constructor for TestAssertion.
     * @param name
     */
    public TestAssertion(String name) {
        super(name);
    }

    /*
     * Test for void assertTrue(boolean)
     */
    public void testAssertTrueboolean() {
        Assertion.assertTrue(true);

        try {
            Assertion.assertTrue(false);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            final String msg = CorePlugin.Util.getString("Assertion.Assertion_failed"); //$NON-NLS-1$
            assertEquals(msg, e.getMessage());
        }
    }

    /*
     * Test for void assertTrue(boolean, String)
     */
    public void testAssertTruebooleanString() {
        Assertion.assertTrue(true,TEST_MESSAGE);

        try {
            Assertion.assertTrue(false,TEST_MESSAGE);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            assertEquals(TEST_MESSAGE, e.getMessage());
        }
    }

    public void testFailed() {
        try {
            Assertion.failed(null);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            assertEquals("null", e.getMessage()); //$NON-NLS-1$
        }

        try {
            Assertion.failed(TEST_MESSAGE);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            assertEquals(TEST_MESSAGE, e.getMessage());
        }
    }

    /*
     * Test for void isEqual(boolean, boolean)
     */
    public void testIsEqualbooleanboolean() {
        Assertion.isEqual(true,true);
        Assertion.isEqual(false,false);

        try {
            Assertion.isEqual(false,true);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            final Object[] params = new Object[]{new Boolean(false),new Boolean(true)};
            final String msg = CorePlugin.Util.getString("Assertion.isEqual",params); //$NON-NLS-1$
            assertEquals(msg, e.getMessage());
        }
    }

    /*
     * Test for void isEqual(boolean, boolean, String)
     */
    public void testIsEqualbooleanbooleanString() {
        Assertion.isEqual(true,true,TEST_MESSAGE);
        Assertion.isEqual(false,false,TEST_MESSAGE);

        try {
            Assertion.isEqual(false,true,TEST_MESSAGE);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            assertEquals(TEST_MESSAGE, e.getMessage());
        }
    }

    /*
     * Test for void isNotEqual(boolean, boolean)
     */
    public void testIsNotEqualbooleanboolean() {
        Assertion.isNotEqual(false,true);
        Assertion.isNotEqual(true,false);

        try {
            Assertion.isNotEqual(false,false);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            final Object[] params = new Object[]{new Boolean(false),new Boolean(false)};
            final String msg = CorePlugin.Util.getString("Assertion.isNotEqual",params); //$NON-NLS-1$
            assertEquals(msg, e.getMessage());
        }
    }

    /*
     * Test for void isNotEqual(boolean, boolean, String)
     */
    public void testIsNotEqualbooleanbooleanString() {
        Assertion.isNotEqual(false,true,TEST_MESSAGE);
        Assertion.isNotEqual(true,false,TEST_MESSAGE);

        try {
            Assertion.isNotEqual(false,false,TEST_MESSAGE);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            assertEquals(TEST_MESSAGE, e.getMessage());
        }
    }

    /*
     * Test for void isEqual(int, int)
     */
    public void testIsEqualintint() {
        Assertion.isEqual(-1,-1);
        Assertion.isEqual(1,1);
        Assertion.isEqual(0,0);
        Assertion.isEqual(10000,10000);

        try {
            Assertion.isEqual(1,-1);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            final Object[] params = new Object[]{new Integer(1),new Integer(-1)};
            final String msg = CorePlugin.Util.getString("Assertion.isEqual",params); //$NON-NLS-1$
            assertEquals(msg, e.getMessage());
        }
    }

    /*
     * Test for void isEqual(int, int, String)
     */
    public void testIsEqualintintString() {
        Assertion.isEqual(-1,-1,TEST_MESSAGE);
        Assertion.isEqual(1,1,TEST_MESSAGE);
        Assertion.isEqual(0,0,TEST_MESSAGE);
        Assertion.isEqual(10000,10000,TEST_MESSAGE);

        try {
            Assertion.isEqual(1,-1,TEST_MESSAGE);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            assertEquals(TEST_MESSAGE, e.getMessage());
        }
    }

    /*
     * Test for void isNotEqual(int, int)
     */
    public void testIsNotEqualintint() {
        Assertion.isNotEqual(-1,-12);
        Assertion.isNotEqual(1,-1);
        Assertion.isNotEqual(0,1);
        Assertion.isNotEqual(10000,-10000);

        try {
            Assertion.isNotEqual(1,1);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            final Object[] params = new Object[]{new Integer(1),new Integer(1)};
            final String msg = CorePlugin.Util.getString("Assertion.isNotEqual",params); //$NON-NLS-1$
            assertEquals(msg, e.getMessage());
        }
    }

    /*
     * Test for void isNotEqual(int, int, String)
     */
    public void testIsNotEqualintintString() {
        Assertion.isNotEqual(-1,-12,TEST_MESSAGE);
        Assertion.isNotEqual(1,-1,TEST_MESSAGE);
        Assertion.isNotEqual(0,1,TEST_MESSAGE);
        Assertion.isNotEqual(10000,-10000,TEST_MESSAGE);

        try {
            Assertion.isNotEqual(1,1,TEST_MESSAGE);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            assertEquals(TEST_MESSAGE, e.getMessage());
        }
    }

    /*
     * Test for void isNonNegative(int)
     */
    public void testIsNonNegativeint() {
        Assertion.isNonNegative(1);
        Assertion.isNonNegative(13);
        Assertion.isNonNegative(0);

        try {
            Assertion.isNonNegative(-1);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            final Object[] params = new Object[]{new Integer(-1)};
            final String msg = CorePlugin.Util.getString("Assertion.isNonNegative",params); //$NON-NLS-1$
            assertEquals(msg, e.getMessage());
        }
    }

    /*
     * Test for void isNonNegative(int, String)
     */
    public void testIsNonNegativeintString() {
        Assertion.isNonNegative(1,TEST_MESSAGE);
        Assertion.isNonNegative(13,TEST_MESSAGE);
        Assertion.isNonNegative(0,TEST_MESSAGE);

        try {
            Assertion.isNonNegative(-1,TEST_MESSAGE);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            assertEquals(TEST_MESSAGE, e.getMessage());
        }
    }

    /*
     * Test for void isNonPositive(int)
     */
    public void testIsNonPositiveint() {
        Assertion.isNonPositive(-1);
        Assertion.isNonPositive(0);
        Assertion.isNonPositive(-100);

        try {
            Assertion.isNonPositive(1);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            final Object[] params = new Object[]{new Integer(1)};
            final String msg = CorePlugin.Util.getString("Assertion.isNonPositive",params); //$NON-NLS-1$
            assertEquals(msg, e.getMessage());
        }
    }

    /*
     * Test for void isNonPositive(int, String)
     */
    public void testIsNonPositiveintString() {
        Assertion.isNonPositive(-1,TEST_MESSAGE);
        Assertion.isNonPositive(0,TEST_MESSAGE);
        Assertion.isNonPositive(-100,TEST_MESSAGE);

        try {
            Assertion.isNonPositive(1,TEST_MESSAGE);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            assertEquals(TEST_MESSAGE, e.getMessage());
        }
    }

    /*
     * Test for void isNegative(int)
     */
    public void testIsNegativeint() {
        Assertion.isNegative(-1);
        Assertion.isNegative(-100);

        try {
            Assertion.isNegative(1);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            final Object[] params = new Object[]{new Integer(1)};
            final String msg = CorePlugin.Util.getString("Assertion.isNegative",params); //$NON-NLS-1$
            assertEquals(msg, e.getMessage());
        }
        try {
            Assertion.isNegative(0);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            final Object[] params = new Object[]{new Integer(0)};
            final String msg = CorePlugin.Util.getString("Assertion.isNegative",params); //$NON-NLS-1$
            assertEquals(msg, e.getMessage());
        }
    }

    /*
     * Test for void isNegative(int, String)
     */
    public void testIsNegativeintString() {
        Assertion.isNegative(-1,TEST_MESSAGE);
        Assertion.isNegative(-100,TEST_MESSAGE);

        try {
            Assertion.isNegative(1,TEST_MESSAGE);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            assertEquals(TEST_MESSAGE, e.getMessage());
        }
        try {
            Assertion.isNegative(0,TEST_MESSAGE);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            assertEquals(TEST_MESSAGE, e.getMessage());
        }
    }

    /*
     * Test for void isPositive(int)
     */
    public void testIsPositiveint() {
        Assertion.isPositive(1);
        Assertion.isPositive(100);

        try {
            Assertion.isPositive(-1);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            final Object[] params = new Object[]{new Integer(-1)};
            final String msg = CorePlugin.Util.getString("Assertion.isPositive",params); //$NON-NLS-1$
            assertEquals(msg, e.getMessage());
        }
        try {
            Assertion.isPositive(0);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            final Object[] params = new Object[]{new Integer(0)};
            final String msg = CorePlugin.Util.getString("Assertion.isPositive",params); //$NON-NLS-1$
            assertEquals(msg, e.getMessage());
        }
    }

    /*
     * Test for void isPositive(int, String)
     */
    public void testIsPositiveintString() {
        Assertion.isPositive(1,TEST_MESSAGE);
        Assertion.isPositive(100,TEST_MESSAGE);

        try {
            Assertion.isPositive(-1,TEST_MESSAGE);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            assertEquals(TEST_MESSAGE, e.getMessage());
        }
        try {
            Assertion.isPositive(0,TEST_MESSAGE);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            assertEquals(TEST_MESSAGE, e.getMessage());
        }
    }

    /*
     * Test for void isEqual(long, long)
     */
    public void testIsEquallonglong() {
        Assertion.isEqual(-1l,-1l);
        Assertion.isEqual(1l,1l);
        Assertion.isEqual(0l,0l);
        Assertion.isEqual(10000l,10000l);

        try {
            Assertion.isEqual(1l,-1l);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            final Object[] params = new Object[]{new Long(1),new Long(-1)};
            final String msg = CorePlugin.Util.getString("Assertion.isEqual",params); //$NON-NLS-1$
            assertEquals(msg, e.getMessage());
        }
    }

    /*
     * Test for void isEqual(long, long, String)
     */
    public void testIsEquallonglongString() {
        Assertion.isEqual(-1l,-1l,TEST_MESSAGE);
        Assertion.isEqual(1l,1l,TEST_MESSAGE);
        Assertion.isEqual(0l,0l,TEST_MESSAGE);
        Assertion.isEqual(10000l,10000l,TEST_MESSAGE);

        try {
            Assertion.isEqual(1l,-1l,TEST_MESSAGE);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            assertEquals(TEST_MESSAGE, e.getMessage());
        }
    }

    /*
     * Test for void isNotEqual(long, long)
     */
    public void testIsNotEquallonglong() {
        Assertion.isNotEqual(-1l,-12l);
        Assertion.isNotEqual(1l,12l);
        Assertion.isNotEqual(0l,-20l);
        Assertion.isNotEqual(10000l,-10000l);

        try {
            Assertion.isNotEqual(1l,1l);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            final Object[] params = new Object[]{new Long(1l),new Long(1l)};
            final String msg = CorePlugin.Util.getString("Assertion.isNotEqual",params); //$NON-NLS-1$
            assertEquals(msg, e.getMessage());
        }
    }

    /*
     * Test for void isNotEqual(long, long, String)
     */
    public void testIsNotEquallonglongString() {
        Assertion.isNotEqual(-1l,-12l,TEST_MESSAGE);
        Assertion.isNotEqual(1l,12l,TEST_MESSAGE);
        Assertion.isNotEqual(0l,-20l,TEST_MESSAGE);
        Assertion.isNotEqual(10000l,-10000l,TEST_MESSAGE);

        try {
            Assertion.isNotEqual(1l,1l,TEST_MESSAGE);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            assertEquals(TEST_MESSAGE, e.getMessage());
        }
    }

    /*
     * Test for void isNonNegative(long)
     */
    public void testIsNonNegativelong() {
        Assertion.isNonNegative(1l);
        Assertion.isNonNegative(13l);
        Assertion.isNonNegative(0l);

        try {
            Assertion.isNonNegative(-1l);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            final Object[] params = new Object[]{new Long(-1l)};
            final String msg = CorePlugin.Util.getString("Assertion.isNonNegative",params); //$NON-NLS-1$
            assertEquals(msg, e.getMessage());
        }
    }

    /*
     * Test for void isNonNegative(long, String)
     */
    public void testIsNonNegativelongString() {
        Assertion.isNonNegative(1l,TEST_MESSAGE);
        Assertion.isNonNegative(13l,TEST_MESSAGE);
        Assertion.isNonNegative(0l,TEST_MESSAGE);

        try {
            Assertion.isNonNegative(-1l,TEST_MESSAGE);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            assertEquals(TEST_MESSAGE, e.getMessage());
        }
    }

    /*
     * Test for void isNonPositive(long)
     */
    public void testIsNonPositivelong() {
        Assertion.isNonPositive(-1l);
        Assertion.isNonPositive(0l);
        Assertion.isNonPositive(-100l);

        try {
            Assertion.isNonPositive(1l);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            final Object[] params = new Object[]{new Long(1)};
            final String msg = CorePlugin.Util.getString("Assertion.isNonPositive",params); //$NON-NLS-1$
            assertEquals(msg, e.getMessage());
        }
    }

    /*
     * Test for void isNonPositive(long, String)
     */
    public void testIsNonPositivelongString() {
        Assertion.isNonPositive(-1l,TEST_MESSAGE);
        Assertion.isNonPositive(0l,TEST_MESSAGE);
        Assertion.isNonPositive(-100l,TEST_MESSAGE);

        try {
            Assertion.isNonPositive(1l,TEST_MESSAGE);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            assertEquals(TEST_MESSAGE, e.getMessage());
        }
    }

    /*
     * Test for void isNegative(long)
     */
    public void testIsNegativelong() {
        Assertion.isNegative(-1l);
        Assertion.isNegative(-100l);

        try {
            Assertion.isNegative(1l);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            final Object[] params = new Object[]{new Long(1)};
            final String msg = CorePlugin.Util.getString("Assertion.isNegative",params); //$NON-NLS-1$
            assertEquals(msg, e.getMessage());
        }
        try {
            Assertion.isNegative(0l);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            final Object[] params = new Object[]{new Long(0)};
            final String msg = CorePlugin.Util.getString("Assertion.isNegative",params); //$NON-NLS-1$
            assertEquals(msg, e.getMessage());
        }
    }

    /*
     * Test for void isNegative(long, String)
     */
    public void testIsNegativelongString() {
        Assertion.isNegative(-1l,TEST_MESSAGE);
        Assertion.isNegative(-100l,TEST_MESSAGE);

        try {
            Assertion.isNegative(1l,TEST_MESSAGE);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            assertEquals(TEST_MESSAGE, e.getMessage());
        }
        try {
            Assertion.isNegative(0l,TEST_MESSAGE);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            assertEquals(TEST_MESSAGE, e.getMessage());
        }
    }

    /*
     * Test for void isPositive(long)
     */
    public void testIsPositivelong() {
        Assertion.isPositive(1l);
        Assertion.isPositive(100l);

        try {
            Assertion.isPositive(-1l);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            final Object[] params = new Object[]{new Long(-1)};
            final String msg = CorePlugin.Util.getString("Assertion.isPositive",params); //$NON-NLS-1$
            assertEquals(msg, e.getMessage());
        }
        try {
            Assertion.isPositive(0l);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            final Object[] params = new Object[]{new Long(0)};
            final String msg = CorePlugin.Util.getString("Assertion.isPositive",params); //$NON-NLS-1$
            assertEquals(msg, e.getMessage());
        }
    }

    /*
     * Test for void isPositive(long, String)
     */
    public void testIsPositivelongString() {
        Assertion.isPositive(1l,TEST_MESSAGE);
        Assertion.isPositive(100l,TEST_MESSAGE);

        try {
            Assertion.isPositive(-1l,TEST_MESSAGE);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            assertEquals(TEST_MESSAGE, e.getMessage());
        }
        try {
            Assertion.isPositive(0l,TEST_MESSAGE);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            assertEquals(TEST_MESSAGE, e.getMessage());
        }
    }

    /*
     * Test for void isNotZeroLength(String)
     */
    public void testIsNotZeroLengthString() {
        Assertion.isNotZeroLength("This is a string"); //$NON-NLS-1$
        Assertion.isNotZeroLength("   "); //$NON-NLS-1$

        try {
            Assertion.isNotZeroLength(""); //$NON-NLS-1$
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            final String msg = CorePlugin.Util.getString("Assertion.isNotZeroLength"); //$NON-NLS-1$
            assertEquals(msg, e.getMessage());
        }
        try {
            Assertion.isNotZeroLength(null);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            final String msg = CorePlugin.Util.getString("Assertion.isNotNull"); //$NON-NLS-1$
            assertEquals(msg, e.getMessage());
        }
    }

    /*
     * Test for void isNotZeroLength(String, String)
     */
    public void testIsNotZeroLengthStringString() {
        Assertion.isNotZeroLength("This is a string",TEST_MESSAGE); //$NON-NLS-1$
        Assertion.isNotZeroLength("   ",TEST_MESSAGE); //$NON-NLS-1$

        try {
            Assertion.isNotZeroLength("",TEST_MESSAGE); //$NON-NLS-1$
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            assertEquals(TEST_MESSAGE, e.getMessage());
        }
        try {
            Assertion.isNotZeroLength(null,TEST_MESSAGE);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            final String msg = CorePlugin.Util.getString("Assertion.isNotNull"); //$NON-NLS-1$
            assertEquals(msg, e.getMessage());
        }
    }

    /*
     * Test for void isNull(Object)
     */
    public void testIsNullObject() {
        Assertion.isNull(null);

        try {
            Assertion.isNull(""); //$NON-NLS-1$
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            final String msg = CorePlugin.Util.getString("Assertion.isNull"); //$NON-NLS-1$
            assertEquals(msg, e.getMessage());
        }
    }

    /*
     * Test for void isNull(Object, String)
     */
    public void testIsNullObjectString() {
        Assertion.isNull(null,TEST_MESSAGE);

        try {
            Assertion.isNull("",TEST_MESSAGE); //$NON-NLS-1$
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            assertEquals(TEST_MESSAGE, e.getMessage());
        }
    }

    /*
     * Test for void isNotNull(Object)
     */
    public void testIsNotNullObject() {
        Assertion.isNotNull(""); //$NON-NLS-1$

        try {
            Assertion.isNotNull(null);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            final String msg = CorePlugin.Util.getString("Assertion.isNotNull"); //$NON-NLS-1$
            assertEquals(msg, e.getMessage());
        }
    }

    /*
     * Test for void isNotNull(Object, String)
     */
    public void testIsNotNullObjectString() {
        Assertion.isNotNull("",TEST_MESSAGE); //$NON-NLS-1$

        try {
            Assertion.isNotNull(null,TEST_MESSAGE);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            assertEquals(TEST_MESSAGE, e.getMessage());
        }
    }

    /*
     * Test for void isIdentical(Object, Object)
     */
    public void testIsIdenticalObjectObject() {
        final String obj1 = ""; //$NON-NLS-1$
        final Integer int1 = new Integer(33);
        Assertion.isIdentical(obj1,obj1);
        Assertion.isIdentical(int1,int1);

        try {
            Assertion.isIdentical(int1,obj1);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            final Object[] params = new Object[]{int1,obj1};
            final String msg = CorePlugin.Util.getString("Assertion.isIdentical",params); //$NON-NLS-1$
            assertEquals(msg, e.getMessage());
        }
    }

    /*
     * Test for void isIdentical(Object, Object, String)
     */
    public void testIsIdenticalObjectObjectString() {
        final String obj1 = ""; //$NON-NLS-1$
        final Integer int1 = new Integer(33);
        Assertion.isIdentical(obj1,obj1,TEST_MESSAGE);
        Assertion.isIdentical(int1,int1,TEST_MESSAGE);

        try {
            Assertion.isIdentical(int1,obj1,TEST_MESSAGE);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            assertEquals(TEST_MESSAGE, e.getMessage());
        }
    }

    /*
     * Test for void isEqual(Object, Object)
     */
    public void testIsEqualObjectObject() {
        final String obj1 = ""; //$NON-NLS-1$
        final String obj2 = ""; //$NON-NLS-1$
        final Integer int1 = new Integer(33);
        final Integer int2 = new Integer(33);
        Assertion.isEqual(obj1,obj2);
        Assertion.isEqual(int1,int2);

        try {
            Assertion.isEqual(int1,obj1);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            final Object[] params = new Object[]{int1,obj1};
            final String msg = CorePlugin.Util.getString("Assertion.isEqual",params); //$NON-NLS-1$
            assertEquals(msg, e.getMessage());
        }
    }

    /*
     * Test for void isEqual(Object, Object, String)
     */
    public void testIsEqualObjectObjectString() {
        final String obj1 = ""; //$NON-NLS-1$
        final String obj2 = ""; //$NON-NLS-1$
        final Integer int1 = new Integer(33);
        final Integer int2 = new Integer(33);
        Assertion.isEqual(obj1,obj2,TEST_MESSAGE);
        Assertion.isEqual(int1,int2,TEST_MESSAGE);

        try {
            Assertion.isEqual(int1,obj1,TEST_MESSAGE);
            fail();
        } catch ( AssertionError e ) {
            // expected, but check the message
            assertEquals(TEST_MESSAGE, e.getMessage());
        }
    }

    public void testIsInstanceOf() {
        Assertion.isInstanceOf(new Integer(1),Integer.class,"name"); //$NON-NLS-1$
        Assertion.isInstanceOf("asdfasdf",String.class,"name2"); //$NON-NLS-1$ //$NON-NLS-2$

        try {
            Assertion.isInstanceOf(new Integer(1),Long.class,"name3"); //$NON-NLS-1$
            fail();
        } catch ( ClassCastException e ) {
            // expected, but check the message
            final Object[] params = new Object[]{"name3", Long.class, Integer.class.getName()}; //$NON-NLS-1$
            final String msg = CorePlugin.Util.getString("Assertion.invalidClassMessage",params); //$NON-NLS-1$
            assertEquals(msg, e.getMessage());
        }
    }

    /*
     * Test for void isNotEmpty(Collection)
     */
    public void testIsNotEmptyCollection() {
    }

    /*
     * Test for void isNotEmpty(Collection, String)
     */
    public void testIsNotEmptyCollectionString() {
    }

    /*
     * Test for void isNotEmpty(Map)
     */
    public void testIsNotEmptyMap() {
    }

    /*
     * Test for void isNotEmpty(Map, String)
     */
    public void testIsNotEmptyMapString() {
    }

    /*
     * Test for void contains(Collection, Object)
     */
    public void testContainsCollectionObject() {
    }

    /*
     * Test for void contains(Collection, Object, String)
     */
    public void testContainsCollectionObjectString() {
    }

    /*
     * Test for void containsKey(Map, Object)
     */
    public void testContainsKeyMapObject() {
    }

    /*
     * Test for void containsKey(Map, Object, String)
     */
    public void testContainsKeyMapObjectString() {
    }

}
