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

package org.teiid.core.util;

import org.teiid.core.CorePlugin;
import org.teiid.core.util.Assertion;

import junit.framework.TestCase;


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
