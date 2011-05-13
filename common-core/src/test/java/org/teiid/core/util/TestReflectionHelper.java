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

package org.teiid.core.util;

import static org.junit.Assert.*;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

/**
 * TestReflectionHelper
 */
@SuppressWarnings("unchecked")
public class TestReflectionHelper {

    // =========================================================================
    //                      H E L P E R   M E T H O D S
    // =========================================================================

    /**
     * Verify the Class[] arrays compare equal element for element.
     * @param msg msg to display
     * @param signatureSought
     * @param signatureFound
     * @since 4.4
     */
    private void helpAssertSameMethodSignature(String msg,
                                               Class[] signatureSought,
                                               Class[] signatureFound) {
        assertEquals(msg + ": sizes differ.", signatureSought.length, signatureFound.length); //$NON-NLS-1$
        for (int i=0; i<signatureSought.length; ++i) {
            assertEquals(msg + " for argument # " + (i + 1), signatureSought[i], signatureFound[i]); //$NON-NLS-1$
        }
    }

    // =========================================================================
    //                         T E S T     C A S E S
    // =========================================================================

    @Test public void testConstructorWithNullTargetClass() {
        try {
            new ReflectionHelper(null);
            fail("Should have caught null target class passed to constructor"); //$NON-NLS-1$
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test public void testConstructorWithValidTargetClass() {
        final ReflectionHelper helper = new ReflectionHelper(String.class);
        assertNotNull(helper);
    }

    //  ===============================================================================================
    //   Test overloaded methods
    //  ===============================================================================================
    @Test public void testFindBestMethodWithSignature_String() throws Exception {
        ReflectionHelper helper = new ReflectionHelper(FakeInterface.class);
        Class[] signatureSought = new Class[] {String.class};
        Method theMethod = helper.findBestMethodWithSignature("method", signatureSought ); //$NON-NLS-1$
        assertNotNull("Failed to find method for args: " + signatureSought, theMethod); //$NON-NLS-1$

        // make sure we got the method we expected
        Class[] signatureFound = theMethod.getParameterTypes();
        Class[] signatureExpected = signatureSought;
        assertEquals("Wrong class", theMethod.getDeclaringClass().getName(), FakeInterface.class.getName()); //$NON-NLS-1$
        helpAssertSameMethodSignature("Found wrong method signature", signatureExpected, signatureFound); //$NON-NLS-1$
    }

    @Test public void testFindBestMethodWithSignature_Serializable() throws Exception {
        ReflectionHelper helper = new ReflectionHelper(FakeInterface.class);
        Class[] signatureSought = new Class[] {Serializable.class};
        Method theMethod = helper.findBestMethodWithSignature("method", signatureSought ); //$NON-NLS-1$
        assertNotNull("Failed to find method for args: " + signatureSought, theMethod); //$NON-NLS-1$

        // make sure we got the method we expected
        Class[] signatureFound = theMethod.getParameterTypes();
        Class[] signatureExpected = new Class[] {Serializable.class};
        assertEquals("Wrong class", theMethod.getDeclaringClass().getName(), FakeInterface.class.getName()); //$NON-NLS-1$
        helpAssertSameMethodSignature("Found wrong method signature", signatureExpected, signatureFound); //$NON-NLS-1$
    }

    @Test public void testFindBestMethodWithSignature_Object() throws Exception {
        ReflectionHelper helper = new ReflectionHelper(FakeInterface.class);
        Class[] signatureSought = new Class[] {NullPointerException.class};
        try {
            helper.findBestMethodWithSignature("method", signatureSought ); //$NON-NLS-1$
            fail("exception expected"); //$NON-NLS-1$
        } catch (NoSuchMethodException e) {
            assertEquals("method Args: [class java.lang.NullPointerException] has multiple possible signatures.", e.getMessage()); //$NON-NLS-1$
        }
    }

    @Test public void testFindBestMethodWithSignature_StringArray() throws Exception {
        ReflectionHelper helper = new ReflectionHelper(FakeInterface.class);
        Class[] signatureSought = new Class[] {String[].class};
        Method theMethod = helper.findBestMethodWithSignature("method", signatureSought ); //$NON-NLS-1$
        assertNotNull("Failed to find method for args: " + signatureSought, theMethod); //$NON-NLS-1$

        // make sure we got the method we expected
        Class[] signatureFound = theMethod.getParameterTypes();
        Class[] signatureExpected = signatureSought;
        assertEquals("Wrong class", theMethod.getDeclaringClass().getName(), FakeInterface.class.getName()); //$NON-NLS-1$
        helpAssertSameMethodSignature("Found wrong method signature", signatureExpected, signatureFound); //$NON-NLS-1$
    }

    @Test public void testFindBestMethodWithSignature_Integer() throws Exception {
        ReflectionHelper helper = new ReflectionHelper(FakeInterface.class);
        Class[] signatureSought = new Class[] {Integer.class};
        Method theMethod = helper.findBestMethodWithSignature("method", signatureSought ); //$NON-NLS-1$
        assertNotNull("Failed to find method for args: " + signatureSought, theMethod); //$NON-NLS-1$

        // make sure we got the method we expected
        Class[] signatureFound = theMethod.getParameterTypes();
        Class[] signatureExpected = signatureSought;
        assertEquals("Wrong class", theMethod.getDeclaringClass().getName(), FakeInterface.class.getName()); //$NON-NLS-1$
        helpAssertSameMethodSignature("Found wrong method signature", signatureExpected, signatureFound); //$NON-NLS-1$
    }

    @Test public void testFindBestMethodWithSignature_long() throws Exception {
        ReflectionHelper helper = new ReflectionHelper(FakeInterface.class);
        Class[] signatureSought = new Class[] {Long.TYPE};
        Method theMethod = helper.findBestMethodWithSignature("method", signatureSought ); //$NON-NLS-1$
        assertNotNull("Failed to find method for args: " + signatureSought, theMethod); //$NON-NLS-1$

        // make sure we got the method we expected
        Class[] signatureFound = theMethod.getParameterTypes();
        Class[] signatureExpected = signatureSought;
        assertEquals("Wrong class", theMethod.getDeclaringClass().getName(), FakeInterface.class.getName()); //$NON-NLS-1$
        helpAssertSameMethodSignature("Found wrong method signature", signatureExpected, signatureFound); //$NON-NLS-1$
    }

    @Test public void testFindBestMethodWithSignature_2ArgSerializableAndNumber() throws Exception {
        ReflectionHelper helper = new ReflectionHelper(FakeInterface.class);
        Class[] signatureSought = new Class[] {Integer.class, Integer.class};
        Method theMethod = helper.findBestMethodWithSignature("method", signatureSought ); //$NON-NLS-1$
        assertNotNull("Failed to find method for args: " + signatureSought, theMethod); //$NON-NLS-1$

        // make sure we got the method we expected
        Class[] signatureFound = theMethod.getParameterTypes();
        Class[] signatureExpected = new Class[] {Serializable.class, Number.class};
        assertEquals("Wrong class", theMethod.getDeclaringClass().getName(), FakeInterface.class.getName()); //$NON-NLS-1$
        helpAssertSameMethodSignature("Found wrong method signature", signatureExpected, signatureFound); //$NON-NLS-1$
    }

    //  ===============================================================================================
    //   Test explicit method names
    //  ===============================================================================================
    @Test public void testFindBestMethodWithSignature_StringAndMethodName() throws Exception {
        ReflectionHelper helper = new ReflectionHelper(FakeInterface.class);
        Class[] signatureSought = new Class[] {String.class};
        Method theMethod = helper.findBestMethodWithSignature("methodString", signatureSought ); //$NON-NLS-1$
        assertNotNull("Failed to find method for args: " + signatureSought, theMethod); //$NON-NLS-1$

        // make sure we got the method we expected
        Class[] signatureFound = theMethod.getParameterTypes();
        Class[] signatureExpected = signatureSought;
        assertEquals("Wrong class", theMethod.getDeclaringClass().getName(), FakeInterface.class.getName()); //$NON-NLS-1$
        helpAssertSameMethodSignature("Found wrong method signature", signatureExpected, signatureFound); //$NON-NLS-1$
    }

    @Test public void testFindBestMethodWithSignature_ObjectAndMethodName() throws Exception {
        ReflectionHelper helper = new ReflectionHelper(FakeInterface.class);
        Class[] signatureSought = new Class[] {NullPointerException.class};
        Method theMethod = helper.findBestMethodWithSignature("methodObject", signatureSought ); //$NON-NLS-1$
        assertNotNull("Failed to find method for args: " + signatureSought, theMethod); //$NON-NLS-1$

        // make sure we got the method we expected
        Class[] signatureFound = theMethod.getParameterTypes();
        Class[] signatureExpected = new Class[] {Object.class};
        assertEquals("Wrong class", theMethod.getDeclaringClass().getName(), FakeInterface.class.getName()); //$NON-NLS-1$
        helpAssertSameMethodSignature("Found wrong method signature", signatureExpected, signatureFound); //$NON-NLS-1$
    }

    @Test public void testFindBestMethodWithSignature_SerializableAndMethodName() throws Exception {
        ReflectionHelper helper = new ReflectionHelper(FakeInterface.class);
        Class[] signatureSought = new Class[] {NullPointerException.class};
        Method theMethod = helper.findBestMethodWithSignature("methodSerializable", signatureSought ); //$NON-NLS-1$
        assertNotNull("Failed to find method for args: " + signatureSought, theMethod); //$NON-NLS-1$

        // make sure we got the method we expected
        Class[] signatureFound = theMethod.getParameterTypes();
        Class[] signatureExpected = new Class[] {Serializable.class};
        assertEquals("Wrong class", theMethod.getDeclaringClass().getName(), FakeInterface.class.getName()); //$NON-NLS-1$
        helpAssertSameMethodSignature("Found wrong method signature", signatureExpected, signatureFound); //$NON-NLS-1$
    }

    @Test public void testFindBestMethodWithSignature_ObjectSerializableAndMethodName() throws Exception {
        ReflectionHelper helper = new ReflectionHelper(FakeInterface.class);
        Class[] signatureSought = new Class[] {NullPointerException.class};
        Method theMethod = helper.findBestMethodWithSignature("methodSerializable", signatureSought ); //$NON-NLS-1$
        assertNotNull("Failed to find method for args: " + signatureSought, theMethod); //$NON-NLS-1$

        // make sure we got the method we expected
        Class[] signatureFound = theMethod.getParameterTypes();
        Class[] signatureExpected = new Class[] {Serializable.class};
        assertEquals("Wrong class", theMethod.getDeclaringClass().getName(), FakeInterface.class.getName()); //$NON-NLS-1$
        helpAssertSameMethodSignature("Found wrong method signature", signatureExpected, signatureFound); //$NON-NLS-1$
    }

    @Test public void testFindBestMethodWithSignature_IntegerSerializableAndMethodName() throws Exception {
        ReflectionHelper helper = new ReflectionHelper(FakeInterface.class);
        Class[] signatureSought = new Class[] {Integer.class};
        Method theMethod = helper.findBestMethodWithSignature("methodSerializable", signatureSought ); //$NON-NLS-1$
        assertNotNull("Failed to find method for args: " + signatureSought, theMethod); //$NON-NLS-1$

        // make sure we got the method we expected
        Class[] signatureFound = theMethod.getParameterTypes();
        Class[] signatureExpected = new Class[] {Serializable.class};
        assertEquals("Wrong class", theMethod.getDeclaringClass().getName(), FakeInterface.class.getName()); //$NON-NLS-1$
        helpAssertSameMethodSignature("Found wrong method signature", signatureExpected, signatureFound); //$NON-NLS-1$
    }

    @Test public void testFindBestMethodWithSignature_StringArrayAndMethodName() throws Exception {
        ReflectionHelper helper = new ReflectionHelper(FakeInterface.class);
        Class[] signatureSought = new Class[] {String[].class};
        Method theMethod = helper.findBestMethodWithSignature("methodStringArray", signatureSought ); //$NON-NLS-1$
        assertNotNull("Failed to find method for args: " + signatureSought, theMethod); //$NON-NLS-1$

        // make sure we got the method we expected
        Class[] signatureFound = theMethod.getParameterTypes();
        Class[] signatureExpected = signatureSought;
        assertEquals("Wrong class", theMethod.getDeclaringClass().getName(), FakeInterface.class.getName()); //$NON-NLS-1$
        helpAssertSameMethodSignature("Found wrong method signature", signatureExpected, signatureFound); //$NON-NLS-1$
    }

    @Test public void testFindBestMethodWithSignature_ListAndMethodName() throws Exception {
        ReflectionHelper helper = new ReflectionHelper(FakeInterface.class);
        Class[] signatureSought = new Class[] {ArrayList.class};
        Method theMethod = helper.findBestMethodWithSignature("methodList", signatureSought ); //$NON-NLS-1$
        assertNotNull("Failed to find method for args: " + signatureSought, theMethod); //$NON-NLS-1$

        // make sure we got the method we expected
        Class[] signatureFound = theMethod.getParameterTypes();
        Class[] signatureExpected = new Class[] {List.class};
        assertEquals("Wrong class", theMethod.getDeclaringClass().getName(), FakeInterface.class.getName()); //$NON-NLS-1$
        helpAssertSameMethodSignature("Found wrong method signature", signatureExpected, signatureFound); //$NON-NLS-1$
    }

    @Test public void testFindBestMethodWithSignature_IntegerAndMethodName() throws Exception {
        ReflectionHelper helper = new ReflectionHelper(FakeInterface.class);
        Class[] signatureSought = new Class[] {Integer.class};
        Method theMethod = helper.findBestMethodWithSignature("methodInteger", signatureSought ); //$NON-NLS-1$
        assertNotNull("Failed to find method for args: " + signatureSought, theMethod); //$NON-NLS-1$

        // make sure we got the method we expected
        Class[] signatureFound = theMethod.getParameterTypes();
        Class[] signatureExpected = signatureSought;
        assertEquals("Wrong class", theMethod.getDeclaringClass().getName(), FakeInterface.class.getName()); //$NON-NLS-1$
        helpAssertSameMethodSignature("Found wrong method signature", signatureExpected, signatureFound); //$NON-NLS-1$
    }

    @Test public void testFindBestMethodWithSignature_IntegerObjectAndMethodName() throws Exception {
        ReflectionHelper helper = new ReflectionHelper(FakeInterface.class);
        Class[] signatureSought = new Class[] {Integer.class};
        Method theMethod = helper.findBestMethodWithSignature("methodObject", signatureSought ); //$NON-NLS-1$
        assertNotNull("Failed to find method for args: " + signatureSought, theMethod); //$NON-NLS-1$

        // make sure we got the method we expected
        Class[] signatureFound = theMethod.getParameterTypes();
        Class[] signatureExpected = new Class[] {Object.class};
        assertEquals("Wrong class", theMethod.getDeclaringClass().getName(), FakeInterface.class.getName()); //$NON-NLS-1$
        helpAssertSameMethodSignature("Found wrong method signature", signatureExpected, signatureFound); //$NON-NLS-1$
    }

    @Test public void testFindBestMethodWithSignature_LongObjectAndMethodName() throws Exception {
        ReflectionHelper helper = new ReflectionHelper(FakeInterface.class);
        Class[] signatureSought = new Class[] {Long.class};
        Method theMethod = helper.findBestMethodWithSignature("methodObject", signatureSought ); //$NON-NLS-1$
        assertNotNull("Failed to find method for args: " + signatureSought, theMethod); //$NON-NLS-1$

        // make sure we got the method we expected
        Class[] signatureFound = theMethod.getParameterTypes();
        Class[] signatureExpected = new Class[] {Object.class};
        assertEquals("Wrong class", theMethod.getDeclaringClass().getName(), FakeInterface.class.getName()); //$NON-NLS-1$
        helpAssertSameMethodSignature("Found wrong method signature", signatureExpected, signatureFound); //$NON-NLS-1$
    }

    @Test public void testFindBestMethodWithSignature_longAndMethodName() throws Exception {
        ReflectionHelper helper = new ReflectionHelper(FakeInterface.class);
        Class[] signatureSought = new Class[] {Long.TYPE};
        Method theMethod = helper.findBestMethodWithSignature("method_long", signatureSought ); //$NON-NLS-1$
        assertNotNull("Failed to find method for args: " + signatureSought, theMethod); //$NON-NLS-1$

        // make sure we got the method we expected
        Class[] signatureFound = theMethod.getParameterTypes();
        Class[] signatureExpected = signatureSought;
        assertEquals("Wrong class", theMethod.getDeclaringClass().getName(), FakeInterface.class.getName()); //$NON-NLS-1$
        helpAssertSameMethodSignature("Found wrong method signature", signatureExpected, signatureFound); //$NON-NLS-1$
    }

    //  ===============================================================================================
    //   Test 2-arg methods
    //  ===============================================================================================
    @Test public void testFindBestMethodWithSignature_2ArgIntegerObjectAndMethodName() throws Exception {
        ReflectionHelper helper = new ReflectionHelper(FakeInterface.class);
        Class[] signatureSought = new Class[] {Integer.class, Integer.class};
        Method theMethod = helper.findBestMethodWithSignature("twoArgMethod_Object_Object", signatureSought ); //$NON-NLS-1$
        assertNotNull("Failed to find method for args: " + signatureSought, theMethod); //$NON-NLS-1$

        // make sure we got the method we expected
        Class[] signatureFound = theMethod.getParameterTypes();
        Class[] signatureExpected = new Class[] {Object.class, Object.class};
        assertEquals("Wrong class", theMethod.getDeclaringClass().getName(), FakeInterface.class.getName()); //$NON-NLS-1$
        helpAssertSameMethodSignature("Found wrong method signature", signatureExpected, signatureFound); //$NON-NLS-1$
    }

    @Test public void testFindBestMethodWithSignature_2ArgLongObjectAndMethodName() throws Exception {
        ReflectionHelper helper = new ReflectionHelper(FakeInterface.class);
        Class[] signatureSought = new Class[] {Long.class, Long.class};
        Method theMethod = helper.findBestMethodWithSignature("twoArgMethod_Object_Object", signatureSought ); //$NON-NLS-1$
        assertNotNull("Failed to find method for args: " + signatureSought, theMethod); //$NON-NLS-1$

        // make sure we got the method we expected
        Class[] signatureFound = theMethod.getParameterTypes();
        Class[] signatureExpected = new Class[] {Object.class, Object.class};
        assertEquals("Wrong class", theMethod.getDeclaringClass().getName(), FakeInterface.class.getName()); //$NON-NLS-1$
        helpAssertSameMethodSignature("Found wrong method signature", signatureExpected, signatureFound); //$NON-NLS-1$
    }

    @Test public void testFindBestMethodWithSignature_2ArgSerializableNumberAndMethodName() throws Exception {
        ReflectionHelper helper = new ReflectionHelper(FakeInterface.class);
        Class[] signatureSought = new Class[] {Long.class, Long.class};
        Method theMethod = helper.findBestMethodWithSignature("twoArgMethod_Serializable_Number", signatureSought ); //$NON-NLS-1$
        assertNotNull("Failed to find method for args: " + signatureSought, theMethod); //$NON-NLS-1$

        // make sure we got the method we expected
        Class[] signatureFound = theMethod.getParameterTypes();
        Class[] signatureExpected = new Class[] {Serializable.class, Number.class};
        assertEquals("Wrong class", theMethod.getDeclaringClass().getName(), FakeInterface.class.getName()); //$NON-NLS-1$
        helpAssertSameMethodSignature("Found wrong method signature", signatureExpected, signatureFound); //$NON-NLS-1$
    }

    //  ===============================================================================================
    //   Test overridden methods
    //  ===============================================================================================
    @Test public void testFindBestMethodWithSignature_SubInterface_2ArgSerializableAndNumber() throws Exception {
        ReflectionHelper helper = new ReflectionHelper(FakeSubInterface.class);
        Class[] signatureSought = new Class[] {Serializable.class, Number.class};
        Method theMethod = helper.findBestMethodWithSignature("method", signatureSought ); //$NON-NLS-1$
        assertNotNull("Failed to find method for args: " + signatureSought, theMethod); //$NON-NLS-1$

        // make sure we got the method we expected
        Class[] signatureFound = theMethod.getParameterTypes();
        Class[] signatureExpected = signatureSought;
        assertEquals("Wrong class", theMethod.getDeclaringClass().getName(), FakeSubInterface.class.getName()); //$NON-NLS-1$
        helpAssertSameMethodSignature("Found wrong method signature", signatureExpected, signatureFound); //$NON-NLS-1$
    }

    @Test public void testFindBestMethodWithSignature_SubInterface_2ArgSerializableAndLong() throws Exception {
        ReflectionHelper helper = new ReflectionHelper(FakeSubInterface.class);
        Class[] signatureSought = new Class[] {Serializable.class, Long.class};
        Method theMethod = helper.findBestMethodWithSignature("method", signatureSought ); //$NON-NLS-1$
        assertNotNull("Failed to find method for args: " + signatureSought, theMethod); //$NON-NLS-1$

        // make sure we got the method we expected
        Class[] signatureFound = theMethod.getParameterTypes();
        Class[] signatureExpected = new Class[] {Serializable.class, Long.class};
        assertEquals("Wrong class", theMethod.getDeclaringClass().getName(), FakeSubInterface.class.getName()); //$NON-NLS-1$
        helpAssertSameMethodSignature("Found wrong method signature", signatureExpected, signatureFound); //$NON-NLS-1$
    }
    
    @Test public void testCreate() throws Exception {
    	ReflectionHelper.create(SomeClass.class.getName(), Arrays.asList(true), null);
    }
    
    /**
     * Test base interface
     */
    public interface FakeInterface {
    	
        void method(String arg);
        void method(Serializable arg);
        void method(Object arg);
        void method(String[] arg);
        void method(List arg);
        void method(Integer arg);
        void method(long arg);
        void method(Serializable arg1, Number arg2);

        void methodString(String arg);
        void methodSerializable(Serializable arg);
        void methodObject(Object arg);
        void methodStringArray(String[] arg);
        void methodList(List arg);
        void methodInteger(Integer arg);
        void method_long(long arg);

        void twoArgMethod_Object_Object(Object arg1, Object arg2);
        void twoArgMethod_Serializable_Number(Serializable arg1, Number arg2);
    }

    /**
     * Test sub interface
     */
    public interface FakeSubInterface extends FakeInterface {

        void method(Number arg1, Long arg2);
        void method(Serializable arg1, Number arg2);
        void method(Serializable arg1, Long arg2);
    }
    
    public static class SomeClass {
    	public SomeClass(boolean primArg) {
		}
    }
}
