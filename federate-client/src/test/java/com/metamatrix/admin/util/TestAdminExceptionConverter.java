/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.admin.util;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import com.metamatrix.admin.api.exception.AdminComponentException;
import com.metamatrix.admin.api.exception.AdminException;
import com.metamatrix.admin.api.exception.AdminProcessingException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.api.exception.MultipleException;
import com.metamatrix.api.exception.MultipleRuntimeException;
import com.metamatrix.core.MetaMatrixCoreException;

/** 
 * @since 4.3
 */
public class TestAdminExceptionConverter extends TestCase {

    private static final String LEVEL_0_MSG = "We did a thing."; //$NON-NLS-1$
    private static final String LEVEL_1_MSG = "We did a bad thing."; //$NON-NLS-1$
    private static final String LEVEL_2_MSG = "We did a bad, bad thing."; //$NON-NLS-1$
    private static final String LEVEL_3_MSG = "We did a bad, bad, bad thing."; //$NON-NLS-1$
    private static final String LEVEL_4_MSG = "We did a bad, bad, bad, bad thing."; //$NON-NLS-1$

    /** 
     * 
     * @since 4.3
     */
    public TestAdminExceptionConverter() {
        super();
    }

    /** 
     * @param name
     * @since 4.3
     */
    public TestAdminExceptionConverter(String name) {
        super(name);
    }
    
    /**
     * Test AdminProcessingException - 1 level deep. 
     * @throws Exception
     * @since 4.3
     */
    public void testConvertProcessingException_Level_1() throws Exception {
        AdminProcessingException root = new AdminProcessingException(LEVEL_1_MSG);

        AdminProcessingException actual = AdminExceptionConverter.convertToProcessingException(root);
        
        helpTestEquals(root, actual);
    }

    /**
     * Test AdminProcessingException - 2 level deep. 
     * @throws Exception
     * @since 4.3
     */
    public void testConvertProcessingException_Level_2() throws Exception {
        AdminProcessingException root = new AdminProcessingException(LEVEL_2_MSG);

        AdminProcessingException top = new AdminProcessingException(LEVEL_1_MSG);
        top.initCause(root);
        AdminProcessingException actual = AdminExceptionConverter.convertToProcessingException(top);
        
        helpTestEquals(root, actual);
    }

    /**
     * Test AdminProcessingException - 3 level deep. 
     * @throws Exception
     * @since 4.3
     */
    public void testConvertProcessingException_Level_3() throws Exception {
        AdminProcessingException root = new AdminProcessingException(LEVEL_3_MSG);

        AdminProcessingException top = new AdminProcessingException(LEVEL_1_MSG);
        AdminProcessingException inbetween = new AdminProcessingException(LEVEL_2_MSG);
        top.initCause(inbetween);
        inbetween.initCause(root);
        AdminProcessingException actual = AdminExceptionConverter.convertToProcessingException(top);
        
        helpTestEquals(root, actual);
    }

    /**
     * Test AdminProcessingException - 4 level deep. 
     * @throws Exception
     * @since 4.3
     */
    public void testConvertProcessingException_Level_4() throws Exception {
        AdminProcessingException root = new AdminProcessingException(LEVEL_4_MSG);

        AdminProcessingException top = new AdminProcessingException(LEVEL_1_MSG);
        AdminProcessingException inbetween1 = new AdminProcessingException(LEVEL_3_MSG);
        top.initCause(inbetween1);
        AdminProcessingException inbetween2 = new AdminProcessingException(LEVEL_2_MSG);
        inbetween1.initCause(inbetween2);
        inbetween2.initCause(root);
        AdminProcessingException actual = AdminExceptionConverter.convertToProcessingException(top);
        
        helpTestEquals(root, actual);
    }

    /**
     * testConvertComponentExceptionAsProcessingException. 
     * @throws Exception
     * @since 4.3
     */
    public void testConvertComponentExceptionAsProcessingException() throws Exception {
        AdminComponentException root = new AdminComponentException(LEVEL_1_MSG);

        AdminProcessingException actual = AdminExceptionConverter.convertToProcessingException(root);
        
        helpTestNotEquals(root, actual);
    }
    
    /**
     * testConvertProcessingExceptionAsComponentException. 
     * @throws Exception
     * @since 4.3
     */
    public void testConvertProcessingExceptionAsComponentException() throws Exception {
        AdminProcessingException root = new AdminProcessingException(LEVEL_1_MSG);

        AdminComponentException actual = AdminExceptionConverter.convertToComponentException(root);
        
        helpTestNotEquals(root, actual);
    }

    /**
     * Test AdminComponentException - 1 level deep. 
     * @throws Exception
     * @since 4.3
     */
    public void testConvertComponentException_Level_1() throws Exception {
        AdminComponentException root = new AdminComponentException(LEVEL_1_MSG);

        AdminComponentException actual = AdminExceptionConverter.convertToComponentException(root);
        
        helpTestEquals(root, actual);
    }

    /**
     * Test AdminComponentException - 2 level deep. 
     * @throws Exception
     * @since 4.3
     */
    public void testConvertComponentException_Level_2() throws Exception {
        AdminComponentException root = new AdminComponentException(LEVEL_2_MSG);

        AdminComponentException top = new AdminComponentException(LEVEL_1_MSG);
        top.initCause(root);
        AdminComponentException actual = AdminExceptionConverter.convertToComponentException(top);
        
        helpTestEquals(root, actual);
    }

    /**
     * Test AdminComponentException - 3 level deep. 
     * @throws Exception
     * @since 4.3
     */
    public void testConvertComponentException_Level_3() throws Exception {
        AdminComponentException root = new AdminComponentException(LEVEL_3_MSG);

        AdminComponentException top = new AdminComponentException(LEVEL_1_MSG);
        AdminComponentException inbetween = new AdminComponentException(LEVEL_2_MSG);
        top.initCause(inbetween);
        inbetween.initCause(root);
        AdminComponentException actual = AdminExceptionConverter.convertToComponentException(top);
        
        helpTestEquals(root, actual);
    }

    /**
     * Test AdminComponentException - 4 level deep. 
     * @throws Exception
     * @since 4.3
     */
    public void testConvertComponentException_Level_4() throws Exception {
        AdminComponentException root = new AdminComponentException(LEVEL_4_MSG);

        AdminComponentException top = new AdminComponentException(LEVEL_1_MSG);
        AdminComponentException inbetween1 = new AdminComponentException(LEVEL_3_MSG);
        top.initCause(inbetween1);
        AdminComponentException inbetween2 = new AdminComponentException(LEVEL_2_MSG);
        inbetween1.initCause(inbetween2);
        inbetween2.initCause(root);
        AdminComponentException actual = AdminExceptionConverter.convertToComponentException(top);
        
        helpTestEquals(root, actual);
    }

    /**
     * Test that we can convert from a MetaMatrix exception. 
     * @throws Exception
     * @since 4.3
     */
    public void testMetaMatrixExceptionConversion() throws Exception {
        // Next 2 statements MUST be on same line for stack traces to compare!
        AdminException expecting = new AdminComponentException(LEVEL_2_MSG); MetaMatrixException root = new MetaMatrixException(LEVEL_2_MSG);
        
        AdminComponentException actual = AdminExceptionConverter.convertToComponentException(root);
        
        helpTestSimilar(expecting, actual);
    }
    
    /**
     * Test that we can convert from a MetaMatrix exception. 
     * @throws Exception
     * @since 4.3
     */
    public void testMetaMatrixComponentException() throws Exception {
        // Next 2 statements MUST be on same line for stack traces to compare!
        AdminException expecting = new AdminComponentException(LEVEL_2_MSG); MetaMatrixComponentException root = new MetaMatrixComponentException(LEVEL_2_MSG);
        
        AdminComponentException actual = AdminExceptionConverter.convertToComponentException(root);
        
        helpTestSimilar(expecting, actual);
    }
    
    /**
     * Test that we can convert from a MetaMatrixCoreException exception. 
     * @throws Exception
     * @since 4.3
     */
    public void testMetaMatrixCoreExceptionConversion() throws Exception {
        // Next 2 statements MUST be on same line for stack traces to compare!
        AdminException expecting = new AdminComponentException(LEVEL_2_MSG); MetaMatrixCoreException root = new MetaMatrixCoreException(LEVEL_2_MSG);
        
        AdminComponentException actual = AdminExceptionConverter.convertToComponentException(root);
        
        helpTestSimilar(expecting, actual);
    }
    
    
    /**
     * Test that we can convert from a BogusException (unknown definition) exception. 
     * @throws Exception
     * @since 4.3
     */
    public void testMetaBogusExceptionConversion() throws Exception {
        // Next 2 statements MUST be on same line for stack traces to compare!
        AdminException expecting = new AdminComponentException(LEVEL_2_MSG); BogusException root = new BogusException(LEVEL_2_MSG);
        
        AdminComponentException actual = AdminExceptionConverter.convertToComponentException(root);
        
        helpTestSimilar(expecting, actual);
    }
    
    /**
     * Test that MetaMatrix multiple exceptions convert properly 
     * @throws Exception
     * @since 4.3
     */
    public void testMetaMatrixMultipleExceptionConversion() throws Exception {
        MultipleException root;
        List rootChildren = new ArrayList();
        
        AdminComponentException expecting;
        
        // Next 2 statements MUST be on same line for stack traces to compare!
        root = new MultipleException(LEVEL_0_MSG); expecting = new AdminComponentException(LEVEL_0_MSG);
        
        // These next statements MUST be on the same lines for stack traces to compare!
        expecting.addChild(new AdminComponentException(LEVEL_1_MSG));  rootChildren.add(new MetaMatrixComponentException(LEVEL_1_MSG));
        expecting.addChild(new AdminComponentException(LEVEL_2_MSG));  rootChildren.add(new MetaMatrixComponentException(LEVEL_2_MSG));
        expecting.addChild(new AdminComponentException(LEVEL_3_MSG));  rootChildren.add(new MetaMatrixComponentException(LEVEL_3_MSG));
        
        root.setExceptions(rootChildren);
        AdminComponentException actual = AdminExceptionConverter.convertToComponentException(root);
        
        helpTestEquals(expecting, actual);
        
        // That only tested top-level exception
        // Now test children
        List expectedChildren = expecting.getChildren();
        rootChildren = root.getExceptions();
        
        assertTrue(expectedChildren.size() == rootChildren.size());
        for ( int i=0; i<rootChildren.size(); ++i) {
            helpTestSimilar((Throwable)expectedChildren.get(i), (Throwable)rootChildren.get(i));
        }
        
    }

    /**
     * Test that MetaMatrix multiple runtime exceptions convert properly 
     * @throws Exception
     * @since 4.3
     */
    public void testMetaMatrixMultipleRuntimeExceptionConversion() throws Exception {
        MultipleRuntimeException root;
        List rootChildren = new ArrayList();
        
        AdminComponentException expecting;
        
        // Next 2 statements MUST be on same line for stack traces to compare!
        root = new MultipleRuntimeException(LEVEL_0_MSG); expecting = new AdminComponentException(LEVEL_0_MSG);
        
        // These next statements MUST be on the same lines for stack traces to compare!
        expecting.addChild(new AdminComponentException(LEVEL_1_MSG));  rootChildren.add(new MetaMatrixComponentException(LEVEL_1_MSG));
        expecting.addChild(new AdminComponentException(LEVEL_2_MSG));  rootChildren.add(new MetaMatrixComponentException(LEVEL_2_MSG));
        expecting.addChild(new AdminComponentException(LEVEL_3_MSG));  rootChildren.add(new MetaMatrixComponentException(LEVEL_3_MSG));
        
        root.setThrowables(rootChildren);
        AdminComponentException actual = AdminExceptionConverter.convertToComponentException(root);
        
        helpTestEquals(expecting, actual);
        
        // That only tested top-level exception
        // Now test children
        List expectedChildren = expecting.getChildren();
        rootChildren = root.getThrowables();
        
        assertTrue(expectedChildren.size() == rootChildren.size());
        for ( int i=0; i<rootChildren.size(); ++i) {
            helpTestSimilar((Throwable)expectedChildren.get(i), (Throwable)rootChildren.get(i));
        }
    }
    /** Test message created by converter */
    public void testConvertToComponentException1() {
    	MetaMatrixComponentException orig = new MetaMatrixComponentException(LEVEL_1_MSG);
    	AdminComponentException e = AdminExceptionConverter.convertToComponentException(orig, LEVEL_1_MSG);
    	assertEquals(LEVEL_1_MSG, e.getMessage());
    }
    
    /** Test message created by converter */
    public void testConvertToComponentException2() {
    	MetaMatrixComponentException orig = new MetaMatrixComponentException(LEVEL_1_MSG);
    	AdminComponentException e = AdminExceptionConverter.convertToComponentException(orig, null);
    	assertEquals("com.metamatrix.api.exception.MetaMatrixComponentException: " + LEVEL_1_MSG , e.getMessage()); //$NON-NLS-1$
    }
    
    /** Test message created by converter */
    public void testConvertToComponentException3() {
    	MetaMatrixComponentException orig = new MetaMatrixComponentException((String)null);
    	AdminComponentException e = AdminExceptionConverter.convertToComponentException(orig, LEVEL_2_MSG);
    	assertEquals(LEVEL_2_MSG , e.getMessage());
    }
    
    /** Test message created by converter */
    public void testConvertToComponentException4() {
    	MetaMatrixComponentException orig = new MetaMatrixComponentException(LEVEL_1_MSG);
    	AdminComponentException e = AdminExceptionConverter.convertToComponentException(orig, LEVEL_2_MSG);
    	assertEquals(LEVEL_2_MSG + ": " + LEVEL_1_MSG , e.getMessage()); //$NON-NLS-1$
    }

//==================================================================================================
//    Helpers
//==================================================================================================

    /** 
     * Check that <i>all</i> parts of two throwables are equal:
     * <ul>
     *   <li>error msg</li>
     *   <li>stacktrace</li>
     *   <li>class names</li>
     * </ul>
     * @param expected
     * @param actual
     * @since 4.3
     */
    private void helpTestEquals(Throwable expected,
                                Throwable actual) {
        assertTrue("Actual's message should contain expected's message", actual.getMessage().indexOf(expected.getMessage()) >= 0);  //$NON-NLS-1$
        String expectedStack = getStackTrace(expected);
        String actualStack = getStackTrace(actual);
        assertEquals(expectedStack, actualStack);
        assertEquals(expected.getClass().getName(), actual.getClass().getName());
    }

    /** 
     * Check that the only difference in the throwables is the class names and the message.
     * Class names are not compared directly.
     * Checks that actual's message contains expected's message.
     * The first line of the stacktrace is filtered because it contains the
     * class name of the exception.  It also contains the error msg, but those
     * <i>are</i> compared via getMessage().
     * @param expected
     * @param actual
     * @since 4.3
     */
    private void helpTestSimilar(Throwable expected,
                                Throwable actual) {
        assertTrue("Actual's message should contain expected's message", actual.getMessage().indexOf(expected.getMessage()) >= 0);  //$NON-NLS-1$
                
        // Compare stack lines separately
        // Size (# lines) is compared for all lines
        // Stack lines are compared except first, which will contain different Exception class names
        String expectedStack = getStackTrace(expected);
        String actualStack = getStackTrace(actual);
        String[] expectedStackLinesSansFirst = expectedStack.split(System.getProperty("line.separator")); //$NON-NLS-1$
        String[] actualStackLinesSansFirst = actualStack.split(System.getProperty("line.separator")); //$NON-NLS-1$
        assertEquals(expectedStackLinesSansFirst.length, actualStackLinesSansFirst.length);
        for ( int i=1; i<expectedStackLinesSansFirst.length; i++) {
            // i set to 1 to start because we don't want to test first lines
            // for equality since they will contain different class names.
            // Note that 1st line also contains the msg, but that's tested above.
            assertEquals(expectedStackLinesSansFirst[i], actualStackLinesSansFirst[i]);
        }
    }

    /** 
     * Check that, although the exception msgs are equal, 
     * the classes and stacktraces are not.
     * Checks that actual's message contains expected's message and class name.
     * @param expected
     * @param actual
     * @since 4.3
     */
    private void helpTestNotEquals(Throwable expected,
                                Throwable actual) {
        assertTrue("Actual's message should contain expected's class name",   //$NON-NLS-1$
                   actual.getMessage().indexOf(expected.getClass().getName()) >= 0);
        assertTrue("Actual's message should contain expected's message", actual.getMessage().indexOf(expected.getMessage()) >= 0);  //$NON-NLS-1$
        String expectedStack = getStackTrace(expected);
        String actualStack = getStackTrace(actual);
        assertNotSame(expectedStack, actualStack);
        assertNotSame(expected.getClass().getName(), actual.getClass().getName());
    }

    /**
     * Get stacktrace as a string for comparing. 
     * @param t a Throwable
     * @return the Stringified stacktrace.
     * @since 4.3
     */
    private static String getStackTrace( final Throwable t ) {
        final ByteArrayOutputStream bas = new ByteArrayOutputStream();
        final PrintWriter pw = new PrintWriter(bas);
        t.printStackTrace(pw);
        pw.close();
        return bas.toString();
    }

}
