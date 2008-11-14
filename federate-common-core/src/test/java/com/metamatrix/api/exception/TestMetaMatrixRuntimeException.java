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

/*
 * Date: Sep 25, 2003
 * Time: 1:18:10 PM
 */
package com.metamatrix.api.exception;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import junit.framework.TestCase;

import com.metamatrix.core.MetaMatrixRuntimeException;

/**
 * JUnit test for MetaMatrixRuntimeException
 */
public final class TestMetaMatrixRuntimeException extends TestCase {
    // =========================================================================
    //                        F R A M E W O R K
    // =========================================================================
    /**
     * Constructor for TestMetaMatrixRuntimeException.
     * @param name
     */
    public TestMetaMatrixRuntimeException(final String name) {
        super(name);
    }

    // =========================================================================
    //                         T E S T   C A S E S
    // =========================================================================

    private static List iterateHierarchy(MetaMatrixRuntimeException e){
        Iterator iter = e.getChildren();
        List result = new ArrayList();
        while (iter.hasNext()){
            result.add(iter.next());//could throw ClassCastException
        }
        return result;
    }

    private static List checkHierarchyManually(MetaMatrixRuntimeException e){
        List result = new ArrayList();
        Throwable throwable = e;
        while (throwable != null){
            if(throwable instanceof MetaMatrixRuntimeException) {
                Throwable child = ((MetaMatrixRuntimeException)throwable).getChild();
                if (child != null){
                    result.add(child);
                }
                throwable = child;
            } else if(throwable instanceof MetaMatrixRuntimeException) {
                Throwable child = ((MetaMatrixRuntimeException)throwable).getChild();
                if (child != null){
                    result.add(child);
                }
                throwable = child;
            } else {
                throwable = null;
            }
        }
        return result;
    }

    /**
     * Purposely try to iterate past the available Objects, triggering
     * a NoSuchElementException
     * @throws java.util.NoSuchElementException
     */
    private static void failIterateHierarchy(MetaMatrixRuntimeException e){
        Iterator iter = e.getChildren();
        while (true){
            iter.next();
            //System.out.println("<!><!> " + iter.next());
        }
    }
    // =========================================================================
    //                         T E S T   C A S E S
    // =========================================================================

    /**
     * Test the
     * {@link com.metamatrix.api.exception.MetaMatrixRuntimeException#getChildren children Iterator}
     * for a simple MetaMatrixRuntimeException with no nested children.
     */
    public void testIteratorForSimpleException(){
        MetaMatrixRuntimeException e = new MetaMatrixRuntimeException("Test"); //$NON-NLS-1$
        assertEquals("Iterator enumerates exceptions the same as manually drilling down: ", //$NON-NLS-1$
                iterateHierarchy(e), checkHierarchyManually(e));
    }

    /**
     * Test the
     * {@link com.metamatrix.api.exception.MetaMatrixRuntimeException#getChildren children Iterator}
     * for a MetaMatrixRuntimeException with many nested children, ending in a non-
     * MetaMatrixRuntimeException Throwable
     */
    public void testIteratorForNestedExceptions(){
        Throwable e1 = new ArithmeticException("Test1"); //$NON-NLS-1$
        MetaMatrixRuntimeException e = new MetaMatrixRuntimeException(e1, "Test2"); //$NON-NLS-1$
        e = new MetaMatrixRuntimeException(e, "Test3"); //$NON-NLS-1$
        e = new MetaMatrixRuntimeException(e, "Test4"); //$NON-NLS-1$
        e = new MetaMatrixRuntimeException(e, "Test5"); //$NON-NLS-1$
        assertEquals("Iterator enumerates exceptions the same as manually drilling down: ", //$NON-NLS-1$
                iterateHierarchy(e), checkHierarchyManually(e));
    }

    /**
     * Test the
     * {@link com.metamatrix.api.exception.MetaMatrixRuntimeException#getChildren children Iterator}
     * for a MetaMatrixRuntimeException with many nested MetaMatrixRuntimeException children
     */
    public void testIteratorForNestedMetaMatrixRuntimeException(){
        MetaMatrixRuntimeException e = new MetaMatrixRuntimeException("Test1"); //$NON-NLS-1$
        e = new MetaMatrixRuntimeException(e, "Test2"); //$NON-NLS-1$
        e = new MetaMatrixRuntimeException(e, "Test3"); //$NON-NLS-1$
        e = new MetaMatrixRuntimeException(e, "Test4"); //$NON-NLS-1$
        assertEquals("Iterator enumerates exceptions the same as manually drilling down: ", //$NON-NLS-1$
                iterateHierarchy(e), checkHierarchyManually(e));
    }

    /**
     * Test the
     * {@link com.metamatrix.api.exception.MetaMatrixRuntimeException#getChildren children Iterator}
     * for a MetaMatrixRuntimeException with many nested MetaMatrixRuntimeException or
     * MetaMatrixRuntimeExceptino children
     */
    public void testIteratorForMixedExceptions(){
        MetaMatrixRuntimeException e = new MetaMatrixRuntimeException("Test1"); //$NON-NLS-1$
        e = new MetaMatrixRuntimeException(e, "Test2"); //$NON-NLS-1$
        MetaMatrixRuntimeException ee = new MetaMatrixRuntimeException(e, "Test3"); //$NON-NLS-1$
        e = new MetaMatrixRuntimeException(ee, "Test4"); //$NON-NLS-1$
        assertEquals("Iterator enumerates exceptions the same as manually drilling down: ", //$NON-NLS-1$
                iterateHierarchy(e), checkHierarchyManually(e));
    }

    /**
     * Tests that the
     * {@link com.metamatrix.api.exception.MetaMatrixException#getChildren children Iterator}
     * fails properly by throwing a NoSuchElementException if the
     * <code>next()</code> method is called after <code>hasNext()</code> returns
     * false
     */
    public void testIteratorFailsProperly(){
        MetaMatrixRuntimeException e = new MetaMatrixRuntimeException("Test1"); //$NON-NLS-1$
        e = new MetaMatrixRuntimeException(e, "Test2"); //$NON-NLS-1$
        NoSuchElementException result = null;
        try{
            failIterateHierarchy(e);
        } catch (NoSuchElementException ee){
            result = ee;
        }
        assertNotNull("Iterator failed properly with NoSuchElementException: ", result); //$NON-NLS-1$
    }

    public void testFailMetaMatrixRuntimeExceptionWithNullMessage() {
        Throwable e = null;
        try {
            new MetaMatrixRuntimeException((String)null);  // should throw NPE
            fail("Should not get here"); //$NON-NLS-1$
        } catch ( Throwable ex ) {
            e = ex;
        }
        assertNotNull(e);
    }

    public void testMetaMatrixRuntimeExceptionWithNullThrowable() {
        final MetaMatrixRuntimeException err = new MetaMatrixRuntimeException((Throwable)null);
        assertNull(err.getChild());
        assertTrue(!err.getChildren().hasNext());
        assertEquals("0", err.getCode()); //$NON-NLS-1$
        assertNull(err.getMessage());
        
    }

    public void testMetaMatrixRuntimeExceptionWithMessage() {
        final MetaMatrixRuntimeException err = new MetaMatrixRuntimeException("Test"); //$NON-NLS-1$
        assertNull(err.getChild());
        assertTrue(!err.getChildren().hasNext());
        assertEquals("0", err.getCode()); //$NON-NLS-1$
        assertEquals("Test", err.getMessage()); //$NON-NLS-1$
        
    }

    public void testMetaMatrixRuntimeExceptionWithCodeAndMessage() {
        final String code = "1234"; //$NON-NLS-1$
        final MetaMatrixRuntimeException err = new MetaMatrixRuntimeException(code, "Test"); //$NON-NLS-1$
        assertNull(err.getChild());
        assertTrue(!err.getChildren().hasNext());
        assertEquals(code, err.getCode());
        assertEquals("Test", err.getMessage()); //$NON-NLS-1$
        
    }

    public void testMetaMatrixRuntimeExceptionWithExceptionAndMessage() {
        final String code = "1234"; //$NON-NLS-1$
        final MetaMatrixRuntimeException child = new MetaMatrixRuntimeException(code, "Child"); //$NON-NLS-1$
        final MetaMatrixRuntimeException err = new MetaMatrixRuntimeException(child, "Test"); //$NON-NLS-1$
        assertSame(child, err.getChild());
        final Iterator iter = err.getChildren();
        assertSame(child, iter.next());
        assertTrue(!iter.hasNext());
        assertEquals(code, err.getCode());
        assertEquals("Test", err.getMessage()); //$NON-NLS-1$
        
    }

    public void testMetaMatrixRuntimeExceptionWithExceptionAndCodeAndMessage() {
        final String code = "1234"; //$NON-NLS-1$
        final MetaMatrixRuntimeException child = new MetaMatrixRuntimeException(code, "Child"); //$NON-NLS-1$
        final MetaMatrixRuntimeException err = new MetaMatrixRuntimeException(child, "Code", "Test"); //$NON-NLS-1$ //$NON-NLS-2$
        assertSame(child, err.getChild());
        final Iterator iter = err.getChildren();
        assertSame(child, iter.next());
        assertTrue(!iter.hasNext());
        assertEquals("Code", err.getCode()); //$NON-NLS-1$
        assertEquals("Test", err.getMessage()); //$NON-NLS-1$
        
    }
}