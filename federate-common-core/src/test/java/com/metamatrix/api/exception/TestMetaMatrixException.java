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

package com.metamatrix.api.exception;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import junit.framework.TestCase;

import com.metamatrix.core.MetaMatrixRuntimeException;

/**
 * Tests the children Iterator of the MetaMatrixException.  Primarily it does
 * this by comparing two Lists for equality: one List is created by Iterating
 * using the
 * {@link com.metamatrix.api.exception.MetaMatrixException#getChildren Iterator},
 * the other List is created by manually using the
 * {@link com.metamatrix.api.exception.MetaMatrixException#getChild getChild}
 * method recursively.
 */
public class TestMetaMatrixException extends TestCase {

	// ################################## FRAMEWORK ################################

	public TestMetaMatrixException(String name) {
		super(name);
	}

	// ################################## TEST HELPERS ################################

    private static List iterateHierarchy(MetaMatrixException e){
        Iterator iter = e.getChildren();
        List result = new ArrayList();
        while (iter.hasNext()){
            result.add(iter.next());//could throw ClassCastException
        }
        return result;
    }

    private static List checkHierarchyManually(MetaMatrixException e){
        List result = new ArrayList();
        Throwable throwable = e;
        while (throwable != null){
            if(throwable instanceof MetaMatrixException) {
                Throwable child = ((MetaMatrixException)throwable).getChild();
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
    private static void failIterateHierarchy(MetaMatrixException e){
        Iterator iter = e.getChildren();
        while (true){
            iter.next();
        }
    }
	// ################################## ACTUAL TESTS ################################

    /**
     * Test the
     * {@link com.metamatrix.api.exception.MetaMatrixException#getChildren children Iterator}
     * for a simple MetaMatrixException with no nested children.
     */
    public void testIteratorForSimpleException(){
        MetaMatrixException e = new MetaMatrixException("Test"); //$NON-NLS-1$
        assertEquals("Iterator enumerates exceptions the same as manually drilling down: ", //$NON-NLS-1$
                iterateHierarchy(e), checkHierarchyManually(e));
    }

    /**
     * Test the
     * {@link com.metamatrix.api.exception.MetaMatrixException#getChildren children Iterator}
     * for a MetaMatrixException with many nested children, ending in a non-
     * MetaMatrixException Throwable
     */
    public void testIteratorForNestedExceptions(){
        Throwable e1 = new ArithmeticException("Test1"); //$NON-NLS-1$
        MetaMatrixException e = new ComponentNotAvailableException(e1, "Test2"); //$NON-NLS-1$
        e = new MetaMatrixException(e, "Test3"); //$NON-NLS-1$
        e = new MetaMatrixProcessingException(e, "Test4"); //$NON-NLS-1$
        e = new MetaMatrixException(e, "Test5"); //$NON-NLS-1$
        assertEquals("Iterator enumerates exceptions the same as manually drilling down: ", //$NON-NLS-1$
                iterateHierarchy(e), checkHierarchyManually(e));
    }

    /**
     * Test the
     * {@link com.metamatrix.api.exception.MetaMatrixException#getChildren children Iterator}
     * for a MetaMatrixException with many nested MetaMatrixException children
     */
    public void testIteratorForNestedMetaMatrixExceptions(){
        MetaMatrixException e = new ComponentNotFoundException("Test1"); //$NON-NLS-1$
        e = new MetaMatrixException(e, "Test2"); //$NON-NLS-1$
        e = new MetaMatrixProcessingException(e, "Test3"); //$NON-NLS-1$
        e = new MetaMatrixException(e, "Test4"); //$NON-NLS-1$
        assertEquals("Iterator enumerates exceptions the same as manually drilling down: ", //$NON-NLS-1$
                iterateHierarchy(e), checkHierarchyManually(e));
    }

    /**
     * Test the
     * {@link com.metamatrix.api.exception.MetaMatrixException#getChildren children Iterator}
     * for a MetaMatrixException with many nested MetaMatrixException or
     * MetaMatrixRuntimeExceptino children
     */
    public void testIteratorForMixedExceptions(){
        MetaMatrixException e = new ComponentNotFoundException("Test1"); //$NON-NLS-1$
        e = new MetaMatrixException(e, "Test2"); //$NON-NLS-1$
        MetaMatrixRuntimeException ee = new MetaMatrixRuntimeException(e, "Test3"); //$NON-NLS-1$
        e = new MetaMatrixProcessingException(ee, "Test4"); //$NON-NLS-1$
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
        MetaMatrixException e = new ComponentNotFoundException("Test1"); //$NON-NLS-1$
        e = new MetaMatrixException(e, "Test2"); //$NON-NLS-1$
        NoSuchElementException result = null;
        try{
            failIterateHierarchy(e);
        } catch (NoSuchElementException ee){
            result = ee;
        }
        assertNotNull("Iterator failed properly with NoSuchElementException: ", result); //$NON-NLS-1$
    }

    public void testFailMetaMatrixExceptionWithNullMessage() {
        Throwable e = null;
        try {
            new MetaMatrixException((String)null);  // should throw NPE
            fail("Should not get here"); //$NON-NLS-1$
        } catch ( Throwable ex ) {
            e = ex;
        }
        assertNotNull(e);
    }

    public void testMetaMatrixExceptionWithNullThrowable() {
        final MetaMatrixException err = new MetaMatrixException((Throwable)null);
        assertNull(err.getChild());
        assertTrue(!err.getChildren().hasNext());
        assertNull(err.getCode());
        assertNull(err.getMessage());
        
    }

    public void testMetaMatrixExceptionWithMessage() {
        final MetaMatrixException err = new MetaMatrixException("Test"); //$NON-NLS-1$
        assertNull(err.getChild());
        assertTrue(!err.getChildren().hasNext());
        assertNull(err.getCode());
        assertEquals("Test", err.getMessage()); //$NON-NLS-1$
        
    }

    public void testMetaMatrixExceptionWithCodeAndMessage() {
        final MetaMatrixException err = new MetaMatrixException("Code", "Test"); //$NON-NLS-1$ //$NON-NLS-2$
        assertNull(err.getChild());
        assertTrue(!err.getChildren().hasNext());
        assertEquals("Code", err.getCode()); //$NON-NLS-1$
        assertEquals("Test", err.getMessage()); //$NON-NLS-1$
        
    }

    public void testMetaMatrixExceptionWithExceptionAndMessage() {
        final MetaMatrixException child = new MetaMatrixException("propertyValuePhrase", "Child"); //$NON-NLS-1$ //$NON-NLS-2$
        final MetaMatrixException err = new MetaMatrixException(child, "Test"); //$NON-NLS-1$
        assertSame(child, err.getChild());
        final Iterator iter = err.getChildren();
        assertSame(child, iter.next());
        assertTrue(!iter.hasNext());
        assertEquals("propertyValuePhrase", err.getCode()); //$NON-NLS-1$
        assertEquals("Test", err.getMessage()); //$NON-NLS-1$
        
    }

    public void testMetaMatrixExceptionWithExceptionAndCodeAndMessage() {
        final MetaMatrixException child = new MetaMatrixException("propertyValuePhrase", "Child"); //$NON-NLS-1$ //$NON-NLS-2$
        final MetaMatrixException err = new MetaMatrixException(child, "Code", "Test"); //$NON-NLS-1$ //$NON-NLS-2$
        assertSame(child, err.getChild());
        final Iterator iter = err.getChildren();
        assertSame(child, iter.next());
        assertTrue(!iter.hasNext());
        assertEquals("Code", err.getCode()); //$NON-NLS-1$
        assertEquals("Test", err.getMessage()); //$NON-NLS-1$
        
    }
}
