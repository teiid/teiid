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

/*
 * Date: Sep 25, 2003
 * Time: 1:18:10 PM
 */
package com.metamatrix.api.exception;

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
        assertEquals("0", err.getCode()); //$NON-NLS-1$
        assertNull(err.getMessage());
        
    }

    public void testMetaMatrixRuntimeExceptionWithMessage() {
        final MetaMatrixRuntimeException err = new MetaMatrixRuntimeException("Test"); //$NON-NLS-1$
        assertNull(err.getChild());
        assertEquals("0", err.getCode()); //$NON-NLS-1$
        assertEquals("Test", err.getMessage()); //$NON-NLS-1$
        
    }

    public void testMetaMatrixRuntimeExceptionWithCodeAndMessage() {
        final String code = "1234"; //$NON-NLS-1$
        final MetaMatrixRuntimeException err = new MetaMatrixRuntimeException(code, "Test"); //$NON-NLS-1$
        assertNull(err.getChild());
        assertEquals(code, err.getCode());
        assertEquals("Test", err.getMessage()); //$NON-NLS-1$
        
    }

    public void testMetaMatrixRuntimeExceptionWithExceptionAndMessage() {
        final String code = "1234"; //$NON-NLS-1$
        final MetaMatrixRuntimeException child = new MetaMatrixRuntimeException(code, "Child"); //$NON-NLS-1$
        final MetaMatrixRuntimeException err = new MetaMatrixRuntimeException(child, "Test"); //$NON-NLS-1$
        assertSame(child, err.getChild());
        assertEquals(code, err.getCode());
        assertEquals("Test", err.getMessage()); //$NON-NLS-1$
        
    }

    public void testMetaMatrixRuntimeExceptionWithExceptionAndCodeAndMessage() {
        final String code = "1234"; //$NON-NLS-1$
        final MetaMatrixRuntimeException child = new MetaMatrixRuntimeException(code, "Child"); //$NON-NLS-1$
        final MetaMatrixRuntimeException err = new MetaMatrixRuntimeException(child, "Code", "Test"); //$NON-NLS-1$ //$NON-NLS-2$
        assertSame(child, err.getChild());
        assertEquals("Code", err.getCode()); //$NON-NLS-1$
        assertEquals("Test", err.getMessage()); //$NON-NLS-1$
        
    }
}