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
package org.teiid.core;

import org.teiid.core.TeiidRuntimeException;

import junit.framework.TestCase;


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
            new TeiidRuntimeException((String)null);  // should throw NPE
            fail("Should not get here"); //$NON-NLS-1$
        } catch ( Throwable ex ) {
            e = ex;
        }
        assertNotNull(e);
    }

    public void testMetaMatrixRuntimeExceptionWithNullThrowable() {
        final TeiidRuntimeException err = new TeiidRuntimeException((Throwable)null);
        assertNull(err.getCause());
        assertNull(err.getCode()); 
        assertNull(err.getMessage());
        
    }

    public void testMetaMatrixRuntimeExceptionWithMessage() {
        final TeiidRuntimeException err = new TeiidRuntimeException("Test"); //$NON-NLS-1$
        assertNull(err.getCause());
        assertNull(err.getCode());
        assertEquals("Test", err.getMessage()); //$NON-NLS-1$
        
    }

    public void testMetaMatrixRuntimeExceptionWithCodeAndMessage() {
        final String code = "1234"; //$NON-NLS-1$
        final TeiidRuntimeException err = new TeiidRuntimeException(code, "Test"); //$NON-NLS-1$
        assertNull(err.getCause());
        assertEquals(code, err.getCode());
        assertEquals("Test", err.getMessage()); //$NON-NLS-1$
        
    }

    public void testMetaMatrixRuntimeExceptionWithExceptionAndMessage() {
        final String code = "1234"; //$NON-NLS-1$
        final TeiidRuntimeException child = new TeiidRuntimeException(code, "Child"); //$NON-NLS-1$
        final TeiidRuntimeException err = new TeiidRuntimeException(child, "Test"); //$NON-NLS-1$
        assertSame(child, err.getCause());
        assertEquals(code, err.getCode());
        assertEquals("Test", err.getMessage()); //$NON-NLS-1$
        
    }

    public void testMetaMatrixRuntimeExceptionWithExceptionAndCodeAndMessage() {
        final String code = "1234"; //$NON-NLS-1$
        final TeiidRuntimeException child = new TeiidRuntimeException(code, "Child"); //$NON-NLS-1$
        final TeiidRuntimeException err = new TeiidRuntimeException(child, "Code", "Test"); //$NON-NLS-1$ //$NON-NLS-2$
        assertSame(child, err.getCause());
        assertEquals("Code", err.getCode()); //$NON-NLS-1$
        assertEquals("Test", err.getMessage()); //$NON-NLS-1$
        
    }
}