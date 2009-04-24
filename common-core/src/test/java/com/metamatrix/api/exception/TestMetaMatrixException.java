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

package com.metamatrix.api.exception;

import junit.framework.TestCase;

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

	// ################################## ACTUAL TESTS ################################

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
        assertNull(err.getCode());
        assertNull(err.getMessage());
        
    }

    public void testMetaMatrixExceptionWithMessage() {
        final MetaMatrixException err = new MetaMatrixException("Test"); //$NON-NLS-1$
        assertNull(err.getChild());
        assertNull(err.getCode());
        assertEquals("Test", err.getMessage()); //$NON-NLS-1$
        
    }

    public void testMetaMatrixExceptionWithCodeAndMessage() {
        final MetaMatrixException err = new MetaMatrixException("Code", "Test"); //$NON-NLS-1$ //$NON-NLS-2$
        assertNull(err.getChild());
        assertEquals("Code", err.getCode()); //$NON-NLS-1$
        assertEquals("Error Code:Code Message:Test", err.getMessage()); //$NON-NLS-1$
        
    }

    public void testMetaMatrixExceptionWithExceptionAndMessage() {
        final MetaMatrixException child = new MetaMatrixException("propertyValuePhrase", "Child"); //$NON-NLS-1$ //$NON-NLS-2$
        final MetaMatrixException err = new MetaMatrixException(child, "Test"); //$NON-NLS-1$
        assertSame(child, err.getChild());
        assertEquals("propertyValuePhrase", err.getCode()); //$NON-NLS-1$
        assertEquals("Error Code:propertyValuePhrase Message:Test", err.getMessage()); //$NON-NLS-1$
        
    }

    public void testMetaMatrixExceptionWithExceptionAndCodeAndMessage() {
        final MetaMatrixException child = new MetaMatrixException("propertyValuePhrase", "Child"); //$NON-NLS-1$ //$NON-NLS-2$
        final MetaMatrixException err = new MetaMatrixException(child, "Code", "Test"); //$NON-NLS-1$ //$NON-NLS-2$
        assertSame(child, err.getChild());
        assertEquals("Code", err.getCode()); //$NON-NLS-1$
        assertEquals("Error Code:Code Message:Test", err.getMessage()); //$NON-NLS-1$
        
    }
}
