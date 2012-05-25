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

package org.teiid.core;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * Tests the children Iterator of the MetaMatrixException.  Primarily it does
 * this by comparing two Lists for equality: one List is created by Iterating
 * using the
 * {@link org.teiid.core.TeiidException#getChildren Iterator},
 * the other List is created by manually using the
 * {@link org.teiid.core.TeiidException#getChild getChild}
 * method recursively.
 */
public class TestMetaMatrixException {

    @Test public void testFailMetaMatrixExceptionWithNullMessage() {
        Throwable e = null;
        try {
            new TeiidException((String)null);  // should throw NPE
            fail("Should not get here"); //$NON-NLS-1$
        } catch ( Throwable ex ) {
            e = ex;
        }
        assertNotNull(e);
    }

    @Test public void testMetaMatrixExceptionWithNullThrowable() {
        final TeiidException err = new TeiidException((Throwable)null);
        assertNull(err.getCode());
        assertNull(err.getMessage());
        
    }

    @Test public void testMetaMatrixExceptionWithMessage() {
        final TeiidException err = new TeiidException("Test"); //$NON-NLS-1$
        assertNull(err.getCode());
        assertEquals("Test", err.getMessage()); //$NON-NLS-1$
        
    }
    public static enum Event implements BundleUtil.Event {
    	Code,
    	propertyValuePhrase,
    }
    @Test public void testMetaMatrixExceptionWithCodeAndMessage() {
        final TeiidException err = new TeiidException(Event.Code, "Test"); //$NON-NLS-1$ 
        assertEquals("Code", err.getCode()); //$NON-NLS-1$
        assertEquals("Code Test", err.getMessage()); //$NON-NLS-1$
    }

    
    @Test public void testMetaMatrixExceptionWithExceptionAndMessage() {
        final TeiidException child = new TeiidException(Event.propertyValuePhrase, "Child"); //$NON-NLS-1$ 
        final TeiidException err = new TeiidException(child, "Test"); //$NON-NLS-1$
        assertEquals("propertyValuePhrase", err.getCode()); //$NON-NLS-1$
        assertEquals("propertyValuePhrase Test", err.getMessage()); //$NON-NLS-1$
        
    }

    @Test public void testMetaMatrixExceptionWithExceptionAndCodeAndMessage() {
        final TeiidException child = new TeiidException(Event.propertyValuePhrase, "Child"); //$NON-NLS-1$
        final TeiidException err = new TeiidException(Event.Code,child, "Test"); //$NON-NLS-1$
        assertEquals("Code", err.getCode()); //$NON-NLS-1$
        assertEquals("Code Test", err.getMessage()); //$NON-NLS-1$
        
    }
}
