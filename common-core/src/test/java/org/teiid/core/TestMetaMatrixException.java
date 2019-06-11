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

package org.teiid.core;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * Tests the children Iterator of the MetaMatrixException.
 */
public class TestMetaMatrixException {

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
