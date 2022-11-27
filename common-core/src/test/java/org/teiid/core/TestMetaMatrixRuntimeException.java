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

/*
 * Date: Sep 25, 2003
 * Time: 1:18:10 PM
 */
package org.teiid.core;

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
        assertEquals("1234 Test", err.getMessage()); //$NON-NLS-1$

    }

    public static enum Event implements BundleUtil.Event {
        Code,
    }
    public void testMetaMatrixRuntimeExceptionWithExceptionAndCodeAndMessage() {
        final String code = "1234"; //$NON-NLS-1$
        final TeiidRuntimeException child = new TeiidRuntimeException(code, "Child"); //$NON-NLS-1$
        final TeiidRuntimeException err = new TeiidRuntimeException(Event.Code, child,"Test"); //$NON-NLS-1$
        assertSame(child, err.getCause());
        assertEquals("Code", err.getCode()); //$NON-NLS-1$
        assertEquals("Code Test", err.getMessage()); //$NON-NLS-1$

    }
}