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

package com.metamatrix.core.util;

import javax.naming.ConfigurationException;
import junit.framework.TestCase;
import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.core.MetaMatrixRuntimeException;

public class TestMetaMatrixExceptionUtil extends TestCase {

    public TestMetaMatrixExceptionUtil(String name) {
        super(name);
    }

    public void testWithoutMessage() {
        NullPointerException npe = new NullPointerException();
        MetaMatrixRuntimeException e = new MetaMatrixRuntimeException(npe);
        assertEquals("MetaMatrixRuntimeException->NullPointerException", MetaMatrixExceptionUtil.getLinkedMessagesVerbose(e)); //$NON-NLS-1$
		assertEquals("nullnull", MetaMatrixExceptionUtil.getLinkedMessages(e)); //$NON-NLS-1$
    }

    public void testWithMessage() {
        NullPointerException npe = new NullPointerException("problem"); //$NON-NLS-1$
        MetaMatrixRuntimeException e = new MetaMatrixRuntimeException(npe);
        assertEquals("MetaMatrixRuntimeException-problem->NullPointerException", MetaMatrixExceptionUtil.getLinkedMessagesVerbose(e)); //$NON-NLS-1$
		assertEquals("problemproblem", MetaMatrixExceptionUtil.getLinkedMessages(e)); //$NON-NLS-1$
    }

    public void testWithAndWithoutMessage() {
        NullPointerException npe = new NullPointerException();
        MetaMatrixCoreException ce = new MetaMatrixCoreException(npe, "problem"); //$NON-NLS-1$
        MetaMatrixRuntimeException e = new MetaMatrixRuntimeException(ce);
        assertEquals("MetaMatrixRuntimeException-problem->MetaMatrixCoreException->NullPointerException", MetaMatrixExceptionUtil.getLinkedMessagesVerbose(e)); //$NON-NLS-1$
		assertEquals("problemproblemnull", MetaMatrixExceptionUtil.getLinkedMessages(e)); //$NON-NLS-1$
    }

    public void testConfigurationException() {
        NullPointerException npe = new NullPointerException("problem1"); //$NON-NLS-1$
        ConfigurationException configException = new ConfigurationException("problem2"); //$NON-NLS-1$
        configException.setRootCause(npe);
        MetaMatrixCoreException e = new MetaMatrixCoreException(configException, "problem3"); //$NON-NLS-1$
        assertEquals("MetaMatrixCoreException-problem3->ConfigurationException-problem2->NullPointerException-problem1", MetaMatrixExceptionUtil.getLinkedMessagesVerbose(e)); //$NON-NLS-1$
		assertEquals("problem3problem2problem1", MetaMatrixExceptionUtil.getLinkedMessages(e)); //$NON-NLS-1$
    }
}
