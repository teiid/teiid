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

package org.teiid.core.util;

import javax.naming.ConfigurationException;

import junit.framework.TestCase;

import org.teiid.core.TeiidException;
import org.teiid.core.TeiidRuntimeException;

public class TestMetaMatrixExceptionUtil extends TestCase {

    public TestMetaMatrixExceptionUtil(String name) {
        super(name);
    }

    public void testWithoutMessage() {
        NullPointerException npe = new NullPointerException();
        TeiidRuntimeException e = new TeiidRuntimeException(npe);
        assertEquals("TeiidRuntimeException->NullPointerException", ExceptionUtil.getLinkedMessagesVerbose(e)); //$NON-NLS-1$
    }

    public void testWithMessage() {
        NullPointerException npe = new NullPointerException("problem"); //$NON-NLS-1$
        TeiidRuntimeException e = new TeiidRuntimeException(npe);
        assertEquals("TeiidRuntimeException-problem->NullPointerException", ExceptionUtil.getLinkedMessagesVerbose(e)); //$NON-NLS-1$
    }

    public void testWithAndWithoutMessage() {
        NullPointerException npe = new NullPointerException();
        TeiidException ce = new TeiidException(npe, "problem"); //$NON-NLS-1$
        TeiidRuntimeException e = new TeiidRuntimeException(ce);
        assertEquals("TeiidRuntimeException-problem->TeiidException->NullPointerException", ExceptionUtil.getLinkedMessagesVerbose(e)); //$NON-NLS-1$
    }

    public void testConfigurationException() {
        NullPointerException npe = new NullPointerException("problem1"); //$NON-NLS-1$
        ConfigurationException configException = new ConfigurationException("problem2"); //$NON-NLS-1$
        configException.setRootCause(npe);
        TeiidException e = new TeiidException(configException, "problem3"); //$NON-NLS-1$
        assertEquals("TeiidException-problem3->ConfigurationException-problem2->NullPointerException-problem1", ExceptionUtil.getLinkedMessagesVerbose(e)); //$NON-NLS-1$
    }
}
