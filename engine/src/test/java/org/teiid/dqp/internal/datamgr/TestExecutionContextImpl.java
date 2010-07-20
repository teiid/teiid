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

package org.teiid.dqp.internal.datamgr;

import org.teiid.core.util.UnitTestUtil;
import org.teiid.dqp.internal.datamgr.ExecutionContextImpl;


import junit.framework.TestCase;

/**
 */
public class TestExecutionContextImpl extends TestCase {

    public TestExecutionContextImpl(String name) {
        super(name);
    }

    public ExecutionContextImpl createContext(String requestID, String partID) {
        return new ExecutionContextImpl("vdb", 1, null,   //$NON-NLS-1$//$NON-NLS-2$ //$NON-NLS-3$
                                        "Connection", "Connector", requestID, partID, "0"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }
    
    public void testEqivalenceSemanticsSame() {
        UnitTestUtil.helpTestEquivalence(0, createContext("100", "1"), createContext("100", "1")); //$NON-NLS-2$ //$NON-NLS-1$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    public void testEqivalenceSemanticsDifferentPart() {
        UnitTestUtil.helpTestEquivalence(1, createContext("100", "1"), createContext("100", "2")); //$NON-NLS-2$ //$NON-NLS-1$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    public void testEqivalenceSemanticsDifferentRequest() {
        UnitTestUtil.helpTestEquivalence(1, createContext("100", "1"), createContext("200", "1")); //$NON-NLS-2$ //$NON-NLS-1$ //$NON-NLS-3$ //$NON-NLS-4$
    }

}
