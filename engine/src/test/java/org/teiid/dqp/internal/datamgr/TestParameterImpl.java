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

import junit.framework.TestCase;

import org.teiid.language.Argument;
import org.teiid.language.Call;
import org.teiid.language.Argument.Direction;

public class TestParameterImpl extends TestCase {

    /**
     * Constructor for TestParameterImpl.
     * @param name
     */
    public TestParameterImpl(String name) {
        super(name);
    }

    public static Argument example(int index) throws Exception {
        Call procImpl = TestProcedureImpl.example();
        return procImpl.getArguments().get(index);
    }

    public void testGetDirection() throws Exception {
        assertEquals(Direction.IN, example(0).getDirection());
        assertEquals(Direction.IN, example(1).getDirection());
    }

    public void testGetType() throws Exception {
        assertTrue(example(0).getType().equals(String.class));
        assertTrue(example(1).getType().equals(Integer.class));
    }

    public void testGetValue() throws Exception {
        assertEquals("x", example(0).getArgumentValue().getValue()); //$NON-NLS-1$
        assertEquals(new Integer(1), example(1).getArgumentValue().getValue());
    }
    
}
