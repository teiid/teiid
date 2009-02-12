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

package com.metamatrix.query.processor;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import com.metamatrix.query.sql.symbol.ElementSymbol;

/**
 */
public class TestNullTupleSource extends TestCase {

    /**
     * Constructor for TestNullTupleSource.
     * @param name
     */
    public TestNullTupleSource(String name) {
        super(name);
    }

    public void testSource() {
        List elements = new ArrayList();
        elements.add(new ElementSymbol("x")); //$NON-NLS-1$
        elements.add(new ElementSymbol("y")); //$NON-NLS-1$
        NullTupleSource nts = new NullTupleSource(elements);   
        
        // Check schema
        assertEquals("Didn't get expected schema", elements, nts.getSchema()); //$NON-NLS-1$
        
        // Walk it and get no data
        nts.openSource();
        List tuple = nts.nextTuple();
        nts.closeSource();

        assertEquals("Didn't get termination tuple for first tuple", null, tuple);             //$NON-NLS-1$
    }

}
