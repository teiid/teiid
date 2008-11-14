/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
 */
package com.metamatrix.data.basic;

import junit.framework.*;
import java.util.*;

public class TestBasicBatch extends TestCase{

    public TestBasicBatch(String name) {
        super(name);
    }
    //=== tests ===//
    public void testAddRow(){
        List row1 = new ArrayList();
        row1.add("5"); //$NON-NLS-1$
        row1.add(new Integer(6));
        List row2 = new ArrayList();
        row2.add("8"); //$NON-NLS-1$
        row2.add(new Integer(9));

        List expectedResults = new ArrayList();
        expectedResults.add(row1);
        expectedResults.add(row2);

        BasicBatch batch = new BasicBatch();
        batch.addRow(row1);
        batch.addRow(row2);

        assertEquals(expectedResults, Arrays.asList(batch.getResults()));
        assertTrue(!batch.isLast());
    }

    public void testSetLast(){
        List row1 = new ArrayList();
        row1.add("5"); //$NON-NLS-1$
        row1.add(new Integer(6));
        List row2 = new ArrayList();
        row2.add("8"); //$NON-NLS-1$
        row2.add(new Integer(9));

        List expectedResults = new ArrayList();
        expectedResults.add(row1);
        expectedResults.add(row2);

        BasicBatch batch = new BasicBatch();
        batch.addRow(row1);
        batch.addRow(row2);
        batch.setLast();

        assertEquals(expectedResults, Arrays.asList(batch.getResults()));
        assertTrue(batch.isLast());
    }

    public void testGetRoeCount(){
        List row1 = new ArrayList();
        row1.add("5"); //$NON-NLS-1$
        row1.add(new Integer(6));
        List row2 = new ArrayList();
        row2.add("8"); //$NON-NLS-1$
        row2.add(new Integer(9));

        BasicBatch batch = new BasicBatch();
        batch.addRow(row1);
        batch.addRow(row2);

        assertTrue(2 == batch.getRowCount());
    }
}
