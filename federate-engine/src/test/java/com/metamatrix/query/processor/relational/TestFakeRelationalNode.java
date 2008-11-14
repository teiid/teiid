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

package com.metamatrix.query.processor.relational;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.util.CommandContext;

public class TestFakeRelationalNode extends TestCase {

    public TestFakeRelationalNode(String arg0) {
        super(arg0);
    }

    
    private List[] createData(int rows) {
        List[] data = new List[rows];
        for(int i=0; i<rows; i++) { 
            data[i] = new ArrayList();
            data[i].add(new Integer(i));
        }   
        return data; 
    }   
    
    public void test() {
        // setup 
        ElementSymbol element = new ElementSymbol("a"); //$NON-NLS-1$
        element.setType(DataTypeManager.DefaultDataClasses.INTEGER);
        List elements = new ArrayList();
        elements.add(element);
        
        List[] data = createData(1000);
        FakeRelationalNode node = new FakeRelationalNode(1, data);
        node.setElements(elements);
        CommandContext context = new CommandContext("pid", "group", null, 100, null, null, null, null); //$NON-NLS-1$ //$NON-NLS-2$
        node.initialize(context, NodeTestUtil.getTestBufferManager(10000), null);
        
        // read from fake node
        try {
            int currentRow = 1;
            while(true) {
                TupleBatch batch = node.nextBatch();
                for(int row = currentRow; row <= batch.getEndRow(); row++) {
                    assertEquals("Rows don't match at " + row, data[row-1], batch.getTuple(row)); //$NON-NLS-1$
                }
                
                if(batch.getTerminationFlag()) {
                    break;
                }
                currentRow += batch.getRowCount();    
            }
        } catch(Exception e) {
            e.printStackTrace();
            fail("Unexpected exception: " + e.getMessage());     //$NON-NLS-1$
        }
    }

}
