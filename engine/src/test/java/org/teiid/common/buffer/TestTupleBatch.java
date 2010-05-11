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

package org.teiid.common.buffer;

import java.util.ArrayList;
import java.util.List;

import org.teiid.common.buffer.TupleBatch;

import junit.framework.TestCase;


public class TestTupleBatch extends TestCase {

    public TestTupleBatch(String name) {
        super(name);
    }

    public TupleBatch exampleBatch(int rowBegin, int numRows, int numColumns) {
        List rows = new ArrayList();
        for(int i=0; i<numRows; i++) {
            List row = new ArrayList();
            for(int j=0; j<numColumns; j++) { 
                row.add("data-" + (rowBegin + i) + "-" + j); //$NON-NLS-1$ //$NON-NLS-2$
            }
            rows.add(row);    
        }            
        return new TupleBatch(rowBegin, rows);
    }

    /*
     * Test for void TupleBatch(int, List)
     */
    public void testTupleBatch() {
        exampleBatch(0, 10, 2);        
    }

}
