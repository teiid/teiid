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

package com.metamatrix.common.buffer.storage.memory;

import java.util.ArrayList;
import java.util.List;

import junit.framework.*;

import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.common.buffer.*;

/**
 */
public class TestMemoryStorageManager extends TestCase {

    /**
     * Constructor for TestMemoryStorageManager.
     * @param arg0
     */
    public TestMemoryStorageManager(String name) {
        super(name);
    }

    public MemoryStorageManager getStorageMgr() {
        MemoryStorageManager mgr = new MemoryStorageManager();
        return mgr;
    }
    
    public TupleBatch exampleBatch(int begin, int end) {
        int count = end-begin+1;
        List[] rows = new List[count];
        for(int i=0; i < count; i++) {
            rows[i] = new ArrayList();
            rows[i].add(new Integer(i+begin));
            rows[i].add("" + (i+begin));     //$NON-NLS-1$
        }
        return new TupleBatch(begin, rows);        
    }
    
    public void helpCompareBatches(TupleBatch expectedBatch, TupleBatch actualBatch) {
        List[] expectedRows = expectedBatch.getAllTuples();
        List[] actualRows = actualBatch.getAllTuples();

        assertEquals("Differing number of rows ", expectedRows.length, actualRows.length); //$NON-NLS-1$
        for(int i=0; i<expectedRows.length; i++) {
            assertEquals("Differing rows at " + i, expectedRows[i], actualRows[i]);     //$NON-NLS-1$
        }
    }
    
    public List[] helpGetRows(TupleBatch batch, int begin, int end) {
        List[] allRows = batch.getAllTuples();
        if(begin == batch.getBeginRow() && end == batch.getEndRow()) {
            return allRows;
        }
        int firstOffset = begin - batch.getBeginRow();
        int count = end - begin + 1;
    
        List[] subRows = new List[count];
        System.arraycopy(allRows, firstOffset, subRows, 0, count);
        return subRows;
    }

    public void testAddGetFullBatch() {
        MemoryStorageManager mgr = getStorageMgr();        
        TupleSourceID tsID = new TupleSourceID("test"); //$NON-NLS-1$
        TupleBatch batch = exampleBatch(1, 100);
        
        try {
            // Add batch
            mgr.addBatch(tsID, batch, null);    
        
            // Get batch
            TupleBatch actual = mgr.getBatch(tsID, 1, null);
        
            // Compare
            helpCompareBatches(batch, actual);
        } catch(MetaMatrixException e) {
            e.printStackTrace();
            fail("Unexpected exception of type " + e.getClass().getName() + ": " + e.getMessage()); //$NON-NLS-1$ //$NON-NLS-2$
        }
        
    }

    public void testAddRemoveGet() {
        MemoryStorageManager mgr = getStorageMgr();        
        TupleSourceID tsID = new TupleSourceID("test"); //$NON-NLS-1$
        TupleBatch batch = exampleBatch(1, 100);
        
        try {
            // Add batch
            mgr.addBatch(tsID, batch, null);    
        
            // Remove whole batch
            mgr.removeBatch(tsID, 1);

            try {    
                // Get batch
                mgr.getBatch(tsID, 1, null);
                fail("Failed to get exception when reading non-existent rows"); //$NON-NLS-1$
                
            } catch(TupleSourceNotFoundException e) {
                // expected
            }
            
        } catch(MetaMatrixException e) {
            e.printStackTrace();
            fail("Unexpected exception of type " + e.getClass().getName() + ": " + e.getMessage()); //$NON-NLS-1$ //$NON-NLS-2$
        }
        
    }

    public void testRemoveAll() {
        MemoryStorageManager mgr = getStorageMgr();        
        TupleSourceID tsID = new TupleSourceID("test"); //$NON-NLS-1$
        
        try {
            // Add batches
            mgr.addBatch(tsID, exampleBatch(1, 100), null);    
            mgr.addBatch(tsID, exampleBatch(101, 200), null);    
        
            // Remove all
            mgr.removeBatches(tsID);
        
            try {    
                // Get batch
                mgr.getBatch(tsID, 1, null);
                fail("Failed to get exception when reading non-existent rows"); //$NON-NLS-1$
                
            } catch(TupleSourceNotFoundException e) {
                // expected
            }

        } catch(MetaMatrixException e) {
            e.printStackTrace();
            fail("Unexpected exception of type " + e.getClass().getName() + ": " + e.getMessage()); //$NON-NLS-1$ //$NON-NLS-2$
        }
        
    }

}
