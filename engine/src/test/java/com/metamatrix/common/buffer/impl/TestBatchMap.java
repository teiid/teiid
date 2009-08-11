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

package com.metamatrix.common.buffer.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.metamatrix.common.buffer.TupleSourceID;

import junit.framework.TestCase;


/** 
 * @since 4.3
 */
public class TestBatchMap extends TestCase{
    
    public TestBatchMap(String arg0) {
        super(arg0);
    }
    
    //no batch
    public void testAddAndGetBatch1() {
        TupleSourceInfo batches = new TupleSourceInfo(null, null, null, null, null);
        assertNull(batches.getBatch(1));
        assertNull(batches.getBatch(2));
    }
    
    //one batch
    public void testAddAndGetBatch2() {
    	TupleSourceInfo batches = new TupleSourceInfo(null, null, null, null, null);  
        ManagedBatch batch1 = createManagedBatch(1, 5);
        batches.addBatch(batch1);
        assertEquals(batch1, batches.getBatch(1));
        assertEquals(batch1, batches.getBatch(2));
        assertEquals(batch1, batches.getBatch(5));
        assertNull(batches.getBatch(6));
        assertNull(batches.getBatch(7));
    }
    
    //two batches
    public void testAddAndGetBatch3() {
    	TupleSourceInfo batches = new TupleSourceInfo(null, null, null, null, null);
    	ManagedBatch batch1 = createManagedBatch(1, 5);
    	ManagedBatch batch2 = createManagedBatch(6, 10);
        batches.addBatch(batch1);
        batches.addBatch(batch2);
        assertEquals(batch1, batches.getBatch(1));
        assertEquals(batch1, batches.getBatch(2));
        assertEquals(batch1, batches.getBatch(5));
        assertEquals(batch2, batches.getBatch(6));
        assertEquals(batch2, batches.getBatch(8));
        assertEquals(batch2, batches.getBatch(10));
        assertNull(batches.getBatch(11));
    }

	private ManagedBatch createManagedBatch(int begin, int end) {
		return new ManagedBatch(new TupleSourceID("x"), begin, end, 0); //$NON-NLS-1$
	}
    
    //more batches, no adding order
    public void testAddAndGetBatch4() {
    	TupleSourceInfo batches = new TupleSourceInfo(null, null, null, null, null);
        Set batchSet = new HashSet();
        List batchList = new ArrayList();
        for(int i=1; i<10000;) {
            ManagedBatch batch = createManagedBatch(i, i + 4);
            batchSet.add(batch);
            batchList.add(batch);
            i += 5;
        }
        Iterator iter = batchSet.iterator();
        while(iter.hasNext()) {
            ManagedBatch next = (ManagedBatch)iter.next();
            batches.addBatch(next);
        }
        for(int i=1; i<10000;) {
            assertEquals(batchList.get(i/5), batches.getBatch(i));
            assertEquals(batchList.get(i/5), batches.getBatch(i+1));
            assertEquals(batchList.get(i/5), batches.getBatch(i+2));
            assertEquals(batchList.get(i/5), batches.getBatch(i+3));
            assertEquals(batchList.get(i/5), batches.getBatch(i+4));
            i += 5;
        }
        assertNull(batches.getBatch(10001));
    }
    
    public void testAddAndRemoveBatch() {
    	TupleSourceInfo batches = new TupleSourceInfo(null, null, null, null, null);
    	ManagedBatch batch1 = createManagedBatch(1, 5);
    	ManagedBatch batch2 = createManagedBatch(6, 10);   
        batches.addBatch(batch1);
        batches.addBatch(batch2);
        assertEquals(batch1, batches.getBatch(1));
        batches.removeBatch(1);
        assertNull(batches.getBatch(1));
        assertNull(batches.getBatch(2));
        assertNull(batches.getBatch(5));
        batches.removeBatch(6);
        assertNull(batches.getBatch(1));
        assertNull(batches.getBatch(2));
        assertNull(batches.getBatch(5));
        assertNull(batches.getBatch(6));
        assertNull(batches.getBatch(8));
        assertNull(batches.getBatch(10));
    }
    
}
