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

import junit.framework.TestCase;


/** 
 * @since 4.3
 */
public class TestBatchMap extends TestCase{
    private BatchMapValueTranslator batchMapValueTranslator = new BatchMapValueTranslator() {
        public int getBeginRow(Object batchMapValue) {
            return ((Batch)batchMapValue).getBeginRow();
        }
        public int getEndRow(Object batchMapValue) {
            return ((Batch)batchMapValue).getEndRow();
        }           
    };
    
    public TestBatchMap(String arg0) {
        super(arg0);
    }
    
    //no batch
    public void testAddAndGetBatch1() {
        BatchMap batches = new BatchMap(batchMapValueTranslator);
        assertTrue(batches.isEmpty());
        assertNull(batches.getBatch(1));
        assertNull(batches.getBatch(2));
    }
    
    //one batch
    public void testAddAndGetBatch2() {
        BatchMap batches = new BatchMap(batchMapValueTranslator);  
        Batch batch1 = new Batch(1,5);
        batches.addBatch(1, batch1);
        assertEquals(batch1, batches.getBatch(1));
        assertEquals(batch1, batches.getBatch(2));
        assertEquals(batch1, batches.getBatch(5));
        assertNull(batches.getBatch(6));
        assertNull(batches.getBatch(7));
    }
    
    //two batches
    public void testAddAndGetBatch3() {
        BatchMap batches = new BatchMap(batchMapValueTranslator);
        Batch batch1 = new Batch(1,5);
        Batch batch2 = new Batch(6,10);   
        batches.addBatch(1, batch1);
        batches.addBatch(6, batch2);
        assertEquals(batch1, batches.getBatch(1));
        assertEquals(batch1, batches.getBatch(2));
        assertEquals(batch1, batches.getBatch(5));
        assertEquals(batch2, batches.getBatch(6));
        assertEquals(batch2, batches.getBatch(8));
        assertEquals(batch2, batches.getBatch(10));
        assertNull(batches.getBatch(11));
    }
    
    //more batches, no adding order
    public void testAddAndGetBatch4() {
        BatchMap batches = new BatchMap(batchMapValueTranslator);
        Set batchSet = new HashSet();
        List batchList = new ArrayList();
        for(int i=1; i<10000;) {
            Batch batch = new Batch(i,i+4);
            batchSet.add(batch);
            batchList.add(batch);
            i += 5;
        }
        Iterator iter = batchSet.iterator();
        while(iter.hasNext()) {
            Batch next = (Batch)iter.next();
            batches.addBatch(next.getBeginRow(), next);
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
        BatchMap batches = new BatchMap(batchMapValueTranslator);
        assertTrue(batches.isEmpty());
        Batch batch1 = new Batch(1,5);
        Batch batch2 = new Batch(6,10);   
        batches.addBatch(1, batch1);
        batches.addBatch(2, batch2);
        assertEquals(batch1, batches.getBatch(1));
        batches.removeBatch(1);
        assertNull(batches.getBatch(1));
        assertNull(batches.getBatch(2));
        assertNull(batches.getBatch(5));
        batches.removeBatch(2);
        assertNull(batches.getBatch(1));
        assertNull(batches.getBatch(2));
        assertNull(batches.getBatch(5));
        assertNull(batches.getBatch(6));
        assertNull(batches.getBatch(8));
        assertNull(batches.getBatch(10));
        assertTrue(batches.isEmpty());
    }
    
    class Batch{
        private int beginRow;
        private int endRow;
        Batch(int beginRow, int endRow){
            Batch.this.beginRow = beginRow;
            Batch.this.endRow = endRow;
        }
        int getBeginRow() {
            return Batch.this.beginRow;
        }
        int getEndRow() {
            return Batch.this.endRow;
        }
    }
}
