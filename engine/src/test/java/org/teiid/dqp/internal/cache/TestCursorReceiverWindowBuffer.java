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

package org.teiid.dqp.internal.cache;

import java.util.ArrayList;
import java.util.List;

import org.teiid.dqp.internal.cache.CursorReceiverWindowBuffer;

import junit.framework.TestCase;
import com.metamatrix.common.util.Intervals;

public class TestCursorReceiverWindowBuffer extends TestCase {
    CursorReceiverWindowBuffer buffer = new CursorReceiverWindowBuffer();

    public void test() {
        add(0,100);
    }

    private void add(int start, int end) {
        int[] range = new int[2];
        range[0] = start;
        range[1] = end;
        List[] data = new List[end+1-start];
        for (int i=0; i<end+1-start; i++) {
            data[i] = new ArrayList();
            data[i].add(new Integer(i+start));
        }
        buffer.add(range, data);
    }

    public void testGetRow() {
        add(1,100);
        checkRow(50);
    }

    public void testGetRowNotZeroBased() {
        add(101,200);
        checkRow(150);
    }

    public void testGetRowMultipleBatches() {
        add(1,100);
        add(101,200);
        checkRow(50);
        checkRow(150);
    }

    public void testForgetBatchs() {
        add(1,100);
        buffer.removeFromCache(new Intervals(1,100));
        assertEquals("[]", buffer.getContents().toString()); //$NON-NLS-1$
    }

    public void testCantForgetBatch() {
        add(1,100);
        buffer.removeFromCache(new Intervals(50,100));
        assertEquals("[1, 100]", buffer.getContents().toString()); //$NON-NLS-1$
    }

    public void testForgetTwoBatches() {
        add(1,100);
        add(101,200);
        buffer.removeFromCache(new Intervals(1,200));
        assertEquals("[]", buffer.getContents().toString()); //$NON-NLS-1$
    }

    private void checkRow(int index) {
        Integer data = (Integer) buffer.getRow(index).get(0);
        assertEquals(index, data.intValue());
    }

}
