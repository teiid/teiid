/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 package org.teiid.common.buffer.impl;

import static org.junit.Assert.*;

import org.junit.Test;

public class TestConcurrentBitSet {

    @Test public void testBitsSet() {
        ConcurrentBitSet bst = new ConcurrentBitSet(50001, 4);
        assertEquals(0, bst.getAndSetNextClearBit());
        assertEquals(12501, bst.getAndSetNextClearBit());
        assertEquals(25002, bst.getAndSetNextClearBit());
        assertEquals(37503, bst.getAndSetNextClearBit());
        assertEquals(1, bst.getAndSetNextClearBit());
        assertEquals(5, bst.getBitsSet());
        bst.clear(1);
        assertEquals(4, bst.getBitsSet());
        bst.clear(12501);
        try {
            bst.clear(30000);
            fail();
        } catch (AssertionError e) {

        }
        assertEquals(3, bst.getBitsSet());

        for (int i = 0; i < bst.getTotalBits()-3;i++) {
            assertTrue(bst.getAndSetNextClearBit() != -1);
        }

        bst.clear(5);
        bst.clear(12505);
        bst.clear(25505);
        bst.clear(37505);

        for (int i = 0; i < 4; i++) {
            int bit = bst.getAndSetNextClearBit();
            assertTrue(bit < bst.getTotalBits() && bit > 0);
        }
    }

    @Test public void testSegmentUse() {
        ConcurrentBitSet bst = new ConcurrentBitSet(50001, 4);
        assertEquals(0, bst.getAndSetNextClearBit(0));
        assertEquals(1, bst.getAndSetNextClearBit(0));
        assertEquals(2, bst.getAndSetNextClearBit(4));
    }

    @Test public void testCompactBitSet() {
        ConcurrentBitSet bst = new ConcurrentBitSet(100000, 1);
        bst.setCompact(true);
        for (int i = 0; i < 100000; i++) {
            assertEquals(i, bst.getAndSetNextClearBit());
        }
        bst.clear(50);
        bst.clear(500);
        bst.clear(5000);
        assertEquals(50, bst.getAndSetNextClearBit());
    }

    @Test public void testCompactHighest() {
        ConcurrentBitSet bst = new ConcurrentBitSet(1 << 19, 1);
        bst.setCompact(true);
        for (int i = 0; i < bst.getTotalBits(); i++) {
            bst.getAndSetNextClearBit();
        }
        assertEquals(bst.getTotalBits()-1, bst.getHighestBitSet(0));
        assertEquals(bst.getTotalBits()-1, bst.getHighestBitSet(1));

        for (int i = bst.getTotalBits()-20; i < bst.getTotalBits(); i++) {
            bst.clear(i);
        }

        assertEquals(bst.getTotalBits()-21, bst.compactHighestBitSet(0));

        for (int i = bst.getTotalBits()-20; i < bst.getTotalBits(); i++) {
            bst.getAndSetNextClearBit();
        }

        assertEquals(-1, bst.getAndSetNextClearBit());

        for (int i = 20; i < bst.getTotalBits(); i++) {
            bst.clear(i);
        }

        assertEquals(bst.getTotalBits()-1, bst.getHighestBitSet(0));
        assertEquals(19, bst.compactHighestBitSet(0));

    }

    @Test public void testCompactHighestEmpty() {
        ConcurrentBitSet bst = new ConcurrentBitSet(1 << 19, 1);
        bst.setCompact(true);
        bst.getAndSetNextClearBit();
        bst.clear(0);
        assertEquals(-1, bst.compactHighestBitSet(0));
    }

}
