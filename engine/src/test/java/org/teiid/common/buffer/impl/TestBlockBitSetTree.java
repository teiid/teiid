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
 
 package org.teiid.common.buffer.impl;

import static org.junit.Assert.*;

import java.util.BitSet;
import java.util.Random;

import org.junit.Test;

public class TestBlockBitSetTree {
	
	@Test public void testBitsSet() {
		BlockBitSetTree bst = new BlockBitSetTree((1 << 20) -1, new TestBlockClosedLongIntHashTable.DummyBlockManager());
		bst.set(1, true);
		bst.set(100, true);
		bst.set(10000, true);
		bst.set(1000000, true);
		assertEquals(4, bst.getBitsSet());
		bst.set(1, false);
		assertEquals(3, bst.getBitsSet());
		assertFalse(bst.get(1));
	}
	
	@Test public void testNextClearSet() {
		BlockBitSetTree bst = new BlockBitSetTree((1 << 20) -1, new TestBlockClosedLongIntHashTable.DummyBlockManager());
		BitSet bst1 = new BitSet();
		Random r = new Random(1);
		for (int i = 0; i < 1000; i++) {
			int rand = r.nextInt() & bst.getMaxIndex();
			bst.set(rand, true);
			bst1.set(rand, true);
			assertTrue(bst.get(rand));
			assertEquals(bst1.nextSetBit(rand), bst.nextSetBit(rand));
			assertEquals(String.valueOf(i), bst1.nextSetBit(rand), bst.nextSetBit(rand));
		}
		
		for (int i = 0; i < 10000; i++) {
			int rand = r.nextInt() & bst.getMaxIndex();
			assertEquals(bst1.nextClearBit(rand), bst.nextClearBit(rand));
			assertEquals(bst1.nextSetBit(rand), bst.nextSetBit(rand));
		}
	}

}
