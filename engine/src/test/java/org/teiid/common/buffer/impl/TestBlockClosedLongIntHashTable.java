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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.junit.Test;
import org.teiid.common.buffer.impl.FileStoreCache.BlockInfo;

public class TestBlockClosedLongIntHashTable {
	
	public static final class DummyBlockManager implements BlockManager {
		List<BlockInfo> blocks = new ArrayList<BlockInfo>();

		@Override
		public int getInode() {
			return 0;
		}
		
		@Override
		public void updateBlock(BlockInfo block) {
			
		}

		@Override
		public void free() {
			blocks.clear();
		}
		
		@Override
		public BlockInfo getBlock(int index) {
			BlockInfo block = blocks.get(index);
			block.buf.rewind();
			return block;
		}

		@Override
		public void freeBlock(int index) {
			blocks.remove(index);
		}

		@Override
		public BlockInfo allocateBlock(int index) {
			assertEquals(index, blocks.size());
			ByteBuffer result = ByteBuffer.wrap(new byte[FileStoreCache.BLOCK_SIZE]);
			blocks.add(new BlockInfo(false, result, index, -1));
			return blocks.get(blocks.size() - 1);
		}
	}

	@Test public void testAgainstHashMap() {
		BlockClosedLongIntHashTable table = new BlockClosedLongIntHashTable(new DummyBlockManager());
		HashMap<Long, Integer> table1 = new HashMap<Long, Integer>(16);
		for (long i = 1; i < 200000; i++) {
			table.put(i, (int)i);
			table1.put(i, (int)i);
		}
		Random r = new Random(0);
		for (int i = 1; i < 2000000; i++) {
			long toRemove = r.nextInt(i); 
			boolean removed = table.remove(toRemove) != BlockClosedLongIntHashTable.EMPTY;
			assertEquals(table1.remove(toRemove) != null, removed);
		}
	}

}
