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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.teiid.common.buffer.impl.FileStoreCache.BlockInfo;

/**
 * Represents the logical structure of a cache group / directory
 * 
 * Implemented by a closed hash table using a single step linear probe with delayed removal.
 * Uses power of 2 hashing.
 * 
 * Provides an extremely simple hash structure that rivals {@link HashMap} performance and
 * is directly mapped to {@link ByteBuffer}s to avoid serialization overhead.
 * 
 * Does not expect keys or values to be negative.
 */
public class BlockClosedLongIntHashTable {
	
	private enum Mode {
		GET,
		UPDATE,
		REMOVE
	}
	
	private static final int BYTES_PER_ROW = 12; //8+4
	private static final int BLOCK_SIZE = FileStoreCache.BLOCK_DATA_BYTES/BYTES_PER_ROW;
	private static final float LOAD_FACTOR = .7f;
	private static final int MIN_SIZE = 1 << (31 - Integer.numberOfLeadingZeros(BLOCK_SIZE)); //should fit in a single block
	
	static final int EMPTY = -1;
	private static final int REMOVED = -2;

	protected int size;
	protected int capacityMask = EMPTY;
	protected BlockManager blockManager;
	
	public BlockClosedLongIntHashTable(BlockManager blockManager) {
		this.blockManager = blockManager;
	}
	
	private void init(int capacity) {
		int currentBlockCount = blockCount(capacityMask + 1);
		int desiredBlockCount = blockCount(capacity);
		if (capacity > capacityMask) {
			for (int i = currentBlockCount; i < desiredBlockCount; i++) {
				BlockInfo bb = blockManager.allocateBlock(i);
				empty(bb.buf, BLOCK_SIZE);
				blockManager.updateBlock(bb);
			}
		}
		capacityMask = capacity - 1;
	}

	private void empty(ByteBuffer bb, int toIndex) {
		bb.position(0);
		for (int j = 0; j < toIndex; j++) {
			bb.putLong(j * BYTES_PER_ROW, EMPTY);
		}
	}

	private static int blockCount(int size) {
		int currentBlockCount = (size) / BLOCK_SIZE;
		if (size%BLOCK_SIZE > 0) {
			currentBlockCount++;
		}
		return currentBlockCount;
	}
	
	public synchronized int put(long key, int value) {
		int result = getOrUpdate(key, value, Mode.UPDATE);
		if ((result == EMPTY || result == REMOVED) && ++size > LOAD_FACTOR*capacityMask) {
			int newCapacity = (capacityMask+1)<<1;
			int oldLength = capacityMask + 1;
			init(newCapacity);
			rehash(oldLength, newCapacity);
		}
		return result;
	}

	public synchronized int get(long key) {
		return getOrUpdate(key, EMPTY, Mode.GET);
	}
	
	public synchronized int remove(long key) {
		int result = getOrUpdate(key, EMPTY, Mode.REMOVE);
		if (result != EMPTY && --size*LOAD_FACTOR < capacityMask>>3 && (capacityMask+1)>>1 >= MIN_SIZE) {
			//reduce the size of the table by half
			int oldLength = capacityMask + 1;
			capacityMask >>= 1;
			rehash(oldLength, oldLength>>1);
			int oldBlocks = blockCount(oldLength);
			int newBlocks = blockCount(capacityMask +1);
			for (int i = oldBlocks-1; i >= newBlocks; i--) {
				blockManager.freeBlock(i);
			}
		}
		return result;
	}
	
	private void rehash(int oldLength, int newLength) {
		BlockInfo lastBlockInfo = null;
		ByteBuffer lastBlock = null;
		for (int i = 0; i < oldLength; i++) {
			int relativeIndex = i%BLOCK_SIZE;
			if (lastBlock == null || relativeIndex == 0) {
				int lastIndex = i/BLOCK_SIZE;
				lastBlockInfo = blockManager.getBlock(lastIndex);
				lastBlock = lastBlockInfo.buf;
				if (i < newLength) {
					byte[] buf = new byte[FileStoreCache.BLOCK_DATA_BYTES];
					lastBlock.position(0);
					lastBlock.get(buf);
					ByteBuffer copyBlock = ByteBuffer.wrap(buf);
					empty(lastBlock, Math.min(BLOCK_SIZE, oldLength - lastIndex*BLOCK_SIZE));
					blockManager.updateBlock(lastBlockInfo);
					lastBlock = copyBlock;
				}
			}
			lastBlock.position(relativeIndex*BYTES_PER_ROW);
			long oldKey = lastBlock.getLong();
			int oldValue = lastBlock.getInt();
			if (oldKey != REMOVED && oldKey != EMPTY) {
				getOrUpdate(oldKey, oldValue, Mode.UPDATE);
			}
		}
	}

	@SuppressWarnings("null")
	protected int getOrUpdate(long key, int value, Mode mode) {
		if (capacityMask == EMPTY) {
			if (mode == Mode.GET || mode == Mode.REMOVE) {
				return EMPTY;
			}
			init(MIN_SIZE);
		}
		int i = hashIndex(key);
		BlockInfo lastBlockInfo = null;
		long old = EMPTY;
		int position = 0;
		while (true) {
			int relativeIndex = i%BLOCK_SIZE;
			if (lastBlockInfo == null || relativeIndex == 0) {
				int index = i/BLOCK_SIZE;
				lastBlockInfo = blockManager.getBlock(index);
			}
			position = relativeIndex*BYTES_PER_ROW;
			old = lastBlockInfo.buf.getLong(position);
			if (old == EMPTY || old == key || (mode == Mode.UPDATE && old == REMOVED)) {
				break;
			}
	        i = (i + 1) & capacityMask;
		}
		int result = EMPTY;
		if (old != EMPTY && old != REMOVED) {
			result = lastBlockInfo.buf.getInt(position + 8);
		}
		switch (mode) {
		case GET:
			return result;
		case UPDATE:
			lastBlockInfo.buf.putLong(position, key);
			lastBlockInfo.buf.putInt(position + 8, value);
			blockManager.updateBlock(lastBlockInfo);
			return result;
		case REMOVE:
			if (old == EMPTY || old == REMOVED) {
				return EMPTY;
			}
			lastBlockInfo.buf.putLong(position, REMOVED);
			blockManager.updateBlock(lastBlockInfo);
			return result;
		default:
			throw new AssertionError();
		}
	}
	
	private int hashIndex(long key) {
		//start with the usual long hash
		int primaryHash = (int)(key ^ (key >>> 32));
		//allow the lower bits to spread the entries
		primaryHash += primaryHash <<= 2;
		primaryHash += primaryHash <<= 3;
		return primaryHash & capacityMask;
	}
	
	public synchronized int size() {
		return size;
	}

	public synchronized Map<Long, Integer> remove() {
		Map<Long, Integer> result = new HashMap<Long, Integer>();
		BlockInfo lastBlockInfo = null;
		int blockIndex = 0;
		for (int i = capacityMask; i >= 0; i--) {
			int relativeIndex = i%BLOCK_SIZE;
			if (lastBlockInfo == null || relativeIndex == BLOCK_SIZE - 1) {
				if (lastBlockInfo != null) {
					blockManager.freeBlock(blockIndex);
				}
				blockIndex = i/BLOCK_SIZE;
				lastBlockInfo = blockManager.getBlock(blockIndex);
			}
			lastBlockInfo.buf.position(relativeIndex*BYTES_PER_ROW);
			long key = lastBlockInfo.buf.getLong();
			if (key != EMPTY && key != REMOVED) {
				result.put(key, lastBlockInfo.buf.getInt());
			}
		}
		blockManager.free();
		return result;
	}

}
