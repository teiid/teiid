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

import java.util.BitSet;

import org.teiid.common.buffer.impl.FileStoreCache.BlockInfo;

/**
 * Extends a {@link BitSet} by adding a cumulative total and a 
 * first level index to speed queries against large bitsets.
 */
public class BlockBitSetTree {
	
	private static final int LOG_BITS_PER_BLOCK = FileStoreCache.LOG_BLOCK_SIZE + 3;
	private static final int MAX_TOP_VALUE = 1 << LOG_BITS_PER_BLOCK;
	private int maxIndex;
	private int bitsSet;
	private int[] topVals;
	private BlockManager blockManager;
	private int blockCount = 0;
	
	public BlockBitSetTree(int maxIndex, BlockManager blockManager) {
		this.maxIndex = maxIndex;
		this.blockManager = blockManager;
		this.topVals = new int[(maxIndex >> (FileStoreCache.LOG_BLOCK_SIZE + 3)) + 1];
	}
	
	public int getMaxIndex() {
		return maxIndex;
	}
	
	/**
	 * Set the given bit at the index.
	 * @param bitIndex
	 * @param value
	 */
	public synchronized void set(int bitIndex, boolean value) {
		getOrSet(bitIndex, value, true);
	}
	
	public synchronized boolean get(int bitIndex) {
		return getOrSet(bitIndex, false, false);
	}
	
	private boolean getOrSet(int bitIndex, boolean value, boolean update) {
		if (bitIndex > maxIndex) {
			throw new ArrayIndexOutOfBoundsException(bitIndex);
		}
		int blockIndex = bitIndex>>LOG_BITS_PER_BLOCK;
		BlockInfo bb = null;
		if (blockIndex >= blockCount) {
			if (!update) {
				return false;
			}
			for (; blockCount < blockIndex+1; blockCount++) {
				bb = blockManager.allocateBlock(blockCount);
				bb.buf.position(0);
				int longsPerBlock = FileStoreCache.BLOCK_SIZE >> 6;
				for (int j = 0; j < longsPerBlock; j++) {
					bb.buf.putLong(0);
				}
			}
		} else {
			bb = blockManager.getBlock(blockIndex);
		}
		int relativeIndex = bitIndex&(MAX_TOP_VALUE-1);
		int longByteIndex = (relativeIndex>>6)<<3;
		long word = bb.buf.getLong(longByteIndex);
		long mask = 1L << bitIndex;
		boolean currentValue = ((word & mask) != 0);
		if (!update) {
			return currentValue;
		}
		if (currentValue == value) {
			return currentValue;
		}
		if (value) {
			word |= mask;
		} else {
			word &= ~mask;
		}
		bb.buf.putLong(longByteIndex, word);
		blockManager.updateBlock(bb);
		int topIndex = bitIndex >> LOG_BITS_PER_BLOCK;
		int increment = value?1:-1;
		bitsSet+=increment;
		topVals[topIndex]+=increment;
		return currentValue;
	}
	
	public synchronized int getBitsSet() {
		return bitsSet;
	}
	
	public synchronized int nextClearBit(int fromIndex) {
		int start = fromIndex >> LOG_BITS_PER_BLOCK;
		for (int i = start; i < topVals.length; i++) {
			if (topVals[i] == MAX_TOP_VALUE) {
				continue;
			}
			if (topVals[i] == 0) {
				if (i == start) {
					return fromIndex;
				}
				return i * MAX_TOP_VALUE;
			}
			int relativeIndex = 0;
			if (i == start) {
				relativeIndex = fromIndex&(MAX_TOP_VALUE-1);
			}
			BlockInfo bb = blockManager.getBlock(i);
			
			int longByteIndex = (relativeIndex>>6)<<3;
			
			long word = ~bb.buf.getLong(longByteIndex) & (-1l << relativeIndex);

			while (true) {
			    if (word != 0) {
			    	return longByteIndex*8 + (i * MAX_TOP_VALUE) + Long.numberOfTrailingZeros(word);
			    }
			    longByteIndex+=8;
			    if (longByteIndex > FileStoreCache.BLOCK_MASK) {
			    	break;
			    }
			    word = ~bb.buf.getLong(longByteIndex);
			}
		}
		return -1;
	}
	
	public synchronized int nextSetBit(int fromIndex) {
		if (bitsSet == 0) {
			return -1;
		}
		int start = fromIndex >> LOG_BITS_PER_BLOCK;
		for (int i = start; i < topVals.length; i++) {
			if (topVals[i] == 0) {
				continue;
			}
			if (topVals[i] == MAX_TOP_VALUE) {
				if (i == start) {
					return fromIndex;
				}
				return i * MAX_TOP_VALUE;
			}
			int relativeIndex = 0;
			if (i == start) {
				relativeIndex = fromIndex&(MAX_TOP_VALUE-1);
			}
			BlockInfo bb = blockManager.getBlock(i);
			
			int longByteIndex = (relativeIndex>>6)<<3;
			
			long word = bb.buf.getLong(longByteIndex) & (-1l << relativeIndex);

			while (true) {
			    if (word != 0) {
			    	return longByteIndex*8 + (i * MAX_TOP_VALUE) + Long.numberOfTrailingZeros(word);
			    }
			    longByteIndex+=8;
			    if (longByteIndex > FileStoreCache.BLOCK_MASK) {
			    	break;
			    }
			    word = bb.buf.getLong(longByteIndex);
			}
		}
		return -1;
	}
	
}
