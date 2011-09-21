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

/**
 * Extends a {@link BitSet} by adding a cumulative total and a 
 * first level index to speed queries against large bitsets.
 */
public class BitSetTree {
	
	public static final int MAX_INDEX = (1 << 24) - 1;
	
	private int bitsSet;
	private int totalBits;
	private short[] topVals = new short[1 << 9];
	private BitSet bitSet = new BitSet();
	
	public boolean get(int index) {
		if (index > MAX_INDEX) {
			throw new ArrayIndexOutOfBoundsException(index);
		}
		return bitSet.get(index);
	}
	
	/**
	 * Set the given bit at the index.  It's expected that the 
	 * bit currently has the opposite value.
	 * @param bitIndex
	 * @param value
	 */
	public void set(int bitIndex, boolean value) {
		if (bitIndex > MAX_INDEX) {
			throw new ArrayIndexOutOfBoundsException(bitIndex);
		}
		bitSet.set(bitIndex, value);
		if (bitIndex >= totalBits) {
			totalBits = bitIndex + 1;
		}
		int topIndex = bitIndex >>> 15;
		int increment = value?1:-1;
		bitsSet+=increment;
		topVals[topIndex]+=increment;
	}
	
	public int getTotalBits() {
		return totalBits;
	}
	
	public int getBitsSet() {
		return bitsSet;
	}
	
	public int nextClearBit(int fromIndex) {
		int start = fromIndex >> 15;
		for (int i = start; i < topVals.length; i++) {
			if (topVals[i] < Short.MAX_VALUE) {
				int searchFrom = fromIndex;
				if (i > start) {
					searchFrom = i << 15;
				}
				return bitSet.nextClearBit(searchFrom);
			}
		}
		return -1;
	}
	
	public int nextSetBit(int fromIndex) {
		if (bitsSet == 0) {
			return -1;
		}
		int start = fromIndex >> 15;
		for (int i = fromIndex >> 15; i < topVals.length; i++) {
			if (topVals[i] > 0) {
				int searchFrom = fromIndex;
				if (i > start) {
					searchFrom = i << 15;
				}
				return bitSet.nextSetBit(searchFrom); 
			}
		}
		return -1;
	}
	
}
