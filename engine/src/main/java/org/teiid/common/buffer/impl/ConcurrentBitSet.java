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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A segmented {@link BitSet} that supports greater concurrency
 * and faster finding of clear bits.
 */
public class ConcurrentBitSet {
	
	private static class Segment {
		int offset;
		int maxBits;
		int startSearch;
		int bitsSet;
		final private BitSet bitSet;
		
		public Segment(int bitCount) {
			bitSet = new BitSet();
			maxBits = bitCount;
		}
	}

	private int bitsPerSegment;
	private int totalBits;
	private AtomicInteger counter = new AtomicInteger();
	private AtomicInteger bitsSet = new AtomicInteger();
	private Segment[] segments;
	
	public ConcurrentBitSet(int maxBits, int concurrencyLevel) {
		if (maxBits < concurrencyLevel) {
			concurrencyLevel = 1;
			while (maxBits > 2*concurrencyLevel) {
				concurrencyLevel <<=1;
			}
		}
		segments = new Segment[concurrencyLevel];
		bitsPerSegment = maxBits/concurrencyLevel;
		int modBits = maxBits%concurrencyLevel;
		if (modBits > 0) {
			bitsPerSegment++;
		}
		for (int i = 0; i < concurrencyLevel; i++) {
			segments[i] = new Segment(bitsPerSegment);
			segments[i].offset = i*bitsPerSegment;
			if (i == concurrencyLevel - 1) {
				segments[i].maxBits -= (bitsPerSegment * concurrencyLevel)-maxBits;
			}
		}
		this.totalBits = maxBits;
	}
	
	public void clear(int bitIndex) {
		checkIndex(bitIndex);
		Segment s = segments[bitIndex/bitsPerSegment];
		bitIndex = bitIndex%bitsPerSegment;
		synchronized (s) {
			if (!s.bitSet.get(bitIndex)) {
				throw new AssertionError(bitIndex + " not set"); //$NON-NLS-1$
			}
			s.bitSet.clear(bitIndex);
			s.bitsSet--;
		}
		bitsSet.decrementAndGet();
	}
	
	/**
	 * Makes a best effort to atomically find the next clear bit and set it
	 * @return the next bit index or -1 if no clear bits are founds
	 */
	public int getAndSetNextClearBit() {
		int start = counter.getAndIncrement();
		int nextBit = -1;
		for (int i = 0; i < segments.length; i++) {
			Segment s = segments[(start+i)&(segments.length-1)];
			synchronized (s) {
				if (s.bitsSet == s.maxBits) {
					continue;
				}
				nextBit = s.bitSet.nextClearBit(s.startSearch);
				if (nextBit >= s.maxBits - 1) {
					s.startSearch = 0;
					nextBit = s.bitSet.nextClearBit(s.startSearch);
					if (nextBit >= s.maxBits) {
						throw new AssertionError("could not find clear bit"); //$NON-NLS-1$
					}
				}
				s.bitsSet++;
				s.bitSet.set(nextBit);
				s.startSearch = nextBit + 1;
				if (s.startSearch == s.maxBits) {
					s.startSearch = 0;
				}
				nextBit += s.offset;
				break;
			}
		}
		if (nextBit != -1) {
			bitsSet.getAndIncrement();
		}
		return nextBit;
	}
	
	private void checkIndex(int bitIndex) {
		if (bitIndex >= totalBits) {
			throw new ArrayIndexOutOfBoundsException(bitIndex);
		}
	}
	
	public int getTotalBits() {
		return totalBits;
	}
	
	public int getBitsSet() {
		return bitsSet.get();
	}
	
	public int getBitsPerSegment() {
		return bitsPerSegment;
	}
	
}
