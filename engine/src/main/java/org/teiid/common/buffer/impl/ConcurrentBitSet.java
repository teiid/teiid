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

import org.teiid.core.util.Assertion;

/**
 * A segmented {@link BitSet} that supports greater concurrency
 * and faster finding of clear bits.
 */
public class ConcurrentBitSet {
	
	private static final int ADDRESS_BITS_PER_TOP_VALUE = 18;
	private static final int MAX_TOP_VALUE = 1 << ADDRESS_BITS_PER_TOP_VALUE;
	
	private static class Segment {
		int offset;
		int maxBits;
		int startSearch;
		int bitsSet;
		int[] topVals;
		final BitSet bitSet;
		
		public Segment(int bitCount) {
			bitSet = new BitSet();
			maxBits = bitCount;
			this.topVals = new int[Math.max(1, maxBits >> ADDRESS_BITS_PER_TOP_VALUE)];
		}
	}

	private int bitsPerSegment;
	private int totalBits;
	private AtomicInteger counter = new AtomicInteger();
	private AtomicInteger bitsSet = new AtomicInteger();
	private Segment[] segments;
	private boolean compact;
	
	public ConcurrentBitSet(int maxBits, int concurrencyLevel) {
		Assertion.assertTrue(maxBits > 0);
		while ((bitsPerSegment = maxBits/concurrencyLevel) < concurrencyLevel) {
			concurrencyLevel >>= 1;
		}
		segments = new Segment[concurrencyLevel];
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
		int segmentBitIndex = bitIndex%bitsPerSegment;
		synchronized (s) {
			if (!s.bitSet.get(segmentBitIndex)) {
				throw new AssertionError(bitIndex + " not set"); //$NON-NLS-1$
			}
			if (compact) {
				s.startSearch = Math.min(s.startSearch, segmentBitIndex);
			}
			s.bitSet.clear(segmentBitIndex);
			s.bitsSet--;
			s.topVals[segmentBitIndex>>ADDRESS_BITS_PER_TOP_VALUE]--;
		}
		bitsSet.decrementAndGet();
	}
	
	/**
	 * Makes a best effort to atomically find the next clear bit and set it
	 * @return the next bit index or -1 if no clear bits are found
	 */
	public int getAndSetNextClearBit() {
		return getAndSetNextClearBit(counter.getAndIncrement());
	}
	
	public int getNextSegment() {
		return counter.getAndIncrement();
	}
	
	public int getAndSetNextClearBit(int start) {
		int nextBit = -1;
		for (int i = 0; i < segments.length; i++) {
			Segment s = segments[(start+i)&(segments.length-1)];
			synchronized (s) {
				if (s.bitsSet == s.maxBits) {
					continue;
				}
				int indexSearchStart = s.startSearch >> ADDRESS_BITS_PER_TOP_VALUE;
				for (int j = indexSearchStart; j < s.topVals.length; j++) {
					if (s.topVals[j] == MAX_TOP_VALUE) {
						continue;
					}
					if (s.topVals[j] == 0) {
						if (j == start) {
							nextBit = s.startSearch;
							break;
						}
						nextBit = j * MAX_TOP_VALUE;
						break;
					}
					int index = j * MAX_TOP_VALUE;
					if (j == indexSearchStart) {
						index = s.startSearch;
					}
					nextBit = s.bitSet.nextClearBit(index);
					if (s.startSearch > 0 && nextBit >= s.maxBits - 1) {
						s.startSearch = 0;
						//fallback full scan
						nextBit = s.bitSet.nextClearBit(s.startSearch);
					}
					break;
				}
				if (nextBit >= s.maxBits) {
					throw new AssertionError("could not find clear bit"); //$NON-NLS-1$
				}
				s.topVals[nextBit>>ADDRESS_BITS_PER_TOP_VALUE]++;
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
	
	/**
	 * Set to try to always allocate against the first available block in a segment.
	 * @param compact
	 */
	public void setCompact(boolean compact) {
		this.compact = compact;
	}
	
}
