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

import java.util.BitSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.teiid.core.util.Assertion;

/**
 * A segmented {@link BitSet} that supports greater concurrency
 * and faster finding of clear bits.
 */
public class ConcurrentBitSet {

    private static final int CONCURRENT_MODIFICATION = -2;
    private static final int ADDRESS_BITS_PER_TOP_VALUE = 18;
    private static final int MAX_TOP_VALUE = 1 << ADDRESS_BITS_PER_TOP_VALUE;

    private static class Segment {
        int offset;
        int maxBits;
        int startSearch;
        int highestBitSet = -1;
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

    /**
     * @param maxBits
     * @param concurrencyLevel - should be a power of 2
     */
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

    /**
     * return an estimate of the number of bits set
     * @param segment
     * @return
     */
    public int getBitsSet(int segment) {
        Segment s = segments[segment&(segments.length-1)];
        return s.bitsSet;
    }

    /**
     * return an estimate of the highest bit (relative index) that has been set
     * @param segment
     * @return
     */
    public int getHighestBitSet(int segment) {
        Segment s = segments[segment&(segments.length-1)];
        return s.highestBitSet;
    }

    /**
     * @param segment
     * @return the next clear bit index as an absolute index - not relative to a segment
     */
    public int getAndSetNextClearBit(int segment) {
        int nextBit = -1;
        for (int i = 0; i < segments.length; i++) {
            Segment s = segments[(segment+i)&(segments.length-1)];
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
                        if (j == segment) {
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
                s.highestBitSet = Math.max(s.highestBitSet, nextBit);
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
     * Set to always allocate against the first available block in a segment.
     * @param compact
     */
    public void setCompact(boolean compact) {
        this.compact = compact;
    }


    public int compactHighestBitSet(int segment) {
        Segment s = segments[segment&(segments.length-1)];
        //first do an unlocked compact
        for (int i = 0; i < 3; i++) {
            int result = tryCompactHighestBitSet(s);
            if (result != CONCURRENT_MODIFICATION) {
                return result;
            }
        }
        synchronized (s) {
            return tryCompactHighestBitSet(s);
        }
    }

    private int tryCompactHighestBitSet(Segment s) {
        int highestBitSet = 0;
        synchronized (s) {
            highestBitSet = s.highestBitSet;
            if (highestBitSet < 0) {
                return -1;
            }
            if (s.bitSet.get(highestBitSet)) {
                return highestBitSet;
            }
        }
        int indexSearchStart = highestBitSet >> ADDRESS_BITS_PER_TOP_VALUE;
        for (int j = indexSearchStart; j >= 0; j--) {
            if (s.topVals[j] == 0) {
                if (j==0) {
                    synchronized (s) {
                        if (s.highestBitSet != highestBitSet) {
                            return CONCURRENT_MODIFICATION;
                        }
                        s.highestBitSet = -1;
                    }
                }
                continue;
            }
            if (s.topVals[j] == MAX_TOP_VALUE) {
                synchronized (s) {
                    if (s.highestBitSet != highestBitSet) {
                        return CONCURRENT_MODIFICATION;
                    }
                    s.highestBitSet = ((j + 1) * MAX_TOP_VALUE) -1;
                }
                break;
            }
            int index = j * MAX_TOP_VALUE;
            int end = index + s.maxBits;
            if (j == indexSearchStart) {
                end = highestBitSet;
            }
            BitSet bs = s.bitSet;
            int offset = 0;
            if (j == indexSearchStart) {
                bs = s.bitSet.get(index, end); //ensures that we look only at a subset of the words
                offset = index;
            }
            index = index - offset;
            end = end - offset - 1;
            while (index < end) {
                int next = bs.nextSetBit(index);
                if (next == -1) {
                    index--;
                    break;
                }
                index = next + 1;
            }
            synchronized (s) {
                if (s.highestBitSet != highestBitSet) {
                    return CONCURRENT_MODIFICATION;
                }
                s.highestBitSet = index + offset;
                return s.highestBitSet;
            }
        }
        return -1;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < segments.length; i++) {
            sb.append(i).append(' ').append(segments[i].bitSet.toString());
            sb.append('\n');
        }
        return sb.toString();
    }

}
