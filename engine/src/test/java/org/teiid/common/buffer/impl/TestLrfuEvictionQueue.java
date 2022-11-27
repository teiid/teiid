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

import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import org.teiid.common.buffer.BaseCacheEntry;
import org.teiid.common.buffer.CacheKey;

public class TestLrfuEvictionQueue {

    @Test public void testPrecision() {
        LrfuEvictionQueue<?> q = new LrfuEvictionQueue<BaseCacheEntry>(new AtomicLong());
        long value = 0;
        for (long i = Integer.MAX_VALUE; i < 10L + Integer.MAX_VALUE; i++) {
            long valueNext = q.computeNextOrderingValue(i, i-1, value);
            assertTrue(valueNext > value);
            value = valueNext;
        }
    }

    @Test public void testKeyCompare() {
        CacheKey key = new CacheKey(-5600000000000000000L, 0L, 0L);
        CacheKey key1 = new CacheKey(3831662765844904176L, 0L, 0L);
        assertTrue(key.compareTo(key1) < 0);
        assertTrue(key1.compareTo(key) > 0);
    }

    @Test public void testTouch() {
        AtomicLong clock = new AtomicLong();
        LrfuEvictionQueue<BaseCacheEntry> q = new LrfuEvictionQueue<BaseCacheEntry>(clock);
        CacheKey key = new CacheKey(0L, 0L, 0L);
        BaseCacheEntry value = new BaseCacheEntry(key);
        q.touch(value);
        assertEquals(1, q.getSize());
        assertNotNull(q.firstEntry(true));
        //advance the clock to perform the remove/add
        clock.set(LrfuEvictionQueue.MIN_INTERVAL);
        q.touch(value);
        assertEquals(1, q.getSize());
        assertNotNull(q.firstEntry(true));
    }

}
