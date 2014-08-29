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

import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import org.teiid.common.buffer.BaseCacheEntry;
import org.teiid.common.buffer.CacheKey;

public class TestLrfuEvictionQueue {
	
	@Test public void testPrecision() {
		LrfuEvictionQueue<?> q = new LrfuEvictionQueue<BaseCacheEntry>(new AtomicLong());
		long value = 0;
		for (long i = Integer.MAX_VALUE; i < 10l + Integer.MAX_VALUE; i++) {
			long valueNext = q.computeNextOrderingValue(i, i-1, value);
			assertTrue(valueNext > value);
			value = valueNext;
		}
	}
	
	@Test public void testKeyCompare() {
		CacheKey key = new CacheKey(-5600000000000000000l, 0l, 0l);
		CacheKey key1 = new CacheKey(3831662765844904176l, 0l, 0l);
		CacheKey key2 = new CacheKey(0l, 0l, 0l);
		assertTrue(key.compareTo(key1) < 0);
		assertTrue(key1.compareTo(key) > 0);
	}
	
}
