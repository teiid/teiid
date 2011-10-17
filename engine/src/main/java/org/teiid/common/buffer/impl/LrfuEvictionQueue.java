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

import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import org.teiid.common.buffer.BaseCacheEntry;
import org.teiid.common.buffer.CacheKey;

/**
 * A Concurrent LRFU eviction queue.  Has assumptions that match buffermanager usage.
 * Null values are not allowed.
 * @param <K>
 * @param <V>
 */
public class LrfuEvictionQueue<V extends BaseCacheEntry> {
	
	//TODO: until Java 7 ConcurrentSkipListMap has a scaling bug in that
	//the level function limits the effective map size to ~ 2^16
	//above which it performs comparably under multi-threaded load to a synchronized LinkedHashMap
	//just with more CPU overhead vs. wait time.
	protected NavigableMap<CacheKey, V> evictionQueue = new ConcurrentSkipListMap<CacheKey, V>();
	protected AtomicLong clock;
    //combined recency/frequency lamda value between 0 and 1 lower -> LFU, higher -> LRU
    //TODO: adaptively adjust this value.  more hits should move closer to lru
	protected double crfLamda;
	protected double inverseCrfLamda = 1 - crfLamda;
	protected int maxInterval;
	
	public LrfuEvictionQueue(AtomicLong clock) {
		this.clock = clock;
		setCrfLamda(.0002);
	}

	public boolean remove(V value) {
		return evictionQueue.remove(value.getKey()) != null;
	}
	
	public void touch(V value, boolean initial) {
		if (!initial) {
			initial = evictionQueue.remove(value.getKey()) == null;			
		}
		recordAccess(value, initial);
		evictionQueue.put(value.getKey(), value);
	}
		
	public Collection<V> getEvictionQueue() {
		return evictionQueue.values();
	}
	
	public V firstEntry(boolean poll) {
		Map.Entry<CacheKey, V> entry = null;
		if (poll) {
			entry = evictionQueue.pollFirstEntry();
		} else {
			entry = evictionQueue.firstEntry();
		}
		if (entry != null) {
			return entry.getValue();
		}
		return null;
	}
	
	protected void recordAccess(V value, boolean initial) {
		assert Thread.holdsLock(value);
		CacheKey key = value.getKey();
		int lastAccess = key.getLastAccess();
		long currentClock = clock.get();
		if (initial && lastAccess == 0) {
			return; //we just want to timestamp this as created and not give it an ordering value
		}
		float orderingValue = key.getOrderingValue();
		orderingValue = computeNextOrderingValue(currentClock, lastAccess,
				orderingValue);
		value.setKey(new CacheKey(key.getId(), (int)currentClock, orderingValue));
	}

	float computeNextOrderingValue(long currentTime,
			int lastAccess, float orderingValue) {
		long longLastAccess = lastAccess&0xffffffffl;
		currentTime &= 0xffffffffl;
		if (longLastAccess > currentTime) {
			currentTime += (1l<<32);
		}
		long delta = currentTime - longLastAccess;
		orderingValue = 
			(float) (//Frequency component
			(delta>maxInterval?0:orderingValue*Math.pow(inverseCrfLamda, delta))
			//recency component
			+ Math.pow(currentTime, crfLamda));
		return orderingValue;
	}
	
	public double getCrfLamda() {
		return crfLamda;
	}
	
	public void setCrfLamda(double crfLamda) {
		this.crfLamda = crfLamda;
		this.inverseCrfLamda = 1 - crfLamda;
		int i = 0;
		for (; i < 30; i++) {
			if ((float)Math.pow(inverseCrfLamda, 1<<i) == 0) {
				break;
			}
		}
		this.maxInterval = i-1;
	}
	
}
