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
	protected int maxInterval; //don't consider the old ordering value after the maxInterval
	protected int minInterval; //cap the frequency gain under this interval (we can make some values too hot otherwise)
	private float minVal;
	
	public LrfuEvictionQueue(AtomicLong clock) {
		this.clock = clock;
		setCrfLamda(.00005); //smaller values tend to work better since we're using interval bounds
	}

	public boolean remove(V value) {
		return evictionQueue.remove(value.getKey()) != null;
	}
	
	public boolean add(V value) {
		return evictionQueue.put(value.getKey(), value) == null;
	}
	
	public void touch(V value) {
		evictionQueue.remove(value.getKey());
		recordAccess(value);
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
	
	/**
     * Callers should be synchronized on value
     */
	public void recordAccess(V value) {
		CacheKey key = value.getKey();
		long lastAccess = key.getLastAccess();
		long currentClock = clock.get();
		double orderingValue = key.getOrderingValue();
		orderingValue = computeNextOrderingValue(currentClock, lastAccess,
				orderingValue);
		value.setKey(new CacheKey(key.getId(), (int)currentClock, orderingValue));
	}

	double computeNextOrderingValue(long currentTime,
			long lastAccess, double orderingValue) {
		long delta = currentTime - lastAccess;
		orderingValue = 
			(delta<maxInterval?(delta<minInterval?minVal:Math.pow(inverseCrfLamda, delta)):0)*orderingValue
			//recency component
			+ Math.pow(currentTime, crfLamda);
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
			float val = (float)Math.pow(inverseCrfLamda, 1<<i);
			if (val == 0) {
				break;
			}
			if (val > .8) {
				minInterval = 1<<i;
				this.minVal = val;
			}
		}
		this.maxInterval = 1<<(i-1);
	}
	
}
