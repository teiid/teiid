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
	protected long maxInterval;
	protected long halfLife;
	
	public LrfuEvictionQueue(AtomicLong clock) {
		this.clock = clock;
		setHalfLife(1<<17);
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
		long orderingValue = key.getOrderingValue();
		orderingValue = computeNextOrderingValue(currentClock, lastAccess,
				orderingValue);
		value.setKey(new CacheKey(key.getId(), currentClock, orderingValue));
	}
	
	long computeNextOrderingValue(long currentTime,
			long lastAccess, long orderingValue) {
		long delta = currentTime - lastAccess;
		if (delta > maxInterval) {
			return currentTime;
		}
		long increase = Math.min(orderingValue, currentTime);
		
		//scale the increase based upon how hot we previously were
		increase>>=1;
		increase *= orderingValue/(double)lastAccess;
		
		if (delta > halfLife) {
			while ((delta-=halfLife) > halfLife && (increase>>=1) > 0) {
			}
		}
		if (delta > 0 && increase > 0) {
			//linear interpolate the rest of the delta (between 1 and 1/2)
			increase = (long) (increase*(halfLife/((double)halfLife + delta)));
		}
		return currentTime + increase;
	}
	
	public void setHalfLife(long halfLife) {
		this.halfLife = halfLife;
		this.maxInterval = 62*this.halfLife;
	}
	
}
