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

import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.teiid.common.buffer.BaseCacheEntry;
import org.teiid.common.buffer.CacheKey;

/**
 * A Concurrent LRFU eviction queue.  Has assumptions that match buffermanager usage.
 * Null values are not allowed.
 * @param <V>
 */
public class LrfuEvictionQueue<V extends BaseCacheEntry> {
	
	private static final long DEFAULT_HALF_LIFE = 1<<16;
	private static final long MIN_INTERVAL = 1<<9;
	//TODO: until Java 7 ConcurrentSkipListMap has a scaling bug in that
	//the level function limits the effective map size to ~ 2^16
	//above which it performs comparably under multi-threaded load to a synchronized LinkedHashMap
	//just with more CPU overhead vs. wait time.
	protected NavigableMap<CacheKey, V> evictionQueue = new ConcurrentSkipListMap<CacheKey, V>();
	protected AtomicLong clock;
	protected long maxInterval;
	protected long halfLife;
	private AtomicInteger size = new AtomicInteger();
	
	public LrfuEvictionQueue(AtomicLong clock) {
		this.clock = clock;
		setHalfLife(DEFAULT_HALF_LIFE);
	}

	public boolean remove(V value) {
		if (evictionQueue.remove(value.getKey()) != null) {
			size.addAndGet(-1);
			return true;
		}
		return false;
	}
	
	public boolean add(V value) {
		if (evictionQueue.put(value.getKey(), value) == null) {
			size.addAndGet(1);
			return true;
		}
		return false;
	}
	
	public void touch(V value) {
		long tick = clock.get();
		if (tick - MIN_INTERVAL < value.getKey().getLastAccess()) {
			return;
		}
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
			if (entry != null) {
				size.addAndGet(-1);
			}
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
	void recordAccess(V value) {
		CacheKey key = value.getKey();
		long lastAccess = key.getLastAccess();
		long currentClock = clock.get();
		long orderingValue = key.getOrderingValue();
		orderingValue = computeNextOrderingValue(currentClock, lastAccess,
				orderingValue);
		assert !this.evictionQueue.containsKey(value.getKey());
		value.setKey(new CacheKey(key.getId(), currentClock, orderingValue));
	}
	
	long computeNextOrderingValue(long currentTime,
			long lastAccess, long orderingValue) {
		long delta = currentTime - lastAccess;
		if (delta > maxInterval) {
			return currentTime;
		}
		//scale the increase based upon how hot we previously were
		long increase = orderingValue + lastAccess;
		
		if (delta > halfLife) {
			while ((delta-=halfLife) > halfLife && (increase>>=1) > 0) {
			}
		}
		increase = Math.min(currentTime, increase);
		return currentTime + increase;
	}
	
	public void setHalfLife(long halfLife) {
		this.halfLife = halfLife;
		this.maxInterval = 62*this.halfLife;
	}
	
	public int getSize() {
		return size.get();
	}
	
	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
		result.append("Size:").append(getSize()).append(" "); //$NON-NLS-1$ //$NON-NLS-2$
		int max = 2000;
		for (CacheKey e : evictionQueue.keySet()) {
			result.append("(").append(e.getOrderingValue()).append(", ") //$NON-NLS-1$ //$NON-NLS-2$
					.append(e.getLastAccess()).append(", ").append(e.getId()) //$NON-NLS-1$
					.append(") "); //$NON-NLS-1$
			if (--max == 0) {
				result.append("..."); //$NON-NLS-1$
			}
		}
		return result.toString();
	}
	
}
