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

import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import org.teiid.common.buffer.BaseCacheEntry;

/**
 * A Concurrent LRFU cache.  Has assumptions that match buffermanager usage.
 * Null values are not allowed.
 * @param <K>
 * @param <V>
 */
public class OrderedCache<K, V extends BaseCacheEntry> {
	
	protected Map<K, V> map; 
	//TODO: until Java 7 ConcurrentSkipListMap has a scaling bug in that
	//the level limits the effective map size to ~ 2^16
	//above which it performs comparably under load to a synchronized LinkedHashMap
	//just with more CPU overhead vs. wait time.
	protected NavigableMap<V, K> evictionQueue = new ConcurrentSkipListMap<V, K>();
	protected Map<K, V> limbo;
	protected AtomicLong clock;
    //combined recency/frequency lamda value between 0 and 1 lower -> LFU, higher -> LRU
    //TODO: adaptively adjust this value.  more hits should move closer to lru
	protected float crfLamda = .0002f;
	
	public OrderedCache(int initialCapacity, float loadFactor, int concurrencyLevel, AtomicLong clock) {
		map = new ConcurrentHashMap<K, V>(initialCapacity, loadFactor, concurrencyLevel);
		limbo = new ConcurrentHashMap<K, V>(initialCapacity, loadFactor, concurrencyLevel);
		this.clock = clock;
	}
		
	public V get(K key) {
		V result = map.get(key);
		if (result == null) {
			result = limbo.get(key);
		}
		if (result != null) {
			synchronized (result) {
				evictionQueue.remove(result);
				recordAccess(result, false);
				evictionQueue.put(result, key);
			}
		}
		return result;
	}
	
	public V remove(K key) {
		V result = map.remove(key);
		if (result != null) {
			synchronized (result) {
				evictionQueue.remove(result);
			}
		}
		return result;
	}
	
	public V put(K key, V value) {
		V result = map.put(key, value);
		if (result != null) {
			synchronized (result) {
				evictionQueue.remove(result);
			}
		}
		synchronized (value) {
			recordAccess(value, result == null);
			evictionQueue.put(value, key);
		}
		return result;
	}
	
	public V evict() {
		Map.Entry<V, K> entry = evictionQueue.pollFirstEntry();
		if (entry == null) {
			return null;
		}
		limbo.put(entry.getValue(), entry.getKey());
		return map.remove(entry.getValue());
	}
	
	public void finishedEviction(K key) {
		limbo.remove(key);
	}
	
	public int size() {
		return map.size();
	}
	
	public Map<V, K> getEvictionQueue() {
		return evictionQueue;
	}
	
	public Map.Entry<V, K> firstEntry() {
		return evictionQueue.firstEntry();
	}
	
	protected void recordAccess(BaseCacheEntry value, boolean initial) {
		float lastAccess = value.getLastAccess();
		value.setLastAccess(clock.get());
		if (initial && lastAccess == 0) {
			return; //we just want to timestamp this as created and not give it an ordering value
		}
		float orderingValue = value.getOrderingValue();
		orderingValue = computeNextOrderingValue(value.getLastAccess(), lastAccess,
				orderingValue);
		value.setOrderingValue(orderingValue);
	}

	float computeNextOrderingValue(float currentTime,
			float lastAccess, float orderingValue) {
		orderingValue = 
			(float) (//Frequency component
			orderingValue*Math.pow(1-crfLamda, currentTime - lastAccess)
			//recency component
			+ Math.pow(currentTime, crfLamda));
		return orderingValue;
	}
	
	public float getCrfLamda() {
		return crfLamda;
	}
	
	public void setCrfLamda(float crfLamda) {
		this.crfLamda = crfLamda;
	}
	
}
