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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

public abstract class PartiallyOrderedCache<K, V> {
	
	private int maxOrderedSize = 1 << 19;
	
	protected Map<K, V> map; 
	//TODO: until Java 7 ConcurrentSkipListMap has a scaling bug in that
	//the level limits the effective map size to ~ 2^16
	//where it performs comparably under load to a synchronized LinkedHashMap
	//just with more CPU overhead vs. wait time.
	//TODO: have the concurrent version be pluggable
	protected NavigableMap<V, K> evictionQueue = new TreeMap<V, K>();
	//when we get to extreme number of entries we overflow into lru
	protected Map<V, K> evictionQueueHead = new LinkedHashMap<V, K>();
	//holds entries that are being evicted, but that might not yet be in a lower caching level
	protected Map<K, V> limbo;
	
	public PartiallyOrderedCache(int initialCapacity, float loadFactor, int concurrencyLevel) {
		map = new ConcurrentHashMap<K, V>(initialCapacity, loadFactor, concurrencyLevel);
		limbo = new ConcurrentHashMap<K, V>(initialCapacity, loadFactor, concurrencyLevel);
	}
	
	public void setMaxOrderedSize(int maxOrderedSize) {
		this.maxOrderedSize = maxOrderedSize;
	}
		
	public V get(K key) {
		V result = map.get(key);
		if (result == null) {
			result = limbo.get(key);
		}
		if (result != null) {
			maintainQueues(key, result, null);
		}
		return result;
	}
	
	public V remove(K key) {
		V result = map.remove(key);
		if (result != null) {
			synchronized (this) {
				if (evictionQueue.remove(result) != null) {
					orderedRemoved();
				} else {
					evictionQueueHead.remove(result);
				}
			}
		}
		return result;
	}

	private void orderedRemoved() {
		if (evictionQueue.size() < (maxOrderedSize>>1) && evictionQueueHead.size() > 0) {
			Iterator<Map.Entry<V,K>> i = evictionQueueHead.entrySet().iterator();
			if (i.hasNext()) {
				Map.Entry<V, K> entry = i.next();
				if (map.containsKey(entry.getValue())) {
					i.remove();
					evictionQueue.put(entry.getKey(), entry.getValue());
				}
			}
		}
	}
	
	public V put(K key, V value) {
		V result = map.put(key, value);
		maintainQueues(key, value, result);
		return result;
	}

	private void maintainQueues(K key, V value, V old) {
		synchronized (this) {
			if (old != null && evictionQueue.remove(old) == null) {
				evictionQueueHead.remove(old);
			}
			recordAccess(key, value, old == null);
			evictionQueue.put(value, key);
			if (evictionQueue.size() > maxOrderedSize) {
				Map.Entry<V, K> last = evictionQueue.pollLastEntry();
				if (last != null) {
					if (map.containsKey(last.getValue()) && !evictionQueue.containsKey(last.getKey())) {
						evictionQueueHead.put(last.getKey(), last.getValue());
					}
				}
			}
		}
	}
	
	public V evict() {
		Map.Entry<V, K> entry = evictionQueue.pollFirstEntry();
		if (entry == null) {
			return null;
		}
		synchronized (this) {
			orderedRemoved();
		}
		limbo.put(entry.getValue(), entry.getKey());
		return map.remove(entry.getValue());
	}
	
	public Map<V, K> getEvictionQueue() {
		return evictionQueue;
	}
	
	public Map.Entry<V, K> firstEntry() {
		return evictionQueue.firstEntry();
	}
	
	public void finishedEviction(K key) {
		limbo.remove(key);
	}
	
	public int size() {
		return map.size();
	}
	
	protected abstract void recordAccess(K key, V value, boolean initial);
	
}
