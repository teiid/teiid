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
package org.teiid.cache;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.teiid.core.util.LRUCache;

public class DefaultCache<K, V> implements Cache<K, V>, Serializable {
	private static final long serialVersionUID = -511120208522577206L;
	public static final int DEFAULT_MAX_SIZE_TOTAL = 250;
	public static final int DEFAULT_MAX_AGE = 1000 * 60 * 60 * 2;
	
	private static class ExpirationEntry<K, V> {
		long expiration;
		K key;
		V value;
		
		public ExpirationEntry(long expiration, K key, V value) {
			this.expiration = expiration;
			this.key = key;
			this.value = value;
		}

		@Override
		public int hashCode() {
			return key.hashCode();
		}
		
		@Override
		public boolean equals(Object obj) {
			if (obj == this) {
				return true;
			}
			if (!(obj instanceof ExpirationEntry<?, ?>)) {
				return false;
			}
			ExpirationEntry<K, V> other = (ExpirationEntry<K, V>)obj;
			return this.key.equals(other.key);
		}
	}
	
	protected LRUCache<K, ExpirationEntry<K, V>> map;
	protected Map<String, Cache> children = new ConcurrentHashMap<String, Cache>();
	protected String name;
	protected long ttl;
	protected Set<ExpirationEntry<K, V>> expirationQueue;
	
	public DefaultCache(String name) {
		this(name, DEFAULT_MAX_SIZE_TOTAL, DEFAULT_MAX_SIZE_TOTAL);
	}
	
	public DefaultCache(String name, int maxEntries, long ttl) {
		this.map = new LRUCache<K, ExpirationEntry<K, V>>(maxEntries) {
			@Override
			protected boolean removeEldestEntry(java.util.Map.Entry<K, ExpirationEntry<K, V>> eldest) {
				if (super.removeEldestEntry(eldest)) {
					Iterator<ExpirationEntry<K, V>> iter = expirationQueue.iterator();
					if (validate(iter.next()) != null) {
						DefaultCache.this.remove(eldest.getKey());
					}
				}
				return false;
			}
		};
		this.expirationQueue = Collections.newSetFromMap(new LRUCache<ExpirationEntry<K, V>, Boolean>(maxEntries));
		this.name = name;
		this.ttl = ttl;
	}
	
	public void addListener(CacheListener listener) {
		throw new UnsupportedOperationException();
	}

	public void clear() {
		synchronized (map) {
			map.clear();
			expirationQueue.clear();
		}
	}

	public V get(K key) {
		synchronized (map) {
			ExpirationEntry<K, V> result = map.get(key);
			if (result != null) {
				return validate(result);
			}
			return null;
		}
	}

	private V validate(ExpirationEntry<K, V> result) {
		if (result.expiration < System.currentTimeMillis()) {
			remove(result.key);
			return null;
		}
		return result.value;
	}

	public Set<K> keySet() {
		synchronized (map) {
			return new HashSet<K>(map.keySet());
		}
	}

	public V put(K key, V value) {
		return this.put(key, value, ttl);
	}
	
	public static long getExpirationTime(long defaultTtl, Long ttl) {
		if (ttl == null) {
			ttl = defaultTtl;
		}
		if (ttl < 0) {
			return Long.MAX_VALUE;
		}
		long result = System.currentTimeMillis() + ttl;
		if (result < ttl) {
			result = Long.MAX_VALUE;
		}
		return result;
	}
	
	public V put(K key, V value, Long timeToLive) {
		if (this.map.getSpaceLimit() == 0) {
			return null;
		}
		synchronized (map) {
			ExpirationEntry<K, V> entry = new ExpirationEntry<K, V>(getExpirationTime(ttl, timeToLive), key, value);
			ExpirationEntry<K, V> result = map.put(key, entry);
			expirationQueue.add(entry);
			if (result != null) {
				return result.value;
			}
			return null;
		}
	}

	public V remove(K key) {
		synchronized (map) {
			ExpirationEntry<K, V> result = map.remove(key);
			if (result != null) {
				expirationQueue.remove(result);
				return result.value;
			}
			return null;
		}
	}

	public int size() {
		synchronized (map) {
			return map.size();
		}
	}
	
	public Collection<V> values() {
		synchronized (map) {
			ArrayList<V> result = new ArrayList<V>(map.size());
			for (ExpirationEntry<K, V> entry : new ArrayList<ExpirationEntry<K, V>>(map.values())) {
				V value = validate(entry);
				if (value != null) {
					result.add(value);
				}
			}
			return result;
		}
	}

	public Cache addChild(String name) {
		Cache c = children.get(name);
		if (c != null) {
			return c;
		}
		
		c = new DefaultCache(name, map.getSpaceLimit(), ttl);
		children.put(name, c);
		return c;
	}

	public Cache getChild(String name) {
		return children.get(name);
	}

	public Collection<Cache> getChildren() {
		return children.values();
	}

	public boolean removeChild(String name) {
		Object obj = children.remove(name);
		return obj != null;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public Set<K> keys() {
		synchronized(this.map) {
			return new HashSet<K>(map.keySet());
		}
	}
	
	Set<ExpirationEntry<K, V>> getExpirationQueue() {
		return expirationQueue;
	}
	
	LRUCache<K, ExpirationEntry<K, V>> getCacheMap() {
		return map;
	}
	
}