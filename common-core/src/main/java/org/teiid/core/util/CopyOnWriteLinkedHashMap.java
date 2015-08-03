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

package org.teiid.core.util;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Provides a mimimally concurrent (concurrent read/exclusive write) {@link LinkedHashMap} for use in read mostly situations.
 * 
 * Does not support modification through entry/value collections.
 * 
 * TODO: this may not be entirely thread safe as after the clone operations there's a chance that the referenced
 * array is replaced by rehashing.
 * 
 * @param <K>
 * @param <V>
 */
public class CopyOnWriteLinkedHashMap<K, V> implements Map<K, V>, Serializable {
	
	private static final long serialVersionUID = -2690353315316696065L;

	@SuppressWarnings("rawtypes")
	private static final LinkedHashMap EMPTY = new LinkedHashMap(2);
	
	@SuppressWarnings("unchecked")
	private volatile LinkedHashMap<K, V> map = EMPTY;
	
	@Override
	public V get(Object arg0) {
		return map.get(arg0);
	}
	
	@Override
	public boolean containsKey(Object key) {
		return map.containsKey(key);
	}

	@Override
	public void clear() {
		map = new LinkedHashMap<K, V>();
	}

	@Override
	public boolean containsValue(Object arg0) {
		return map.containsValue(arg0);
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		return Collections.unmodifiableSet(map.entrySet());
	}

	@Override
	public boolean isEmpty() {
		return map.isEmpty();
	}

	@Override
	public Set<K> keySet() {
		return Collections.unmodifiableSet(map.keySet());
	}

	@Override
	public synchronized V put(K arg0, V arg1) {
		@SuppressWarnings("unchecked")
		LinkedHashMap<K, V> next = (LinkedHashMap<K, V>) map.clone();
		V result = next.put(arg0, arg1);
		map = next;
		return result;
	}

	@Override
	public synchronized void putAll(Map<? extends K, ? extends V> arg0) {
		if (arg0.isEmpty()) {
			return;
		}
		@SuppressWarnings("unchecked")
		LinkedHashMap<K, V> next = (LinkedHashMap<K, V>) map.clone();
		next.putAll(arg0);
		map = next;
	}

	@Override
	public synchronized V remove(Object arg0) {
		if (map.containsKey(arg0)) {
			@SuppressWarnings("unchecked")
			LinkedHashMap<K, V> next = (LinkedHashMap<K, V>) map.clone();
			V result = next.remove(arg0);
			map = next;
			return result;
		}
		return null;
	}

	@Override
	public int size() {
		return map.size();
	}

	@Override
	public Collection<V> values() {
		return Collections.unmodifiableCollection(map.values());
	}
	
	@Override
	public int hashCode() {
		return map.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		return map.equals(obj);
	}
	
	@Override
	public String toString() {
		return map.toString();
	}

}
