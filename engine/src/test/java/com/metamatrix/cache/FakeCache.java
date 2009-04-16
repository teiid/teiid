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

package com.metamatrix.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FakeCache<K, V> implements Cache<K, V> {
	
    public static class FakeCacheFactory implements CacheFactory{

		public Cache get(Type type, CacheConfiguration config) {
			return new FakeCache("root"); //$NON-NLS-1$
		}
		
		public void destroy() {
		}
    }
    
	Map<K, V> map = new HashMap();
	Map<String, Cache> children = new HashMap();
	String name;
	public FakeCache(String name) {
		this.name = name;
	}
	public void addListener(CacheListener listener) {
	}

	public void clear() {
		map.clear();
	}

	public V get(K key) {
		return map.get(key);
	}

	public Set<K> keySet() {
		return map.keySet();
	}

	public V put(K key, V value) {
		return map.put(key, value);
	}

	public V remove(K key) {
		return map.remove(key);
	}

	public int size() {
		return map.size();
	}
	
	public Collection<V> values() {
		return map.values();
	}

	@Override
	public void removeListener() {
	}

	@Override
	public Cache addChild(String name) {
		if (children.get(name) != null) {
			return children.get(name);
		}
		
		Cache c = new FakeCache(name);
		children.put(name, c);
		return c;
	}

	@Override
	public Cache getChild(String name) {
		return children.get(name);
	}

	@Override
	public List<Cache> getChildren() {
		return new ArrayList<Cache>(children.values());
	}

	@Override
	public boolean removeChild(String name) {
		Object obj = children.remove(name);
		return obj != null;
	}

	@Override
	public String getName() {
		return name;
	}
}
