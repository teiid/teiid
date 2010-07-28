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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.core.util.LRUCache;

public class DefaultCache<K, V> implements Cache<K, V>, Serializable {
	private static final long serialVersionUID = -511120208522577206L;
	public static final int DEFAULT_MAX_SIZE_TOTAL = 250;
	
	Map<K, V> map;
	Map<String, Cache> children = new HashMap();
	String name;
	
	public DefaultCache(String name) {
		this(name, DEFAULT_MAX_SIZE_TOTAL);
	}
	
	public DefaultCache(String name, int maxSize) {
		if(maxSize < 0){
			maxSize = DEFAULT_MAX_SIZE_TOTAL;
		}			
		this.map = Collections.synchronizedMap(new LRUCache<K, V>(maxSize));
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
		
		Cache c = new DefaultCache(name);
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