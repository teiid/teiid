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

package org.teiid.cache.jboss;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.teiid.cache.Cache;


/**
 * Implementation of Cache using Infinispan
 */
public class JBossCache<K, V> implements Cache<String, V> {

	protected org.infinispan.Cache<String, V> cacheStore;
	private final String name; 
	
	public JBossCache(org.infinispan.Cache<String, V> cacheStore, String cacheName) {
		this.cacheStore = cacheStore;
		this.name = cacheName;
	}
	
	private String fqn(String key) {
		return this.name+"."+key; //$NON-NLS-1$
	}
	
	@Override
	public V get(String key) {
		return this.cacheStore.get(fqn(key));
	}
	
	public V put(String key, V value) {
		return this.cacheStore.put(fqn(key), value);
	}
	
	@Override
	public V put(String key, V value, Long ttl) {
		return this.cacheStore.put(fqn(key), value, ttl, TimeUnit.SECONDS);
	}

	@Override
	public V remove(String key) {
		return this.cacheStore.remove(fqn(key));
	}
	
	@Override
	public int size() {
		return this.cacheStore.size();
	}
	
	@Override
	public void clear() {
		this.cacheStore.clear();
	}
	
	@Override
	public String getName() {
		return this.name;
	}

	@Override
	public Set<String> keys() {
		return this.cacheStore.keySet();
	}
}
