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

package org.teiid.cache.infinispan;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.transaction.TransactionMode;
import org.teiid.cache.Cache;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;


/**
 * Implementation of Cache using Infinispan
 */
public class InfinispanCache<K, V> implements Cache<K, V> {

	protected org.infinispan.AdvancedCache<K, V> cacheStore;
	private final String name; 
	private ClassLoader classloader;
	private boolean transactional;
	
	public InfinispanCache(org.infinispan.Cache<K, V> cacheStore, String cacheName, ClassLoader classloader) {
		assert(cacheStore != null);
		this.cacheStore = cacheStore.getAdvancedCache();
		TransactionMode transactionMode = this.cacheStore.getCacheConfiguration().transaction().transactionMode();
		this.transactional = transactionMode == TransactionMode.TRANSACTIONAL;
		LogManager.logDetail(LogConstants.CTX_RUNTIME, "Added", transactionMode, "infinispan cache", cacheName); //$NON-NLS-1$ //$NON-NLS-2$
		this.name = cacheName;
		this.classloader = classloader;
	}
	
	@Override
	public boolean isTransactional() {
		return transactional;
	}
	
	@Override
	public V get(K key) {
		return this.cacheStore.with(this.classloader).get(key);
	}
	
	public V put(K key, V value) {
		return this.cacheStore.with(this.classloader).put(key, value);
	}
	
	@Override
	public V put(K key, V value, Long ttl) {
		if (ttl != null) {
			return this.cacheStore.with(this.classloader).put(key, value, ttl, TimeUnit.MILLISECONDS);
		}
		return this.cacheStore.with(this.classloader).put(key, value);
	}

	@Override
	public V remove(K key) {
		return this.cacheStore.with(this.classloader).remove(key);
	}
	
	@Override
	public int size() {
		return this.cacheStore.with(this.classloader).size();
	}
	
	@Override
	public void clear() {
		this.cacheStore.with(this.classloader).clear();
	}
	
	@Override
	public String getName() {
		return this.name;
	}

	@Override
	public Set<K> keySet() {
		return this.cacheStore.with(this.classloader).keySet();
	}
}
